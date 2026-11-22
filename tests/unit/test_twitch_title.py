"""Anonymous live-title provider (synthetic responses only; no network)."""
import json
import time
import unittest

from app.core import content_gate as cg
from app.ingestion import twitch_playback as tp
from app.ingestion import twitch_title as tt


def doc(login="lacy", stream="s1", title="GTA 6 day one", lb=True):
    user = {"login": login, "stream": ({"id": stream} if stream else None),
            "lastBroadcast": ({"title": title} if lb else None)}
    return json.dumps({"data": {"user": user}}).encode()


class Op:
    def __init__(self, *answers):
        self.answers, self.calls = list(answers), []

    def __call__(self, req, timeout, limit, deadline, should_stop):
        self.calls.append(json.loads(req.data.decode()))
        a = self.answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class TitleProvider(unittest.TestCase):
    def setUp(self):
        tt.reset()
        self.m = Clock()

    def tearDown(self):
        cg.set_provider(None)
        tt.reset()

    def fetch(self, op, ch="Lacy"):
        return tt.fetch(ch, opener=op, clock=lambda: 5000.0, mono=self.m)

    def test_live_title(self):
        op = Op((200, doc()))
        ev = self.fetch(op)
        self.assertEqual(ev, cg.Evidence("lacy", "GTA 6 day one", 5000.0, True))
        q = op.calls[0]
        self.assertEqual(q["variables"], {"login": "lacy"})
        self.assertNotIn("Authorization", str(q))

    def test_offline_is_typed_not_error(self):
        ev = self.fetch(Op((200, doc(stream=None, title="old GTA 6"))))
        self.assertFalse(ev.live)
        self.assertEqual(ev.title, "")           # lastBroadcast never used

    def test_gate_end_to_end(self):
        now = time.time()
        for body, want in [
            (doc(title="GTA 6 live"), "ok"),
            (doc(title="Minecraft"), "mismatch"),
            (doc(stream=None, title="GTA 6"), "not_live"),
            (doc(login="other", title="GTA 6"), "wrong_channel"),
        ]:
            tt.reset()
            op = Op((200, body))
            cg.set_provider(lambda u, op=op: tt.fetch(
                u, opener=op, clock=time.time, mono=self.m))
            self.assertEqual(cg.check("lacy").reason, want, body)

    def test_inconclusive_raises_and_gate_closes(self):
        bad = [
            (200, b"not json"), (200, b"[]"), (200, b'{"data":{"user":null}}'),
            (200, b'{"data":{}}'), (200, doc(login="")),
            (200, json.dumps({"data": {"user": {"login": "lacy",
                "stream": {"id": ""}, "lastBroadcast": {"title": "GTA 6"}}}}).encode()),
            (200, doc(lb=False)), (200, doc(title="   ")),
            (200, json.dumps({"data": {"user": {"login": "lacy",
                "stream": {"id": 5}, "lastBroadcast": {"title": "GTA 6"}}}}).encode()),
            (500, b""), (302, b""),
        ]
        for ans in bad:
            tt.reset()
            with self.assertRaises(tt.TitleUnavailable, msg=str(ans)):
                self.fetch(Op(ans))
        tt.reset()
        cg.set_provider(lambda u: tt.fetch(u, opener=Op((200, b"x")), mono=self.m))
        self.assertEqual(cg.check("lacy").reason, "unavailable")

    def test_block_statuses_back_off_without_retry(self):
        for code in (401, 403, 429):
            tt.reset()
            op = Op((code, b""), (200, doc()))
            with self.assertRaises(tt.TitleUnavailable):
                self.fetch(op)
            self.m.t += tt.BLOCK_BACKOFF_S - 1
            with self.assertRaises(tt.TitleUnavailable) as c:
                self.fetch(op)
            self.assertEqual(c.exception.kind, "backoff")
            self.assertEqual(len(op.calls), 1)       # no request in the window
            self.m.t += 2
            self.assertTrue(self.fetch(op).live)     # only after the window

    def test_challenge_backs_off(self):
        body = json.dumps({"errors": [{"message": "failed integrity check"}]}).encode()
        op = Op((200, body), (200, doc()))
        with self.assertRaises(tt.TitleUnavailable) as c:
            self.fetch(op)
        self.assertEqual(c.exception.kind, "challenge")
        with self.assertRaises(tt.TitleUnavailable):
            self.fetch(op)
        self.assertEqual(len(op.calls), 1)

    def test_transport_error_short_backoff_and_no_leak(self):
        op = Op(tp.PlaybackRefused("error", "https://x/?token=SECRETVALUE"),
                (200, doc()))
        with self.assertRaises(tt.TitleUnavailable) as c:
            self.fetch(op)
        self.assertNotIn("SECRET", repr(c.exception) + str(c.exception.__cause__))
        self.m.t += tt.ERROR_BACKOFF_S + 1
        self.assertTrue(self.fetch(op).live)

    def test_rate_limit_reuses_evidence_with_original_time(self):
        op = Op((200, doc()), (200, doc(title="Minecraft")))
        first = self.fetch(op)
        self.m.t += tt.MIN_INTERVAL_S - 1
        self.assertIs(self.fetch(op), first)
        self.assertEqual(len(op.calls), 1)
        self.m.t += 2
        self.assertEqual(self.fetch(op).title, "Minecraft")

    def test_invalid_names_no_network(self):
        for name in ("", "ab", "a b", "x" * 26, "../x", None):
            op = Op()
            with self.assertRaises(tt.TitleUnavailable):
                tt.fetch(name, opener=op, mono=self.m)
            self.assertEqual(op.calls, [])

    def test_unknown_login_in_reply_is_wrong_channel(self):
        cg.set_provider(lambda u: tt.fetch(
            u, opener=Op((200, doc(login="impostor"))), clock=time.time, mono=self.m))
        self.assertEqual(cg.check("lacy").reason, "wrong_channel")


if __name__ == "__main__":
    unittest.main()
