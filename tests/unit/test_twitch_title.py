"""Anonymous live-title provider (synthetic responses only; no network)."""
import json
import os
import stat
import tempfile
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
        self.tmp = tempfile.TemporaryDirectory()
        tt._latch_path_override = os.path.join(self.tmp.name, "state", tt.LATCH_FILE)
        tt._latched = None
        tt.reset()
        self.m = Clock()

    def tearDown(self):
        cg.set_provider(None)
        tt.reset()
        tt._latched = None
        tt._latch_path_override = None
        self.tmp.cleanup()

    def restart(self):
        """Simulate an ordinary process restart: memory gone, file stays."""
        tt._latched = None
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

    def test_access_refusal_latches_terminally_no_timed_retry(self):
        for code in (401, 403, 429):
            tt._latched = None
            tt.reset_latch(authorized=True)
            tt.reset()
            op = Op((code, b""), (200, doc()), (200, doc()))
            with self.assertRaises(tt.TitleUnavailable) as c:
                self.fetch(op)
            self.assertEqual(c.exception.kind, "blocked")
            for dt in (1, 601, 86400 * 30):          # timers never reopen it
                self.m.t += dt
                with self.assertRaises(tt.TitleUnavailable) as c:
                    self.fetch(op, "Other_Chan")      # any channel
                self.assertEqual(c.exception.kind, "latched")
            self.assertEqual(len(op.calls), 1, code)  # exactly one request

    def test_latch_survives_restart_and_has_private_mode(self):
        op = Op((403, b""), (200, doc()))
        with self.assertRaises(tt.TitleUnavailable):
            self.fetch(op)
        path = tt.latch_path()
        self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
        with open(path) as fh:
            body = fh.read()
        self.assertEqual(set(json.loads(body)), {"kind", "at"})
        self.restart()
        with self.assertRaises(tt.TitleUnavailable) as c:
            self.fetch(op)
        self.assertEqual(c.exception.kind, "latched")
        self.assertEqual(len(op.calls), 1)
        # provider path through the gate stays closed too
        cg.set_provider(lambda u: tt.fetch(u, opener=op, mono=self.m))
        self.assertEqual(cg.check("lacy").reason, "unavailable")
        self.assertEqual(len(op.calls), 1)

    def test_graphql_errors_latch(self):
        cases = {"challenge": ["failed integrity check", "Captcha required"],
                 "blocked": ["Forbidden", "TooManyRequests", "rate limit exceeded",
                             "unauthenticated", "Unauthorized"]}
        for kind, msgs in cases.items():
            for msg in msgs:
                tt._latched = None
                tt.reset_latch(authorized=True)
                tt.reset()
                body = json.dumps({"errors": [{"message": msg}]}).encode()
                op = Op((200, body), (200, doc()))
                with self.assertRaises(tt.TitleUnavailable) as c:
                    self.fetch(op)
                self.assertEqual(c.exception.kind, kind, msg)
                self.m.t += 10 ** 6
                with self.assertRaises(tt.TitleUnavailable) as c:
                    self.fetch(op)
                self.assertEqual(c.exception.kind, "latched", msg)
                self.assertEqual(len(op.calls), 1, msg)

    def test_shared_typed_refusals_latch(self):
        for kind in ("blocked", "challenge", "restricted"):
            tt._latched = None
            tt.reset_latch(authorized=True)
            tt.reset()
            op = Op(tp.PlaybackRefused(kind, "https://x/?token=SECRETVALUE"),
                    (200, doc()))
            with self.assertRaises(tt.TitleUnavailable) as c:
                self.fetch(op)
            self.assertEqual(c.exception.kind, kind)
            self.assertNotIn("SECRET", repr(c.exception))
            self.restart()
            self.m.t += 10 ** 6
            with self.assertRaises(tt.TitleUnavailable) as c:
                self.fetch(op)
            self.assertEqual(c.exception.kind, "latched")
            self.assertEqual(len(op.calls), 1, kind)

    def test_unreadable_latch_file_stays_closed(self):
        os.makedirs(os.path.dirname(tt.latch_path()))
        with open(tt.latch_path(), "w") as fh:
            fh.write("{not json")
        op = Op((200, doc()))
        with self.assertRaises(tt.TitleUnavailable) as c:
            self.fetch(op)
        self.assertEqual(c.exception.kind, "latched")
        self.assertEqual(op.calls, [])

    def test_unwritable_state_dir_blocks_every_request_across_starts(self):
        tt._latch_path_override = "/proc/definitely/not/writable/latch.json"
        op = Op((429, b""), (200, doc()), (200, doc()))
        for _ in range(3):                           # three fresh "starts"
            tt._latched = None
            tt.reset()
            with self.assertRaises(tt.TitleUnavailable) as c:
                self.fetch(op)
            self.assertEqual(c.exception.kind, "no_state")
        self.assertEqual(op.calls, [])               # no request ever issued
        cg.set_provider(lambda u: tt.fetch(u, opener=op, mono=self.m))
        self.assertEqual(cg.check("lacy").reason, "unavailable")
        self.assertEqual(op.calls, [])

    def test_refusal_wins_over_inflight_success_on_other_channel(self):
        import threading
        started, release = threading.Event(), threading.Event()
        out = {}

        def slow(req, timeout, limit, deadline, should_stop):
            started.set()
            release.wait(5)
            return 200, doc(login="otherchan", title="GTA 6")

        def run():
            try:
                out["ev"] = tt.fetch("otherchan", opener=slow, clock=lambda: 5000.0,
                                     mono=self.m)
            except tt.TitleUnavailable as exc:
                out["err"] = exc.kind
        th = threading.Thread(target=run)
        th.start()
        self.assertTrue(started.wait(5))
        with self.assertRaises(tt.TitleUnavailable):
            tt.fetch("lacy", opener=Op((429, b"")), mono=self.m)
        release.set()
        th.join(5)
        self.assertEqual(out, {"err": "latched"})    # positive never escapes
        with self.assertRaises(tt.TitleUnavailable):  # nor from the cache
            tt.fetch("otherchan", opener=Op(), mono=self.m)

    def test_cached_positive_not_returned_after_latch(self):
        op = Op((200, doc()))
        self.assertTrue(self.fetch(op).live)
        tt._set_latch("blocked", 1.0)
        with self.assertRaises(tt.TitleUnavailable) as c:
            self.fetch(op)
        self.assertEqual(c.exception.kind, "latched")

    def fences(self):
        d = tt.pending_dir()
        return sorted(os.listdir(d)) if os.path.isdir(d) else []

    def test_fence_removed_after_every_committed_outcome(self):
        for ans in ((200, doc()), (500, b""), (200, b"junk")):
            tt.reset()
            op = Op(ans)
            try:
                self.fetch(op)
            except tt.TitleUnavailable:
                pass
            self.assertEqual(self.fences(), [], ans)

    def test_terminal_commit_failure_then_fresh_process_makes_no_request(self):
        real = tt._write

        def flaky(path, d):
            if path.endswith(tt.LATCH_FILE):
                raise OSError("disk")             # only the latch write fails
            return real(path, d)
        tt._write = flaky
        try:
            op = Op((429, b""), (200, doc()))
            with self.assertRaises(tt.TitleUnavailable):
                self.fetch(op)                    # readiness (fence) ok, then 429
        finally:
            tt._write = real
        self.assertFalse(os.path.exists(tt.latch_path()))
        self.assertEqual(len(self.fences()), 1)   # fence kept: commit failed
        self.restart()                            # fresh process
        with self.assertRaises(tt.TitleUnavailable) as c:
            self.fetch(op)
        self.assertEqual(c.exception.kind, "latched")
        self.assertEqual(len(op.calls), 1)        # no additional request

    def test_crash_between_response_and_commit_stays_closed(self):
        real_set = tt._set_latch

        def die(*a, **k):
            raise SystemExit("orderly exit during commit")
        tt._set_latch = die                       # real finalizer runs
        try:
            op = Op((429, b""), (200, doc()))
            with self.assertRaises(SystemExit):
                self.fetch(op)
        finally:
            tt._set_latch = real_set
        self.assertEqual(len(self.fences()), 1)
        self.assertFalse(os.path.exists(tt.latch_path()))
        self.restart()
        with self.assertRaises(tt.TitleUnavailable) as c:
            self.fetch(op)
        self.assertEqual(c.exception.kind, "latched")
        self.assertEqual(len(op.calls), 1)

    def test_crash_during_success_commit_also_stays_closed(self):
        real_close = tt._fence_close
        tt._fence_close = lambda name: None
        try:
            op = Op((200, doc()), (200, doc()))
            self.assertTrue(self.fetch(op).live)
        finally:
            tt._fence_close = real_close
        self.restart()
        with self.assertRaises(tt.TitleUnavailable) as c:
            self.fetch(op)
        self.assertEqual(c.exception.kind, "latched")
        self.assertEqual(len(op.calls), 1)
        self.assertTrue(tt.reset_latch(authorized=True))
        self.assertEqual(self.fences(), [])
        self.assertTrue(self.fetch(op).live)

    def test_concurrent_requests_keep_separate_fences(self):
        import threading
        started, release = threading.Event(), threading.Event()
        out = {}

        def slow(req, timeout, limit, deadline, should_stop):
            started.set()
            release.wait(5)
            return 200, doc(login="slowchan")

        th = threading.Thread(target=lambda: out.update(
            ev=tt.fetch("slowchan", opener=slow, clock=lambda: 5000.0, mono=self.m)))
        th.start()
        self.assertTrue(started.wait(5))
        self.assertEqual(len(self.fences()), 1)   # slow request's fence
        mine = self.fences()[0]
        self.assertTrue(self.fetch(Op((200, doc(login="fastchan"))), "fastchan").live)
        self.assertEqual(self.fences(), [mine])   # the other fence untouched
        self.assertIsNone(tt.latched())            # own fences are not "leftover"
        release.set()
        th.join(5)
        self.assertTrue(out["ev"].live)
        self.assertEqual(self.fences(), [])

    def test_only_explicit_authorized_reset_clears(self):
        op = Op((429, b""), (200, doc()))
        with self.assertRaises(tt.TitleUnavailable):
            self.fetch(op)
        tt.reset()                                    # test helper: not a reset
        self.assertFalse(tt.reset_latch())
        self.assertFalse(tt.reset_latch(authorized=1))
        self.assertIsNotNone(tt.latched())
        self.assertTrue(tt.reset_latch(authorized=True))
        self.assertIsNone(tt.latched())
        self.assertTrue(self.fetch(op).live)

    def test_transient_failures_do_not_latch_and_retry_bounded(self):
        for ans in ((500, b""), (302, b""), (200, b"junk"),
                    tp.PlaybackRefused("error", "x"), RuntimeError("boom")):
            tt.reset()
            op = Op(ans, (200, doc()))
            with self.assertRaises(tt.TitleUnavailable):
                self.fetch(op)
            self.assertIsNone(tt.latched())
            with self.assertRaises(tt.TitleUnavailable) as c:   # inside window
                self.fetch(op)
            self.assertEqual(c.exception.kind, "backoff")
            self.assertEqual(len(op.calls), 1)
            self.m.t += tt.ERROR_BACKOFF_S + 1
            self.assertTrue(self.fetch(op).live)

    def test_rejected_query_has_longer_backoff_no_latch(self):
        body = json.dumps({"errors": [{"message": "unknown field"}]}).encode()
        op = Op((200, body), (200, doc()))
        with self.assertRaises(tt.TitleUnavailable) as c:
            self.fetch(op)
        self.assertEqual(c.exception.kind, "rejected")
        self.assertIsNone(tt.latched())
        self.m.t += tt.ERROR_BACKOFF_S + 1
        with self.assertRaises(tt.TitleUnavailable):
            self.fetch(op)
        self.assertEqual(len(op.calls), 1)
        self.m.t += tt.REJECT_BACKOFF_S
        self.assertTrue(self.fetch(op).live)

    def test_transport_error_no_leak(self):
        op = Op(tp.PlaybackRefused("error", "https://x/?token=SECRETVALUE"))
        with self.assertRaises(tt.TitleUnavailable) as c:
            self.fetch(op)
        self.assertNotIn("SECRET", repr(c.exception) + str(c.exception.__cause__))

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

class RetainTransportOnly(TitleProvider):
    """A transport-only failure may reuse the last PASSING evidence by its
    ORIGINAL fetch time. Nothing newer or stronger is ever overridden."""

    def gate(self, op):
        cg.set_provider(lambda u: tt.fetch(u, opener=op, clock=self.m,
                                           mono=self.m))
        return lambda: cg.check("lacy", now=self.m)

    def test_transport_failure_keeps_fresh_pass_but_never_past_age(self):
        op = Op((200, doc()), tp.PlaybackRefused("error", "deadline reached"),
                tp.PlaybackRefused("error", "deadline reached"),
                tp.PlaybackRefused("error", "deadline reached"))
        check = self.gate(op)
        self.assertTrue(check().ok)
        self.m.t += 20
        self.assertTrue(check().ok)               # transport blip, age 20
        self.m.t += 5
        self.assertTrue(check().ok)               # inside backoff, still age 25
        self.assertEqual(len(op.calls), 2)
        self.m.t += 40                            # age 65, backoff over
        self.assertTrue(check().ok)
        self.m.t += tt.ERROR_BACKOFF_S + 30       # age > MAX_TITLE_AGE_S
        r = check()
        self.assertFalse(r.ok, r.reason)

    def test_transport_failure_without_prior_pass_is_closed(self):
        op = Op(tp.PlaybackRefused("error", "x"))
        self.assertEqual(self.gate(op)().reason, "unavailable")

    def test_newer_negative_answer_is_never_overridden(self):
        offline = (200, doc(stream=None))
        op = Op((200, doc()), offline,
                tp.PlaybackRefused("error", "x"))
        check = self.gate(op)
        self.assertTrue(check().ok)
        self.m.t += 20
        self.assertEqual(check().reason, "not_live")
        self.m.t += 40
        self.assertFalse(check().ok)              # transport after offline: closed

    def test_non_transport_inconclusive_answers_are_not_retained(self):
        for bad in ((500, b""), (200, b"junk"),
                    (200, json.dumps({"errors": [{"message": "x"}]}).encode())):
            tt.reset()
            op = Op((200, doc()), bad)
            check = self.gate(op)
            self.assertTrue(check().ok)
            self.m.t += 20
            self.assertEqual(check().reason, "unavailable", str(bad))
            self.m.t += 5                         # inside backoff: still closed
            self.assertFalse(check().ok, str(bad))

    def test_terminal_refusal_is_never_retained(self):
        op = Op((200, doc()), (429, b""))
        check = self.gate(op)
        self.assertTrue(check().ok)
        self.m.t += 20
        self.assertFalse(check().ok)
        self.assertEqual(tt.latched(), "blocked")
        self.m.t += 1
        self.assertFalse(check().ok)
        self.assertEqual(len(op.calls), 2)

    def test_generic_exception_and_cancellation_are_not_retained(self):
        for bad in (RuntimeError("boom"), tp.PlaybackRefused("error", "cancelled"),
                    tp.PlaybackRefused("quality", "x")):
            tt.reset()
            op = Op((200, doc()), bad)
            check = self.gate(op)
            self.assertTrue(check().ok)
            self.m.t += 20
            self.assertFalse(check().ok, repr(bad))
            self.m.t += 5                         # backoff keeps it closed
            self.assertFalse(check().ok, repr(bad))

    def test_provider_replacement_drops_kept_evidence(self):
        op = Op((200, doc()), tp.PlaybackRefused("error", "x"))
        check = self.gate(op)
        self.assertTrue(check().ok)
        check = self.gate(op)                     # set_provider again
        self.m.t += 20
        self.assertFalse(check().ok)


if __name__ == "__main__":
    unittest.main()
