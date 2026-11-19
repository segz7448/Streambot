import asyncio
import tempfile
import types
import unittest
from pathlib import Path

from app.bot import ops
from app.bot.inbox import PreAckInbox
from app.bot.watermark import UpdateWatermark


def _upd(update_id, text, user_id=42):
    return types.SimpleNamespace(
        update_id=update_id,
        message=types.SimpleNamespace(text=text),
        effective_user=types.SimpleNamespace(id=user_id))


class PreAckWrapperTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.inbox = PreAckInbox(str(self.dir / "inbox.json"))
        self.wm = UpdateWatermark(str(self.dir / "updates.json"))
        import unittest.mock as _mock
        # feed-validation now resolves hostnames; these tests exercise
        # relay process management and journaling, not DNS - stub it
        p = _mock.patch("socket.getaddrinfo",
                        lambda *a, **k: [(2, 1, 6, "", ("8.8.8.8", 0))])
        p.start()
        self.addCleanup(p.stop)

    def _put(self, update):
        seen = []
        async def orig(u):
            seen.append(u)
        wrapped = ops.make_pre_ack_wrapper(self.inbox, self.wm, orig,
                                     lambda: 42)
        asyncio.run(wrapped(update))
        return seen

    def test_commands_persisted_before_queueing(self):
        seen = self._put(_upd(1, "/status"))
        self.assertEqual(len(seen), 1)  # original put still called
        self._put(_upd(2, "hello"))
        self._put(_upd(3, "/clipnow chan"))
        texts = [e["text"] for e in self.inbox.pending()]
        self.assertEqual(texts, ["/status", "/clipnow chan"])

    def test_already_handled_not_re_added(self):
        self.wm.mark_handled(5)
        self._put(_upd(5, "/status"))
        self.assertEqual(self.inbox.pending(), [])


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        import unittest.mock as _mock
        # feed-validation now resolves hostnames; journaling tests stub DNS
        p = _mock.patch("socket.getaddrinfo",
                        lambda *a, **k: [(2, 1, 6, "", ("8.8.8.8", 0))])
        p.start()
        self.addCleanup(p.stop)

    def test_round_trip_and_command_mismatch(self):
        ops.journal_record(self.dir, 7, "clipnow", "Clip created: x")
        self.assertEqual(ops.journal_result(self.dir, 7, "clipnow"),
                         "Clip created: x")
        self.assertIsNone(ops.journal_result(self.dir, 7, "relay"))
        self.assertIsNone(ops.journal_result(self.dir, 8, "clipnow"))
        self.assertIsNone(ops.journal_result(self.dir, 0, "clipnow"))

    def test_relay_replay_returns_recorded_result(self):
        (self.dir / "relay_enabled").write_text("1")
        from app.distribution import relay as relay_mod
        orig = ops.Relayer.restream
        ops.Relayer.restream = lambda self, f, d, ffmpeg_bin="ffmpeg": [
            ["sleep", "30"] for _ in d]
        try:
            first = ops.relay_reply(self.dir, "start http://feed rtmp://a",
                                    update_id=99)
            self.assertIn("Restreaming", first)
            second = ops.relay_reply(self.dir, "start http://feed rtmp://a",
                                     update_id=99)
            self.assertIn("Already processed", second)
            from app.distribution.relay import RestreamManager
            mgr = RestreamManager(str(self.dir / "restream.pids"))
            self.assertEqual(len(mgr.active()), 1)  # not started twice
            mgr.stop()
        finally:
            ops.Relayer.restream = orig


if __name__ == "__main__":
    unittest.main()
