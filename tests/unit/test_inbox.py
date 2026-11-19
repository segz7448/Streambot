import os
import tempfile
import unittest
from pathlib import Path

from app.bot.commands import CommandRouter
from app.bot.inbox import PreAckInbox
from app.bot.ops import drain_inbox
from app.bot.watermark import UpdateWatermark


class InboxTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.inbox = PreAckInbox(str(self.dir / "inbox.json"))

    def test_add_pending_remove(self):
        self.inbox.add(1, 42, "/status")
        self.assertEqual(self.inbox.pending()[0]["text"], "/status")
        self.inbox.add(1, 42, "/status")  # idempotent re-add
        self.assertEqual(len(self.inbox.pending()), 1)
        self.inbox.remove(1)
        self.assertEqual(self.inbox.pending(), [])

    def test_file_is_owner_only(self):
        self.inbox.add(1, 42, "/status")
        mode = os.stat(self.dir / "inbox.json").st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def _router(self, replies):
        router = CommandRouter(admin_ids=[])
        router.register("ok", lambda c: replies.append(c.user_id) or "done",
                        "test")
        def boom(_c):
            raise RuntimeError("crash")
        router.register("boom", boom, "crashes")
        return router

    def test_drain_replays_stranded_and_drops_handled(self):
        watermark = UpdateWatermark(str(self.dir / "updates.json"))
        replies = []
        router = self._router(replies)
        self.inbox.add(10, 42, "/ok")     # stranded by crash
        self.inbox.add(11, 42, "/ok")     # actually handled before crash
        watermark.mark_failed(10)   # service recorded the crash
        watermark.mark_handled(11)
        sent = []
        result = drain_inbox(self.dir, router, watermark, self.inbox,
                             reply=lambda uid, text: sent.append((uid, text)))
        self.assertEqual(result, {"replayed": 1, "dropped": 1, "failed": 0})
        self.assertEqual(self.inbox.pending(), [])
        self.assertTrue(watermark.already_handled(10))
        self.assertEqual(sent, [(42, "done")])

    def test_drain_keeps_failures_for_next_boot(self):
        watermark = UpdateWatermark(str(self.dir / "updates.json"))
        router = self._router([])
        self.inbox.add(20, 42, "/boom")
        result = drain_inbox(self.dir, router, watermark, self.inbox)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(len(self.inbox.pending()), 1)
        # next boot: same entry retried again, now succeeding path free
        router2 = CommandRouter(admin_ids=[])
        router2.register("boom", lambda c: "recovered", "fixed")
        result2 = drain_inbox(self.dir, router2, watermark, self.inbox)
        self.assertEqual(result2["replayed"], 1)
        self.assertEqual(self.inbox.pending(), [])


if __name__ == "__main__":
    unittest.main()
