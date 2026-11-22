"""Owner-invoked recovery entrypoint: /recoverclips finishes sealed
PENDING clips from interrupted sessions to HELD (nothing delivered),
reports honestly, and stays admin-only through the router."""
import unittest
from pathlib import Path
from unittest import mock

from app.bot import sessions
from app.bot.commands import CommandRouter, UnauthorizedError
from tests.unit.test_finalize_interrupted import FinalizeTests


class RecoverclipsReplyTests(FinalizeTests):
    def test_sealed_clip_recovered_and_reported(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        reply = sessions.recoverclips_reply(self.dir, "ffmpeg", "ffprobe")
        self.assertIn("1 clip", reply)
        self.assertIn("HELD", reply)
        self.assertIn("Nothing was delivered", reply)
        self.assertEqual(self._clip(cid)[0], "HELD")
        self.assertEqual(self._jobs(), [])

    def test_nothing_recoverable(self):
        reply = sessions.recoverclips_reply(self.dir, "ffmpeg", "ffprobe")
        self.assertIn("nothing", reply.lower())

    def test_fenced_remainder_reported_not_retried(self):
        cid = self._add_clip(sealed=False)   # legacy/unsealed: fenced
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        reply = sessions.recoverclips_reply(self.dir, "ffmpeg", "ffprobe")
        self.assertIn("fenced", reply.lower())
        self.assertEqual(self._clip(cid)[0], "PENDING")
        self.assertEqual(self._jobs(), [])

    def test_second_run_idempotent(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        sessions.recoverclips_reply(self.dir, "ffmpeg", "ffprobe")
        reply = sessions.recoverclips_reply(self.dir, "ffmpeg", "ffprobe")
        self.assertIn("nothing", reply.lower())
        self.assertEqual(self._clip(cid)[0], "HELD")


class RecoverclipsRouterTests(FinalizeTests):
    def _router(self):
        r = CommandRouter(admin_ids=[42])
        r.register(
            "recoverclips",
            lambda c: sessions.recoverclips_reply(self.dir, "ffmpeg",
                                                  "ffprobe"),
            "finish clips left by interrupted sessions to HELD")
        return r

    def test_non_admin_rejected_nothing_runs(self):
        cid = self._add_clip()
        r = self._router()
        with self.assertRaises(UnauthorizedError):
            r.dispatch("/recoverclips", user_id=7)
        self.assertEqual(self._clip(cid)[0], "PENDING")

    def test_admin_dispatch_runs(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        reply = self._router().dispatch("/recoverclips", user_id=42)
        self.assertIn("HELD", reply)
        self.assertEqual(self._clip(cid)[0], "HELD")

    def test_help_advertises_registered_command(self):
        reply = self._router().dispatch("/help", user_id=42)
        self.assertIn("/recoverclips", reply)


if __name__ == "__main__":
    unittest.main()
