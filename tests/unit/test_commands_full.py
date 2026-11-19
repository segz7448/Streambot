import os
import sqlite3
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from app.bot import commands_full as cf
from app.core.database import migrate


class CommandsFullTests(unittest.TestCase):
    def setUp(self):
        import sqlite3
        self.dir = Path(tempfile.mkdtemp())
        conn = sqlite3.connect(str(self.dir / "nova.db"))
        migrate(conn)
        conn.close()

    def test_addsource_validate_register_dupe_remove(self):
        self.assertTrue(cf.addsource_reply(self.dir, "").startswith("Usage:"))
        self.assertIn("registered", cf.addsource_reply(self.dir, "SomeUser"))
        self.assertIn("already", cf.addsource_reply(self.dir, "someuser"))
        self.assertIn("removed", cf.removesource_reply(self.dir, "someuser"))
        self.assertIn("No source", cf.removesource_reply(self.dir, "someuser"))

    def test_checklive_unconfigured_fails_loudly(self):
        self.assertIn("not configured", cf.checklive_reply(self.dir, "x"))

    def test_startclip_requires_source_then_input(self):
        self.assertIn("/addsource", cf.startclip_reply(self.dir, "ghost"))
        cf.addsource_reply(self.dir, "chan")
        reply = cf.startclip_reply(self.dir, "chan")
        self.assertIn("cannot start", reply)
        self.assertIn("stays registered", reply)

    def test_sessions_clips_empty(self):
        self.assertIn("No sessions", cf.sessions_reply(self.dir, ""))
        self.assertIn("No clips", cf.clips_reply(self.dir, ""))
        self.assertTrue(cf.clipdetails_reply(self.dir, "x").startswith("Usage"))
        self.assertIn("No clip", cf.clipdetails_reply(self.dir, "1"))

    def test_stopclip_idle(self):
        self.assertIn("No clipping session", cf.stopclip_reply(self.dir, ""))

    def test_settings_and_quality(self):
        self.assertIn("No settings", cf.settings_reply(self.dir, ""))
        self.assertIn("saved", cf.settings_reply(self.dir, "note hello"))
        self.assertIn("note = hello", cf.settings_reply(self.dir, ""))
        self.assertIn("vertical", cf.quality_reply(self.dir, ""))
        self.assertIn("set to square", cf.quality_reply(self.dir, "square"))
        self.assertIn("Unknown preset", cf.quality_reply(self.dir, "4k"))
        self.assertIn("square", cf.quality_reply(self.dir, ""))

    def test_subtitles_out_of_scope(self):
        self.assertIn("out of scope", cf.SUBTITLES_REPLY)

    def test_logs_ring(self):
        self.assertIn("No errors", cf.logs_reply(self.dir, ""))
        cf.record_error(self.dir, "boom happened")
        self.assertIn("boom happened", cf.logs_reply(self.dir, ""))
        mode = os.stat(self.dir / "errors.log").st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def test_restart_honest(self):
        self.assertIn("host-controlled", cf.RESTART_REPLY)


if __name__ == "__main__":
    unittest.main()


class PublishCommandTests(unittest.TestCase):

    def _clip(self, d, clip_id=1):
        import time
        conn = sqlite3.connect(str(d / "nova.db"))
        migrate(conn)
        conn.close()
        clip = d / "clip1.mp4"
        clip.write_bytes(b"mp4data")
        with sqlite3.connect(str(d / "nova.db")) as conn:
            conn.execute(
                "INSERT INTO clips(id, file_path, status, created_at)"
                " VALUES(?,?,?,?)", (clip_id, str(clip), "HELD", time.time()))
        return clip

    def test_usage_and_missing_clip(self):
        d = Path(tempfile.mkdtemp())
        self.assertEqual(cf.publish_reply(d, "abc"), cf.USAGE_PUBLISH)
        self.assertEqual(cf.publish_reply(d, ""), cf.USAGE_PUBLISH)
        conn = sqlite3.connect(str(d / "nova.db"))
        migrate(conn)
        conn.close()
        self.assertIn("No clip #9", cf.publish_reply(d, "9"))

    def test_unconfigured_fails_closed_no_journal(self):
        d = self._clip(Path(tempfile.mkdtemp())).parent
        r = cf.publish_reply(d, "1", 555)
        self.assertIn("not configured", r)
        from app.bot import ops
        self.assertIsNone(ops.journal_result(d, 555, "publish"))

    def test_success_journaled_and_replay_guarded(self):
        d = self._clip(Path(tempfile.mkdtemp())).parent
        fake = mock.Mock()
        fake.publish.return_value = "vid9"
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r = cf.publish_reply(d, "1 My title", 556)
        self.assertIn("PRIVATE", r)
        self.assertIn("vid9", r)
        fake.publish.assert_called_once()
        args, kwargs = fake.publish.call_args
        self.assertEqual(kwargs.get("privacy"), "private")
        self.assertEqual(args[1], "My title")
        # replay: same update id returns the recorded result, no 2nd upload
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r2 = cf.publish_reply(d, "1 My title", 556)
        self.assertIn("replay guard", r2)
        fake.publish.assert_called_once()

    def test_api_error_journaled_failed(self):
        from app.distribution.youtube import YouTubeUploadFailed
        d = self._clip(Path(tempfile.mkdtemp())).parent
        fake = mock.Mock()
        fake.publish.side_effect = YouTubeUploadFailed("upload -> HTTP 403")
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r = cf.publish_reply(d, "1", 557)
        self.assertIn("failed", r)
        from app.bot import ops
        self.assertIn("failed", ops.journal_result(d, 557, "publish"))

    def test_network_failure_marked_uncertain_not_retried(self):
        import urllib.error
        d = self._clip(Path(tempfile.mkdtemp())).parent
        fake = mock.Mock()
        fake.publish.side_effect = urllib.error.URLError("reset")
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r = cf.publish_reply(d, "1", 558)
        self.assertIn("uncertain", r)
        from app.bot import ops
        unc = ops.journal_uncertain(d)
        self.assertEqual(len(unc), 1)
        self.assertEqual(unc[0]["command"], "publish")

    def test_publish_in_side_effecting(self):
        from app.bot import ops
        self.assertIn("publish", ops.SIDE_EFFECTING)
