import os
import tempfile
import unittest
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
