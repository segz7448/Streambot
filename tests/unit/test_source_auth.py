import sqlite3
import tempfile
import unittest
from pathlib import Path

from app.bot import source_auth as sa
from app.core.database import migrate


class SourceAuthTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        c = sqlite3.connect(str(self.d / "nova.db")); migrate(c); c.close()

    def test_default_off_and_scopes_are_separate(self):
        self.assertFalse(sa.allowed(self.d, "somestreamer", "record"))
        self.assertFalse(sa.allowed(self.d, "somestreamer", "repost"))
        sa.set_scope(self.d, "somestreamer", "record", True)
        self.assertTrue(sa.allowed(self.d, "somestreamer", "record"))
        self.assertFalse(sa.allowed(self.d, "somestreamer", "repost"))
        self.assertFalse(sa.allowed(self.d, "other", "record"))

    def test_unreadable_or_invalid_is_not_authorized(self):
        sa.set_scope(self.d, "somestreamer", "record", True)
        self.assertFalse(sa.allowed(self.d / "nope", "somestreamer", "record"))
        self.assertFalse(sa.allowed(self.d, "somestreamer", "bogus"))
        self.assertFalse(sa.allowed(self.d, "bad name!", "record"))

    def test_command_requires_explicit_scope_and_state(self):
        self.assertIn("Usage", sa.sourceauth_reply(self.d, ""))
        self.assertIn("Usage", sa.sourceauth_reply(self.d, "somestreamer record"))
        self.assertFalse(sa.allowed(self.d, "somestreamer", "record"))
        out = sa.sourceauth_reply(self.d, "@SomeStreamer record on")
        self.assertIn("record: ON", out)
        self.assertTrue(sa.allowed(self.d, "somestreamer", "record"))
        self.assertIn("repost off", sa.sourceauth_reply(self.d, "somestreamer"))
        self.assertIn("Could NOT", sa.sourceauth_reply(self.d / "nope", "somestreamer repost on"))


if __name__ == "__main__":
    unittest.main()
