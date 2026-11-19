import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.bot import ops


class OpsTests(unittest.TestCase):
    def setUp(self):
        self.state = Path(tempfile.mkdtemp())

    def test_clipnow_unconfigured_fails_loudly(self):
        reply = ops.clipnow_reply(self.state, "somechannel")
        self.assertIn("not configured", reply)
        self.assertIn("clips:edit", reply)

    def test_clipnow_missing_arg(self):
        (self.state / "twitch_user_token").write_text("t")
        (self.state / "twitch_client_id").write_text("c")
        self.assertTrue(ops.clipnow_reply(self.state, " ")
                        .startswith("Usage:"))

    def test_clipnow_creation_failure_reported(self):
        (self.state / "twitch_user_token").write_text("t")
        (self.state / "twitch_client_id").write_text("c")
        from app.ingestion.twitch_clips import ClipFailed
        with mock.patch.object(ops, "TwitchClipsClient") as cls:
            inst = cls.return_value
            inst.resolve_broadcaster_id.side_effect = ClipFailed("nope")
            self.assertIn("Clip creation failed", ops.clipnow_reply(
                self.state, "chan"))

    def test_destinations_round_trip(self):
        self.assertIn("No delivery destinations",
                      ops.destinations_reply(self.state, ""))
        self.assertIn("registered",
                      ops.destinations_reply(self.state, "add 5 clips"))
        self.assertIn("5 - clips", ops.destinations_reply(self.state, ""))
        self.assertIn("removed", ops.destinations_reply(self.state, "remove 5"))
        self.assertIn("No destination",
                      ops.destinations_reply(self.state, "remove 5"))
        self.assertTrue(ops.destinations_reply(self.state, "bogus")
                        .startswith("Usage:"))


if __name__ == "__main__":
    unittest.main()
