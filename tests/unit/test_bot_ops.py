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

    def test_relay_refuses_keyed_destination(self):
        reply = ops.relay_reply(self.state,
                                "start http://feed rtmp://live.twitch.tv/app/live_123")
        self.assertIn("Refused", reply)
        self.assertNotIn("live_123", reply)
        self.assertIn("No restream", ops.stoprelay_reply(self.state))

    def test_relay_usage_and_stop_idle(self):
        self.assertTrue(ops.relay_reply(self.state, "").startswith("Usage:"))
        self.assertTrue(ops.relay_reply(self.state, "start http://feed")
                        .startswith("Usage:"))
        self.assertIn("No restream", ops.stoprelay_reply(self.state))

    def test_relay_start_and_stop_real_process(self):
        # uses sleep as a stand-in for ffmpeg: real subprocess lifecycle
        from unittest import mock as m
        from app.distribution import relay as relay_mod
        orig = ops.Relayer.restream
        ops.Relayer.restream = lambda self, f, d, ffmpeg_bin="ffmpeg": [
            ["sleep", "30"] for _ in d]
        try:
            reply = ops.relay_reply(self.state,
                                    "start http://feed rtmp://a rtmp://b")
            self.assertIn("2 destination", reply)
            from app.distribution.relay import RestreamManager
            mgr = RestreamManager(str(self.state / "restream.pids"))
            self.assertEqual(len(mgr.active()), 2)
            self.assertIn("already running",
                          ops.relay_reply(self.state,
                                          "start http://feed rtmp://a"))
            self.assertIn("stopped", ops.stoprelay_reply(self.state))
            self.assertEqual(mgr.active(), [])
        finally:
            ops.Relayer.restream = orig


if __name__ == "__main__":
    unittest.main()
