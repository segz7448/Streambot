import os
import tempfile
import unittest

from app.distribution.relay import DestinationStore, Relayer


class RelayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = DestinationStore(os.path.join(self.tmp, "dest.json"))

    def test_add_list_remove(self):
        self.store.add(123, "clips channel")
        self.assertEqual(self.store.list()[0]["chat_id"], 123)
        self.assertTrue(self.store.remove(123))
        self.assertEqual(self.store.list(), [])

    def test_permissions_are_owner_only(self):
        self.store.add(1, "a")
        self.assertEqual(os.stat(self.store.path).st_mode & 0o777, 0o600)

    def test_relay_sends_to_each_destination(self):
        sent = []
        self.store.add(1, "a"); self.store.add(2, "b")
        relay = Relayer(self.store, lambda cid, p, c: sent.append(cid) or cid * 10)
        results = relay.relay_clip("/tmp/x.mp4", "cap")
        self.assertEqual(sent, [1, 2])
        self.assertEqual(results, {1: 10, 2: 20})

    def test_partial_failure_is_visible(self):
        self.store.add(1, "a"); self.store.add(2, "b")
        def sender(cid, p, c):
            if cid == 1:
                raise RuntimeError("boom")
            return 99
        results = Relayer(self.store, sender).relay_clip("/tmp/x.mp4")
        self.assertTrue(str(results[1]).startswith("FAILED"))
        self.assertEqual(results[2], 99)

    def test_restream_requires_feed_and_destinations(self):
        relay = Relayer(self.store, lambda c, p, t: 0)
        with self.assertRaises(ValueError):
            relay.restream("", ["rtmp://a"])
        with self.assertRaises(ValueError):
            relay.restream("http://feed", [])

    def test_restream_builds_owner_feed_commands(self):
        relay = Relayer(self.store, lambda c, p, t: 0)
        cmds = relay.restream("http://owner/feed",
                              ["rtmp://dest1/key"], ffmpeg_bin="ffmpeg")
        self.assertEqual(len(cmds), 1)
        self.assertIn("http://owner/feed", cmds[0])
        self.assertIn("rtmp://dest1/key", cmds[0])


if __name__ == "__main__":
    unittest.main()
