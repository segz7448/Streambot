import os
from pathlib import Path
import tempfile
import unittest

from app.distribution.relay import DestinationStore, Relayer


class RelayTests(unittest.TestCase):
    def setUp(self):
        from tests.process_helpers import track_children
        self.children = track_children(self)
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

    def test_zombie_child_is_reaped(self):
        # SIGKILLed child becomes a zombie until reaped; the manager
        # must not count it as alive (found by on-device probe)
        import os
        import signal
        import time
        from app.distribution.relay import RestreamManager
        mgr = RestreamManager(os.path.join(self.tmp, "rt.pids"))
        pids = mgr.start([["sleep", "30"]])
        os.kill(pids[0], signal.SIGKILL)
        time.sleep(0.3)
        self.assertEqual(mgr.active(), [])
        self.assertEqual(mgr.stop(), 0)

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
                              ["rtmp://dest1"], ffmpeg_bin="ffmpeg")
        self.assertEqual(len(cmds), 1)
        self.assertIn("http://owner/feed", cmds[0])
        self.assertIn("rtmp://dest1", cmds[0])

    def test_secret_bearing_destination_refused(self):
        relay = Relayer(self.store, lambda c, p, t: 0)
        with self.assertRaises(ValueError):
            relay.restream("http://feed", ["rtmp://dest1/secretkey"])


if __name__ == "__main__":
    unittest.main()


class FeedHostGuardTests(unittest.TestCase):
    """Mode A feed URLs must not be silently network-unrestricted."""

    def test_private_and_local_hosts_refused(self):
        from app.distribution.relay import validate_feed
        for url in ("http://127.0.0.1:8080/admin",
                    "http://10.0.0.5/feed",
                    "http://192.168.1.10/feed",
                    "http://169.254.169.254/latest/meta-data",
                    "http://localhost/feed",
                    "http://router.local/feed",
                    "http://0.0.0.0/feed"):
            with self.assertRaises(ValueError, msg=url):
                validate_feed(url, Path("/tmp/feeds"))

    def test_public_feed_accepted(self):
        from app.distribution.relay import validate_feed
        self.assertEqual(
            validate_feed("https://cdn.example.com/broadcaster-feed",
                          Path("/tmp/feeds"),
                          resolver=lambda name: {"8.8.8.8"}),
            "https://cdn.example.com/broadcaster-feed")


class FeedHostCanonicalFormTests(unittest.TestCase):
    """Alternate numeric notations must not bypass the public check."""

    def test_alternate_loopback_forms_refused(self):
        from app.distribution.relay import validate_feed
        for url in ("http://2130706433/feed",        # integer 127.0.0.1
                    "http://127.1/feed",             # short dotted
                    "http://0x7f000001/feed",        # hex
                    "http://0177.0.0.1/feed"):       # octal
            with self.assertRaises(ValueError, msg=url):
                validate_feed(url, Path("/tmp/feeds"))

    def test_hostname_resolving_private_refused(self):
        from app.distribution.relay import validate_feed
        fake = lambda name: {"127.0.0.1"}
        with self.assertRaises(ValueError):
            validate_feed("https://evil.example/feed", Path("/tmp/feeds"),
                          resolver=fake)

    def test_hostname_mixed_resolution_refused(self):
        from app.distribution.relay import validate_feed
        fake = lambda name: {"8.8.8.8", "10.1.2.3"}
        with self.assertRaises(ValueError):
            validate_feed("https://mixed.example/feed", Path("/tmp/feeds"),
                          resolver=fake)

    def test_hostname_resolving_public_accepted(self):
        from app.distribution.relay import validate_feed
        fake = lambda name: {"8.8.8.8"}
        self.assertEqual(
            validate_feed("https://cdn.example.com/feed", Path("/tmp/feeds"),
                          resolver=fake),
            "https://cdn.example.com/feed")

    def test_unresolvable_host_refused(self):
        import socket
        from app.distribution.relay import validate_feed
        def boom(name):
            raise socket.gaierror()
        with self.assertRaises(ValueError):
            validate_feed("https://gone.example/feed", Path("/tmp/feeds"),
                          resolver=boom)

class RestreamLifecycleRegressionTests(unittest.TestCase):
    def setUp(self):
        from tests.process_helpers import track_children
        self.children = track_children(self)
        self.root = Path(tempfile.mkdtemp())

    def test_new_manager_stops_and_reaps_all_complete_identities(self):
        from app.distribution.relay import RestreamManager
        path = str(self.root / 'restream.pids')
        first = RestreamManager(path)
        pids = first.start([['sleep', '30'], ['sleep', '30']])
        records = first._records()
        self.assertTrue(all(r['start'] and r['cmd'] for r in records))
        second = RestreamManager(path)
        self.assertEqual(second.stop(), 2)
        self.assertTrue(all(proc.poll() is not None for proc in self.children))
        self.assertEqual(second._records(), [])

    def test_transient_empty_identity_is_retried_before_persist(self):
        from app.distribution.relay import RestreamManager
        from unittest import mock
        mgr = RestreamManager(str(self.root / 'restream.pids'))
        real = mgr._identity
        calls = [0]
        def identity(pid):
            calls[0] += 1
            if calls[0] == 1:
                return {'start': '1', 'cmd': ''}
            return real(pid)
        with mock.patch.object(mgr, '_identity', side_effect=identity):
            mgr.start([['sleep', '30']])
        self.assertGreaterEqual(calls[0], 2)
        self.assertTrue(mgr._records()[0]['cmd'])
        self.assertEqual(mgr.stop(), 1)

    def test_missing_identity_aborts_and_reaps_only_owned_launch(self):
        from app.distribution.relay import RestreamManager
        from unittest import mock
        mgr = RestreamManager(str(self.root / 'restream.pids'))
        with mock.patch.object(mgr, '_identity', return_value=None):
            with self.assertRaisesRegex(RuntimeError, 'identity unavailable'):
                mgr.start([['sleep', '30']])
        self.assertEqual(len(self.children), 1)
        self.assertIsNotNone(self.children[0].poll())
        self.assertEqual(mgr._records(), [])

    def test_unverified_live_record_retained_never_signaled(self):
        import subprocess
        from app.distribution.relay import RestreamManager
        proc = subprocess.Popen(['sleep', '30'])
        mgr = RestreamManager(str(self.root / 'restream.pids'))
        try:
            mgr._save([{'pid': proc.pid, 'start': 'wrong-start', 'cmd': 'sleep 30'}])
            self.assertEqual(mgr.stop(), 0)
            self.assertIsNone(proc.poll())
            self.assertEqual(mgr._records()[0]['pid'], proc.pid)
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_empty_saved_identity_is_not_valid_even_when_current_empty(self):
        import subprocess
        from unittest import mock
        from app.distribution.relay import RestreamManager
        proc = subprocess.Popen(['sleep', '30'])
        mgr = RestreamManager(str(self.root / 'restream.pids'))
        try:
            mgr._save([{'pid': proc.pid, 'start': 'same', 'cmd': ''}])
            with mock.patch.object(mgr, '_identity', return_value={'start':'same', 'cmd':''}):
                self.assertEqual(mgr.stop(), 0)
            self.assertIsNone(proc.poll())
            self.assertEqual(mgr._records()[0]['pid'], proc.pid)
        finally:
            proc.terminate()
            proc.wait(timeout=5)
