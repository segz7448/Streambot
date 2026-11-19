import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app.bot.pairing import PairingStore


class PairingTests(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.path = str(Path(self.td.name) / "admin.json")
        self.store = PairingStore(self.path)

    def tearDown(self):
        self.td.cleanup()

    def test_no_admin_initially(self):
        self.assertIsNone(self.store.admin_id())

    def test_wrong_code_rejected(self):
        self.store.begin_challenge("right-code", now=1000.0)
        self.assertFalse(self.store.try_pair("wrong-code", 42, now=1001.0))
        self.assertIsNone(self.store.admin_id())

    def test_expired_code_rejected(self):
        self.store.begin_challenge("code", ttl_seconds=10, now=1000.0)
        self.assertFalse(self.store.try_pair("code", 42, now=1011.0))
        self.assertIsNone(self.store.admin_id())

    def test_correct_code_pairs_once(self):
        self.store.begin_challenge("code", now=1000.0)
        self.assertTrue(self.store.try_pair("code", 42, now=1001.0))
        self.assertEqual(self.store.admin_id(), 42)
        # second attempt with same code cannot re-pair or change admin
        self.assertFalse(self.store.try_pair("code", 99, now=1002.0))
        self.assertEqual(self.store.admin_id(), 42)

    def test_state_persists_across_instances(self):
        self.store.begin_challenge("code", now=1000.0)
        self.assertTrue(self.store.try_pair("code", 42, now=1001.0))
        self.assertEqual(PairingStore(self.path).admin_id(), 42)

    def test_state_file_permissions(self):
        import os
        import stat
        self.store.begin_challenge("code", now=1000.0)
        mode = stat.S_IMODE(os.stat(self.path).st_mode)
        self.assertEqual(mode, 0o600)

    def test_challenge_cleared_on_success(self):
        self.store.begin_challenge("code", now=1000.0)
        self.store.try_pair("code", 42, now=1001.0)
        self.assertNotIn("pending", self.store._read())

    def test_clear_challenge(self):
        self.store.begin_challenge("code", now=1000.0)
        self.store.clear_challenge()
        self.assertFalse(self.store.try_pair("code", 42, now=1001.0))

    def test_generated_code_charset(self):
        code = PairingStore.generate_code()
        self.assertEqual(len(code), 8)
        self.assertNotIn("0", code)
        self.assertNotIn("l", code)


if __name__ == "__main__":
    unittest.main()
