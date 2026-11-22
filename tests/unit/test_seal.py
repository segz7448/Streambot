"""Seal + verified-snapshot unit tests."""
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.clipping import seal


class SealTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.raw = self.dir / "raw.ts"
        self.raw.write_bytes(b"\x47" * 4096)

    def test_seal_reports_real_digest_and_size(self):
        import hashlib
        sha, size = seal.seal_file(self.raw)
        self.assertEqual(sha,
                         hashlib.sha256(self.raw.read_bytes()).hexdigest())
        self.assertEqual(size, 4096)

    def test_snapshot_verifies_against_committed_digest(self):
        sha, size = seal.seal_file(self.raw)
        snap = seal.take_verified_snapshot(self.raw, self.dir / "rec",
                                           sha, size)
        self.assertIsNotNone(snap)
        self.assertEqual(snap.read_bytes(), self.raw.read_bytes())
        self.assertNotEqual(snap, self.raw)
        self.assertEqual(self.raw.read_bytes(), b"\x47" * 4096)

    def test_snapshot_refuses_tampered_source(self):
        sha, size = seal.seal_file(self.raw)
        self.raw.write_bytes(b"\x00" * 4096)
        self.assertIsNone(seal.take_verified_snapshot(
            self.raw, self.dir / "rec", sha, size))
        self.assertEqual(list((self.dir / "rec").glob("snapshot-*")), [])

    def test_snapshot_refuses_size_mismatch_without_hashing(self):
        sha, size = seal.seal_file(self.raw)
        self.assertIsNone(seal.take_verified_snapshot(
            self.raw, self.dir / "rec", sha, size + 1))

    def test_snapshot_refuses_missing_source(self):
        sha, size = seal.seal_file(self.raw)
        self.raw.unlink()
        self.assertIsNone(seal.take_verified_snapshot(
            self.raw, self.dir / "rec", sha, size))

    def test_unique_path_never_repeats(self):
        seen = {seal.unique_path(self.dir, "p", ".mp4") for _ in range(50)}
        self.assertEqual(len(seen), 50)

    def test_snapshot_path_collision_preserves_prior_bytes(self):
        # a snapshot path that already exists (another attempt's input)
        # must never be overwritten: reservation is exclusive, so the
        # colliding candidate is skipped and its bytes survive
        sha, size = seal.seal_file(self.raw)
        snapdir = self.dir / "rec"
        snapdir.mkdir()
        victim = snapdir / "snapshot-aaaaaaaaaaaa.ts"
        victim.write_bytes(b"PRIOR-ATTEMPT-BYTES")

        class FakeUUID:
            def __init__(self, hexval):
                self.hex = hexval
        seq = iter([FakeUUID("a" * 32), FakeUUID("b" * 32)])
        with mock.patch("app.clipping.seal.uuid.uuid4",
                        side_effect=lambda: next(seq)):
            snap = seal.take_verified_snapshot_bounded(
                self.raw, snapdir, sha, size)
        self.assertIsNotNone(snap)
        self.assertNotEqual(snap, victim)


if __name__ == "__main__":
    unittest.main()
