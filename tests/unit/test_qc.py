"""Phase 012 QC tests: logic-level, ffmpeg interactions injected."""
import os
import subprocess
import tempfile
import unittest
from unittest import mock

from app.qc import duplicates
from app.qc.verify import QCResult, check_media


def _probe_payload(streams, duration="12.5"):
    import json
    return json.dumps({"streams": streams, "format": {"duration": duration}})


GOOD_STREAMS = [
    {"codec_type": "video", "width": 1080, "height": 1920},
    {"codec_type": "audio"},
]


class CheckMediaTests(unittest.TestCase):
    def _run(self, streams, duration, decode_rc=0, decode_err=""):
        def fake_run(args, **kw):
            proc = mock.Mock()
            if "ffprobe" in args[0]:
                proc.returncode = 0
                proc.stdout = _probe_payload(streams, duration)
            else:
                proc.returncode = decode_rc
                proc.stderr = decode_err
            return proc
        with mock.patch.object(subprocess, "run", side_effect=fake_run):
            return check_media("ffprobe", "ffmpeg", "x.mp4")

    def test_good_media_passes(self):
        r = self._run(GOOD_STREAMS, "12.5")
        self.assertTrue(r.ok, r.issues)

    def test_missing_audio_flagged(self):
        r = self._run([GOOD_STREAMS[0]], "12.5")
        self.assertFalse(r.ok)
        self.assertIn("no audio stream", r.issues)

    def test_short_duration_flagged(self):
        r = self._run(GOOD_STREAMS, "0.4")
        self.assertFalse(r.ok)
        self.assertTrue(any("duration" in i for i in r.issues))

    def test_decode_errors_flagged(self):
        r = self._run(GOOD_STREAMS, "12.5", decode_err="corrupt frame")
        self.assertFalse(r.ok)
        self.assertTrue(any("decode" in i for i in r.issues))

    def test_probe_failure_fails_closed(self):
        with mock.patch.object(subprocess, "run",
                               side_effect=RuntimeError("boom")):
            r = check_media("ffprobe", "ffmpeg", "x.mp4")
        self.assertFalse(r.ok)


class DuplicateTests(unittest.TestCase):
    def test_content_hash_identical(self):
        with tempfile.NamedTemporaryFile(delete=False) as a, \
             tempfile.NamedTemporaryFile(delete=False) as b:
            a.write(b"\x42" * 5000)
            b.write(b"\x42" * 5000)
            a.close(); b.close()
        self.assertEqual(duplicates.content_hash(a.name),
                         duplicates.content_hash(b.name))
        os.unlink(a.name); os.unlink(b.name)

    def test_hamming(self):
        self.assertEqual(duplicates.hamming(0b1010, 0b1010), 0)
        self.assertEqual(duplicates.hamming(0b1010, 0b0101), 4)

    def test_ahash_from_gray_bytes(self):
        # darker left half, brighter right half
        frame = bytes([10] * 32 + [240] * 32)
        def fake_run(args, **kw):
            proc = mock.Mock()
            proc.stdout = frame
            return proc
        with mock.patch.object(subprocess, "run", side_effect=fake_run):
            h = duplicates.frame_ahash("ffmpeg", "x.mp4")
        self.assertEqual(h >> 32 & 0xFFFFFFFF, 0xFFFFFFFF)  # bright half set
        self.assertEqual(h & 0xFFFFFFFF, 0)                 # dark half unset

    def test_ahash_raises_on_short_frame(self):
        def fake_run(args, **kw):
            proc = mock.Mock()
            proc.stdout = b"\x00" * 10
            return proc
        with mock.patch.object(subprocess, "run", side_effect=fake_run):
            with self.assertRaises(RuntimeError):
                duplicates.frame_ahash("ffmpeg", "x.mp4")


if __name__ == "__main__":
    unittest.main()
