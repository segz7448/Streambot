"""Real-ffmpeg reproduction of the session-8 segment timeline defect.

Runs the PRODUCTION segment args (RollingBuffer.segment_args: -c copy
-f segment -segment_time 5 -reset_timestamps 1) over a 2s-GOP source and
compares the buffer's reported timeline against ffprobe ground truth.
Keyframe-aligned cuts make actual durations differ from the nominal 5s;
the buffer must report actual durations and cumulative content time.
"""
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from app.recording.buffer import RollingBuffer, ffprobe_probe

FFMPEG = shutil.which("ffmpeg")
FFPROBE = shutil.which("ffprobe")


@unittest.skipUnless(FFMPEG and FFPROBE, "ffmpeg/ffprobe not available")
class RealSegmentTimelineTests(unittest.TestCase):
    def test_segments_match_container_truth(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src.ts"
            subprocess.run(
                [FFMPEG, "-hide_banner", "-loglevel", "error",
                 "-f", "lavfi", "-i", "testsrc2=size=160x120:rate=30",
                 "-f", "lavfi", "-i", "sine=frequency=440",
                 "-t", "14", "-g", "60", "-keyint_min", "60",
                 "-sc_threshold", "0", "-c:v", "libx264", "-preset",
                 "ultrafast", "-pix_fmt", "yuv420p", "-c:a", "aac",
                 "-f", "mpegts", str(src)], check=True)
            buf = RollingBuffer(tmp, session_id=1, segment_seconds=5,
                                probe=ffprobe_probe(FFPROBE))
            subprocess.run(
                [FFMPEG] + buf.segment_args(str(src)), check=True,
                cwd=tmp)
            buf.mark_drained()   # ffmpeg exited: every segment is final
            segs = buf.segments()
            self.assertGreaterEqual(len(segs), 2)
            truth = [float(subprocess.run(
                [FFPROBE, "-v", "error", "-show_entries",
                 "format=duration", "-of", "csv=p=0", str(s.path)],
                capture_output=True, text=True, check=True).stdout.strip())
                for s in segs]
            # actual durations are reported, not the nominal 5.0
            self.assertTrue(any(abs(t - 5.0) > 0.5 for t in truth),
                            f"fixture itself is degenerate: {truth}")
            for s, t in zip(segs, truth):
                self.assertAlmostEqual(s.duration_seconds, t, places=2)
            # created_at is cumulative ACTUAL content time off the anchor
            anchor = segs[0].created_at
            cum = anchor
            for s, t in zip(segs, truth):
                if s is not segs[0]:
                    cum += t
                self.assertAlmostEqual(s.created_at, cum, places=2)


@unittest.skipUnless(FFMPEG and FFPROBE, "ffmpeg/ffprobe not available")
class RealAnalysisAuditTests(unittest.TestCase):
    def test_invalid_media_records_failure_not_success(self):
        # reviewer: real analyzers ignored nonzero ffmpeg rc and returned
        # empty, so invalid media was audited as successfully analyzed
        import os
        from app.core.database import connect, migrate
        from app.detection.detector import CandidateDetector
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "t.db")
            conn = connect(db)
            migrate(conn)
            conn.execute("INSERT INTO twitch_sources"
                         " (username, created_at, updated_at)"
                         " VALUES ('synthetic', 0, 0)")
            conn.execute("INSERT INTO stream_sessions (source_id, state)"
                         " VALUES (1, 'RECORDING')")
            conn.commit()
            conn.close()
            from app.recording.buffer import RollingBuffer
            buf = RollingBuffer(tmp, session_id=1, segment_seconds=5)
            for i in range(2):
                p = buf.dir / f"seg-{i:06d}.ts"
                p.write_bytes(b"\0" * 1024)   # not media
                os.utime(p, (1000 + i, 1000 + i))
            det = CandidateDetector(FFMPEG, buf, db)
            self.assertEqual(det.process_new_segments(), 0)
            conn = connect(db)
            rows = [tuple(r) for r in conn.execute(
                "SELECT sequence, audio_ok, visual_ok"
                " FROM segment_analysis ORDER BY sequence")]
            conn.close()
            self.assertEqual(rows, [(0, 0, 0)])  # seg0 closed; seg1 open


@unittest.skipUnless(FFMPEG and FFPROBE, "ffmpeg/ffprobe not available")
class KnownFrameExtractionTests(unittest.TestCase):
    """Clip extraction must land on the content the event time NAMES, not
    on self-consistent wrong math: solid-color segments with known
    durations, an event inside the blue block, then the extracted clip's
    actual frames are sampled."""

    def _segment(self, path, color, seconds):
        subprocess.run(
            [FFMPEG, "-hide_banner", "-loglevel", "error",
             "-f", "lavfi", "-i",
             f"color=c={color}:size=160x120:rate=10",
             "-t", str(seconds), "-g", "10",
             "-c:v", "libx264", "-preset", "ultrafast",
             "-pix_fmt", "yuv420p", str(path)], check=True)

    def _avg_rgb(self, path, t):
        out = subprocess.run(
            [FFMPEG, "-hide_banner", "-loglevel", "error",
             "-ss", f"{t:.2f}", "-i", str(path), "-frames:v", "1",
             "-vf", "scale=1:1", "-f", "rawvideo", "-pix_fmt", "rgb24",
             "-"], capture_output=True, check=True).stdout
        self.assertEqual(len(out), 3)
        return out[0], out[1], out[2]

    def test_extracted_clip_contains_the_event_content(self):
        import os
        from app.recording.buffer import RollingBuffer, ffprobe_probe
        from app.clipping.extractor import extract_clip
        with tempfile.TemporaryDirectory() as tmp:
            buf = RollingBuffer(tmp, session_id=1, segment_seconds=5,
                                probe=ffprobe_probe(FFPROBE))
            blocks = [("red", 6), ("lime", 2), ("blue", 4), ("yellow", 4)]
            m0 = 1000.0
            for i, (color, secs) in enumerate(blocks):
                p = buf.dir / f"seg-{i:06d}.ts"
                self._segment(str(p), color, secs)
                os.utime(p, (m0, m0) if i == 0 else (m0 + i, m0 + i))
            buf.mark_drained()
            segs = buf.segments()
            # sanity: probed durations match the known blocks
            self.assertEqual([round(s.duration_seconds)
                              for s in segs], [6, 2, 4, 4])
            event = m0 + 4.0        # 2s into blue (m0+2 .. m0+6)
            out = Path(tmp) / "clip.mp4"
            res = extract_clip(FFMPEG, buf, event, str(out),
                               pre_seconds=3.0, post_seconds=3.0)
            self.assertIsNotNone(res)
            # window [m0+1, m0+7] over lime|blue|yellow starting 1s into
            # lime: output 0-1s lime, 1-5s blue, 5-6s yellow
            r, g, b = self._avg_rgb(out, 0.5)
            self.assertTrue(g > 100 and r < 100 and b < 100,
                            f"expected lime at 0.5s, got {(r, g, b)}")
            r, g, b = self._avg_rgb(out, 3.0)
            self.assertTrue(b > 100 and r < 100 and g < 100,
                            f"expected blue at 3.0s, got {(r, g, b)}")
            r, g, b = self._avg_rgb(out, 5.5)
            self.assertTrue(r > 100 and g > 100 and b < 100,
                            f"expected yellow at 5.5s, got {(r, g, b)}")


if __name__ == "__main__":
    unittest.main()
