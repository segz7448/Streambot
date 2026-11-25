"""Phase 010 clipping plan tests (no ffmpeg execution)."""
import os
import tempfile
import time
import unittest

from app.clipping.extractor import extract_args, plan_clip
from app.recording.buffer import RollingBuffer


class PlanClipTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.buf = RollingBuffer(self.tmp.name, session_id=1, segment_seconds=5,
                                 pre_event_seconds=30, post_event_seconds=30,
                                 max_storage_mb=64)
        now = time.time()
        # segments end at now-30, now-25, ..., now-5 (6 segments, 30s footage)
        for i in range(6):
            p = self.buf.dir / f"seg-{i:06d}.ts"
            p.write_bytes(b"\0" * 2048)
            end = now - 30 + 5 * (i + 1)
            os.utime(p, (end, end))
        self.now = now

    def tearDown(self):
        self.tmp.cleanup()

    def test_plan_covers_event_window(self):
        plan = plan_clip(self.buf, self.now - 16, pre_seconds=10, post_seconds=5)
        self.assertIsNotNone(plan)
        self.assertGreaterEqual(plan.duration, 4)
        self.assertLessEqual(plan.duration, 16)
        self.assertGreaterEqual(len(plan.segments), 3)

    def test_plan_none_when_no_segments(self):
        buf = RollingBuffer(self.tmp.name, session_id=99, segment_seconds=5,
                            pre_event_seconds=30, post_event_seconds=30,
                            max_storage_mb=64)
        self.assertIsNone(plan_clip(buf, self.now - 16))

    def test_plan_none_when_prefootage_rotated(self):
        old = self.now - 600
        self.assertIsNone(plan_clip(self.buf, old, pre_seconds=10))

    def test_stream_start_preroll_clips_only_when_opted_in(self):
        ev = self.now - 26          # 4s after the recording began
        self.assertIsNone(plan_clip(self.buf, ev, pre_seconds=10,
                                    post_seconds=3))
        plan = plan_clip(self.buf, ev, pre_seconds=10, post_seconds=3,
                         allow_stream_start=True)
        self.assertIsNotNone(plan)
        self.assertEqual(plan.start_offset, 0.0)
        self.assertAlmostEqual(plan.duration, 7.0, delta=0.6)

    def test_phone_case_mark_2s_in_with_3s_preroll(self):
        ev = self.now - 30 + 2.2    # exact phone-run shape: 2.2s, pre 3s
        self.assertIsNone(plan_clip(self.buf, ev, pre_seconds=3,
                                    post_seconds=2))
        plan = plan_clip(self.buf, ev, pre_seconds=3, post_seconds=2,
                         allow_stream_start=True)
        self.assertIsNotNone(plan)
        self.assertEqual(plan.start_offset, 0.0)

    def test_rotation_is_never_treated_as_stream_start(self):
        (self.buf.dir / "seg-000000.ts").unlink()    # earliest rotated out
        ev = self.now - 21
        self.assertIsNone(plan_clip(self.buf, ev, pre_seconds=10,
                                    post_seconds=3,
                                    allow_stream_start=True))

    def test_stream_start_opt_in_needs_the_moment_itself_covered(self):
        self.assertIsNone(plan_clip(self.buf, self.now - 40, pre_seconds=10,
                                    post_seconds=3,
                                    allow_stream_start=True))

    def test_extract_args_exact_cut(self):
        plan = plan_clip(self.buf, self.now - 16, pre_seconds=10, post_seconds=5)
        args = extract_args("ffmpeg", "list.txt", plan, "out.mp4")
        self.assertIn("concat", args)
        self.assertIn("-ss", args)
        self.assertIn("libx264", args)
        self.assertEqual(args[-1], "out.mp4")


if __name__ == "__main__":
    unittest.main()
