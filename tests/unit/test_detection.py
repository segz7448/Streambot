"""Phase 008 detection tests: heuristics with injected analyzers, no ffmpeg."""
import os
import tempfile
import time
import unittest

from app.core.database import connect, migrate
from app.detection import audio
from app.detection.detector import CandidateDetector, ensure_state_table
from app.recording.buffer import RollingBuffer


class LoudnessSpikeTests(unittest.TestCase):
    def test_spike_detected_over_baseline(self):
        samples = ([audio.LoudnessSample(t * 0.1, -30.0) for t in range(30)]
                   + [audio.LoudnessSample(3.0 + i * 0.1, -18.0) for i in range(5)])
        spikes = audio.loudness_spikes(samples, baseline=-30.0)
        self.assertTrue(spikes)
        self.assertAlmostEqual(spikes[0][1], -18.0)

    def test_silence_floor_ignored(self):
        samples = [audio.LoudnessSample(t * 0.1, -70.0) for t in range(20)]
        self.assertEqual(audio.loudness_spikes(samples, baseline=-30.0), [])

    def test_no_spike_on_flat_signal(self):
        samples = [audio.LoudnessSample(t * 0.1, -25.0) for t in range(50)]
        self.assertEqual(audio.loudness_spikes(samples, baseline=None), [])

    def test_spike_dedup_within_two_seconds(self):
        samples = ([audio.LoudnessSample(t * 0.1, -30.0) for t in range(30)]
                   + [audio.LoudnessSample(3.0 + i * 0.1, -15.0) for i in range(15)])
        spikes = audio.loudness_spikes(samples, baseline=-30.0)
        self.assertEqual(len(spikes), 1)


class DetectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "nova.db")
        conn = connect(self.db)
        migrate(conn)
        conn.execute("INSERT INTO twitch_sources (username, created_at, updated_at)"
                     " VALUES ('synthetic', 0, 0)")
        conn.execute("INSERT INTO stream_sessions (source_id, state)"
                     " VALUES (1, 'RECORDING')")
        conn.commit()
        conn.close()
        self.buf = RollingBuffer(self.tmp.name, session_id=1, segment_seconds=5,
                                 pre_event_seconds=15, post_event_seconds=15,
                                 max_storage_mb=64)

    def tearDown(self):
        self.tmp.cleanup()

    def _make_segments(self, count, start=0):
        for i in range(start, start + count):
            p = os.path.join(str(self.buf.dir), f"seg-{i:06d}.ts")
            with open(p, "wb") as fh:
                fh.write(b"\0" * 1024)
            os.utime(p, (time.time() - (count - i), time.time() - (count - i)))

    def test_process_new_segments_records_candidates(self):
        self._make_segments(3)
        det = CandidateDetector(
            "ffmpeg", self.buf, self.db,
            audio_fn=lambda ff, p: [audio.LoudnessSample(t * 0.1, -30.0) for t in range(30)]
                                   + [audio.LoudnessSample(3.1, -15.0)],
            scene_fn=lambda ff, p: [(1.0, 0.9)])
        written = det.process_new_segments()
        # newest segment skipped as possibly-active; 2 analyzed x (1 audio + 1 scene)
        self.assertEqual(written, 4)
        conn = connect(self.db)
        kinds = sorted(r[0] for r in conn.execute(
            "SELECT kind FROM candidate_events").fetchall())
        conn.close()
        self.assertEqual(kinds, ["audio_spike", "audio_spike",
                                 "scene_change", "scene_change"])

    def test_second_run_is_incremental(self):
        self._make_segments(3)
        det = CandidateDetector("ffmpeg", self.buf, self.db,
                                audio_fn=lambda ff, p: [],
                                scene_fn=lambda ff, p: [(0.5, 0.8)])
        self.assertEqual(det.process_new_segments(), 2)
        self.assertEqual(det.process_new_segments(), 0)
        self._make_segments(1, start=3)  # newer segment; previous tail finalizes
        self.assertEqual(det.process_new_segments(), 1)

    def test_analysis_failure_does_not_raise(self):
        self._make_segments(2)
        def boom(ff, p):
            raise RuntimeError("ffmpeg exploded")
        det = CandidateDetector("ffmpeg", self.buf, self.db,
                                audio_fn=boom, scene_fn=boom)
        self.assertEqual(det.process_new_segments(), 0)


if __name__ == "__main__":
    unittest.main()
