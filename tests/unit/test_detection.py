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

    def test_sustained_episode_merges_to_one_spike(self):
        # regression for the on-phone defect: a 4-5s loud burst produced 3
        # overlapping clips (rising edges at 0.4/2.4/4.5s). One episode,
        # one spike, reported at the episode peak.
        quiet = [audio.LoudnessSample(t * 0.5, -30.0) for t in range(6)]
        loud = [audio.LoudnessSample(3.0 + i * 0.5, v) for i, v in
                enumerate([-18.0, -15.0, -12.0, -14.0, -16.0, -19.0, -22.0])]
        tail = [audio.LoudnessSample(6.5 + i * 0.5, -30.0) for i in range(4)]
        spikes = audio.loudness_spikes(quiet + loud + tail, baseline=-30.0)
        self.assertEqual(len(spikes), 1)
        self.assertAlmostEqual(spikes[0][0], 3.0)
        self.assertAlmostEqual(spikes[0][1], -12.0)

    def test_distinct_episodes_stay_separate(self):
        quiet = [audio.LoudnessSample(t * 0.5, -30.0) for t in range(6)]
        burst1 = [audio.LoudnessSample(3.0 + i * 0.5, -15.0) for i in range(4)]
        gap = [audio.LoudnessSample(5.0 + i * 0.5, -30.0) for i in range(6)]
        burst2 = [audio.LoudnessSample(8.0 + i * 0.5, -14.0) for i in range(4)]
        spikes = audio.loudness_spikes(
            quiet + burst1 + gap + burst2, baseline=-30.0)
        self.assertEqual(len(spikes), 2)
        self.assertAlmostEqual(spikes[0][0], 3.0)
        self.assertAlmostEqual(spikes[1][0], 8.0)

    def test_silence_ends_episode(self):
        samples = ([audio.LoudnessSample(t * 0.5, -30.0) for t in range(6)]
                   + [audio.LoudnessSample(3.0, -15.0)]
                   + [audio.LoudnessSample(3.5, -70.0)]
                   + [audio.LoudnessSample(4.0 + i * 0.5, -30.0) for i in range(3)]
                   + [audio.LoudnessSample(5.5, -15.0)])
        spikes = audio.loudness_spikes(samples, baseline=-30.0)
        self.assertEqual(len(spikes), 2)


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

    def test_sustained_episode_writes_one_event(self):
        # regression for the on-phone defect: one sustained loud burst in a
        # segment must record ONE audio_spike candidate, not one per rising
        # edge, so the clipper produces one clip per moment.
        self._make_segments(3)
        burst = ([audio.LoudnessSample(t * 0.5, -30.0) for t in range(6)]
                 + [audio.LoudnessSample(3.0 + i * 0.5, v) for i, v in
                    enumerate([-18.0, -15.0, -12.0, -14.0, -16.0, -19.0])]
                 + [audio.LoudnessSample(6.0 + i * 0.5, -30.0) for i in range(4)])
        det = CandidateDetector("ffmpeg", self.buf, self.db,
                                audio_fn=lambda ff, p: burst,
                                scene_fn=lambda ff, p: [])
        written = det.process_new_segments()
        self.assertEqual(written, 2)  # 2 finalized segments x 1 merged spike
        conn = connect(self.db)
        rows = conn.execute(
            "SELECT kind, confidence FROM candidate_events"
            " WHERE kind='audio_spike'").fetchall()
        conn.close()
        self.assertEqual(len(rows), 2)
        # confidence from the episode peak (-12 LUFS), not the first edge
        self.assertAlmostEqual(rows[0][1], min(1.0, (-12.0 + 60) / 60))

    def test_episode_spanning_segment_boundary_records_once(self):
        # reviewer repro: quiet baseline, loud tail of segment A continuing
        # at the start of segment B must yield ONE candidate, not two.
        self._make_segments(3)

        def audio_fn(ff, p):
            if "seg-000000" in str(p):
                return ([audio.LoudnessSample(t * 0.5, -30.0) for t in range(8)]
                        + [audio.LoudnessSample(4.0 + i * 0.5, -15.0)
                           for i in range(4)])  # loud tail, episode open
            return ([audio.LoudnessSample(i * 0.5, -15.0) for i in range(4)]
                    + [audio.LoudnessSample(2.0 + i * 0.5, -30.0)
                       for i in range(6)])  # continuation then quiet
        det = CandidateDetector("ffmpeg", self.buf, self.db,
                                audio_fn=audio_fn, scene_fn=lambda ff, p: [])
        det.process_new_segments()
        conn = connect(self.db)
        n = conn.execute("SELECT count(*) FROM candidate_events"
                         " WHERE kind='audio_spike'").fetchone()[0]
        ep = conn.execute("SELECT episode_active FROM detection_state"
                          " WHERE session_id=1").fetchone()[0]
        conn.close()
        self.assertEqual(n, 1)
        self.assertEqual(ep, 0)  # episode closed by the quiet tail

    def test_two_bursts_across_boundary_stay_separate(self):
        self._make_segments(3)

        def audio_fn(ff, p):
            if "seg-000000" in str(p):
                return ([audio.LoudnessSample(t * 0.5, -30.0) for t in range(8)]
                        + [audio.LoudnessSample(4.0 + i * 0.5, -15.0)
                           for i in range(3)]
                        + [audio.LoudnessSample(5.5, -30.0)])  # burst ends
            return ([audio.LoudnessSample(i * 0.5, -30.0) for i in range(5)]
                    + [audio.LoudnessSample(2.5 + i * 0.5, -14.0)
                       for i in range(3)]
                    + [audio.LoudnessSample(4.0, -30.0)])
        det = CandidateDetector("ffmpeg", self.buf, self.db,
                                audio_fn=audio_fn, scene_fn=lambda ff, p: [])
        det.process_new_segments()
        conn = connect(self.db)
        n = conn.execute("SELECT count(*) FROM candidate_events"
                         " WHERE kind='audio_spike'").fetchone()[0]
        conn.close()
        self.assertEqual(n, 2)  # genuinely separated moments preserved

    def test_analysis_failure_does_not_raise(self):
        self._make_segments(2)
        def boom(ff, p):
            raise RuntimeError("ffmpeg exploded")
        det = CandidateDetector("ffmpeg", self.buf, self.db,
                                audio_fn=boom, scene_fn=boom)
        self.assertEqual(det.process_new_segments(), 0)


if __name__ == "__main__":
    unittest.main()


class SceneChangeParserTests(unittest.TestCase):
    def test_parse_metadata_output(self):
        from app.detection import visual
        stderr_sample = (
            "[Parsed_metadata_1 @ 0x0] frame:0   pts:0       pts_time:0\n"
            "[Parsed_metadata_1 @ 0x0] lavfi.scene_score=0.000000\n"
            "[Parsed_metadata_1 @ 0x0] frame:25  pts:25000   pts_time:1.0\n"
            "[Parsed_metadata_1 @ 0x0] lavfi.scene_score=0.521865\n")
        import re
        last_t = None
        out = []
        for line in stderr_sample.splitlines():
            f = visual._FRAME.search(line)
            if f:
                last_t = float(f.group(1))
                continue
            s = visual._SCORE.search(line)
            if s and last_t is not None and float(s.group(1)) >= 0.4:
                out.append((last_t, float(s.group(1))))
        self.assertEqual(out, [(1.0, 0.521865)])
