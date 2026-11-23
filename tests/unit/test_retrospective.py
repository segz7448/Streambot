"""Retrospective (mode C) pipeline unit tests with injected stage fakes."""
import json
import tempfile
import unittest
from pathlib import Path

from app.clipping import retrospective
from app.detection.audio import LoudnessSample
from app.intelligence.memory import Memory


def _samples(n=100, spike_t=5.0, spike_len=3.0):
    out = []
    for i in range(n):
        t = i * 0.1
        m = -14.0 if spike_t <= t <= spike_t + spike_len else -45.0
        out.append(LoudnessSample(t, m))
    return out


class ContentWindowTests(unittest.TestCase):
    def test_window_sized_from_content_not_padded(self):
        start, dur = retrospective.content_window(_samples(), 5.0,
                                                  pre_roll=2.0)
        self.assertEqual(start, 3.0)
        self.assertLessEqual(dur, 8.0)   # ~3s moment + 2s pre + 2s tail
        self.assertGreaterEqual(dur, 5.0)

    def test_min_duration_floor(self):
        start, dur = retrospective.content_window(
            [LoudnessSample(0.0, -45.0), LoudnessSample(0.1, -14.0),
             LoudnessSample(0.2, -45.0)], 0.1, pre_roll=0.0)
        self.assertGreaterEqual(dur, retrospective.MIN_CLIP_SECONDS)


class ProcessRecordingTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        (self.dir / "src.mp4").write_bytes(b"fake")
        self.mem = Memory(str(self.dir / "memory.json"))

    def _run(self, **over):
        args = dict(
            ffmpeg="ff", ffprobe="fp", src_path=str(self.dir / "src.mp4"),
            work_dir=str(self.dir / "work"), db_path=str(self.dir / "n.db"),
            memory=self.mem,
            audio_fn=lambda ff, p: _samples(),
            scene_fn=lambda ff, p: [],
            extract_fn=lambda ff, s, o, st, d: Path(o).write_bytes(b"x" * 20000) or Path(o),
            edit_fn=lambda ff, s, o, preset, sub, fx, rt, watermark=None, framing=None: Path(o).write_bytes(b"y" * 20000) or Path(o),
            qc_fn=lambda fp, ff, p, a, b, c, d: type("QC", (), {"ok": True, "failures": []})(),
            validate_fn=lambda fp, p: {"duration": 60.0, "width": 320,
                                       "height": 240},
        )
        args.update(over)
        return retrospective.process_recording(**args)

    def test_full_chain_records_clip_and_memory(self):
        result = self._run()
        self.assertEqual(result["status"], "held")
        self.assertEqual(result["event_kind"], "audio_spike")
        self.assertTrue(result["extracted"] and result["edited"])
        self.assertTrue(result["qc_passed"])
        # duration from content window, not the 180s cap, not padded
        self.assertLess(result["plan"]["target_seconds"], 60)
        self.assertTrue(any("not padded" in r or "content window" in r
                            for r in result["plan"]["reasons"]))
        # DB rows visible to /clips
        import sqlite3
        conn = sqlite3.connect(str(self.dir / "n.db"))
        clip = conn.execute("SELECT status, preset FROM clips").fetchone()
        self.assertEqual(clip[0], "HELD")
        self.assertEqual(clip[1], "vertical")
        session = conn.execute(
            "SELECT ingest_mode FROM stream_sessions").fetchone()
        self.assertEqual(session[0], "upload")
        conn.close()
        # memory recorded with provenance
        clips = self.mem.recall("clip")
        self.assertEqual(len(clips), 1)
        self.assertEqual(clips[0]["confidence"], "observed")

    def test_owner_duration_preference_overrides(self):
        self.mem.remember("preference", "shorts_target_seconds", "45",
                          "owner", confidence="stated")
        result = self._run()
        self.assertEqual(result["plan"]["target_seconds"], 45)

    def test_no_candidates_fabricates_nothing(self):
        result = self._run(audio_fn=lambda ff, p: [
            LoudnessSample(i * 0.1, -45.0) for i in range(100)])
        self.assertEqual(result["status"], "no_candidates")
        self.assertNotIn("clip_id", result)

    def test_qc_failure_not_recorded_as_clip(self):
        bad = type("QC", (), {"ok": False, "failures": ["silent audio"]})()
        result = self._run(qc_fn=lambda *a: bad)
        self.assertEqual(result["status"], "qc_failed")
        import sqlite3
        conn = sqlite3.connect(str(self.dir / "n.db"))
        self.addCleanup(conn.close)
        try:
            rows = conn.execute("SELECT count(*) FROM clips").fetchone()
        except sqlite3.Error:
            rows = (0,)
        self.assertEqual(rows[0], 0)

    def test_planned_duration_and_framing_reach_editor(self):
        self.mem.remember("preference", "shorts_target_seconds", "180", "owner")
        seen = {}
        def edit(ff, src, dst, preset, *a, **kw):
            seen["limit"] = preset.max_duration_seconds
            seen["framing"] = kw["framing"]
            Path(dst).write_bytes(b"y" * 20000)
        result = self._run(edit_fn=edit)
        self.assertEqual(seen, {"limit": 180, "framing": "full"})
        self.assertEqual(result["plan"]["framing"], "full")

    def test_watermark_from_memory_passed_to_editor(self):
        self.mem.remember("preference", "watermark", "@streamer", "owner",
                          confidence="stated")
        seen = {}

        def edit(ff, s, o, preset, sub, fx, rt, watermark=None, framing=None):
            seen["watermark"] = watermark
            Path(o).write_bytes(b"y" * 20000)
            return Path(o)
        result = self._run(edit_fn=edit)
        self.assertEqual(seen["watermark"], "@streamer")
        self.assertEqual(result["plan"]["watermark"], "@streamer")


if __name__ == "__main__":
    unittest.main()


class RetrospectiveAdmissionEnqueueTests(unittest.TestCase):
    """A retrospective HELD clip must reach upload eligibility only via
    the genuine durable admission path: one clip_intake_admit job whose
    consumer performs the real ClipIntakeAuthority verdict."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        (self.dir / "src.mp4").write_bytes(b"fake")
        self.mem = Memory(str(self.dir / "memory.json"))

    def test_held_clip_enqueues_real_admission_job(self):
        import json as _json
        import sqlite3 as _sq
        result = retrospective.process_recording(
            ffmpeg="ff", ffprobe="fp", src_path=str(self.dir / "src.mp4"),
            work_dir=str(self.dir / "work"), db_path=str(self.dir / "n.db"),
            memory=self.mem,
            audio_fn=lambda ff, p: _samples(),
            scene_fn=lambda ff, p: [],
            extract_fn=lambda ff, s, o, st, d: Path(o).write_bytes(b"x" * 20000) or Path(o),
            edit_fn=lambda ff, s, o, preset, sub, fx, rt, watermark=None, framing=None: Path(o).write_bytes(b"y" * 20000) or Path(o),
            qc_fn=lambda fp, ff, p, a, b, c, d: type("QC", (), {"ok": True, "failures": []})(),
            validate_fn=lambda fp, p: {"duration": 60.0, "width": 320,
                                       "height": 240})
        self.assertEqual(result["status"], "held")
        conn = _sq.connect(str(self.dir / "n.db"))
        jobs = conn.execute(
            "SELECT kind, payload_json, state FROM job_queue").fetchall()
        conn.close()
        admits = [j for j in jobs if j[0] == "clip_intake_admit"]
        self.assertEqual(len(admits), 1)
        payload = _json.loads(admits[0][1])
        self.assertEqual(payload["clip_row_id"], result["clip_id"])
        self.assertEqual(admits[0][2], "PENDING")


class GeneratedAdmissionCycleTests(unittest.TestCase):
    """Generated completion -> real admission cycle -> bound handoff:
    the first ACCEPTED verdict comes from handoff.run_once reading
    stored pipeline metadata, never a direct admit or registry edit."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        (self.dir / "src.mp4").write_bytes(b"fake")
        self.mem = Memory(str(self.dir / "memory.json"))

    def _run(self, **over):
        args = dict(
            ffmpeg="ff", ffprobe="fp", src_path=str(self.dir / "src.mp4"),
            work_dir=str(self.dir / "work"),
            db_path=str(self.dir / "nova.db"), memory=self.mem,
            source_label="synthetic-fixture",
            audio_fn=lambda ff, p: _samples(),
            scene_fn=lambda ff, p: [],
            extract_fn=lambda ff, s, o, st, d: Path(o).write_bytes(b"x" * 20000) or Path(o),
            edit_fn=lambda ff, s, o, preset, sub, fx, rt, watermark=None, framing=None: Path(o).write_bytes(b"y" * 20000) or Path(o),
            qc_fn=lambda fp, ff, p, a, b, c, d: type("QC", (), {"ok": True, "failures": []})(),
            validate_fn=lambda fp, p: {"duration": 60.0, "width": 320,
                                       "height": 240},
        )
        args.update(over)
        return retrospective.process_recording(**args)

    def test_first_acceptance_comes_from_consumer_with_stored_title(self):
        import sqlite3 as _sq
        from app.bot import handoff
        result = self._run()
        self.assertEqual(result["status"], "held")
        status = handoff.run_once(
            self.dir,
            probe_fn=lambda p: {"format": {"duration": "30.0"},
                                "streams": []})
        self.assertEqual(status, handoff.PROCESSED)
        conn = _sq.connect(str(self.dir / "nova.db"))
        reg = conn.execute(
            "SELECT status, title, streamer, handoff_job_id"
            " FROM clip_intake").fetchone()
        jobs = conn.execute(
            "SELECT kind, state FROM job_queue ORDER BY id").fetchall()
        conn.close()
        self.assertEqual(reg[0], "ACCEPTED")
        self.assertIn("synthetic-fixture", reg[1])  # synthetic labeled
        self.assertEqual(reg[2], "synthetic-fixture")
        self.assertIsNotNone(reg[3])  # bound handoff job id
        kinds = {k: s for k, s in jobs}
        self.assertEqual(kinds.get("clip_intake_admit"), "COMPLETED")
        self.assertIn("clip_intake_handoff", kinds)

    def test_admission_enqueue_failure_rolls_back_held_clip(self):
        import sqlite3 as _sq
        from app.core import jobs as jobs_mod
        orig = jobs_mod.JobQueue.enqueue

        def boom(self, *a, **k):
            raise RuntimeError("simulated outbox write failure")
        jobs_mod.JobQueue.enqueue = boom
        try:
            with self.assertRaises(RuntimeError):
                self._run()
        finally:
            jobs_mod.JobQueue.enqueue = orig
        conn = _sq.connect(str(self.dir / "nova.db"))
        clips = conn.execute("SELECT COUNT(*) FROM clips").fetchone()[0]
        jobs_n = conn.execute(
            "SELECT COUNT(*) FROM job_queue").fetchone()[0]
        conn.close()
        # atomic: no stranded HELD clip and no orphan admission job
        self.assertEqual(clips, 0)
        self.assertEqual(jobs_n, 0)
