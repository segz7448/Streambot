import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app.clipping.auto import AutoClipper
from app.core.database import connect, migrate
from app.intelligence.providers import HeuristicProvider


class FakeSegment:
    def __init__(self, seq, created):
        self.sequence = seq
        self.created_at = created
        self.path = Path(f"/tmp/seg-{seq}.ts")


class FakeBuffer:
    def __init__(self):
        self.session_id = 1
        self.segment_seconds = 5.0
        self.session_seconds = 60.0


def _seed_event(db, detected_at, kind, conf):
    conn = connect(db)
    conn.execute("INSERT INTO twitch_sources (id, username, created_at,"
                 " updated_at) VALUES (1, 'chan', 0, 0)")
    conn.execute("INSERT INTO stream_sessions (id, source_id, started_at,"
                 " state) VALUES (1, 1, 0, 'RECORDING')")
    conn.execute("INSERT INTO candidate_events"
                 " (session_id, detected_at, kind, confidence)"
                 " VALUES (1,?,?,?)", (detected_at, kind, conf))
    conn.commit()
    conn.close()


def _realish_extract(*a, **k):
    out = Path(a[3])
    out.write_bytes(b"\x00" * 256)
    return out


class AutoClipTests(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.db = str(Path(self.td.name) / "t.db")
        conn = connect(self.db)
        migrate(conn)
        conn.close()
        _seed_event(self.db, 100.0, "audio_spike", 0.9)

    def tearDown(self):
        self.td.cleanup()

    def _clipper(self, extract_fn):
        return AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                           "ffmpeg", self.td.name, min_score=0.6,
                           extract_fn=extract_fn)

    def test_extracts_above_threshold(self):
        produced = self._clipper(_realish_extract)\
            .process_pending()
        self.assertEqual(produced, 1)
        conn = connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT status FROM candidate_events").fetchone()[0], "CLIPPED")
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM clips").fetchone()[0], 1)
        conn.close()

    def test_rejects_below_threshold(self):
        conn = connect(self.db)
        conn.execute("UPDATE candidate_events SET confidence=0.1")
        conn.commit()
        conn.close()
        produced = self._clipper(_realish_extract)\
            .process_pending()
        self.assertEqual(produced, 0)
        conn = connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT status FROM candidate_events").fetchone()[0], "REJECTED")
        conn.close()

    def test_extraction_failure_marks_failed_not_dropped(self):
        def boom(*a, **k):
            raise RuntimeError("ffmpeg died")
        produced = self._clipper(boom).process_pending()
        self.assertEqual(produced, 0)
        conn = connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT status FROM candidate_events").fetchone()[0], "FAILED")
        conn.close()

    def test_rotated_window_marks_expired(self):
        produced = self._clipper(lambda *a, **k: None).process_pending()
        self.assertEqual(produced, 0)
        conn = connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT status FROM candidate_events").fetchone()[0], "EXPIRED")
        conn.close()

    def test_processed_once(self):
        clipper = self._clipper(_realish_extract)
        self.assertEqual(clipper.process_pending(), 1)
        self.assertEqual(clipper.process_pending(), 0)

    def _add_event(self, detected_at, kind, conf):
        conn = connect(self.db)
        conn.execute("INSERT INTO candidate_events"
                     " (session_id, detected_at, kind, confidence)"
                     " VALUES (1,?,?,?)", (detected_at, kind, conf))
        conn.commit()
        conn.close()

    def test_cross_kind_same_moment_merges_to_one_clip(self):
        # an audio_spike and a scene_change fired by one moment must not
        # each produce a clip of the same content; the higher-scored event
        # wins and the other is marked MERGED, never silently dropped.
        self._add_event(101.5, "scene_change", 0.8)
        produced = self._clipper(_realish_extract)\
            .process_pending()
        self.assertEqual(produced, 1)
        conn = connect(self.db)
        statuses = sorted(r[0] for r in conn.execute(
            "SELECT status FROM candidate_events ORDER BY detected_at"))
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM clips").fetchone()[0], 1)
        conn.close()
        self.assertEqual(statuses, ["CLIPPED", "MERGED"])

    def test_distinct_moments_not_merged(self):
        self._add_event(140.0, "audio_spike", 0.9)
        produced = self._clipper(_realish_extract)\
            .process_pending()
        self.assertEqual(produced, 2)
        conn = connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM clips").fetchone()[0], 2)
        conn.close()

    def test_quota_exhaustion_queues_jobs_no_silent_substitution(self):
        from app.intelligence.providers import QuotaExhausted

        class BrokeProvider:
            name = "broke"

            def score_moments(self, *a, **k):
                raise QuotaExhausted("429")

        clipper = AutoClipper(FakeBuffer(), self.db, BrokeProvider(),
                              "ffmpeg", self.td.name, min_score=0.6,
                              extract_fn=_realish_extract)
        self.assertEqual(clipper.process_pending(), 0)
        conn = connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT status FROM candidate_events").fetchone()[0], "QUEUED")
        # nothing extracted, nothing evicted
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM clips").fetchone()[0], 0)
        conn.close()
        # queued jobs are retried once the provider recovers
        clipper.provider = HeuristicProvider()
        self.assertEqual(clipper.process_pending(), 1)

    def test_crash_after_extract_before_commit_no_duplicate_rows(self):
        # Simulate death between a successful extraction and the DB
        # commit: first pass raises after writing the file. The retried
        # pass re-extracts (same deterministic path, idempotent) and the
        # unique index + INSERT OR IGNORE keep exactly one clips row.
        calls = {"n": 0}

        def flaky(ffmpeg, buffer, ts, out, **kw):
            calls["n"] += 1
            Path(out).write_text("clip-bytes")
            if calls["n"] == 1:
                raise SystemExit("simulated crash after file write")
            return Path(out)

        clipper = self._clipper(flaky)
        # first pass dies mid-flight (uncaught crash propagates)
        self.assertRaises(SystemExit, clipper.process_pending)
        conn = connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM clips").fetchone()[0], 0)
        self.assertEqual(conn.execute(
            "SELECT status FROM candidate_events").fetchone()[0],
            "PENDING")
        conn.close()
        # retry after restart: file overwritten in place, one row only
        self.assertEqual(clipper.process_pending(), 1)
        conn = connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM clips").fetchone()[0], 1)
        conn.close()
        # and a further pass is a no-op (terminal-state dedup)
        self.assertEqual(clipper.process_pending(), 0)


if __name__ == "__main__":
    unittest.main()


class SealEvidenceTests(unittest.TestCase):
    """Producer sealing: unique never-reused raw paths and durable
    digest/size evidence committed WITH the PENDING row."""

    def setUp(self):
        self.td = TemporaryDirectory()
        self.db = str(Path(self.td.name) / "t.db")
        conn = connect(self.db)
        migrate(conn)
        conn.close()
        _seed_event(self.db, 100.0, "audio_spike", 0.9)

    def tearDown(self):
        self.td.cleanup()

    def test_pending_row_carries_seal_evidence(self):
        produced = AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                               "ffmpeg", self.td.name, min_score=0.6,
                               extract_fn=_realish_extract).process_pending()
        self.assertEqual(produced, 1)
        conn = connect(self.db)
        row = conn.execute(
            "SELECT file_path, sealed_sha256, sealed_bytes, sealed_at"
            " FROM clips").fetchone()
        conn.close()
        path = Path(row["file_path"])
        self.assertTrue(path.exists())
        import hashlib
        self.assertEqual(row["sealed_sha256"],
                         hashlib.sha256(path.read_bytes()).hexdigest())
        self.assertEqual(row["sealed_bytes"], path.stat().st_size)
        self.assertIsNotNone(row["sealed_at"])

    def test_paths_unique_across_concurrent_producers(self):
        # the deterministic-path flaw: two producers select the SAME
        # PENDING event before either extracts, then extract
        # concurrently - unique paths keep them from clobbering the
        # input a finalizer is about to seal
        import threading
        barrier = threading.Barrier(2)
        paths = []
        def grab(*a, **k):
            out = Path(a[3])
            paths.append(out)
            barrier.wait(10)          # both inside extraction at once
            out.write_bytes(b"\x00" * 256)
            return out
        clippers = [
            AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                        "ffmpeg", self.td.name, min_score=0.6,
                        extract_fn=grab)
            for _ in range(2)]
        threads = [threading.Thread(target=c.process_pending)
                   for c in clippers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(paths), 2)
        self.assertNotEqual(paths[0], paths[1])
        # exactly one row wins the event; the winner's file is kept and
        # sealed, the loser's orphan (exclusively its own path) is
        # removed, and produced counts only the winner
        conn = connect(self.db)
        rows = conn.execute("SELECT file_path FROM clips").fetchall()
        conn.close()
        self.assertEqual(len(rows), 1)
        kept = Path(rows[0]["file_path"])
        self.assertIn(kept, paths)
        self.assertTrue(kept.exists())
        removed = [p for p in paths if p != kept]
        self.assertEqual(len(removed), 1)
        self.assertFalse(removed[0].exists())

    def test_unsealable_output_fails_event_not_silent(self):
        # an extractor that creates no file must fail the event, never
        # publish unsealed input
        produced = AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                               "ffmpeg", self.td.name, min_score=0.6,
                               extract_fn=lambda *a, **k: Path(a[3]))            .process_pending()
        self.assertEqual(produced, 0)
        conn = connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT status FROM candidate_events").fetchone()[0],
            "FAILED")
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM clips WHERE sealed_sha256 IS NOT NULL"
        ).fetchone()[0], 0)
        conn.close()


class ClipDurationRecordTests(unittest.TestCase):
    """Regression: clips.duration_seconds must record the real extracted
    window, not the buffer's segment length. The session finish path,
    the planner, the delivery caption and the intake validation all read
    duration_seconds; segment_seconds (5s) understated a ~30s clip."""

    def setUp(self):
        self.td = TemporaryDirectory()
        self.db = str(Path(self.td.name) / "t.db")
        conn = connect(self.db)
        migrate(conn)
        conn.close()
        _seed_event(self.db, 100.0, "audio_spike", 0.9)

    def tearDown(self):
        self.td.cleanup()

    def _duration(self):
        conn = connect(self.db)
        row = conn.execute("SELECT duration_seconds FROM clips").fetchone()
        conn.close()
        return row[0]

    def test_real_extractor_duration_recorded(self):
        clipper = AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                              "ffmpeg", self.td.name, min_score=0.6,
                              extract_fn=lambda *a, **k: (_realish_extract(*a, **k),
                                                          27.5))
        self.assertEqual(clipper.process_pending(), 1)
        self.assertEqual(self._duration(), 27.5)

    def test_legacy_path_extractor_records_window_estimate(self):
        # an extractor returning only a path (test fakes, older callers):
        # the intended pre+post window is the estimate, never 5s segments
        clipper = AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                              "ffmpeg", self.td.name, min_score=0.6,
                              extract_fn=_realish_extract)
        self.assertEqual(clipper.process_pending(), 1)
        self.assertEqual(self._duration(), 30.0)

    def test_real_plan_duration_matches_layout(self):
        # end-to-end through the real plan_clip: an event mid-buffer with
        # the default 20s pre / 10s post window plans a ~30s duration
        from app.clipping.extractor import plan_clip

        class Buf(FakeBuffer):
            def segments(self):
                return [FakeSegment(i, 40.0 + (i + 1) * 5.0)
                        for i in range(20)]

            def event_window(self, event_time):
                return [s.path for s in self.segments()]

        plan = plan_clip(Buf(), 100.0, 20.0, 10.0)
        self.assertIsNotNone(plan)
        self.assertAlmostEqual(plan.duration, 30.0, delta=5.0)
