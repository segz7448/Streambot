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
        produced = self._clipper(lambda *a, **k: Path("/tmp/clip.mp4"))\
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
        produced = self._clipper(lambda *a, **k: Path("/tmp/clip.mp4"))\
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
        clipper = self._clipper(lambda *a, **k: Path("/tmp/clip.mp4"))
        self.assertEqual(clipper.process_pending(), 1)
        self.assertEqual(clipper.process_pending(), 0)

    def test_quota_exhaustion_queues_jobs_no_silent_substitution(self):
        from app.intelligence.providers import QuotaExhausted

        class BrokeProvider:
            name = "broke"

            def score_moments(self, *a, **k):
                raise QuotaExhausted("429")

        clipper = AutoClipper(FakeBuffer(), self.db, BrokeProvider(),
                              "ffmpeg", self.td.name, min_score=0.6,
                              extract_fn=lambda *a, **k: Path("/tmp/c.mp4"))
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
