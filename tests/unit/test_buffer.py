import tempfile
import time
import unittest
from pathlib import Path

from app.core.database import connect, migrate
from app.recording.buffer import RollingBuffer


def make_segments(buf, count, size=1000):
    buf.dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i in range(count):
        p = buf.dir / f"seg-{i:06d}.ts"
        p.write_bytes(b"x" * size)
        paths.append(p)
    return paths


class RollingBufferTests(unittest.TestCase):
    def test_event_window_covers_pre_and_post(self):
        with tempfile.TemporaryDirectory() as tmp:
            buf = RollingBuffer(tmp, session_id=1, segment_seconds=5,
                                pre_event_seconds=10, post_event_seconds=10)
            now = time.time()
            for i, p in enumerate(make_segments(buf, 10)):
                t = now - (10 - i) * 5
                import os
                os.utime(p, (t, t))
            window = buf.event_window(now - 25)
            self.assertTrue(window)
            self.assertTrue(all(isinstance(p, Path) for p in window))

    def test_storage_cap_removes_oldest(self):
        with tempfile.TemporaryDirectory() as tmp:
            buf = RollingBuffer(tmp, session_id=1, segment_seconds=5,
                                max_storage_mb=1)
            make_segments(buf, 10, size=200_000)  # 2 MB total
            removed = buf.enforce_storage_cap()
            self.assertGreater(removed, 0)
            remaining = sum(p.stat().st_size for p in buf.dir.glob("seg-*.ts"))
            self.assertLessEqual(remaining, 1 * 1024 * 1024)

    def test_reconcile_inserts_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "t.db")
            conn = connect(db)
            self.addCleanup(conn.close)
            migrate(conn)
            conn.execute(
                "INSERT INTO twitch_sources (username, created_at, updated_at)"
                " VALUES ('chan', 0, 0)")
            conn.execute(
                "INSERT INTO stream_sessions (source_id, state)"
                " VALUES (1, 'RECORDING')")
            buf = RollingBuffer(tmp, session_id=1)
            make_segments(buf, 3)
            # only CLOSED segments get rows: the open tail's values are
            # not final, so it is never persisted while still growing
            self.assertEqual(buf.reconcile(db), 2)
            self.assertEqual(buf.reconcile(db), 0)  # idempotent
            buf.mark_drained()                      # tail closes
            self.assertEqual(buf.reconcile(db), 1)
            self.assertEqual(buf.reconcile(db), 0)


if __name__ == "__main__":
    unittest.main()
