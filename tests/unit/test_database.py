import tempfile
import unittest
from pathlib import Path

from app.core.database import MIGRATIONS, connect, migrate


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "t.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_migrate_creates_schema_and_is_idempotent(self):
        conn = connect(self.db)
        first = migrate(conn)
        self.assertEqual(first, [m[0] for m in MIGRATIONS])
        second = migrate(conn)
        self.assertEqual(second, [])
        tables = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        for expected in ("twitch_sources", "stream_sessions", "recording_segments",
                         "candidate_events", "ai_evaluations", "clips",
                         "distribution_destinations", "distribution_sessions",
                         "settings", "job_queue", "error_logs",
                         "schema_migrations"):
            self.assertIn(expected, tables)

    def test_foreign_keys_enforced(self):
        conn = connect(self.db)
        migrate(conn)
        with self.assertRaises(Exception):
            with conn:
                conn.execute(
                    "INSERT INTO stream_sessions (source_id, state)"
                    " VALUES (999, 'PENDING')")


if __name__ == "__main__":
    unittest.main()
