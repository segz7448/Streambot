import tempfile
import unittest
from pathlib import Path

from app.core.database import connect, migrate
from app.core.jobs import JobQueue


class JobQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = connect(str(Path(self.tmp.name) / "t.db"))
        self.addCleanup(self.conn.close)
        migrate(self.conn)
        self.q = JobQueue(self.conn)

    def tearDown(self):
        self.tmp.cleanup()

    def test_enqueue_claim_complete(self):
        job_id = self.q.enqueue("clip", {"event_id": 1})
        claimed = self.q.claim("worker-1")
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], job_id)
        self.assertIsNone(self.q.claim("worker-2"))
        self.q.complete(job_id)
        self.assertEqual(self.q.counts().get("COMPLETED"), 1)

    def test_fail_retries_then_fails(self):
        job_id = self.q.enqueue("clip", {}, max_attempts=2)
        self.q.claim("w")
        self.q.fail(job_id, "boom")
        row = self.conn.execute("SELECT * FROM job_queue WHERE id=?",
                                (job_id,)).fetchone()
        self.assertEqual(row["state"], "PENDING")
        # simulate the retry backoff having elapsed
        self.conn.execute("UPDATE job_queue SET run_after = 0 WHERE id=?",
                          (job_id,))
        self.q.claim("w")
        self.q.fail(job_id, "boom again")
        row = self.conn.execute("SELECT * FROM job_queue WHERE id=?",
                                (job_id,)).fetchone()
        self.assertEqual(row["state"], "FAILED")

    def test_recover_interrupted_requeues(self):
        job_id = self.q.enqueue("clip", {})
        self.q.claim("dead-worker")
        self.assertEqual(self.q.recover_interrupted(), 1)
        row = self.conn.execute("SELECT * FROM job_queue WHERE id=?",
                                (job_id,)).fetchone()
        self.assertEqual(row["state"], "PENDING")
        self.assertIsNone(row["locked_by"])


if __name__ == "__main__":
    unittest.main()
