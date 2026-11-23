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

    def _notices(self):
        if not self.conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND"
                " name='schedule_notices'").fetchone():
            return []
        return self.conn.execute(
            "SELECT * FROM schedule_notices ORDER BY id").fetchall()

    def test_terminal_notice_exactly_once_and_guarded(self):
        job_id = self.q.enqueue("clip", {}, max_attempts=1)
        self.q.claim("w")
        self.q.fail(job_id, "boom", terminal_notice="terminal",
                    expected_worker="w")
        row = self.conn.execute("SELECT * FROM job_queue WHERE id=?",
                                (job_id,)).fetchone()
        self.assertEqual(row["state"], "FAILED")
        self.assertEqual(len(self._notices()), 1)
        # duplicate terminal call on the already-FAILED job: no second
        # notice, no state overwrite
        self.q.fail(job_id, "overwrite", terminal_notice="second",
                    expected_worker="w")
        row = self.conn.execute("SELECT * FROM job_queue WHERE id=?",
                                (job_id,)).fetchone()
        self.assertEqual(row["last_error"], "boom")
        self.assertEqual(len(self._notices()), 1)

    def test_terminal_notice_missing_job_no_orphan(self):
        self.q.fail(999999, "ghost", terminal_notice="orphan",
                    expected_worker="w")
        self.assertEqual(self._notices(), [])

    def test_terminal_notice_wrong_worker_no_flip(self):
        job_id = self.q.enqueue("clip", {}, max_attempts=1)
        self.q.claim("real-worker")
        self.q.fail(job_id, "boom", terminal_notice="terminal",
                    expected_worker="other-worker")
        row = self.conn.execute("SELECT * FROM job_queue WHERE id=?",
                                (job_id,)).fetchone()
        self.assertEqual(row["state"], "ANALYZING")
        self.assertEqual(self._notices(), [])

    def _row(self, job_id):
        return self.conn.execute("SELECT * FROM job_queue WHERE id=?",
                                 (job_id,)).fetchone()

    def test_stale_attempt_callback_terminal_no_mutation(self):
        # claim attempt 1, real recovery, re-claim attempt 2 (same
        # worker), then a STALE attempt-1 terminal callback: nothing
        # may change and no notice may be enqueued
        job_id = self.q.enqueue("clip", {}, max_attempts=1)
        self.assertEqual(self.q.claim("w")["attempts"], 1)
        self.q.recover_interrupted()
        self.assertEqual(self.q.claim("w")["attempts"], 2)
        self.q.fail(job_id, "stale old error", terminal_notice="stale",
                    expected_worker="w", expected_attempts=1)
        row = self._row(job_id)
        self.assertEqual(row["state"], "ANALYZING")
        self.assertIsNone(row["last_error"])
        self.assertEqual(self._notices(), [])
        # the LIVE attempt-2 callback still works and notifies once
        self.q.fail(job_id, "real error", terminal_notice="terminal",
                    expected_worker="w", expected_attempts=2)
        row = self._row(job_id)
        self.assertEqual(row["state"], "FAILED")
        self.assertEqual(row["last_error"], "real error")
        self.assertEqual(len(self._notices()), 1)
        self.assertEqual(self._notices()[0]["text"], "terminal")

    def test_stale_attempt_callback_retry_branch_no_mutation(self):
        job_id = self.q.enqueue("clip", {}, max_attempts=3)
        self.assertEqual(self.q.claim("w")["attempts"], 1)
        self.q.recover_interrupted()
        self.assertEqual(self.q.claim("w")["attempts"], 2)
        self.q.fail(job_id, "stale", expected_worker="w",
                    expected_attempts=1)
        row = self._row(job_id)
        self.assertEqual(row["state"], "ANALYZING")  # not re-queued
        self.assertIsNone(row["last_error"])
        # live attempt-2 retry still requeues
        self.q.fail(job_id, "real", expected_worker="w",
                    expected_attempts=2)
        row = self._row(job_id)
        self.assertEqual(row["state"], "PENDING")
        self.assertEqual(row["last_error"], "real")

    def test_stale_attempt_exhausted_no_notice_no_mutation(self):
        # exhausted no-notice finish (contention/defer style): the
        # captured-identity guard applies even without a notice
        job_id = self.q.enqueue("clip", {}, max_attempts=1)
        self.assertEqual(self.q.claim("w")["attempts"], 1)
        self.q.recover_interrupted()
        self.assertEqual(self.q.claim("w")["attempts"], 2)
        self.q.fail(job_id, "stale old error", expected_worker="w",
                    expected_attempts=1)
        row = self._row(job_id)
        self.assertEqual(row["state"], "ANALYZING")
        self.assertIsNone(row["last_error"])
        # live attempt-2 exhaustion flips without any notice
        self.q.fail(job_id, "real error", expected_worker="w",
                    expected_attempts=2)
        row = self._row(job_id)
        self.assertEqual(row["state"], "FAILED")
        self.assertEqual(row["last_error"], "real error")
        self.assertEqual(self._notices(), [])

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
