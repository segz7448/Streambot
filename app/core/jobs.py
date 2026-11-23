"""Database-backed job queue.

Workers claim jobs atomically, so multiple processes never run the same job
twice. States follow the lifecycle in the design brief; a crashed worker's
jobs are recovered by ``recover_interrupted``. Media payloads live on the
filesystem; payloads here are small JSON references.
"""
from __future__ import annotations

import json
import sqlite3
import time
from typing import Optional

TERMINAL_STATES = ("COMPLETED", "FAILED", "CANCELLED")


class JobQueue:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def enqueue(self, kind: str, payload: dict, state: str = "PENDING",
                max_attempts: int = 3, run_after: float = 0.0) -> int:
        now = time.time()
        cur = self.conn.execute(
            "INSERT INTO job_queue (kind, payload_json, state, max_attempts,"
            " run_after, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (kind, json.dumps(payload), state, max_attempts, run_after, now, now))
        return int(cur.lastrowid)

    def claim(self, worker: str, kinds: Optional[list[str]] = None) -> Optional[sqlite3.Row]:
        now = time.time()
        sql = ("SELECT id FROM job_queue WHERE state = 'PENDING' AND run_after <= ?")
        params: list[object] = [now]
        if kinds:
            sql += f" AND kind IN ({','.join('?' for _ in kinds)})"
            params.extend(kinds)
        sql += " ORDER BY id LIMIT 1"
        with self.conn:
            row = self.conn.execute(sql, params).fetchone()
            if row is None:
                return None
            cur = self.conn.execute(
                "UPDATE job_queue SET state = 'ANALYZING', locked_by = ?,"
                " locked_at = ?, attempts = attempts + 1, updated_at = ?"
                " WHERE id = ? AND state = 'PENDING'",
                (worker, now, now, row["id"]))
            if cur.rowcount != 1:
                return None
        return self.conn.execute(
            "SELECT * FROM job_queue WHERE id = ?", (row["id"],)).fetchone()

    def complete(self, job_id: int) -> None:
        self._finish(job_id, "COMPLETED", None)

    def _ownership_guard(self, expected_worker, expected_attempts,
                         sql: str, params: list) -> tuple[str, list]:
        """Fence a mutation to the CAPTURED claim identity: the active
        ANALYZING state plus the attempt count (and worker) read from
        the job row at claim time. A stale callback from a recovered
        and re-claimed attempt then matches nothing. Only callers
        opting into ownership get the guard; legacy callers keep the
        historical unguarded write."""
        if expected_attempts is None and expected_worker is None:
            return sql, params
        sql += " AND state = 'ANALYZING'"
        if expected_attempts is not None:
            sql += " AND attempts = ?"
            params.append(expected_attempts)
        if expected_worker is not None:
            sql += " AND locked_by = ?"
            params.append(expected_worker)
        return sql, params

    def fail(self, job_id: int, error: str, *,
             terminal_notice: Optional[str] = None,
             expected_worker: Optional[str] = None,
             expected_attempts: Optional[int] = None) -> None:
        """terminal_notice is used only when THIS failure exhausts the
        job's attempts: the owner notice then commits in the same
        transaction as the FAILED flip (outbox-idempotent, exactly one
        row per terminal outcome). Bounded retries stay silent. When
        expected_worker/expected_attempts carry the captured claim
        identity, a duplicate, stale or missing call mutates nothing
        and never notifies."""
        row = self.conn.execute(
            "SELECT attempts, max_attempts FROM job_queue WHERE id = ?",
            (job_id,)).fetchone()
        if row and row["attempts"] < row["max_attempts"]:
            backoff = min(60, 2 ** row["attempts"])
            sql, params = self._ownership_guard(
                expected_worker, expected_attempts,
                "UPDATE job_queue SET state = 'PENDING', run_after = ?,"
                " locked_by = NULL, locked_at = NULL, last_error = ?,"
                " updated_at = ? WHERE id = ?",
                [time.time() + backoff, error[:2000], time.time(), job_id])
            with self.conn:
                self.conn.execute(sql, params)
        else:
            self._finish(job_id, "FAILED", error, notice=terminal_notice,
                         expected_worker=expected_worker,
                         expected_attempts=expected_attempts)

    def _finish(self, job_id: int, state: str, error: Optional[str],
                *, notice: Optional[str] = None,
                expected_worker: Optional[str] = None,
                expected_attempts: Optional[int] = None) -> None:
        if notice is not None:
            from . import schedule
            self.conn.executescript(schedule.DDL)
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                sql, params = self._ownership_guard(
                    expected_worker, expected_attempts,
                    "UPDATE job_queue SET state = ?, locked_by = NULL,"
                    " locked_at = NULL, last_error = ?, updated_at = ?"
                    " WHERE id = ?",
                    [state, error[:2000] if error else None, time.time(),
                     job_id])
                if expected_attempts is None and expected_worker is None:
                    # notice callers always opt into ownership of the
                    # active claim; keep a minimal active-state guard
                    sql += " AND state = 'ANALYZING'"
                cur = self.conn.execute(sql, params)
                if cur.rowcount == 1:
                    schedule.notice_on_conn(self.conn, notice)
                # rowcount 0: duplicate/stale/missing - no state
                # overwrite, no notice.
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
            self.conn.execute("COMMIT")
            return
        sql, params = self._ownership_guard(
            expected_worker, expected_attempts,
            "UPDATE job_queue SET state = ?, locked_by = NULL, locked_at = NULL,"
            " last_error = ?, updated_at = ? WHERE id = ?",
            [state, error[:2000] if error else None, time.time(), job_id])
        with self.conn:
            self.conn.execute(sql, params)

    def recover_interrupted(self) -> int:
        """Requeue jobs whose worker died. Called once at startup before any
        worker starts, so a recovered process never duplicates running work."""
        with self.conn:
            cur = self.conn.execute(
                "UPDATE job_queue SET state = 'PENDING', locked_by = NULL,"
                " locked_at = NULL, updated_at = ?"
                " WHERE state NOT IN ('PENDING', 'COMPLETED', 'FAILED', 'CANCELLED')",
                (time.time(),))
            return cur.rowcount

    def counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT state, COUNT(*) AS n FROM job_queue GROUP BY state").fetchall()
        return {row["state"]: row["n"] for row in rows}
