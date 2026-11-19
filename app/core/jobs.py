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

    def fail(self, job_id: int, error: str) -> None:
        row = self.conn.execute(
            "SELECT attempts, max_attempts FROM job_queue WHERE id = ?",
            (job_id,)).fetchone()
        if row and row["attempts"] < row["max_attempts"]:
            backoff = min(60, 2 ** row["attempts"])
            with self.conn:
                self.conn.execute(
                    "UPDATE job_queue SET state = 'PENDING', run_after = ?,"
                    " locked_by = NULL, locked_at = NULL, last_error = ?,"
                    " updated_at = ? WHERE id = ?",
                    (time.time() + backoff, error[:2000], time.time(), job_id))
        else:
            self._finish(job_id, "FAILED", error)

    def _finish(self, job_id: int, state: str, error: Optional[str]) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE job_queue SET state = ?, locked_by = NULL, locked_at = NULL,"
                " last_error = ?, updated_at = ? WHERE id = ?",
                (state, error[:2000] if error else None, time.time(), job_id))

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
