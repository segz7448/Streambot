"""Authoritative clip-intake registry: exactly-once idempotency, the daily
admission cap, and the durable handoff, decided in one SQLite transaction.

Rationale: the n8n Cloud draft (2026-11-21) proved that n8n Data Tables
cannot make this decision safely - they have no uniqueness constraint and no
atomic check-and-set, so concurrent duplicate submissions were both
acknowledged and the cap boundary raced. This module keeps the authoritative
decision in the bot's own local database (the same nova.db file that owns
the clips and job_queue tables), which has real transactions: the clip
lookup, existence check, day count, registry write and handoff enqueue
happen inside one BEGIN IMMEDIATE transaction, and SQLite serializes
writers across threads and processes on the host. Concurrent duplicate
admissions produce one registration and idempotent replays, and the cap can
never over-admit. There is no compensating delete and no shared-row hazard:
the reservation and the verdict are the same transaction.

Admission-to-record atomicity: every fresh ACCEPTED decision also inserts
exactly one durable pending record (job_queue kind 'clip_intake_handoff')
in the SAME commit and stores its id on the registry row. A crash after
admission therefore cannot leave an admitted clip without its durable
record, and a replay (accepted=True, replayed=True) returns the EXISTING
handoff_job_id without creating another one. SCOPE: nothing in this repo
consumes that record yet. It is durable pending admission data, not a
functioning upload handoff; a future worker can claim it through the
existing JobQueue atomic-claim path. No upload or delivery is performed
or implied here.

Trusted validation path: admit_clip() reads duration_seconds from the
bot's own clips table inside the decision transaction. That value is
pipeline-recorded (the extraction pipeline stores the segment duration);
it is trusted because it is the bot's own database value rather than
caller-supplied input. It is NOT independent media verification.
admit() (opaque non-bot ids) performs bounded INPUT validation only -
caller-supplied duration_sec is recorded as metadata and is NOT media
verification.

Admission stability: admit_clip() resolves the registry row BEFORE
touching media state, so a prior ACCEPTED verdict is stable even if the
clips row is later deleted or changed. Media-readiness checks (clip
exists, status HELD, non-blank stored file_path, duration in range) gate
only NEW admissions and flips of recorded rejections; they never revoke a
durable prior admission. NOTE: the file_path check validates the stored
path string only; existence of the file on disk is NOT verified.

Legacy contract: rows written by pre-handoff versions (status ACCEPTED
with NULL handoff_job_id) are REFUSED fail-closed on replay: the registry
row is preserved, no new record is minted, and the verdict carries
reason 'legacy_no_handoff'. Recovery of such rows is an explicit operator
decision, out of scope here. Unledgered or partially applied intake
schemas converge safely: v1 DDL is IF NOT EXISTS and the v2 column add
runs only when the column is missing, each version ledgered atomically.

Policy ownership: the daily cap (DAILY_CAP) and the admission day are
decided HERE, at the owning boundary, not by callers. The admission date is
derived from the authority's own clock in its configured timezone
(DEFAULT_TIMEZONE, provisional America/Los_Angeles per the n8n draft;
disclose before live use). Callers cannot split quotas across arbitrary
dates or widen the cap.

Metadata contract: registry rows are immutable once written, except the
REJECTED_CAP -> ACCEPTED flip, which touches only status/intake_date/
daily_count/handoff_job_id/updated_at. Metadata supplied for an
already-recorded clip_id is ignored; the verdict flags this with
metadata_ignored=True so a conflicting retry is visible instead of silently
merged.

Schema isolation and crash safety: the registry uses its own migration
ledger (clip_intake_schema_migrations), never the shared schema_migrations
sequence in app.core.database, and each migration version runs as ONE
explicit BEGIN IMMEDIATE transaction covering the ledger read, the DDL
(applied statement by statement - executescript would commit early) and the
ledger insert. Concurrent cold-start constructors serialize on the write
lock and the loser sees the applied version; a crash mid-version rolls the
whole version back (SQLite DDL is transactional), so a retry always starts
from a ledger-consistent state.

Precondition: the base bot schema (app.core.database.migrate) must already
be applied to the database - the bot does this at startup before serving.
The constructor fails fast if job_queue is missing rather than failing the
first admission later.
"""
from __future__ import annotations

import json
import math
import re
import sqlite3
import time
from datetime import datetime
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from app.core.database import connect

DAILY_CAP = 8
DEFAULT_TIMEZONE = "America/Los_Angeles"  # provisional; disclose before live
HANDOFF_JOB_KIND = "clip_intake_handoff"
RESERVED_CLIP_ID_PREFIX = "cliprow:"  # bot-row namespace used by admit_clip
ADMISSIBLE_CLIP_STATUS = "HELD"  # only QC-held clips are newly admissible
MAX_CLIP_ID_LEN = 128
MAX_TEXT_LEN = 200
MAX_URL_LEN = 2048
MAX_DURATION_SEC = 180.0  # brief's 3-minute clip limit

_DATE_RE = re.compile(r"\A\d{4}-\d{2}-\d{2}\Z")

_V1_DDL = """
CREATE TABLE IF NOT EXISTS clip_intake (
    clip_id TEXT PRIMARY KEY,
    intake_date TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ACCEPTED', 'REJECTED_CAP')),
    title TEXT,
    streamer TEXT,
    video_url TEXT,
    duration_sec REAL,
    daily_count INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_clip_intake_day
    ON clip_intake(intake_date, status);
"""


def _migration_v1(conn: sqlite3.Connection) -> None:
    for stmt in _V1_DDL.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)


def _migration_v2(conn: sqlite3.Connection) -> None:
    # Idempotent: a database left with the column but without the ledger
    # row (killed by an older, non-atomic implementation) converges instead
    # of failing on a duplicate column.
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(clip_intake)")}
    if "handoff_job_id" not in cols:
        conn.execute(
            "ALTER TABLE clip_intake ADD COLUMN handoff_job_id INTEGER")


# Private ledger; version numbers here are independent of database.MIGRATIONS.
# Entries are (version, callable(conn)); a plain SQL string is also accepted
# (split on ';', used by tests exercising failure injection).
_INTAKE_MIGRATIONS = [
    (1, _migration_v1),
    (2, _migration_v2),
]


def _migrate_intake(conn: sqlite3.Connection, migrations) -> None:
    """Apply pending versions, each as one explicit serialized transaction.

    BEGIN IMMEDIATE takes the write lock up front: a concurrent constructor
    blocks on busy_timeout, then re-reads the ledger INSIDE its transaction
    and sees the winner's applied versions. Each version's apply is
    idempotent and runs inside the transaction (never executescript, which
    commits the pending transaction first), so a failure anywhere in a
    version rolls back the whole version, DDL included, and a retry starts
    from a ledger-consistent state.
    """
    for version, apply in sorted(migrations, key=lambda m: m[0]):
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS clip_intake_schema_migrations ("
                "version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)")
            applied = {
                row["version"]
                for row in conn.execute(
                    "SELECT version FROM clip_intake_schema_migrations")
            }
            if version not in applied:
                if callable(apply):
                    apply(conn)
                else:
                    for stmt in str(apply).split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            conn.execute(stmt)
                conn.execute(
                    "INSERT INTO clip_intake_schema_migrations"
                    " (version, applied_at) VALUES (?, ?)",
                    (version, time.time()))
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def _is_real_number(value: object) -> bool:
    # bool is a subclass of int; it is never a valid duration/id.
    return isinstance(value, (int, float)) and not isinstance(value, bool) \
        and math.isfinite(value)


class ClipIntakeAuthority:
    """One authority per database file. Each decision opens its own
    connection, so threads and processes can share the instance safely; the
    database file lock (via BEGIN IMMEDIATE + busy_timeout) is what orders
    concurrent callers.

    timezone: IANA name for the admission day boundary (policy at this
    boundary, provisional default per the n8n draft). _clock and
    _migrations are test seams; production callers never pass them.
    """

    def __init__(self, db_path: str, *, timezone: str = DEFAULT_TIMEZONE,
                 _clock: Optional[Callable[[], float]] = None,
                 _migrations: Optional[list[tuple[int, str]]] = None):
        self.db_path = db_path
        self._tz = ZoneInfo(timezone)  # raises on an unknown timezone
        self._clock = _clock or time.time
        conn = connect(db_path)
        try:
            tables = {r["name"] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if "job_queue" not in tables:
                raise RuntimeError(
                    "ClipIntakeAuthority requires the base bot schema; run"
                    " app.core.database.migrate on this database first")
            _migrate_intake(conn, _migrations or _INTAKE_MIGRATIONS)
        finally:
            conn.close()

    def _today(self) -> str:
        return datetime.fromtimestamp(self._clock(), self._tz).date() \
            .isoformat()

    def _connect(self) -> sqlite3.Connection:
        return connect(self.db_path)

    def admit_clip(self, clip_row_id: int, *,
                   title: Optional[str] = None,
                   streamer: Optional[str] = None) -> dict:
        """Authoritative admission for a clip the bot itself produced.

        Ground truth comes from the clips table row (inside the decision
        transaction): for a NEW admission the clip must exist, be QC-held
        (status HELD) with a non-blank stored file_path and a usable
        duration within the brief's 3-minute limit. The file_path check
        validates the stored path string only; it does NOT verify that the
        file exists on disk. clip_id in the registry
        is 'cliprow:<id>', the pipeline's own identity, so replays of the
        same clip can never double-register. A recorded ACCEPTED verdict is
        stable: it is returned from the registry even if the clips row was
        later deleted or changed (media-readiness gates new decisions, it
        does not revoke durable ones). Validation rejections are not
        recorded - a fixed clip may become admissible later.
        """
        if isinstance(clip_row_id, bool) or not isinstance(clip_row_id, int) \
                or clip_row_id < 1:
            raise ValueError("clip_row_id must be a positive integer")
        for label, value in (("title", title), ("streamer", streamer)):
            if value is not None and (not isinstance(value, str)
                                      or len(value) > MAX_TEXT_LEN):
                raise ValueError(f"{label} too long or not a string")
        clip_id = f"{RESERVED_CLIP_ID_PREFIX}{clip_row_id}"
        intake_date = self._today()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing = conn.execute(
                    "SELECT status FROM clip_intake WHERE clip_id = ?",
                    (clip_id,)).fetchone()
                if existing is not None and existing["status"] == "ACCEPTED":
                    # Stable prior admission: return the recorded verdict
                    # without touching media state, even if the clips row
                    # was since deleted or changed.
                    verdict = self._decide(conn, clip_id, intake_date,
                                           title=title, streamer=streamer,
                                           _now=self._clock())
                    conn.execute("COMMIT")
                    return verdict
                clip = conn.execute(
                    "SELECT duration_seconds, file_path, status FROM clips"
                    " WHERE id = ?", (clip_row_id,)).fetchone()
                count = self._day_count(conn, intake_date)
                if clip is None:
                    conn.execute("COMMIT")
                    return _rejected("unknown_clip", clip_id, intake_date,
                                     count)
                path = clip["file_path"]
                if clip["status"] != ADMISSIBLE_CLIP_STATUS \
                        or not isinstance(path, str) or not path.strip():
                    conn.execute("COMMIT")
                    return _rejected("clip_not_ready", clip_id, intake_date,
                                     count)
                dur = clip["duration_seconds"]
                if not _is_real_number(dur) or not (0 < dur <= MAX_DURATION_SEC):
                    conn.execute("COMMIT")
                    return _rejected("invalid_duration", clip_id,
                                     intake_date, count)
                verdict = self._decide(conn, clip_id, intake_date,
                                       title=title, streamer=streamer,
                                       duration_sec=float(dur),
                                       _now=self._clock())
                conn.execute("COMMIT")
                return verdict
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

    def admit(self, clip_id: str, *,
              title: Optional[str] = None,
              streamer: Optional[str] = None,
              video_url: Optional[str] = None,
              duration_sec: Optional[float] = None) -> dict:
        """Admission by opaque id, for ids that are not bot clip rows.

        NOTE: this path performs bounded input validation only. The
        caller-supplied duration_sec is recorded as metadata; it is NOT
        media verification. The trusted media path is admit_clip().

        Returns a verdict dict:
          accepted         bool - the clip is durably registered
          replayed         bool - True when an earlier call made the decision
                           and this call only learns it (no new registration
                           and NO new pending record)
          reason           None, 'daily_cap', 'unknown_clip',
                           'clip_not_ready', 'invalid_duration' or
                           'legacy_no_handoff'
          intake_date      str - the authority-derived date the stored
                           registration belongs to (on a replay this is the
                           ORIGINAL decision's date)
          daily_count      int - accepted rows for that date after the
                           decision
          handoff_job_id   int - (accepted verdicts only) id of the single
                           durable pending record backing this admission
          metadata_ignored bool - present True when this call supplied
                           metadata for an already-recorded clip_id that
                           differs from the stored row; the stored row wins
                           (immutability contract)
        """
        self._validate_caller(clip_id, title, streamer, video_url,
                              duration_sec)
        intake_date = self._today()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                verdict = self._decide(conn, clip_id, intake_date,
                                       title=title, streamer=streamer,
                                       video_url=video_url,
                                       duration_sec=duration_sec,
                                       _now=self._clock())
                conn.execute("COMMIT")
                return verdict
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

    @staticmethod
    def _validate_caller(clip_id, title, streamer, video_url,
                         duration_sec) -> None:
        if not isinstance(clip_id, str) or not clip_id.strip():
            raise ValueError("clip_id must be a non-empty string")
        if len(clip_id) > MAX_CLIP_ID_LEN:
            raise ValueError("clip_id too long")
        if clip_id.startswith(RESERVED_CLIP_ID_PREFIX):
            raise ValueError("clip_id uses the reserved bot-row namespace")
        for label, value in (("title", title), ("streamer", streamer)):
            if value is not None and (not isinstance(value, str)
                                      or len(value) > MAX_TEXT_LEN):
                raise ValueError(f"{label} too long or not a string")
        if video_url is not None and (not isinstance(video_url, str)
                                      or len(video_url) > MAX_URL_LEN):
            raise ValueError("video_url too long or not a string")
        if duration_sec is not None:
            if not _is_real_number(duration_sec) \
                    or not (0 < duration_sec <= MAX_DURATION_SEC):
                raise ValueError("duration_sec out of range")

    def _enqueue_handoff(self, conn: sqlite3.Connection, clip_id: str,
                         intake_date: str, now: float) -> int:
        """Insert the single durable pending record for this admission.
        Caller must hold BEGIN IMMEDIATE. Mirrors JobQueue.enqueue's row
        shape. NOTE: no consumer exists in this repo yet; the row is
durable pending admission data, not a functioning upload handoff."""
        cur = conn.execute(
            "INSERT INTO job_queue (kind, payload_json, state, max_attempts,"
            " run_after, created_at, updated_at) VALUES (?, ?, 'PENDING',"
            " 3, 0, ?, ?)",
            (HANDOFF_JOB_KIND,
             json.dumps({"clip_id": clip_id, "intake_date": intake_date}),
             now, now))
        return int(cur.lastrowid)

    def _decide(self, conn: sqlite3.Connection, clip_id: str,
                intake_date: str, *,
                title: Optional[str] = None, streamer: Optional[str] = None,
                video_url: Optional[str] = None,
                duration_sec: Optional[float] = None,
                _now: float) -> dict:
        """The atomic decision. Caller must hold BEGIN IMMEDIATE."""
        row = conn.execute(
            "SELECT status, intake_date, daily_count, title, streamer,"
            " video_url, duration_sec, handoff_job_id FROM clip_intake"
            " WHERE clip_id = ?", (clip_id,)).fetchone()
        metadata_ignored = False
        if row is not None:
            supplied = {"title": title, "streamer": streamer,
                        "video_url": video_url,
                        "duration_sec": duration_sec}
            metadata_ignored = any(
                v is not None and v != row[k] for k, v in supplied.items())
        if row is not None and row["status"] == "ACCEPTED":
            if row["handoff_job_id"] is None:
                # Pre-handoff legacy row: preserved as-is, never re-minted,
                # refused fail-closed. Nothing may act on this verdict;
                # recovery is an explicit operator decision.
                verdict = {"accepted": False, "replayed": True,
                           "reason": "legacy_no_handoff",
                           "clip_id": clip_id,
                           "intake_date": row["intake_date"],
                           "daily_count": int(row["daily_count"]),
                           "handoff_job_id": None, "legacy": True}
                if metadata_ignored:
                    verdict["metadata_ignored"] = True
                return verdict
            verdict = {"accepted": True, "replayed": True,
                       "reason": None, "clip_id": clip_id,
                       "intake_date": row["intake_date"],
                       "daily_count": int(row["daily_count"]),
                       "handoff_job_id": int(row["handoff_job_id"])}
            if metadata_ignored:
                verdict["metadata_ignored"] = True
            return verdict
        count = self._day_count(conn, intake_date)
        if count >= DAILY_CAP:
            if row is None:
                conn.execute(
                    "INSERT INTO clip_intake (clip_id, intake_date,"
                    " status, title, streamer, video_url,"
                    " duration_sec, daily_count, created_at,"
                    " updated_at) VALUES (?, ?, 'REJECTED_CAP',"
                    " ?, ?, ?, ?, ?, ?, ?)",
                    (clip_id, intake_date, title, streamer,
                     video_url, duration_sec, count, _now, _now))
                replayed = False
            else:
                replayed = True
            verdict = _rejected("daily_cap", clip_id, intake_date, count,
                                replayed=replayed)
            if metadata_ignored:
                verdict["metadata_ignored"] = True
            return verdict
        # Fresh acceptance: registry row and its single pending record
        # commit together, so admission can never exist without its durable
        # record, and no later replay can mint a second one.
        job_id = self._enqueue_handoff(conn, clip_id, intake_date, _now)
        if row is None:
            conn.execute(
                "INSERT INTO clip_intake (clip_id, intake_date,"
                " status, title, streamer, video_url, duration_sec,"
                " daily_count, handoff_job_id, created_at, updated_at)"
                " VALUES (?, ?, 'ACCEPTED', ?, ?, ?, ?, ?, ?, ?, ?)",
                (clip_id, intake_date, title, streamer, video_url,
                 duration_sec, count + 1, job_id, _now, _now))
        else:
            # Earlier REJECTED_CAP and capacity now exists: the same
            # transaction flips the stored verdict and enqueues the one
            # handoff, so a retry after a new day (or freed slot) is
            # admitted exactly once. Stored metadata is immutable.
            conn.execute(
                "UPDATE clip_intake SET status = 'ACCEPTED',"
                " intake_date = ?, daily_count = ?, handoff_job_id = ?,"
                " updated_at = ? WHERE clip_id = ?",
                (intake_date, count + 1, job_id, _now, clip_id))
        verdict = {"accepted": True, "replayed": False,
                   "reason": None, "clip_id": clip_id,
                   "intake_date": intake_date,
                   "daily_count": count + 1,
                   "handoff_job_id": job_id}
        if metadata_ignored:
            verdict["metadata_ignored"] = True
        return verdict

    def _day_count(self, conn: sqlite3.Connection, intake_date: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM clip_intake"
            " WHERE intake_date = ? AND status = 'ACCEPTED'",
            (intake_date,)).fetchone()
        return int(row["n"])

    def day_count(self, intake_date: str) -> int:
        """Read-only count for diagnostics; not part of the decision path."""
        if not isinstance(intake_date, str) or not _DATE_RE.match(intake_date):
            raise ValueError("intake_date must be YYYY-MM-DD")
        conn = self._connect()
        try:
            return self._day_count(conn, intake_date)
        finally:
            conn.close()

    def status_of(self, clip_id: str) -> Optional[str]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT status FROM clip_intake WHERE clip_id = ?",
                (clip_id,)).fetchone()
            return None if row is None else str(row["status"])
        finally:
            conn.close()


def _rejected(reason: str, clip_id: str, intake_date: str,
              daily_count: int, replayed: bool = False) -> dict:
    return {"accepted": False, "replayed": replayed, "reason": reason,
            "clip_id": clip_id, "intake_date": intake_date,
            "daily_count": daily_count}
