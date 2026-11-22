"""SQLite access and schema migrations.

Uses the stdlib sqlite3 module (available in AGT's Python 3.12). Migrations
are ordered, versioned and idempotent: each runs once inside a transaction
and is recorded in schema_migrations. Media files live on the filesystem;
the database stores metadata and file references only.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Iterable

MIGRATIONS: list[tuple[int, str]] = [
    (1, """
    CREATE TABLE twitch_sources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        broadcaster_id TEXT,
        display_name TEXT,
        authorization_status TEXT NOT NULL DEFAULT 'unknown',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        UNIQUE(username)
    );
    CREATE TABLE stream_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_id INTEGER NOT NULL REFERENCES twitch_sources(id),
        state TEXT NOT NULL DEFAULT 'PENDING',
        started_at REAL,
        ended_at REAL,
        ingest_mode TEXT,
        notes TEXT
    );
    CREATE TABLE recording_segments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER NOT NULL REFERENCES stream_sessions(id),
        file_path TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        duration_seconds REAL,
        created_at REAL NOT NULL,
        UNIQUE(session_id, sequence)
    );
    CREATE TABLE candidate_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER NOT NULL REFERENCES stream_sessions(id),
        detected_at REAL NOT NULL,
        kind TEXT NOT NULL,
        confidence REAL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        metadata_json TEXT
    );
    CREATE TABLE ai_evaluations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER NOT NULL REFERENCES candidate_events(id),
        adapter TEXT NOT NULL,
        score REAL,
        summary TEXT,
        created_at REAL NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING'
    );
    CREATE TABLE clips (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER REFERENCES candidate_events(id),
        session_id INTEGER REFERENCES stream_sessions(id),
        file_path TEXT,
        preset TEXT,
        duration_seconds REAL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        subtitle_status TEXT,
        qc_report_json TEXT,
        created_at REAL NOT NULL,
        delivered_at REAL
    );
    CREATE TABLE subtitle_tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        clip_id INTEGER NOT NULL REFERENCES clips(id),
        language TEXT NOT NULL DEFAULT 'en',
        file_path TEXT,
        status TEXT NOT NULL DEFAULT 'PENDING'
    );
    CREATE TABLE distribution_destinations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        name TEXT NOT NULL,
        rtmp_url_secret_ref TEXT,
        state TEXT NOT NULL DEFAULT 'IDLE',
        created_at REAL NOT NULL,
        UNIQUE(kind, name)
    );
    CREATE TABLE distribution_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        destination_id INTEGER NOT NULL REFERENCES distribution_destinations(id),
        state TEXT NOT NULL DEFAULT 'PENDING',
        started_at REAL,
        ended_at REAL,
        stats_json TEXT
    );
    CREATE TABLE settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE TABLE job_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'PENDING',
        attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 3,
        run_after REAL NOT NULL DEFAULT 0,
        locked_by TEXT,
        locked_at REAL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        last_error TEXT
    );
    CREATE INDEX idx_job_queue_claim ON job_queue(state, run_after);
    CREATE TABLE error_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL,
        level TEXT NOT NULL,
        source TEXT NOT NULL,
        message TEXT NOT NULL
    );
    """),
    (2, """
    -- At-least-once extraction with exactly-once clips rows: if the
    -- process dies after extract_clip wrote the file but before the
    -- clips row commits, the retried pass re-extracts to the same
    -- deterministic path (idempotent) and INSERT OR IGNORE keeps the
    -- row count at one.
    CREATE UNIQUE INDEX IF NOT EXISTS clips_event_unique
        ON clips(event_id) WHERE event_id IS NOT NULL;
    """),
    (3, """
    -- Durable per-clip publish reservation: ownership/dedup authority
    -- for YouTube uploads, independent of provider update IDs and of
    -- the pruned command journal. See app/distribution/publish_records.py
    CREATE TABLE IF NOT EXISTS publish_records (
        clip_id INTEGER PRIMARY KEY,
        state TEXT NOT NULL,
        video_id TEXT,
        attempts INTEGER NOT NULL DEFAULT 0,
        reserved_by TEXT,
        last_error TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        attempt_token TEXT
    );
    """),
    (4, """
    -- Sealed-input recovery: durable producer completion evidence
    -- (digest/size committed with the PENDING row) and owner-token
    -- claims shared by the original finalizer and recovery. Rows
    -- without seal evidence are legacy and stay fenced.
    ALTER TABLE clips ADD COLUMN sealed_sha256 TEXT;
    ALTER TABLE clips ADD COLUMN sealed_bytes INTEGER;
    ALTER TABLE clips ADD COLUMN sealed_at REAL;
    ALTER TABLE clips ADD COLUMN recovery_owner TEXT;
    ALTER TABLE clips ADD COLUMN claimed_at REAL;
    """),
]

JOB_STATES = (
    "PENDING", "DISCOVERING", "WAITING_FOR_AUTHORIZED_INPUT", "CONNECTING",
    "RECORDING", "ANALYZING", "CLIPPING", "EDITING", "QUALITY_CHECK",
    "COMPLETED", "FAILED", "CANCELLED",
)


def connect(db_path: str) -> sqlite3.Connection:
    path = Path(db_path)
    if path.parent and str(path.parent) != ".":
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def migrate(conn: sqlite3.Connection,
            migrations: Iterable[tuple[int, str]] = MIGRATIONS) -> list[int]:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)")
    applied = {
        row["version"]
        for row in conn.execute("SELECT version FROM schema_migrations")
    }
    newly_applied: list[int] = []
    for version, sql in sorted(migrations):
        if version in applied:
            continue
        with conn:
            conn.executescript(sql)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (version, time.time()))
        newly_applied.append(version)
    return newly_applied
