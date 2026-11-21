"""Real provenance fixtures: every test clip gets an actual feed-origin
(mode A) session row, exactly like production clips, so the unmodified
production gate runs in these tests."""
import sqlite3
import time
from pathlib import Path

from app.core import database


def feed_session_id(conn, mode="feed", user="fixture-feed"):
    n = time.time()
    conn.execute("INSERT OR IGNORE INTO twitch_sources (username,created_at,updated_at) VALUES (?,?,?)", (user, n, n))
    sid = conn.execute("SELECT id FROM twitch_sources WHERE username=?", (user,)).fetchone()[0]
    r = conn.execute("SELECT id FROM stream_sessions WHERE source_id=? AND ingest_mode=? LIMIT 1", (sid, mode)).fetchone()
    if r:
        return r[0]
    return conn.execute("INSERT INTO stream_sessions (source_id,state,ingest_mode) VALUES (?, 'ENDED', ?)", (sid, mode)).lastrowid


def seed_clip(state_dir, clip_id, mode="feed"):
    """Idempotently ensure schema + a clip row with a real feed session."""
    conn = database.connect(str(Path(state_dir) / "nova.db"))
    try:
        database.migrate(conn)
        with conn:
            sid = feed_session_id(conn, mode)
            if conn.execute("SELECT 1 FROM clips WHERE id=?", (int(clip_id),)).fetchone():
                conn.execute("UPDATE clips SET session_id=? WHERE id=? AND session_id IS NULL", (sid, int(clip_id)))
            else:
                conn.execute("INSERT INTO clips (id,session_id,status,created_at) VALUES (?,?,'HELD',?)", (int(clip_id), sid, time.time()))
    finally:
        conn.close()
