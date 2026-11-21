"""Clip origin and repost authorization, read from durable rows only.

Origin is resolved clip -> session -> source and must resolve completely.
Known non-playback origins are explicit: ingest_mode 'feed' (mode A) and
'upload' (mode C). 'playback' requires CURRENT repost authorization for that
source (settings srcauth:<user>:repost == '1'). Everything else (missing
schema, missing clip, no session, orphan session or source, NULL or unknown
mode, unreadable settings, any DB error) is denied with a distinct reason:
unresolved provenance is never treated as non-playback.
"""
from __future__ import annotations

import sqlite3
import time
from typing import NamedTuple

PLAYBACK = "playback"
NON_PLAYBACK = frozenset({"feed", "upload"})
DENIED_PREFIX = "repost_denied:"          # missing grant: owner may grant later
UNREADABLE_PREFIX = "provenance_unreadable:"  # needs inspection, no auto retry


class Origin(NamedTuple):
    allowed: bool
    user: str | None
    reason: str          # "" when allowed
    retryable: bool      # True only for a missing/revoked repost grant

    def __bool__(self):
        return self.allowed


def _no(reason, user=None, retry=False):
    return Origin(False, user, reason, retry)


def _repost_allowed_real(conn, clip_id) -> Origin:
    try:
        have = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        need = {"clips", "stream_sessions", "twitch_sources", "settings"}
        if not need <= have:
            return _no("provenance schema missing")
        c = conn.execute("SELECT session_id FROM clips WHERE id=?",
                         (int(clip_id),)).fetchone()
        if c is None:
            return _no("clip row missing")
        if c[0] is None:
            return _no("clip has no session origin")
        s = conn.execute("SELECT source_id, ingest_mode FROM stream_sessions"
                         " WHERE id=?", (c[0],)).fetchone()
        if s is None:
            return _no("session row missing")
        t = conn.execute("SELECT username FROM twitch_sources WHERE id=?",
                         (s[0],)).fetchone()
        if t is None:
            return _no("source row missing")
        mode, user = s[1], t[0]
        if mode in NON_PLAYBACK:
            return Origin(True, user, "", False)
        if mode != PLAYBACK:
            return _no("unknown ingest mode", user)
        v = conn.execute("SELECT value FROM settings WHERE key=?",
                         (f"srcauth:{user}:repost",)).fetchone()
        if v is not None and v[0] == "1":
            return Origin(True, user, "", False)
        return _no("repost not authorized for this playback source", user, True)
    except (sqlite3.Error, ValueError, TypeError):
        return _no("provenance unreadable")


def prefixed(o: Origin) -> str:
    return (DENIED_PREFIX if o.retryable else UNREADABLE_PREFIX) + o.reason


def _job_clip(kind, payload_json):
    import json
    try:
        d = json.loads(payload_json)
        if kind == "clip_intake_admit":
            return int(d["clip_row_id"])
        return int(str(d["clip_id"]).split(":", 1)[1])   # handoff/native jobs
    except Exception:
        return None


def requeue_denied(conn, user) -> int:
    """Called INSIDE the grant's transaction (no commit here). Requeues only
    FAILED jobs of the supported kinds that were refused for a missing grant
    AND whose clip resolves to this source. Each re-runs the full gate."""
    from .intake import HANDOFF_JOB_KIND
    n = 0
    rows = conn.execute(
        "SELECT id, kind, payload_json FROM job_queue WHERE state='FAILED'"
        " AND last_error LIKE ? AND kind IN ('clip_intake_admit', ?)",
        (DENIED_PREFIX + "%", HANDOFF_JOB_KIND)).fetchall()
    for jid, kind, pj in rows:
        cid = _job_clip(kind, pj)
        if cid is None:
            continue
        o = _repost_allowed_real_for_user(conn, cid, user)
        if not o:
            continue
        conn.execute(
            "UPDATE job_queue SET state='PENDING', attempts=0, locked_by=NULL,"
            " locked_at=NULL, last_error=NULL, run_after=0, updated_at=?"
            " WHERE id=? AND state='FAILED'", (time.time(), jid))
        n += 1
    return n


def _repost_allowed_real_for_user(conn, clip_id, user) -> bool:
    """True when the clip is playback-origin from exactly this source."""
    try:
        r = conn.execute(
            "SELECT t.username, s.ingest_mode FROM clips c"
            " JOIN stream_sessions s ON c.session_id=s.id"
            " JOIN twitch_sources t ON s.source_id=t.id WHERE c.id=?",
            (int(clip_id),)).fetchone()
        return bool(r and r[0] == user and r[1] == PLAYBACK)
    except (sqlite3.Error, ValueError, TypeError):
        return False


repost_allowed = _repost_allowed_real
