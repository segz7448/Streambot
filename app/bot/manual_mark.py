"""Telegram command for owner-selected moments in an active session.

The command writes only a durable candidate event. Extraction, edit and QC
stay on the existing session/recovery path. Event time starts from command
receipt minus seconds-ago and is kept exactly; it is never clamped back to
an older durable boundary. AutoClipper defers the mark until durable
segments cover the moment plus post-roll, within a bounded wait.
"""
from __future__ import annotations

import math
import time
from pathlib import Path

from ..clipping.manual import MANUAL_MARK_KIND
from ..core.database import connect, migrate
from . import ops
from . import sessions as session_registry

MARK_MAX_WAIT_SECONDS = 45.0     # bounded wait for durable post-roll
MAX_SECONDS_AGO = 20.0          # the live clip's pre-event window


def _valid_seconds(text: str):
    try:
        value = float(text)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value < 0 or value > MAX_SECONDS_AGO:
        return None
    return value


def _live_sessions():
    with session_registry._lock:
        return {
            name: sess for name, sess in session_registry._sessions.items()
            if getattr(sess, "_session_id", None) is not None
            and not sess._stopping.is_set()
            and sess.alive()
        }


def _parse_args(args: str, live: dict):
    parts = args.split()
    if not parts:
        if len(live) == 1:
            return next(iter(live)), 0.0, None
        return None, None, ("Usage: /clipmark <twitch-username> "
                            "[seconds-ago, 0-20]")
    first = parts[0]
    seconds = _valid_seconds(first)
    if seconds is not None:
        if len(parts) > 2:
            return None, None, ("Usage: /clipmark <twitch-username> "
                                "[seconds-ago, 0-20]")
        if len(parts) == 2:
            return parts[1].strip().lstrip("@").lower(), seconds, None
        if len(live) == 1:
            return next(iter(live)), seconds, None
        return None, None, ("More than one session is active; use "
                            "/clipmark <twitch-username> [seconds-ago].")
    if first.startswith("-") and len(parts) == 1:
        return None, None, "seconds-ago must be between 0 and 20."
    username = first.strip().lstrip("@").lower()
    seconds = 0.0
    if len(parts) == 2:
        seconds = _valid_seconds(parts[1])
        if seconds is None:
            return None, None, "seconds-ago must be between 0 and 20."
    elif len(parts) > 2:
        return None, None, ("Usage: /clipmark <twitch-username> "
                            "[seconds-ago, 0-20]")
    if not username:
        return None, None, ("Usage: /clipmark <twitch-username> "
                            "[seconds-ago, 0-20]")
    return username, seconds, None


def _record_mark(state_dir: Path, username: str, session_id: int,
                 seconds_ago: float, update_id: int) -> str:
    """Insert the durable event atomically after verifying live state and
    stable buffer timing. No clip or extraction is claimed by this reply."""
    db_path = str(Path(state_dir) / "nova.db")
    conn = connect(db_path)
    try:
        migrate(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT s.state, t.username FROM stream_sessions s"
                " JOIN twitch_sources t ON t.id=s.source_id"
                " WHERE s.id=?", (session_id,)).fetchone()
            if row is None or row["username"] != username:
                conn.rollback()
                return (f"No active recording session for {username}. "
                        "Nothing was marked.")
            if row["state"] != "RECORDING":
                conn.rollback()
                return (f"The session for {username} is not recording yet "
                        f"({row['state']}). Nothing was marked.")
            seg = conn.execute(
                "SELECT MAX(created_at) FROM recording_segments"
                " WHERE session_id=?", (session_id,)).fetchone()
            latest = seg[0] if seg else None
            if latest is None:
                conn.rollback()
                return (f"No durable recorded segment exists for {username} "
                        "yet. Wait for the first segment, then mark again.")
            first = conn.execute(
                "SELECT MIN(created_at - COALESCE(duration_seconds, 5.0))"
                " FROM recording_segments WHERE session_id=?",
                (session_id,)).fetchone()[0]
            receipt_at = time.time()
            requested_time = receipt_at - seconds_ago
            latest = float(latest)
            durable_lag = max(0.0, receipt_at - latest)
            # The marked moment is preserved: event time is the requested
            # moment itself, never clamped backward onto an older durable
            # boundary. Extraction waits (bounded) until durable segments
            # cover requested_time + post-roll; see AutoClipper.
            event_time = requested_time
            if first is not None and event_time < float(first) - 0.5:
                conn.rollback()
                return ("That moment is outside the retained buffer. "
                        "Nothing was marked.")
            metadata = {
                "schema": 3,
                "origin": "owner_manual",
                "source": username,
                "update_id": int(update_id) if update_id else None,
                "seconds_ago": seconds_ago,
                "mark_basis": "command_receipt_minus_seconds_ago",
                "command_received_at": receipt_at,
                "requested_event_time": requested_time,
                "durable_lag_seconds": durable_lag,
                "wait_deadline_at": receipt_at + MARK_MAX_WAIT_SECONDS,
                "recorded_at": receipt_at,
            }
            import json
            cur = conn.execute(
                "INSERT INTO candidate_events"
                " (session_id, detected_at, kind, confidence,"
                " metadata_json) VALUES (?,?,?,?,?)",
                (session_id, event_time, MANUAL_MARK_KIND, None,
                 json.dumps(metadata, sort_keys=True)))
            conn.commit()
            event_id = cur.lastrowid
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()
    when = f"{seconds_ago:g}s before command receipt"
    timing_note = ""
    if requested_time > latest:
        timing_note = (
            f" The moment is {requested_time - latest:.1f}s past the newest "
            "durable segment, so extraction waits (up to "
            f"{MARK_MAX_WAIT_SECONDS:.0f}s) for recorded footage to cover "
            "it. If it never does, the mark fails explicitly instead of "
            "producing a clip without it.")
    return (f"Manual mark #{event_id} recorded for {username} ({when})."
            f"{timing_note} Extraction, edit and QC will run through the "
            "existing session path; the clip is not delivered yet.")


def clipmark_reply(state_dir: Path, args: str, update_id: int = 0) -> str:
    """Record the administrator's selected moment for an active session.

    Replay behavior is operation-level: after the candidate insert succeeds
    the result is journaled; a re-delivered Telegram update returns that
    result instead of inserting another mark. The crash window before the
    journal write is handled by ops.SIDE_EFFECTING as uncertain, never
    blind-retried."""
    state_dir = Path(state_dir)
    cached = ops.journal_result(state_dir, update_id, "clipmark")
    if cached is not None:
        return "Already processed (replay guard): " + cached
    live = _live_sessions()
    username, seconds_ago, error = _parse_args(args, live)
    if error is not None:
        return error
    sess = live.get(username)
    if sess is None:
        return (f"No active recording session for {username}. "
                "Nothing was marked.")
    result = _record_mark(state_dir, username,
                          int(sess._session_id), seconds_ago, update_id)
    if result.startswith("Manual mark #"):
        ops.journal_record(state_dir, update_id, "clipmark", result)
    return result
