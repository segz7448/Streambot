"""Clip origin and repost authorization, read from durable rows only.

A clip is playback-origin when its session row has ingest_mode='playback'
(written when the session is created, with the authorization snapshot in
the session notes). Origin is never inferred from a username. For a
playback-origin clip, repost must be currently authorized for that
session's source (settings srcauth:<user>:repost == '1'); revocation
applies to every later publish attempt. Any read problem blocks.
"""
from __future__ import annotations

import sqlite3

PLAYBACK = "playback"


def repost_allowed(conn, clip_id) -> tuple:
    """(allowed, source_username_or_None). Evaluate inside the caller's
    transaction on its own connection. Non-playback clips are allowed
    here (their own gates apply)."""
    try:
        have = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"clips", "stream_sessions", "twitch_sources"} <= have:
            return True, None        # no session schema: no playback origin can exist
        row = conn.execute(
            "SELECT s.ingest_mode, t.username FROM clips c"
            " JOIN stream_sessions s ON c.session_id = s.id"
            " JOIN twitch_sources t ON s.source_id = t.id"
            " WHERE c.id = ?", (int(clip_id),)).fetchone()
        if row is None:
            return True, None
        mode, user = row[0], row[1]
        if mode != PLAYBACK:
            return True, None
        v = conn.execute("SELECT value FROM settings WHERE key=?",
                         (f"srcauth:{user}:repost",)).fetchone()
        return (bool(v and v[0] == "1")), user
    except (sqlite3.Error, ValueError, TypeError):
        return False, None
