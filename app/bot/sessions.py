"""Session-owning command logic for /startclip and /stopclip.

Unlike the pure-stdlib handlers, these own live session objects in the
service process. Feed specifiers are validated exactly like relay
endpoints before any process starts.
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from ..distribution.relay import validate_feed
from ..pipeline.session import ClipSession

_lock = threading.Lock()
_sessions: dict[str, ClipSession] = {}


def _db(state_dir: Path) -> sqlite3.Connection:
    return sqlite3.connect(str(state_dir / "nova.db"))


def _feed_for(state_dir: Path, username: str) -> str | None:
    feeds = state_dir / "feeds"
    url_file = feeds / f"{username}.url"
    if url_file.exists():
        return url_file.read_text().strip()
    for ext in (".mp4", ".flv", ".mkv"):
        candidate = feeds / f"{username}{ext}"
        if candidate.exists():
            return str(candidate)
    return None


def startclip_reply(state_dir: Path, username: str, ffmpeg: str,
                    ffprobe: str,
                    session_factory=ClipSession) -> str:
    """Start a capture session over the source's authorized feed. Loud
    failure when no feed is registered; never pretends work started."""
    username = username.strip().lstrip("@").lower()
    if not username:
        return "Usage: /startclip <twitch-username>"
    with _db(state_dir) as conn:
        row = conn.execute(
            "SELECT id FROM twitch_sources WHERE username=?",
            (username,)).fetchone()
    if row is None:
        return f"{username} is not registered. Use /addsource first."
    feed_spec = _feed_for(state_dir, username)
    if feed_spec is None:
        return (f"No authorized feed for {username}: place a "
                "broadcaster-provided feed URL in "
                f"feeds/{username}.url or a recording at "
                f"feeds/{username}.mp4. Nothing is recording. "
                "Continuous capture of arbitrary Twitch channels is "
                "not available without one (architecture boundary).")
    try:
        feed = validate_feed(feed_spec, state_dir / "feeds")
    except ValueError as exc:
        return f"Refused: {exc}. Session NOT started."
    if not feed.startswith(("http://", "https://")):
        # a local recording feed is validated exactly like a mode C
        # upload before any process starts (fail closed)
        from ..clipping.retrospective import (ValidationError,
                                              validate_recording)
        try:
            validate_recording(ffprobe, feed)
        except ValidationError as exc:
            return (f"Refused: local feed failed validation ({exc}). "
                    "Session NOT started.")
    with _lock:
        existing = _sessions.get(username)
        if existing is not None:
            if not existing._stopping.is_set():
                return f"A session for {username} is already running."
            if existing.alive():
                # still finalizing: the shared workdir and output paths
                # would collide and clips/handles could be lost
                return (f"The previous session for {username} is still "
                        "finalizing its clips. Try again in a moment - "
                        "nothing was started.")
        session = session_factory(ffmpeg, ffprobe, feed, username,
                                  state_dir)
        session.start()
        _sessions[username] = session
    return (f"Clipping session started for {username} from its "
            "registered feed: recording, detecting and clipping now. "
            "Clips are HELD for your review - /deliveries flush sends "
            "them. /stopclip ends the session.")


def stopclip_reply(state_dir: Path) -> str:
    with _lock:
        stopped = 0
        for name, session in list(_sessions.items()):
            if not session._stopping.is_set():
                session.stop()
                stopped += 1
    if stopped:
        return (f"Stopping {stopped} session(s). Final clips still go "
                "through QC and are held.")
    return "No clipping session is running."


def sessions_reply(state_dir: Path) -> str:
    with _lock:
        live = {k: s for k, s in _sessions.items()}
    lines = []
    for name, s in live.items():
        state = "stopping" if s._stopping.is_set() else "running"
        extra = f", error: {s.error}" if s.error else ""
        lines.append(f"{name}: {state}, {s.clips_finished} clip(s) held"
                     f"{extra}")
    with _db(state_dir) as conn:
        try:
            rows = conn.execute(
                "SELECT id, state, ingest_mode FROM stream_sessions"
                " ORDER BY id DESC LIMIT 5").fetchall()
        except sqlite3.Error:
            rows = []
    history = [f"#{r[0]} {r[1]} ({r[2] or 'live'})" for r in rows]
    out = []
    if lines:
        out.append("Active: " + "; ".join(lines))
    if history:
        out.append("Recent sessions: " + ", ".join(history))
    return "\n".join(out) if out else "No clipping sessions yet."
