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
        state = getattr(s, "phase", None) or (
            "stopping" if s._stopping.is_set() else "running")
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


class _Reservation:
    """Placeholder owning the username while a start is in flight (resolve
    runs outside the lock). Looks like a running session to every other
    start; /stopclip cancels it."""
    def __init__(self):
        self._stopping = threading.Event()
        self.error = None
        self.clips_finished = 0

    def stop(self):
        self._stopping.set()

    def alive(self):
        # owned until released in the starter's finally: a stopped
        # reservation cannot be replaced while its resolver still runs
        return not self._released

    _released = False


def startplayback_reply(state_dir: Path, username: str, ffmpeg: str,
                        ffprobe: str, session_factory=ClipSession,
                        resolve_fn=None, hooks: dict | None = None) -> str:
    """/startplayback <user>: capture a streamer's PUBLIC live playback.
    Requires a registered source and record authorization (default OFF).
    The username is reserved under the lock BEFORE the up-front resolve, and
    the reservation, record permission and cancellation are rechecked before
    launching, so starts cannot overlap or replace another session. The reply
    says the session is STARTING (not yet confirmed recording); /sessions
    shows its durable state."""
    from . import source_auth
    from ..ingestion import twitch_playback as tp
    username = username.strip().lstrip("@").lower()
    if not username:
        return "Usage: /startplayback <twitch-username>"
    with _db(state_dir) as conn:
        row = conn.execute("SELECT id FROM twitch_sources WHERE username=?",
                           (username,)).fetchone()
    if row is None:
        return f"{username} is not registered. Use /addsource first."
    if not source_auth.allowed(state_dir, username, "record"):
        return (f"Recording is not authorized for {username} (default OFF). "
                f"Nothing started. Only with the streamer's permission: "
                f"/sourceauth {username} record on")
    res = _Reservation()
    with _lock:
        existing = _sessions.get(username)
        if existing is not None and (not existing._stopping.is_set()
                                     or existing.alive()):
            return (f"A session for {username} is already running, starting "
                    "or finalizing. Nothing was started.")
        _sessions[username] = res

    def release():
        with _lock:
            res._released = True
            if _sessions.get(username) is res:
                del _sessions[username]
    try:
        resolved = (resolve_fn or tp.resolve)(
            username, authorized=True,
            should_stop=lambda: res._stopping.is_set() or not
            source_auth.allowed(state_dir, username, "record"),
            **(hooks or {}))
    except tp.PlaybackRefused as exc:
        release()
        why = {"offline": "the channel is offline or unknown",
               "restricted": "Twitch restricted access",
               "challenge": "Twitch presented a challenge (not bypassed)",
               "blocked": "rate limited or blocked (not retried)",
               "quality": "no variant within the quality cap"}.get(
                   exc.kind, f"playback unavailable ({exc.kind})")
        return f"Not started: {why}. Nothing is recording."
    except BaseException:
        release()
        raise
    pb = {"initial_resolved": resolved}
    pb.update({k: v for k, v in (hooks or {}).items()
               if k in ("opener", "host_ok")})
    try:
        with _lock:
            if _sessions.get(username) is not res or res._stopping.is_set():
                msg = ("Start cancelled before launch (a stop arrived or the "
                       "slot changed). Nothing is recording.")
            elif not source_auth.allowed(state_dir, username, "record"):
                msg = (f"Recording authorization for {username} was turned "
                       "off during startup. Nothing is recording.")
            else:
                msg = None
                session = session_factory(ffmpeg, ffprobe, "", username,
                                          state_dir, playback=pb)
                session.start()
                res._released = True
                _sessions[username] = session
        if msg is not None:
            release()
            return msg
    except BaseException:
        release()
        raise
    repost = source_auth.allowed(state_dir, username, "repost")
    return (f"Playback capture for {username} is STARTING (not yet confirmed "
            "recording; /sessions shows its state). Clips are HELD for you "
            "only. Repost to YouTube is "
            + ("ON for this streamer." if repost else
               "OFF: playback clips cannot be published, and clips made "
               "while it is off are NOT queued and NOT backfilled if you "
               f"turn it on later (/sourceauth {username} repost on, only "
               "with the streamer's permission).")
            + " /stopclip ends the session.")
