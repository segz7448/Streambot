"""Session-owning command logic for /startclip and /stopclip.

Unlike the pure-stdlib handlers, these own live session objects in the
service process. Feed specifiers are validated exactly like relay
endpoints before any process starts.
"""
from __future__ import annotations

import contextlib
import logging
import sqlite3
import re
import threading
from pathlib import Path

from ..core import content_gate
from ..distribution.relay import validate_feed
from ..pipeline.session import ClipSession

_lock = threading.Lock()
_sessions: dict[str, ClipSession] = {}


@contextlib.contextmanager
def _db(state_dir: Path):
    """Connection that commits on success, rolls back on error and ALWAYS
    closes (sqlite3's own context manager never closes)."""
    conn = sqlite3.connect(str(state_dir / "nova.db"))
    try:
        with conn:
            yield conn
    finally:
        conn.close()


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


_tl = threading.local()


def _is_live_feed(feed) -> bool:
    """Any URL-scheme feed is live capture (scheme compared case-folded and
    stripped); only a plain local path is the fixture/recording path."""
    return re.match(r"^[a-z][a-z0-9+.\-]*://", str(feed).strip().lower()) is not None


def _gate_refusal(username: str, cached: bool = False) -> str | None:
    """Shared GTA6 boundary for every live-capture start. None when the
    selected streamer's CURRENT live title matches; otherwise the refusal
    text (fail closed). cached=True is the non-blocking form required under
    the registry lock; the provider is never called there."""
    res = (content_gate.check_cached if cached else content_gate.check)(username)
    if res.ok:
        return None
    _tl.refusal = "qualification"
    return (f"Not started: {content_gate.describe(res.reason)} for {username}. "
            "Nothing is recording.")
_log = logging.getLogger(__name__)


def _bind(session) -> None:
    """Stamp the scheduler attempt id on a session BEFORE it launches, so
    the durable row (notes.sched_attempt) and the in-memory token agree."""
    aid = getattr(_tl, "attempt", None)
    session.attempt_id = aid
    session._sched_token = aid


def _launched() -> None:
    _tl.started = True


def start_scheduled(state_dir: Path, username: str, ffmpeg: str,
                    ffprobe: str, attempt_id: str,
                    resolve_fn=None) -> tuple[str, str]:
    """Scheduler launch for an already-committed attempt id. Uses the
    broadcaster-authorized feed when registered, else public playback,
    which refuses unless record auth is ON for this source (default OFF).
    Kinds: 'started' (a session thread was launched and is registered under
    the attempt id), 'not_started' (definitive, retryable, e.g. offline),
    'unauthorized' (owner has not enabled recording; backs off, no network),
    'qualification' (selected stream's current live title does not match, or
    is unavailable: nothing started, waits for the gate, not a rights issue),
    'blocked' (Twitch restricted / challenged / throttled: never retried).
    An exception means unknown; the registry then tells what is running."""
    _tl.attempt, _tl.started, _tl.refusal = attempt_id, False, None
    try:
        if _feed_for(state_dir, username) is not None:
            reply = startclip_reply(state_dir, username, ffmpeg, ffprobe)
        else:
            reply = startplayback_reply(state_dir, username, ffmpeg, ffprobe,
                                        resolve_fn=resolve_fn)
        if _tl.started:
            return "started", reply
        if _tl.refusal in ("restricted", "challenge", "blocked", "quality", "invalid"):
            return "blocked", reply
        if _tl.refusal == "unauthorized":
            return "unauthorized", reply
        if _tl.refusal == "qualification":
            return "qualification", reply
        return "not_started", reply
    finally:
        _tl.attempt, _tl.started, _tl.refusal = None, False, None


def session_terminated(state_dir: Path, username: str, attempt_id) -> bool:
    """POSITIVE proof only. True when the exact attempt's session object is
    registered and finished, or its durable stream_sessions row (matched by
    notes.sched_attempt) carries notes.drained=true, written by the session's own
    finalizer after its children were drained (crash recovery marking a row
    INTERRUPTED is NOT proof). Everything else is not proof."""
    with _lock:
        s = _match(username, attempt_id)
    if s is not None:
        try:
            return (not s.alive()) and bool(getattr(s, "_finalized", False))
        except Exception:
            return False
    try:
        import sqlite3
        from ..core import schedule as _sc
        sid = _sc.find_session_id(state_dir, attempt_id)
        if sid is None:
            return False
        c = sqlite3.connect(f"file:{Path(state_dir) / 'nova.db'}?mode=ro", uri=True, timeout=5)
        try:
            r = c.execute("SELECT notes FROM stream_sessions WHERE id=?",
                          (sid,)).fetchone()
        finally:
            c.close()
        import json as _j
        return bool(r and r[0] and _j.loads(r[0]).get("drained") is True)
    except Exception:
        return False


def writer_active(state_dir: Path, username: str, quiet_s: float = 300.0) -> bool:
    """Fail-closed check for a possibly orphaned writer. True (treat as
    owned/ambiguous) if a file in the streamer's session directory changed
    within quiet_s, if a process other than this one has that directory in
    its command line, or if anything could not be inspected (unreadable
    directory, /proc missing, a process whose command line is denied).
    False only when everything was inspected and nothing matched. It is
    evidence of quiet, not proof: a stalled writer older than quiet_s with a
    command line that does not name the directory is not detectable."""
    import os
    import time
    work = Path(state_dir) / "sessions" / username
    now = time.time()
    if work.exists():
        def _err(_e):
            raise OSError("unreadable session directory")
        try:
            for root, _dirs, files in os.walk(work, onerror=_err):
                for f in files:
                    if now - os.stat(os.path.join(root, f)).st_mtime < quiet_s:
                        return True
        except OSError:
            return True
    me = str(os.getpid())
    try:
        pids = os.listdir("/proc")
    except OSError:
        return True
    needle = str(work).encode()
    for pid in pids:
        if not pid.isdigit() or pid == me:
            continue
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                if needle in fh.read():
                    return True
        except (FileNotFoundError, ProcessLookupError):
            continue                      # exited while scanning
        except OSError:
            return True                   # cannot inspect: not verified quiet
    return False


def _match(username: str, attempt_id):
    sess = _sessions.get(username)
    if (sess is not None and attempt_id is not None
            and getattr(sess, "_sched_token", None) == attempt_id):
        return sess
    return None


def session_running(username: str, attempt_id) -> bool:
    with _lock:
        s = _match(username, attempt_id)
        return s is not None and s.alive()


def session_alive(username: str, attempt_id) -> bool:
    with _lock:
        s = _match(username, attempt_id)
        return s is not None and s.alive() and not s._stopping.is_set()


def session_stopping(username: str, attempt_id) -> bool:
    with _lock:
        s = _match(username, attempt_id)
        return s is not None and s._stopping.is_set()


def session_error(username: str, attempt_id) -> str:
    with _lock:
        s = _match(username, attempt_id)
        return (s.error or "") if s is not None else ""


def stop_session(username: str, attempt_id) -> bool:
    """Stop exactly the session carrying attempt_id; False (no effect) when
    it is gone, already stopping, or replaced by a later session."""
    with _lock:
        s = _match(username, attempt_id)
        if s is not None and not s._stopping.is_set():
            s.stop()
            return True
    return False


def _discard_failed_start(username: str, session) -> None:
    """start() raised (lock held by the caller). A session that is not
    running holds nothing; one that somehow is stays registered so its
    slot and exact object remain owned and adoptable."""
    try:
        alive = session.alive()
    except Exception:
        alive = True
    if not alive and _sessions.get(username) is session:
        del _sessions[username]


def session_terminal(username: str, attempt_id, state_dir=None) -> str:
    """Typed terminal reason of the exact attempt: from the live session
    object while registered, else from its durable row (notes.terminal,
    written at completion) so a crash before the next tick loses nothing."""
    with _lock:
        s = _match(username, attempt_id)
    if s is not None:
        k = getattr(s, "terminal_kind", None)
        if k:
            return k
    if state_dir is None:
        return ""
    try:
        import json as _j
        import sqlite3
        from ..core import schedule as _sc
        sid = _sc.find_session_id(state_dir, attempt_id)
        if sid is None:
            return ""
        c = sqlite3.connect(f"file:{Path(state_dir) / 'nova.db'}?mode=ro", uri=True, timeout=5)
        try:
            r = c.execute("SELECT notes FROM stream_sessions WHERE id=?", (sid,)).fetchone()
        finally:
            c.close()
        k = (_j.loads(r[0]).get("terminal") if r and r[0] else "") or ""
        return k if isinstance(k, str) else ""
    except Exception:
        return ""


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
    if _is_live_feed(feed_spec):
        refused = _gate_refusal(username)   # live capture: GTA6 title gate,
        if refused is not None:             # before any feed validation
            return refused
    try:
        feed = validate_feed(feed_spec, state_dir / "feeds")
    except ValueError as exc:
        return f"Refused: {exc}. Session NOT started."
    if not _is_live_feed(feed):
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
        if _is_live_feed(feed):
            refused = _gate_refusal(username, cached=True)  # no provider here
            if refused is not None:
                return refused
        session = session_factory(ffmpeg, ffprobe, feed, username,
                                  state_dir)
        _bind(session)
        _sessions[username] = session      # tracked BEFORE any effect
        try:
            session.start()
        except BaseException:
            _discard_failed_start(username, session)
            raise
        _launched()
    return (f"Clipping session started for {username} from its "
            "registered feed: recording, detecting and clipping now. "
            "Clips are HELD for your review - /deliveries flush sends "
            "them. /stopclip ends the session.")


def stopclip_reply(state_dir: Path) -> str:
    scheduled = []
    with _lock:
        stopped = 0
        for name, session in list(_sessions.items()):
            if not session._stopping.is_set():
                session.stop()
                stopped += 1
                tok = getattr(session, "_sched_token", None)
                if tok:
                    scheduled.append(tok)
    for tok in scheduled:      # a user stop cancels the plan: no silent relaunch
        try:
            from ..core import schedule as _sched
            _sched.user_stopped(state_dir, tok)
        except Exception:
            _log.exception("could not cancel the schedule for a stopped session")
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
        _tl.refusal = "unauthorized"
        return (f"Recording is not authorized for {username} (default OFF). "
                f"Nothing started. Only with the streamer's permission: "
                f"/sourceauth {username} record on")
    refused = _gate_refusal(username)       # before any network or media
    if refused is not None:
        return refused
    res = _Reservation()
    res._sched_token = getattr(_tl, "attempt", None)
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
        _tl.refusal = exc.kind
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
            elif (refused := _gate_refusal(username, cached=True)) is not None:
                msg = refused      # title stopped matching during startup
            else:
                msg = None
                session = session_factory(ffmpeg, ffprobe, "", username,
                                          state_dir, playback=pb)
                _bind(session)
                res._released = True
                _sessions[username] = session  # tracked BEFORE any effect
                try:
                    session.start()
                except BaseException:
                    _discard_failed_start(username, session)
                    raise
                _launched()
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
