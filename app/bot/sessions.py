"""Session-owning command logic for /startclip and /stopclip.

Unlike the pure-stdlib handlers, these own live session objects in the
service process. Feed specifiers are validated exactly like relay
endpoints before any process starts.
"""
from __future__ import annotations

import contextlib
import logging
import os
import sqlite3
import re
import threading
import time
from pathlib import Path

from ..core import content_gate
from ..distribution.relay import validate_feed
from ..pipeline.session import ClipSession
from . import source_lock

_lock = threading.Lock()
_sessions: dict[str, ClipSession] = {}

_DEFAULT_MAX_HEIGHT = 720


def _playback_max_height() -> int:
    """Capture quality cap (px height) for public playback recording.

    Default 720 (the historical cap). NOVA_PLAYBACK_MAX_HEIGHT raises or
    lowers it for new sessions; an unparsable or out-of-range value falls
    back to the default with a warning, never a silent widening. Raising
    the cap only lets a higher SOURCE variant through (recorded as a
    source copy); it does not by itself prove the device sustains the
    higher rate, and a 16:9 source cropped to a 9:16 output still needs
    enlargement - the crop window of a 1920x1080 frame is only ~607x1080.
    """
    raw = os.environ.get("NOVA_PLAYBACK_MAX_HEIGHT", "").strip()
    if not raw:
        return _DEFAULT_MAX_HEIGHT
    try:
        height = int(raw)
    except ValueError:
        _log.warning("NOVA_PLAYBACK_MAX_HEIGHT=%r unparsable; using %d",
                     raw, _DEFAULT_MAX_HEIGHT)
        return _DEFAULT_MAX_HEIGHT
    if not 144 <= height <= 2160:
        _log.warning("NOVA_PLAYBACK_MAX_HEIGHT=%d out of range; using %d",
                     height, _DEFAULT_MAX_HEIGHT)
        return _DEFAULT_MAX_HEIGHT
    return height


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
        lock = getattr(session, "_source_lock", None)
        if lock is not None:
            lock.release()
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
    res = None
    if _is_live_feed(feed_spec):
        # own the username (and /stopclip's cancel handle) BEFORE the
        # blocking title check; the provider itself runs outside the lock
        res = _Reservation()
        res._sched_token = getattr(_tl, "attempt", None)
        with _lock:
            existing = _sessions.get(username)
            if existing is not None and (not existing._stopping.is_set()
                                         or existing.alive()):
                return f"A session for {username} is already running."
            _sessions[username] = res
    try:
        return _startclip_owned(state_dir, username, ffmpeg, ffprobe,
                                session_factory, feed_spec, res)
    finally:
        if res is not None:
            with _lock:
                if _sessions.get(username) is res:
                    del _sessions[username]


def _startclip_owned(state_dir, username, ffmpeg, ffprobe, session_factory,
                     feed_spec, res):
    if res is not None:
        refused = _gate_refusal(username)   # live capture: GTA6 title gate,
        if res._stopping.is_set():          # before any feed validation
            return ("Not started: stopped before launch. Nothing is "
                    "recording.")
        if refused is not None:
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
        if res is not None and res._stopping.is_set():
            return ("Not started: stopped before launch. Nothing is "
                    "recording.")
        existing = _sessions.get(username)
        if existing is not None and existing is not res:
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
        # cross-process exclusion: the source lock outlives this
        # process's in-memory registry, so another process (acceptance
        # runner, CLI) cannot start the same source concurrently
        if existing is not None:
            old = getattr(existing, "_source_lock", None)
            if old is not None:
                old.release()   # replaced session already ended; free its lock
        lock, why = source_lock.acquire(state_dir, username)
        if lock is None:
            return f"Not started: {why}. Nothing is recording."
        session = session_factory(ffmpeg, ffprobe, feed, username,
                                  state_dir)
        session._source_lock = lock
        session._on_finalized = lock.release
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


def active_status() -> str:
    """In-memory owned-session snapshot, without source names or network I/O."""
    with _lock:
        phases = [getattr(s, "phase", None) or "registered"
                  for s in _sessions.values()]
    if not phases:
        return "no in-process session registered"
    return f"{len(phases)} in-process session(s): " + ", ".join(phases)


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
        # blocking title check AFTER the reservation (so /stopclip can
        # cancel it) and outside the registry lock
        refused = _gate_refusal(username)
        if res._stopping.is_set():
            release()
            return "Not started: stopped before launch. Nothing is recording."
        if refused is not None:
            release()
            return refused
        max_height = _playback_max_height()
        resolved = (resolve_fn or tp.resolve)(
            username, authorized=True, max_height=max_height,
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
    pb = {"initial_resolved": resolved, "max_height": max_height}
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
                old = getattr(existing, "_source_lock", None)
                if old is not None:
                    old.release()   # replaced session already ended
                lock, why = source_lock.acquire(state_dir, username)
                if lock is None:
                    msg = (f"Not started: {why}. Nothing is recording.")
                else:
                    msg = None
                    session = session_factory(ffmpeg, ffprobe, "", username,
                                              state_dir, playback=pb)
                    session._source_lock = lock
                    session._on_finalized = lock.release
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


def _interrupted_unfinished(conn) -> dict:
    """{clip_id: sealed?} for unfinished clips of interrupted sessions."""
    return {r[0]: bool(r[1]) for r in conn.execute(
        "SELECT c.id, c.sealed_sha256 IS NOT NULL FROM clips c"
        " JOIN stream_sessions s ON s.id = c.session_id"
        " WHERE s.state='INTERRUPTED'"
        " AND c.status IN ('PENDING','RECOVERING')").fetchall()}


def recoverclips_reply(state_dir: Path, ffmpeg: str, ffprobe: str, *,
                       should_continue=None) -> str:
    """Synchronous recovery worker (runs on the background thread via
    RecoveryTracker, never on the command loop): finish SEALED PENDING
    clips left by interrupted sessions to HELD for owner review. No
    automatic boot wiring calls this. Nothing is delivered or queued
    for upload. Legacy/unsealed clips and previously-interrupted
    recovery claims stay fenced; no capture is resumed and no writer is
    declared dead. The report is derived from ACTUAL per-clip row
    states after the run, so failed edits/QC and fenced remainders are
    stated, never hidden behind 'nothing to do'."""
    from ..core.database import connect
    from ..pipeline.session import finalize_interrupted_sessions
    state_dir = Path(state_dir)
    db = state_dir / "nova.db"
    eligible: dict = {}
    if db.exists():
        conn = connect(str(db))
        try:
            eligible = _interrupted_unfinished(conn)
        finally:
            conn.close()
    # the recovery call always runs (even with nothing pending) so the
    # outcome reflects actual work; "nothing to do" is only honest when
    # nothing was pending AND nothing was finished
    held = finalize_interrupted_sessions(state_dir, ffmpeg, ffprobe,
                                         should_continue=should_continue)
    if held == 0 and not eligible:
        return ("Recovery: nothing to do - no interrupted sessions "
                "with unfinished clips.")
    return _recovery_outcome_text(state_dir, held, eligible)


def _recovery_outcome_text(state_dir: Path, held: int,
                           eligible: dict, outcomes=None) -> str:
    """Structured per-run outcome: exact per-clip terminal results the
    run itself reported (including clips that became eligible after the
    pre-read), merged with post-run row states for pre-read clips it
    did not touch; never global counts."""
    from ..core.database import connect
    state_dir = Path(state_dir)
    worked = dict(outcomes or [])
    rest = {i: s for i, s in eligible.items() if i not in worked}
    after: dict = {}
    if rest:
        conn = connect(str(state_dir / "nova.db"))
        try:
            after = {r[0]: r[1] for r in conn.execute(
                "SELECT id, status FROM clips WHERE id IN ("
                + ",".join(str(int(i)) for i in rest)
                + ")").fetchall()}
        finally:
            conn.close()
    recovered = sum(1 for oc in worked.values() if oc == "HELD")
    failed = sum(1 for oc in worked.values()
                 if oc in ("FAILED", "QC_FAILED"))
    interrupted = sum(1 for oc in worked.values()
                      if oc == "interrupted")
    recovered += sum(1 for i in rest if after.get(i) == "HELD")
    failed += sum(1 for i in rest
                  if after.get(i) in ("FAILED", "QC_FAILED"))
    fenced = sum(1 for oc in worked.values() if oc == "fenced")
    fenced += sum(1 for i in rest
                  if after.get(i) in ("PENDING", "RECOVERING"))
    if recovered == 0 and held:
        # HELD via a path without outcome detail (e.g. idempotent
        # re-claim): report the helper's own count, never a guess
        recovered = held
    lines = []
    if recovered:
        lines.append(
            f"Recovery: {recovered} clip(s) from interrupted sessions "
            "finished to HELD for your review (/deliveries). "
            "Nothing was delivered or queued for upload.")
    if failed:
        lines.append(
            f"{failed} clip(s) could not be finished (edit or quality "
            "check failed) - now marked failed; future runs do not "
            "retry them.")
    if interrupted:
        lines.append(
            f"{interrupted} clip(s) were stopped mid-recovery by "
            "shutdown and left pending; re-send /recoverclips to "
            "continue.")
    if fenced:
        lines.append(
            f"{fenced} clip(s) remain fenced (legacy/unsealed input, "
            "input no longer matching the producer's sealed digest, "
            "authorization off, or a previously interrupted recovery). "
            "They are left untouched; eligible sealed clips are "
            "re-checked on each run.")
    if not lines:
        return "Recovery: ran, but no clip changed state - see /logs."
    return "\n".join(lines)


_RECOVERY_JOIN_S = 20.0   # bounded shutdown drain for the worker


def _run_db_conn(state_dir):
    """Connection with BOTH schemas present - the recovery_runs
    migration (core.database) and the durable notice outbox
    (core.schedule) - so a run finish and its completion notice can
    commit in ONE transaction."""
    from ..core.database import connect, migrate
    from ..core import schedule as _sched
    state_dir = Path(state_dir)
    conn = connect(str(state_dir / "nova.db"))
    migrate(conn)
    conn.executescript(_sched.DDL)
    return conn


def recovery_run_start(state_dir) -> int | None:
    """Durable accepted-run record written BEFORE the owner is told the
    run started, so a restart never loses the run's identity. None when
    the record could not be written: the run still proceeds and its
    completion is recorded best-effort."""
    try:
        conn = _run_db_conn(state_dir)
        try:
            cur = conn.execute(
                "INSERT INTO recovery_runs (started_at) VALUES (?)",
                (time.time(),))
            return cur.lastrowid
        finally:
            conn.close()
    except Exception:
        _log.exception("recovery run record could not be written")
        return None


def recovery_run_finish(state_dir, run_id, outcome: str,
                        queue: bool = True) -> None:
    """Outcome + owner notice in ONE transaction: the completion record
    and its durable retryable delivery are never separated. A failure
    here is logged loudly, never hidden; the outcome text still reaches
    the owner via the in-band reply/status path."""
    from ..core.schedule import notice_on_conn
    try:
        conn = _run_db_conn(state_dir)
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                nid = None
                if run_id is not None:
                    conn.execute(
                        "UPDATE recovery_runs SET finished_at=?,"
                        " outcome=? WHERE id=?",
                        (time.time(), outcome, run_id))
                    row = conn.execute(
                        "SELECT notice_id FROM recovery_runs"
                        " WHERE id=?", (run_id,)).fetchone()
                    nid = row[0] if row else None
                # idempotent under retries: one completion notice per
                # run, no matter how often finish is retried
                if queue and nid is None:
                    nid = notice_on_conn(conn, outcome)
                    if run_id is not None:
                        conn.execute(
                            "UPDATE recovery_runs SET notice_id=?"
                            " WHERE id=?", (nid, run_id))
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        finally:
            conn.close()
    except Exception:
        _log.exception("recovery outcome could not be recorded durably")


def reconcile_recovery_runs(state_dir) -> int:
    """Boot reconcile: a run that never recorded an outcome was cut off
    with the service. Mark it finished with the TRUTH (its result is
    unknown; committed clips stay HELD, unfinished clips stay pending or
    fenced) and queue the owner notice in the same transaction. This
    NEVER replays recovery work or media."""
    from ..core.schedule import notice_on_conn
    state_dir = Path(state_dir)
    if not (state_dir / "nova.db").exists():
        return 0
    n = 0
    try:
        conn = _run_db_conn(state_dir)
        try:
            rows = conn.execute(
                "SELECT id FROM recovery_runs WHERE finished_at IS NULL"
            ).fetchall()
            for r in rows:
                text = ("A clip recovery run did not report its result "
                        "before the service stopped. Clips it committed "
                        "stay HELD; unfinished clips stay pending or "
                        "fenced. Re-send /recoverclips for a current "
                        "report.")
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute(
                        "UPDATE recovery_runs SET finished_at=?,"
                        " outcome=? WHERE id=?",
                        (time.time(), text, r[0]))
                    nid = notice_on_conn(conn, text)
                    conn.execute(
                        "UPDATE recovery_runs SET notice_id=?"
                        " WHERE id=?", (nid, r[0]))
                except BaseException:
                    conn.execute("ROLLBACK")
                    raise
                conn.execute("COMMIT")
                n += 1
        finally:
            conn.close()
    except Exception:
        _log.exception("recovery run reconcile failed")
    return n


class RecoveryTracker:
    """At most ONE background recovery run per service: /recoverclips
    returns a quick ack while edits (minutes per clip) run on a daemon
    thread, so the command loop (stopclip, sourceauth, status) is never
    blocked. The completion result is handed to notify() (the service
    queues it into the durable notice outbox); it is also kept for
    status(). A stranded /recoverclips is NEVER
    blind-replayed at boot (SIDE_EFFECTING in ops): the owner is asked
    to re-send instead."""

    def __init__(self):
        self._lock = threading.Lock()
        self._running_since = None
        self._last_result = None
        self._thread = None
        self._started = threading.Event()
        self._cancel = threading.Event()
        self._child_lock = threading.Lock()
        self._child_pgid = None
        self._child_birth = None
        self._ffmpeg = None

    def begin(self, state_dir, ffmpeg, ffprobe, notify) -> str:
        with self._lock:
            if self._running_since is not None:
                waited = int(time.time() - self._running_since)
                return (f"Recovery is already running ({waited}s). "
                        "You'll get the result when it finishes.")
            self._running_since = time.time()
            self._started.clear()
            self._cancel.clear()
            with self._child_lock:
                self._child_pgid = None
        # durable accepted-run record BEFORE the owner is told the run
        # started (the ack reply clears the inbox entry after this)
        run_id = recovery_run_start(state_dir)
        if run_id is None:
            # fail CLOSED: without durable acceptance no work starts and
            # no "started" ack is given; _thread stays joinable
            with self._lock:
                self._running_since = None
            self._thread = threading.Thread(target=lambda: None,
                                            daemon=True,
                                            name="recoverclips")
            self._thread.start()
            return ("Recovery could not be recorded durably, so "
                    "nothing was started (fail-closed). Please try "
                    "again; if this persists, see /logs.")
        self._thread = threading.Thread(
            target=self._run, args=(Path(state_dir), ffmpeg, ffprobe,
                                    notify, run_id),
            daemon=True, name="recoverclips")
        try:
            self._thread.start()
        except Exception:
            # never leave a permanent "running" lie: free the tracker,
            # record the truth, and tell the owner nothing ran
            with self._lock:
                self._running_since = None
            recovery_run_finish(
                state_dir, run_id,
                "Recovery could not start its background worker; "
                "nothing ran.", queue=False)
            return ("Recovery could not start its background worker; "
                    "nothing ran. Please re-send /recoverclips.")
        # handshake: return only once the worker has actually STARTED
        # the recovery call, never waiting for the (possibly long) work
        self._started.wait(5)
        return ("Recovery started in the background: finishing clips "
                "from interrupted sessions to HELD for your review. "
                "Nothing is delivered or queued for upload; you'll get "
                "the result here when it finishes.")

    def request_cancel(self) -> None:
        """Service shutdown: stop the run. The CURRENT edit child is
        killed (verified teardown), later clips are never started;
        unfinished clips return to PENDING for a later run."""
        self._cancel.set()
        self._kill_recorded_child()

    def wait_done(self, timeout: float = _RECOVERY_JOIN_S) -> bool:
        """Bounded drain for shutdown: True when no worker remains
        alive. A False return means ownership could NOT be retained
        through the current work - the caller must report that, never
        claim a clean stop."""
        t = self._thread
        if t is None:
            return True
        t.join(timeout)
        return not t.is_alive()

    def _on_spawn(self, pid, pgid, token):
        # cancel fence FIRST: a child bound after cancellation is
        # refused; edit_clip then tears down THAT exact child itself
        # (an unbindable writer never keeps running)
        if self._cancel.is_set():
            return False
        # record birth identity (kernel start time) so a later kill can
        # distinguish this child from a reused process-group number
        birth = None
        try:
            with open(f"/proc/{pid}/stat", "rb") as fh:
                birth = fh.read().rsplit(b")", 1)[1].split()[19]
        except Exception:
            birth = None
        with self._child_lock:
            self._child_pgid = pgid
            self._child_birth = birth
        return True

    def _kill_recorded_child(self) -> None:
        """Best-effort teardown of the CURRENT edit child (a process-
        group leader recorded at spawn). Fail-closed on identity: the
        group is only signalled while /proc still shows an ffmpeg
        leader, so a reused pgid is never hit."""
        import os
        import signal
        from ..editing.editor import _group_alive
        with self._child_lock:
            pgid = self._child_pgid
            birth = self._child_birth
        if pgid is None:
            return
        # birth identity: the group leader must be the SAME process
        # recorded at spawn (kernel start time), not a reused number
        try:
            with open(f"/proc/{pgid}/stat", "rb") as fh:
                now = fh.read().rsplit(b")", 1)[1].split()[19]
        except Exception:
            return                        # gone or unverifiable
        if birth is None or now != birth:
            _log.warning("recorded recovery child pgid %s failed birth "
                         "identity; not signalling", pgid)
            return
        # and its real executable must be the ffmpeg binary we spawned -
        # never an unrelated process with ffmpeg in its arguments
        import os as _os
        try:
            exe = _os.readlink(f"/proc/{pgid}/exe")
        except OSError:
            return
        if _os.path.basename(exe) != _os.path.basename(
                self._ffmpeg or "ffmpeg"):
            _log.warning("recorded recovery child pgid %s is %s, not "
                         "the spawned ffmpeg; not signalling", pgid, exe)
            return
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(pgid, sig)
            except (ProcessLookupError, PermissionError):
                return
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and _group_alive(pgid):
                time.sleep(0.05)
            if not _group_alive(pgid):
                return

    def _run(self, state_dir, ffmpeg, ffprobe, notify, run_id) -> None:
        try:
            result = self._execute(state_dir, ffmpeg, ffprobe)
        except Exception as exc:
            # the helper commits per clip and never rolls back committed
            # media; a top-level failure is reported, not hidden
            result = (f"Recovery stopped early ({exc.__class__.__name__}"
                      "). Already-finished clips stay HELD; nothing was "
                      "delivered. Re-send /recoverclips to continue.")
        # durable FIRST: outcome record and owner notice commit together
        # before anything in-band; a restart after this point can never
        # lose the result
        recovery_run_finish(state_dir, run_id, result)
        with self._lock:
            self._running_since = None
            self._last_result = result
            with self._child_lock:
                self._child_pgid = None
        try:
            notify(result)
        except Exception:
            # the durable record and queued notice already exist; the
            # callback is only a prompt-delivery wake
            _log.warning("recovery result callback failed")

    def _execute(self, state_dir, ffmpeg, ffprobe) -> str:
        # the tracked run ALWAYS invokes the recovery call (the ack
        # handshake marks exactly that point), even when there is
        # nothing to recover; the outcome text is derived from actual
        # row states afterwards
        from ..core.database import connect
        from ..pipeline.session import finalize_interrupted_sessions
        state_dir = Path(state_dir)
        db = state_dir / "nova.db"
        eligible: dict = {}
        if db.exists():
            conn = connect(str(db))
            try:
                eligible = _interrupted_unfinished(conn)
            finally:
                conn.close()
        self._ffmpeg = ffmpeg
        outcomes: list = []

        def _on_outcome(cid, oc, _acc=outcomes):
            _acc.append((cid, oc))
            # the clip's edit child is done: never keep a stale pgid
            with self._child_lock:
                self._child_pgid = None
                self._child_birth = None

        self._started.set()
        # positional: cooperative cancel between units, current-child
        # registration for verified teardown, exact per-clip outcomes;
        # the callee defaults keep older signatures working
        held = finalize_interrupted_sessions(
            state_dir, ffmpeg, ffprobe,
            lambda: not self._cancel.is_set(),
            self._on_spawn,
            _on_outcome)
        # "nothing to do" only when the run ACTUALLY did nothing: clips
        # that became eligible after the pre-read above are processed
        # and their exact outcomes (including failures) are reported,
        # never hidden behind the stale snapshot
        if held == 0 and not outcomes and not eligible:
            return ("Recovery: nothing to do - no interrupted sessions "
                    "with unfinished clips.")
        return _recovery_outcome_text(state_dir, held, eligible,
                                      outcomes)

    def status(self) -> str:
        with self._lock:
            if self._running_since is not None:
                waited = int(time.time() - self._running_since)
                suffix = " (stopping after the current clip)" \
                    if self._cancel.is_set() else ""
                return f"Recovery is running ({waited}s so far){suffix}."
            if self._last_result is not None:
                return self._last_result
            return "No recovery has run since this service started."


_RECOVERY_TRACKER = RecoveryTracker()


def recovery_tracker() -> RecoveryTracker:
    """The service-wide tracker (one background run at a time)."""
    return _RECOVERY_TRACKER


def recoverclips_reply_tracked(state_dir: Path, ffmpeg: str,
                               ffprobe: str, notify=None, *,
                               _tracker=None) -> str:
    """/recoverclips via the service: quick ack; the real work runs in
    the background (see RecoveryTracker). notify(result_text) is called
    once when the run finishes; the service wires it to the paired
    admin."""
    tracker = _tracker if _tracker is not None else _RECOVERY_TRACKER
    if notify is None:
        notify = lambda _text: None
    return tracker.begin(state_dir, ffmpeg, ffprobe, notify)
