"""Mode A: command-driven clipping sessions over an authorized feed.

/startclip starts a supervisor session for a registered source whose
feed the owner placed under state/feeds (<username>.url with a
credential-free http(s) URL, or <username>.mp4 local recording). The
session runs the REAL chain with no smoke-script orchestration:
record -> rolling buffer -> detect candidates -> auto-clip -> edit
(with planner outputs: preset + owner watermark) -> QC -> clip HELD
for explicit /deliveries flush. /stopclip stops sessions. A session
left RECORDING by a crash is marked INTERRUPTED on boot - never
silently resumed.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import os
import re
import threading
import uuid
import time
from pathlib import Path

from ..bot.restartproof import read_start_time, claim_owner_dead, \
    proof_for_session, record_spawn_intent, record_spawned
from ..core.database import connect, migrate
from ..core.logging import get_logger

log = get_logger("pipeline.session")


class ClipSession:
    DRAIN_WAIT_SECONDS = 90.0

    def __init__(self, ffmpeg: str, ffprobe: str, feed: str,
                 source_label: str, state_dir: Path,
                 segment_seconds: int = 5, tick_seconds: float = 3.0,
                 clip_pre_seconds: float = 20.0,
                 clip_post_seconds: float = 10.0,
                 playback: dict | None = None,
                 acceptance_deadline: float | None = None,
                 max_session_clips: int | None = None):
        # playback: None for mode A feeds. For public playback: a dict with
        # optional initial_resolved and test/owned-fixture hooks
        # (resolve_fn, capture_fn, opener, host_ok, worker_kwargs).
        if acceptance_deadline is not None and (playback is None or
                max_session_clips != 1):
            raise ValueError("bounded acceptance requires playback and max1")
        self.acceptance_deadline = acceptance_deadline
        self.max_session_clips = max_session_clips
        self.playback = playback
        self.ffmpeg = ffmpeg
        self.ffprobe = ffprobe
        self.feed = feed
        self.source_label = source_label
        self.state_dir = Path(state_dir)
        self.segment_seconds = segment_seconds
        self.tick_seconds = tick_seconds
        self.clip_pre_seconds = clip_pre_seconds
        self.clip_post_seconds = clip_post_seconds
        self._stopping = threading.Event()
        self._thread: threading.Thread | None = None
        self.error: str | None = None
        self.clips_finished = 0
        self.phase = "starting"
        # live http(s) feeds are re-gated while running; a local recording
        # is a fixture, not live capture
        self._live_gated = re.match(r"^[a-z][a-z0-9+.\-]*://",
                                    str(feed).strip().lower()) is not None
        self._gate_at = 0.0
        self._gate_ok = True     # last real gate result (retained, not assumed)
        self._gate_closed = False

    @property
    def terminal_kind(self):
        """Typed reason the playback worker stopped for good (restricted,
        challenge, blocked, unauthorized, quality, invalid), 'unauthorized'
        when recording authorization was revoked mid-run, else None."""
        if self._gate_closed:
            return "qualification"
        w = getattr(self, "_worker", None)
        if w is None:
            return None
        kind = getattr(w, "terminal_kind", None)
        if kind:
            return kind
        if getattr(w, "end_reason", None) == "gate_closed":
            return "qualification"
        if getattr(w, "end_reason", None) == "revoked":
            return "unauthorized"
        return None

    def _gate_still_ok(self, poll: float = 30.0) -> bool:
        """Blocking (the provider may be slow): run it through _owned, never
        on the event loop. Between polls the LAST real result is returned, so
        a False is never forgotten. The poll interval is a lower bound on
        how often the provider is asked, not a wall-clock guarantee."""
        import time as _t
        from ..core import content_gate
        now = _t.monotonic()
        if self._gate_at and now - self._gate_at < poll:
            return self._gate_ok
        ok = content_gate.check(self.source_label).ok
        self._gate_ok = ok
        self._gate_at = _t.monotonic()
        return ok

    def start(self) -> None:
        self._thread = threading.Thread(
            target=lambda: asyncio.run(self._run()), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stopping.set()

    def alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def join(self, timeout: float = 30.0) -> None:
        if self._thread:
            self._thread.join(timeout)

    def _notes(self, now: float) -> str:
        """Durable origin and authorization snapshot (session row)."""
        attempt = getattr(self, "attempt_id", None)
        from ..bot import restartproof as _rp
        if self.playback is None:
            if attempt is None:
                return "mode A session"
            return json.dumps(_rp.record_leader(
                {"mode": "feed", "sched_attempt": attempt}))
        from ..bot import source_auth
        return json.dumps(_rp.record_leader({
            "sched_attempt": attempt,
            "origin": "twitch_public_playback", "mode": "playback",
            "source": self.source_label,
            "record_authorized_at_start": source_auth.allowed(
                self.state_dir, self.source_label, "record"),
            "repost_authorized_at_start": source_auth.allowed(
                self.state_dir, self.source_label, "repost"),
            "started_at": now,
            "note": "authorization is re-read at every publish and capture "
                    "check; this snapshot is provenance only"}))

    def _finish_clips(self, session_id: int, out_dir: Path,
                      runtime_dir: Path) -> None:
        """Edit + QC pending clips. Runs on a worker thread (via
        asyncio.to_thread) so ffmpeg never stalls the recorder's event
        loop; opens its own DB connection for that thread."""
        from ..editing.editor import edit_clip
        from ..editing.presets import PRESETS
        from ..intelligence.memory import Memory
        from ..intelligence.planner import plan_clip
        from ..qc.verify import check_media
        conn = connect(str(self.state_dir / "nova.db"))
        try:
            self._finish_clips_db(conn, session_id, out_dir, runtime_dir)
        finally:
            conn.close()

    def _finish_clips_db(self, conn, session_id: int, out_dir: Path,
                         runtime_dir: Path) -> None:
        auth_on_conn = None
        if self.playback is not None:
            # the same claim/finalization authorization policy recovery
            # uses, applied where that policy actually applies: record
            # scope gates PUBLIC PLAYBACK capture (startplayback), so a
            # mid-finalize revocation blocks the claim and the edit.
            # Local-feed sessions never required record scope at start,
            # so no claim-time scope applies to them.
            def auth_on_conn(c, u=self.source_label):
                r = c.execute("SELECT value FROM settings WHERE key=?",
                              (f"srcauth:{u}:record",)).fetchone()
                return bool(r and r[0] == "1")
        held = finish_pending_clips(
            conn, state_dir=self.state_dir, session_id=session_id,
            source_label=self.source_label, out_dir=out_dir,
            runtime_dir=runtime_dir, ffmpeg=self.ffmpeg,
            ffprobe=self.ffprobe, auto_deliver=True,
            pre_spawn=getattr(self, "_pre_spawn", None),
            on_spawn=getattr(self, "_on_spawn_writer", None),
            auth_on_conn=auth_on_conn,
            enqueue_admission=self.acceptance_deadline is None)
        self.clips_finished += len(held)

    async def _run(self) -> None:
        """Outer guard: ANY failure in setup or run is a terminal error with
        the session row left INTERRUPTED (never stuck STARTING/RECORDING)."""
        self._session_id = None
        try:
            await self._run_inner()
        except BaseException as exc:
            self.phase = "interrupted"
            self.error = f"{exc.__class__.__name__}: {exc}"[:300]
            log.exception("clip session %s failed", self.source_label)
            sid = self._session_id
            if sid is not None:
                try:
                    c = connect(str(self.state_dir / "nova.db"))
                    c.execute("UPDATE stream_sessions SET state='INTERRUPTED',"
                              " ended_at=? WHERE id=? AND state IN"
                              " ('STARTING','RECORDING')", (time.time(), sid))
                    c.commit(); c.close()
                except Exception:
                    log.exception("could not mark failed session interrupted")
            if not isinstance(exc, Exception):
                raise
        finally:
            # every owned child and thread was drained before _run_inner
            # returned (a timed-out drain keeps the thread alive instead).
            # This is the only positive proof of termination.
            self._mark_drained()

    _inflight = 0

    @property
    def _inflight_lock(self):
        lk = self.__dict__.get("_inflight_lk")
        if lk is None:
            lk = self.__dict__.setdefault("_inflight_lk", threading.Lock())
        return lk

    async def _owned(self, fn, *args):
        """Run blocking work in a thread that is COUNTED thread-side: the
        count drops only when the thread function really returns, whatever
        happened to the awaiting coroutine (cancellation, repeated cancel)."""
        from ..core.owned import run_counted

        def inc():
            with self._inflight_lock:
                self._inflight += 1

        def dec():
            with self._inflight_lock:
                self._inflight -= 1
        return await run_counted(lambda: fn(*args), inc, dec)

    def _owner_drained(self, task=None) -> bool:
        """Positive attestation by every owner: the recorder task finished,
        the worker attests no process/thread of its own remains, no
        session-owned executor work is outstanding, AND every writer in
        the session's registry is verifiably dead with an empty group.
        Executor completion alone never releases ownership: when cleanup
        failed or anything is unverifiable, ownership stays unresolved
        (fail closed) rather than attesting drained."""
        w = getattr(self, "_worker", None)
        if w is None or not hasattr(w, "drained"):
            return False
        try:
            if task is not None and not task.done():
                return False
            with self._inflight_lock:
                if self._inflight:
                    return False
            if not bool(w.drained()):
                return False
        except Exception:
            return False
        return self._registered_writers_dead()

    def _registered_writers_dead(self) -> bool:
        """Fail-closed check of this session's writer registry: every
        recorded intent must be bound, every bound pid verifiably dead
        (start-time comparison, PID-reuse safe), and every recorded group
        verifiably empty. An unreadable registry, an unresolved intent,
        a live pid, a surviving group member, or any visibility
        uncertainty is NOT death."""
        import os
        from ..bot import restartproof as _rp
        sid = getattr(self, "_session_id", None)
        if sid is None:
            return True              # no session registry could exist yet
        registry = (Path(self.state_dir) / "sessions" / self.source_label
                    / f"writers-{sid}.json")
        # the manifest is initialized before any session work, so a
        # missing file means a LOST manifest, not "no writers": block
        if not registry.exists():
            return False
        items = _rp._load_registry(registry)
        if items is None or not isinstance(items, list):
            return False             # unreadable registry: unknown writers
        for it in items:
            if not isinstance(it, dict) or "pid" not in it:
                return False         # unresolved intent: writer may exist
            # explicit complete schema: pid, start and pgid must all be
            # valid positive identities; a missing or malformed field
            # fails closed (a missing start must never look like reuse)
            if not (_rp._valid_pos_int(it.get("pid"))
                    and _rp._valid_pos_int(it.get("start"))
                    and _rp._valid_pos_int(it.get("pgid"))):
                return False
            dead, _why = _rp._pid_dead(it["pid"], it["start"], "/proc")
            if not dead:
                return False
            try:
                os.killpg(it["pgid"], 0)
                return False         # group still has members
            except ProcessLookupError:
                pass                 # group verifiably empty
            except Exception:
                return False         # uncertainty is not death
        return True

    def _mark_drained(self) -> None:
        """Persist the terminal outcome always; persist drained=true ONLY if
        every owner attested (never inferred from reaching this point)."""
        proven = bool(getattr(self, "_owners_drained", False)) and \
            self._owner_drained(getattr(self, "_task", None))
        sid = getattr(self, "_session_id", None)
        if sid is not None:
            try:
                c = connect(str(self.state_dir / "nova.db"))
                try:
                    row = c.execute("SELECT notes FROM stream_sessions WHERE id=?",
                                    (sid,)).fetchone()
                    try:
                        n = json.loads(row[0]) if row and row[0] else {}
                    except Exception:
                        n = {}
                    if proven:
                        n["drained"] = True
                    tk = self.terminal_kind
                    if tk:
                        n["terminal"] = tk
                    c.execute("UPDATE stream_sessions SET notes=? WHERE id=?",
                              (json.dumps(n), sid))
                    c.commit()
                finally:
                    c.close()
            except Exception:
                log.exception("could not record drained session")
        if proven:
            self._finalized = True
            hook = getattr(self, "_on_finalized", None)
            if hook is not None:
                try:
                    hook()
                except Exception:
                    log.exception("finalized hook failed")

    @staticmethod
    def _mark_state(db_path, session_id, state, error=None) -> None:
        try:
            c = connect(db_path)
            c.execute("UPDATE stream_sessions SET state=?, ended_at=? WHERE id=?",
                      (state, time.time(), session_id))
            if error:
                row = c.execute("SELECT notes FROM stream_sessions WHERE id=?",
                                (session_id,)).fetchone()
                try:
                    n = json.loads(row[0]) if row and row[0] else {}
                except Exception:
                    n = {}
                n["error"] = error
                c.execute("UPDATE stream_sessions SET notes=? WHERE id=?",
                          (json.dumps(n), session_id))
            c.commit(); c.close()
        except Exception:
            log.exception("session state mark failed")

    def _mark_recording(self) -> None:
        """Worker callback: first validated segments exist."""
        sid = self._session_id
        if sid is None:
            return
        self.phase = "recording"
        try:
            c = connect(str(self.state_dir / "nova.db"))
            c.execute("UPDATE stream_sessions SET state='RECORDING' WHERE id=?"
                      " AND state='STARTING'", (sid,))
            c.commit(); c.close()
        except Exception:
            log.exception("mark recording failed")

    async def _run_inner(self) -> None:
        from ..clipping.auto import AutoClipper
        from ..core.lifecycle import HealthRegistry, Service, Supervisor
        from ..detection.detector import CandidateDetector
        from ..intelligence.providers import HeuristicProvider
        from ..recording.buffer import RollingBuffer, ffprobe_probe
        from ..recording.ffmpeg import FFmpegRunner
        from ..recording.worker import RecordingWorker

        work = self.state_dir / "sessions" / self.source_label
        work.mkdir(parents=True, exist_ok=True)
        db_path = str(self.state_dir / "nova.db")
        conn = connect(db_path)
        migrate(conn)
        now = time.time()
        conn.execute(
            "INSERT OR IGNORE INTO twitch_sources (username, created_at,"
            " updated_at) VALUES (?,?,?)", (self.source_label, now, now))
        source_id = conn.execute(
            "SELECT id FROM twitch_sources WHERE username=?",
            (self.source_label,)).fetchone()[0]
        cur = conn.execute(
            "INSERT INTO stream_sessions (source_id, state, started_at,"
            " ingest_mode, notes) VALUES (?,?,?,?,?)",
            (source_id,
             "STARTING" if self.playback is not None else "RECORDING", now,
             "playback" if self.playback is not None else "feed",
             self._notes(now)))
        session_id = cur.lastrowid
        self._session_id = session_id
        conn.commit()
        conn.close()

        # durable writer manifest BEFORE any session work: after this,
        # a missing manifest means it was LOST, and lost blocks every
        # drained attestation (fail closed) instead of passing as
        # "no writers". Failure here fails the session before any
        # writer exists.
        from ..bot import restartproof as _rp
        if not _rp.init_registry(work / f"writers-{session_id}.json"):
            raise RuntimeError(
                f"writer manifest for session {session_id} not "
                "initializable; refusing to run unrecorded")

        buf = RollingBuffer(str(work), session_id=session_id,
                            segment_seconds=self.segment_seconds,
                            pre_event_seconds=30, post_event_seconds=30,
                            max_storage_mb=256)
        # durable writer registry: every owned writer process is recorded
        # at spawn so a later boot can POSITIVELY prove it dead (restart
        # fence release); never used as evidence of life while we run
        from ..bot import restartproof as _rp
        writers_registry = work / f"writers-{session_id}.json"
        def _pre_spawn(_path=writers_registry):
            # durable intent BEFORE the child exists; None forbids spawning
            return _rp.record_spawn_intent(_path)
        def _register(pid, pgid, token, _path=writers_registry):
            # False means the caller terminates the just-spawned child:
            # an unidentifiable writer never keeps running
            return _rp.record_spawned(_path, token, pid, pgid)
        self._pre_spawn, self._on_spawn_writer = _pre_spawn, _register
        buf.probe = ffprobe_probe(self.ffprobe, pre_spawn=_pre_spawn,
                                  on_spawn=_register)
        runner = FFmpegRunner(self.ffmpeg, self.ffprobe, max_concurrent=1,
                              pre_spawn=_pre_spawn, on_spawn=_register)
        if self.playback is not None:
            from ..recording.playback_worker import PlaybackWorker
            pb = dict(self.playback)
            worker = PlaybackWorker(
                buf, self.source_label, db_path, self.state_dir, self.ffmpeg,
                initial_resolved=pb.pop("initial_resolved", None),
                on_recording=self._mark_recording,
                ffprobe=self.ffprobe, pre_spawn=_pre_spawn, on_spawn=_register,
                acceptance_deadline=self.acceptance_deadline,
                single_cycle=self.acceptance_deadline is not None, **pb)
        else:
            worker = RecordingWorker(runner, buf, self.feed, db_path,
                                     sweep_seconds=max(1, self.segment_seconds))
        self._worker = worker
        detector = CandidateDetector(self.ffmpeg, buf, db_path)
        # extraction children are registered writers too: a crash
        # mid-extraction must not leave an unrecorded writer behind
        from ..clipping.extractor import extract_clip as _extract
        import functools
        clipper = AutoClipper(buf, db_path, HeuristicProvider(),
                              self.ffmpeg, str(work),
                              pre_seconds=self.clip_pre_seconds,
                              post_seconds=self.clip_post_seconds,
                              max_session_clips=self.max_session_clips,
                              extract_fn=functools.partial(
                                  _extract, pre_spawn=_pre_spawn,
                                  on_spawn=_register))
        sup = Supervisor(
            [Service("record", worker.run,
                     heartbeat_seconds=max(1, self.segment_seconds - 1),
                     restart=False)],  # a session's recorder completes;
            HealthRegistry())          # live-feed restart is future work
        task = asyncio.create_task(sup.run())
        self._task = task
        crashed = False
        finishing = False
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(self.tick_seconds)
                if self.playback is None and self._live_gated:
                    # check BEFORE and after each expensive step; the result
                    # is retained, so a closed gate stops downstream work
                    if not await self._owned(self._gate_still_ok):
                        self._gate_closed = True
                        self.error = "live title gate closed; capture ended"
                        break
                await self._owned(detector.process_new_segments)
                if (self.playback is None and self._live_gated
                        and not self._gate_ok):
                    self._gate_closed = True
                    self.error = "live title gate closed; capture ended"
                    break
                # closed segments become durable recording_segments rows
                # WHILE recording (workers only reconcile at start/stop);
                # manual-mark coverage depends on these rows
                await self._owned(reconcile_durable_segments, buf, db_path)
                await self._owned(clipper.process_pending)
                await self._owned(notify_mark_outcomes, self.state_dir)
                if not finishing:
                    # edit/QC is heavy; never block the recorder loop
                    finishing = True
                    try:
                        await self._owned(
                            self._finish_clips, session_id, work,
                            self.state_dir / "runtime")
                    finally:
                        finishing = False
                if self.playback is not None and getattr(worker, "finished", False):
                    # the Supervisor swallows service crashes and idles until
                    # stopped, so the playback worker reports its own end
                    if worker.end_reason in ("failed", "not_started"):
                        raise RuntimeError(worker.error_text or "playback failed")
                    break
                if task.done():
                    exc = task.exception() if not task.cancelled() \
                        else None
                    if exc is not None:
                        raise exc
                    break  # finite feed exhausted
        except Exception as exc:
            crashed = True
            self.error = f"{exc.__class__.__name__}: {exc}"
            log.exception("clip session %s failed", self.source_label)
        finally:
            self._stopping.set()
            sup.stopping.set()
            self.phase = "stopping"
            skip_finalize = False
            try:
                if self.playback is not None:
                    # asyncio.wait never cancels the task on timeout; a
                    # repeated cancellation must not abandon cleanup
                    t_end = time.monotonic() + self.DRAIN_WAIT_SECONDS
                    while not task.done() and time.monotonic() < t_end:
                        try:
                            await asyncio.wait({task}, timeout=0.5)
                        except asyncio.CancelledError:
                            continue
                else:
                    await asyncio.wait_for(task, timeout=20)
            except Exception:
                pass
            # Ownership: this session stays ALIVE and OWNS the slot until EVERY
            # owner attests it is drained (recorder task, worker processes and
            # threads, session executor work). Teardown failures are retried,
            # never skipped; never analyze or finalize under a live writer.
            worker_was_live = False
            attested = False
            while True:
                if self._owner_drained(task):
                    attested = True
                    break
                if not task.done() or not (getattr(worker, "drained", lambda: False)()):
                    worker_was_live = True
                    self.phase = "cleanup-pending"
                    self.error = ("recorder or helper still running after stop; "
                                  "cleanup pending, analysis skipped")
                    self._mark_state(db_path, session_id, "INTERRUPTED")
                ens = getattr(worker, "ensure_stopped", None)
                if ens is not None:
                    try:
                        await ens()
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass            # retried next round; ownership is kept
                try:
                    await asyncio.sleep(0.5)
                except asyncio.CancelledError:
                    continue
            if worker_was_live:
                self.phase = "interrupted"
                self.error = ("cleanup completed late; session interrupted, "
                              "analysis skipped")
                self._mark_state(db_path, session_id, "INTERRUPTED")
                skip_finalize = True
            self._owners_drained = attested
            # finalize: analyze what remains, clip, finish
            if not skip_finalize:
                self.phase = "finalizing"
                try:
                    # every writer attested drained above, so the capture's
                    # final segment is safe to analyze here (it is skipped
                    # during the live loop as possibly-still-written); the
                    # buffer tail closes so its true duration persists
                    buf.mark_drained()
                    await self._owned(detector.process_new_segments, True)
                    await self._owned(reconcile_durable_segments, buf,
                                      db_path)
                    await self._owned(clipper.process_pending)
                    await self._owned(notify_mark_outcomes, self.state_dir)
                    await self._owned(
                        self._finish_clips, session_id, work,
                        self.state_dir / "runtime")
                    conn = connect(db_path)
                    if self.playback is not None:
                        try:
                            row = conn.execute("SELECT notes FROM stream_sessions WHERE id=?", (session_id,)).fetchone()
                            n = json.loads(row[0]) if row and row[0] else {}
                            n["end_reason"] = getattr(worker, "end_reason", "unknown")
                            n["error"] = self.error
                            conn.execute("UPDATE stream_sessions SET notes=? WHERE id=?", (json.dumps(n), session_id))
                        except Exception:
                            log.exception("playback notes update failed")
                    conn.execute(
                        "UPDATE stream_sessions SET state=?, ended_at=?"
                        " WHERE id=?",
                        ("INTERRUPTED" if crashed else "ENDED",
                         time.time(), session_id))
                    conn.commit()
                    conn.close()
                    self.phase = "interrupted" if crashed else "ended"
                except Exception as exc:
                    log.error("session finalize failed: %s", exc.__class__.__name__)
                    # sanitized (class name only), durable, never left STARTING/RECORDING
                    self.phase = "interrupted"
                    self.error = f"finalize failed: {exc.__class__.__name__}"
                    self._mark_state(db_path, session_id, "INTERRUPTED", self.error)


def reconcile_durable_segments(buf, db_path) -> int:
    """Record CLOSED on-disk segments as durable recording_segments rows
    during recording. reconcile() never writes the open tail."""
    try:
        return buf.reconcile(db_path)
    except Exception as exc:
        log.warning("live segment reconcile failed: %s", exc.__class__.__name__)
        return 0


def notify_mark_outcomes(state_dir) -> int:
    from ..bot import ops
    try:
        return ops.notify_manual_mark_outcomes(Path(state_dir))
    except Exception as exc:
        log.warning("mark outcome notify failed: %s", exc.__class__.__name__)
        return 0


def recover_interrupted_sessions(state_dir: Path) -> int:
    """Crash recovery: sessions left RECORDING by a dead process are
    marked INTERRUPTED. Never silently resumed."""
    db = Path(state_dir) / "nova.db"
    if not db.exists():
        return 0
    conn = connect(str(db))
    # boot recovery can run before anything else on an old state dir
    # whose schema predates stream_sessions; migrate is idempotent
    migrate(conn)
    cur = conn.execute(
        "UPDATE stream_sessions SET state='INTERRUPTED', ended_at=?"
        " WHERE state IN ('RECORDING','STARTING')", (time.time(),))
    conn.commit()
    conn.close()
    return cur.rowcount


class _AuthorizationRevoked(Exception):
    pass


class _ClaimLost(Exception):
    pass


def _claim_clip(conn, clip_id, token: str, auth_on_conn=None) -> bool:
    """Exclusive owner-token claim, one short transaction. Only a free
    PENDING row can be claimed; rowcount 1 wins. Interrupted RECOVERING
    rows are NOT reclaimed here: only reclaim_dead_claims may reset
    one, and only on positive proof the exact recorded owner process
    is dead; an unprovable claim stays fenced forever. The claim and
    the owner process's birth evidence (pid + /proc start tick +
    process group) commit atomically; when that evidence cannot be
    read the claim is refused (fail closed). When the caller supplies
    auth_on_conn, CURRENT record authorization is re-read inside this
    same transaction BEFORE the claim: a revocation landing after any
    earlier preflight prevents the claim - and therefore the edit -
    entirely."""
    my_start = read_start_time(os.getpid())
    if my_start is None:
        # fail closed BEFORE claiming: a claim whose owner process
        # cannot later be proven dead would fence the clip forever
        log.warning("claim refused for clip %s: own process birth "
                    "unreadable", clip_id)
        return False
    conn.execute("BEGIN IMMEDIATE")
    try:
        if auth_on_conn is not None and not auth_on_conn(conn):
            conn.rollback()
            raise _AuthorizationRevoked()
        cur = conn.execute(
            "UPDATE clips SET status='RECOVERING', recovery_owner=?,"
            " claimed_at=? WHERE id=? AND status='PENDING'"
            " AND recovery_owner IS NULL",
            (token, time.time(), clip_id))
        won = cur.rowcount == 1
        if won:
            # the claim and its owner-process birth evidence commit
            # ATOMICALLY: a later recovery can reclaim this claim only
            # on positive proof this exact process (and group) is dead
            conn.execute(
                "INSERT INTO clip_claims (clip_id, token, pid, start,"
                " pgid, claimed_at) VALUES (?,?,?,?,?,?)",
                (clip_id, token, os.getpid(), my_start, os.getpgrp(),
                 time.time()))
        conn.commit()
        return won
    except Exception:
        conn.rollback()
        raise


def _release_claim(conn, clip_id, token: str) -> None:
    """Return an owned claim to PENDING, but only while we still own the
    token; a lost claim is left fenced, never reset."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE clips SET status='PENDING', recovery_owner=NULL,"
            " claimed_at=NULL WHERE id=? AND status='RECOVERING'"
            " AND recovery_owner=?", (clip_id, token))
        conn.execute(
            "UPDATE clip_claims SET finished_at=? WHERE clip_id=?"
            " AND token=?", (time.time(), clip_id, token))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def finish_pending_clips(conn, *, state_dir, session_id: int, source_label,
                         out_dir: Path, runtime_dir: Path, ffmpeg, ffprobe,
                         auto_deliver: bool = False, pre_spawn=None,
                         on_spawn=None, enqueue_admission: bool = True,
                         per_clip_authorization=None,
                         claim_token: str | None = None,
                         output_name=None,
                         input_resolver=None,
                         auth_on_conn=None,
                         require_sealed: bool = False,
                         outcome_cb=None) -> list:
    """Edit + QC every PENDING clip row of one session: plan, edit,
    ffprobe-measured final duration, atomic HELD + metadata + admission
    outbox per clip. Per-clip isolated and idempotent: only PENDING rows
    are touched, so HELD/admitted clips and their delivery state are
    never re-processed. The repost_allowed provenance gate decides the
    admission enqueue, exactly as in the live finalize path. auto_deliver
    (owner-preference immediate delivery) is used ONLY by the live
    finalize path; boot recovery passes False so nothing is flushed."""
    from ..editing.editor import edit_clip, SpawnRefused, SpawnUncertain
    from ..editing.presets import PRESETS
    from ..intelligence.memory import Memory
    from ..intelligence.planner import plan_clip
    from ..qc.verify import check_media, probe
    state_dir = Path(state_dir)
    if claim_token is None:
        # every finalizer claims under an explicit owner token; the live
        # path's token names its session, recovery's names its run
        claim_token = f"live-{session_id}-{uuid.uuid4().hex[:8]}"
    from ..clipping import seal as _seal
    mem = Memory(str(state_dir / "memory.json"))

    def _outcome(clip_id, outcome):
        # exact per-clip terminal outcomes for the caller's run report;
        # reporting must never break finishing
        if outcome_cb is None:
            return
        try:
            outcome_cb(clip_id, outcome)
        except Exception:
            log.warning("outcome callback failed for clip %s", clip_id)
    # require_sealed fences legacy/unsealed rows: recovery only ever
    # touches input carrying durable producer completion evidence
    rows = conn.execute(
        "SELECT id, file_path, duration_seconds, sealed_sha256,"
        " sealed_bytes FROM clips WHERE session_id=?"
        " AND status='PENDING'"
        + (" AND sealed_sha256 IS NOT NULL" if require_sealed else ""),
        (session_id,)).fetchall()
    held_ids = []
    finished = 0
    for clip_id, path, dur, seal_sha, seal_bytes in rows:
        # exclusive ownership: the claim is an owner-token CAS shared by
        # the original finalizer and recovery. rowcount 1 wins; anything
        # else (already HELD, owned by another run, fenced RECOVERING)
        # is skipped, never stolen.
        try:
            if not _claim_clip(conn, clip_id, claim_token, auth_on_conn):
                continue
        except _AuthorizationRevoked:
            # revoked between preflight and claim: the claim was never
            # taken, the row stays PENDING, and no edit is started
            log.warning("authorization revoked before claim; leaving "
                        "remaining clips PENDING")
            break
        # a long edit can span an authorization change: re-check before
        # EVERY clip when the caller supplies a live check (boot recovery
        # does; the live finalize path keeps its existing behavior)
        if per_clip_authorization is not None and \
                not per_clip_authorization():
            log.warning("authorization changed; leaving %d clip(s) PENDING",
                        len(rows) - len(held_ids))
            _release_claim(conn, clip_id, claim_token)
            _outcome(clip_id, "interrupted")
            break
        edited = None
        published = False
        try:
            # the caller may substitute a verified private input (the
            # recovery snapshot); None from the resolver means the input
            # could not be trusted: release the claim, fence the clip
            input_path = path
            if input_resolver is not None:
                input_path = input_resolver(
                    clip_id, path, seal_sha, seal_bytes)
                if input_path is None:
                    _release_claim(conn, clip_id, claim_token)
                    # verified-untrusted input is a real run outcome:
                    # claimed, checked, fenced back to PENDING
                    _outcome(clip_id, "fenced")
                    continue
            plan = plan_clip("auto", source_label, mem,
                             content_seconds=float(dur or 0))
            # SPAWN FENCE: re-attest the exact claim against the row's
            # CURRENT state immediately before any writer exists. A
            # claim that was reclaimed (owner proven dead) or otherwise
            # lost while we planned fails here and no edit child is
            # ever spawned for it: a paused owner resuming after a
            # reclaim cannot produce a second writer for the same clip.
            cur = conn.execute(
                "UPDATE clips SET claimed_at=? WHERE id=?"
                " AND status='RECOVERING' AND recovery_owner=?",
                (time.time(), clip_id, claim_token))
            if cur.rowcount != 1:
                log.warning("claim lost before edit spawn for clip %s;"
                            " no writer started", clip_id)
                _outcome(clip_id, "interrupted")
                continue
            if output_name is None:
                # per-attempt exclusive output ownership: the path is
                # reserved O_CREAT|O_EXCL before any writer is spawned.
                # A name alone - random or deterministic - is not a
                # reservation.
                edited = _seal.reserve_output(
                    out_dir, f"clip-edited-{clip_id}", ".mp4")
            else:
                edited = _seal.reserve_exact_or_unique(
                    Path(out_dir) / output_name(clip_id))
            edit_clip(ffmpeg, str(input_path), str(edited),
                      replace(PRESETS.get(plan.preset, PRESETS["vertical"]),
                              max_duration_seconds=max(1, min(180, plan.target_seconds))),
                      None, 0.5, str(runtime_dir),
                      watermark=plan.watermark or None,
                      pre_spawn=pre_spawn, on_spawn=on_spawn,
                      framing=plan.framing)
            qc = check_media(ffprobe, ffmpeg, str(edited),
                             1.0, 100, 100, True)
            if qc.ok:
                measured = None
                try:
                    measured = float(
                        probe(ffprobe, str(edited))
                        .get("format", {}).get("duration", 0) or 0)
                except Exception:
                    measured = None
                final_dur = (measured if measured and measured > 0
                             else float(dur or 0))
                # durability before durability evidence: the final media
                # and its directory are fsynced BEFORE the durable HELD
                # commit, so HELD never names a file a crash could lose
                _seal.fsync_tree(edited)
                from ..bot.handoff import ADMIT_JOB_KIND
                from ..core.jobs import JobQueue
                conn.execute("BEGIN IMMEDIATE")
                try:
                    # auth and the status effect in ONE transaction: a
                    # revocation landing mid-edit wins the race
                    if auth_on_conn is not None and not auth_on_conn(conn):
                        raise _AuthorizationRevoked()
                    cur = conn.execute(
                        "UPDATE clips SET file_path=?, preset=?,"
                        " duration_seconds=?, status='HELD',"
                        " qc_report_json=?, recovery_owner=NULL,"
                        " claimed_at=NULL"
                        " WHERE id=? AND status='RECOVERING'"
                        " AND recovery_owner=?",
                        (str(edited), plan.preset, final_dur,
                         json.dumps({"qc_passed": True,
                                     "plan_reasons": plan.reasons}),
                         clip_id, claim_token))
                    if cur.rowcount != 1:
                        raise _ClaimLost()
                    conn.execute(
                        "UPDATE clip_claims SET finished_at=?"
                        " WHERE clip_id=? AND token=?",
                        (time.time(), clip_id, claim_token))
                    ev = conn.execute(
                        "SELECT metadata_json FROM candidate_events"
                        " WHERE id = (SELECT event_id FROM clips"
                        " WHERE id = ?)", (clip_id,)).fetchone()
                    try:
                        meta = (json.loads(ev["metadata_json"])
                                if ev and ev["metadata_json"] else {})
                        if not isinstance(meta, dict):
                            meta = {}
                    except Exception:
                        meta = {}
                    meta["title"] = plan.title
                    conn.execute(
                        "UPDATE candidate_events SET metadata_json = ?"
                        " WHERE id = (SELECT event_id FROM clips"
                        " WHERE id = ?)", (json.dumps(meta), clip_id))
                    from ..core.provenance import repost_allowed
                    # boot recovery passes enqueue_admission=False: clips
                    # reach HELD only, never an automatic release effect
                    if enqueue_admission and repost_allowed(conn, clip_id)[0]:
                        JobQueue(conn).enqueue(
                            ADMIT_JOB_KIND, {"clip_row_id": int(clip_id)})
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                published = True
                # PUBLICATION COMMIT POINT: the row is durably HELD and
                # the admission outbox (when enabled) is committed in
                # the same transaction. Everything below is ancillary:
                # it must never roll back, unlink or fail the published
                # clip. Cleanup of the owned output is only for
                # unpublished pre-commit failures (handled above).
                finished += 1
                held_ids.append(clip_id)
                _outcome(clip_id, "HELD")
                try:
                    mem.remember(
                        "clip", f"session:{session_id}:{clip_id}",
                        f"{final_dur:.0f}s {plan.preset} clip from "
                        f"{source_label}", "mode A session",
                        confidence="observed")
                except Exception as exc:
                    log.warning("post-commit memory note failed for "
                                "clip %s (clip stays HELD, output "
                                "preserved): %s", clip_id, exc)
            else:
                conn.execute(
                    "UPDATE clips SET status='QC_FAILED',"
                    " qc_report_json=?, recovery_owner=NULL,"
                    " claimed_at=NULL"
                    " WHERE id=? AND recovery_owner=?",
                    (json.dumps({"qc_passed": False}),
                     clip_id, claim_token))
                conn.execute(
                    "UPDATE clip_claims SET finished_at=? WHERE clip_id=?"
                    " AND token=?", (time.time(), clip_id, claim_token))
                conn.commit()
                _outcome(clip_id, "QC_FAILED")
        except _AuthorizationRevoked:
            conn.rollback()
            edited.unlink(missing_ok=True)   # our own unique output only
            _release_claim(conn, clip_id, claim_token)
            log.warning("authorization revoked mid-finish; clip %s "
                        "returned to PENDING", clip_id)
            _outcome(clip_id, "fenced")
            break
        except _ClaimLost:
            # someone else took the row mid-edit: leave it untouched
            # (fenced), remove only our own unique output
            conn.rollback()
            edited.unlink(missing_ok=True)
            log.warning("claim lost for clip %s; left fenced", clip_id)
            _outcome(clip_id, "fenced")
            continue
        except SpawnUncertain as exc:
            # a writer WAS spawned but could not be bound and its death
            # is not positively confirmed: it may still be alive,
            # writing the reserved output. Keep the claim (the row
            # stays RECOVERING with our owner token and an unfinished
            # claim record), keep the reserved output, and leave the
            # spawn intent unresolved so future reclaim proof blocks:
            # fenced, no retry, no delete.
            conn.rollback()
            log.warning("edit writer unbound, death unconfirmed for "
                        "clip %s (%s); left fenced as RECOVERING with "
                        "output preserved", clip_id, exc)
            _outcome(clip_id, "interrupted")
            continue
        except SpawnRefused as exc:
            # the writer was REFUSED, not run: the durable spawn intent
            # was not recordable (no child ever existed) or the spawned
            # child's death was positively confirmed after the bind
            # failure. Not an edit failure: the clip goes back to
            # PENDING intact, never burned to terminal FAILED, so a
            # manifest-complete session or a separately reviewed
            # legacy import path can still recover it. Only our own
            # reserved, never-published output is removed.
            conn.rollback()
            if edited is not None:
                edited.unlink(missing_ok=True)
            _release_claim(conn, clip_id, claim_token)
            log.warning("edit spawn refused for clip %s (%s); "
                        "returned to PENDING", clip_id, exc)
            _outcome(clip_id, "interrupted")
            continue
        except Exception as exc:
            if published:
                # ancillary post-commit failure only: the clip IS
                # durably HELD - never report a second, contradictory
                # terminal result and never touch committed media
                log.warning("post-commit ancillary step failed for "
                            "clip %s (clip stays HELD, output "
                            "preserved): %s", clip_id, exc)
                continue
            if per_clip_authorization is not None and \
                    not per_clip_authorization():
                # interrupted by an external stop (shutdown cancel)
                # mid-edit: NOT a clip failure - the row goes back to
                # PENDING for a later run, never marked FAILED. Only
                # our own unpublished output is removed.
                if edited is not None and not published:
                    edited.unlink(missing_ok=True)
                try:
                    _release_claim(conn, clip_id, claim_token)
                except Exception:
                    log.warning("claim release after interruption "
                                "failed for clip %s; left fenced",
                                clip_id)
                log.warning("finish interrupted for clip %s; returned "
                            "to PENDING", clip_id)
                _outcome(clip_id, "interrupted")
                break
            log.warning("finish failed for clip %s: %s", clip_id, exc)
            if edited is not None and not published:
                # our own reserved per-attempt output that was NEVER
                # published; remove it. After the durable commit the
                # guard holds: committed media is never unlinked here,
                # no matter which ancillary call raised.
                edited.unlink(missing_ok=True)
            conn.execute(
                "UPDATE clips SET status='FAILED', recovery_owner=NULL,"
                " claimed_at=NULL WHERE id=? AND recovery_owner=?",
                (clip_id, claim_token))
            conn.execute(
                "UPDATE clip_claims SET finished_at=? WHERE clip_id=?"
                " AND token=?", (time.time(), clip_id, claim_token))
            conn.commit()
            _outcome(clip_id, "FAILED")
    conn.commit()
    if held_ids and auto_deliver:
        # automatic delivery ergonomics: when the owner preference
        # is on (default), QC-passed clips go to the paired admin
        # right away instead of waiting for /deliveries flush.
        # Per-clip journaled in ops.deliver_one_clip; a delivery
        # failure leaves the clip HELD, never fails the finish.
        try:
            from ..bot import ops
            outcome = ops.auto_deliver_clips(state_dir, held_ids)
            if outcome["enabled"]:
                log.info("auto-delivery for session %s: %d sent, "
                         "%d uncertain", session_id,
                         outcome["delivered"], outcome["uncertain"])
        except Exception as exc:
            log.warning("auto-delivery skipped for session %s: %s",
                        session_id, exc)
    return held_ids


# claim owner token families: exactly the shapes _claim_clip is ever
# called with - live-<session id>-<8 hex> from the live finalizer and
# recovery-<12 hex> from a /recoverclips run. Anything else is not a
# claim this code can reason about and is fenced forever.
_LIVE_TOKEN = re.compile(r"live-(\d+)-([0-9a-f]{8})")
_RECOVERY_TOKEN = re.compile(r"recovery-[0-9a-f]{12}")


def _claim_dead_owner(conn, clip_row, username: str, state_dir: Path,
                      proc: str = "/proc") -> tuple[bool, str]:
    """(True, why) ONLY on positive proof that BOTH the claim's exact
    owner process AND the session's full writer set are dead. Never
    infers death from incomplete visibility; an unprovable claim stays
    fenced forever.

    Legs, after the owner token is validated against a known family
    (and for live-*, against THIS clip's session):
      1. If a clip_claims birth record exists for this exact
         (clip, token), the recorded owner process (pid + start tick +
         group) must be verifiably dead with no survivor in its group.
         A recovery-* claim without a record, or any already-finished
         record, can never be proven and stays fenced.
      2. ALWAYS the full session writer proof: recorded leader + every
         durably registered writer dead, no unresolved spawn intent
         (a lost registration blocks forever), no survivor in the
         recorded groups, complete /proc visibility, and the
         writer-quiet backstop. An owner-dead claim NEVER bypasses
         this: an edit child starts its own process group, so the
         owner's empty group says nothing about a surviving exporter.
    """
    token = clip_row["recovery_owner"] or ""
    sid = int(clip_row["session_id"])
    m = _LIVE_TOKEN.fullmatch(token)
    if m is not None and int(m.group(1)) != sid:
        m = None            # a live claim of a DIFFERENT session
    if m is None and _RECOVERY_TOKEN.fullmatch(token) is None:
        return False, "unrecognized claim owner identity"
    claim = conn.execute(
        "SELECT pid, start, pgid, finished_at FROM clip_claims"
        " WHERE clip_id=? AND token=?",
        (int(clip_row["id"]), token)).fetchone()
    if claim is not None:
        if claim["finished_at"] is not None:
            return False, "claim already finished"
        ok, why = claim_owner_dead(int(claim["pid"]),
                                   int(claim["start"]),
                                   int(claim["pgid"]), proc)
        if not ok:
            return False, why
    elif _RECOVERY_TOKEN.fullmatch(token) is not None:
        return False, "recovery claim has no owner record"
    # legacy live-* claims (recorded before the registry existed, e.g.
    # session 9 clip 6) have no birth record: the session proof alone
    # decides. Every other claim reaches here with its owner already
    # proven dead - and STILL must pass the full writer proof.
    return proof_for_session(state_dir, username, sid, proc=proc)


def reclaim_dead_claims(conn, session_id: int, username: str,
                        state_dir: Path, proc: str = "/proc") -> dict:
    """Reset this session's RECOVERING clips to PENDING, but only when
    BOTH the exact claim owner process and the session's full recorded
    writer set are verifiably dead (combined proof: an edit child runs
    in its own process group, so the owner's death alone proves
    nothing about a surviving exporter). Death is proven OUTSIDE the
    transaction from current process state, then the reset is an
    exact-(clip, owner) compare-and-swap INSIDE one transaction: a
    claimant that replaced the owner since the proof is never reset,
    and a proven-dead owner that was merely paused can never spawn a
    second writer - the spawn fence in finish_pending_clips re-attests
    the claim immediately before any edit child exists. Unprovable
    claims stay fenced forever. Returns {"reclaimed": n, "fenced": ids}.
    """
    out = {"reclaimed": 0, "fenced": []}
    rows = conn.execute(
        "SELECT id, session_id, recovery_owner FROM clips"
        " WHERE session_id=? AND status='RECOVERING'",
        (session_id,)).fetchall()
    for r in rows:
        cid = int(r["id"])
        token = r["recovery_owner"]
        try:
            provable, why = _claim_dead_owner(conn, r, username,
                                              state_dir, proc)
        except Exception as exc:   # a proof error must never unblock
            log.warning("claim proof error for clip %s: %s", cid, exc)
            provable, why = False, "proof error"
        if not provable:
            log.info("clip %s claim %s stays fenced: %s", cid, token, why)
            out["fenced"].append(cid)
            continue
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "UPDATE clips SET status='PENDING', recovery_owner=NULL,"
                " claimed_at=NULL WHERE id=? AND status='RECOVERING'"
                " AND recovery_owner=?", (cid, token))
            if cur.rowcount != 1:
                conn.rollback()
                out["fenced"].append(cid)
                continue
            conn.execute(
                "UPDATE clip_claims SET finished_at=? WHERE clip_id=?"
                " AND token=?", (time.time(), cid, token))
            conn.commit()
            out["reclaimed"] += 1
            log.warning("clip %s claim reclaimed after owner-death"
                        " proof (%s)", cid, why)
        except Exception:
            conn.rollback()
            raise
    return out


def extract_pending_manual_marks(state_dir, ffmpeg, ffprobe,
                                 should_continue=None, on_spawn=None,
                                 proc: str = "/proc",
                                 clipper_factory=None) -> int:
    """Extract durable owner marks left PENDING by an interrupted session.

    This is the candidate-producing half of manual-mark recovery. It runs
    only after the session's recorded leader and every recorded writer are
    positively dead, then registers its extraction children in the same
    session writer manifest. Heuristic candidates are untouched: recovery
    must not turn a restart into a second automatic-selection pass.
    Returns the number of raw clips produced; finishing them to HELD is
    the existing sealed-clip recovery step.
    """
    from ..bot import source_auth
    from ..bot.restartproof import (proof_for_session, record_spawn_intent,
                                    record_spawned)
    from ..clipping.auto import AutoClipper
    from ..clipping.extractor import extract_clip as _extract
    from ..clipping.manual import MANUAL_MARK_KIND
    from ..intelligence.providers import HeuristicProvider
    from ..recording.buffer import RollingBuffer, ffprobe_probe
    import functools

    state_dir = Path(state_dir)
    db_path = str(state_dir / "nova.db")
    if not Path(db_path).exists():
        return 0
    conn = connect(db_path)
    try:
        migrate(conn)
        rows = conn.execute(
            "SELECT s.id, t.username FROM stream_sessions s"
            " JOIN twitch_sources t ON t.id=s.source_id"
            " WHERE s.state='INTERRUPTED' AND EXISTS ("
            " SELECT 1 FROM candidate_events e WHERE e.session_id=s.id"
            " AND e.kind=? AND e.status IN ('PENDING','QUEUED'))",
            (MANUAL_MARK_KIND,)).fetchall()
    finally:
        conn.close()

    produced = 0
    for sid, username in rows:
        if should_continue is not None and not should_continue():
            log.warning("manual-mark recovery stopped early (shutdown)")
            break
        if not source_auth.allowed(state_dir, username, "record"):
            log.warning("manual-mark recovery skipped for session %s (%s): "
                        "record authorization is off", sid, username)
            continue
        ok, why = proof_for_session(state_dir, username, sid, proc=proc)
        if not ok:
            log.warning("manual-mark recovery fenced for session %s: %s",
                        sid, why)
            continue
        work = state_dir / "sessions" / username
        registry = work / f"writers-{sid}.json"

        def _pre_spawn(_p=registry):
            return record_spawn_intent(_p)

        def _on_spawn(pid, pgid, token, _p=registry, _hook=on_spawn):
            bound = record_spawned(_p, token, pid, pgid)
            if not bound:
                return False
            if _hook is None:
                return True
            try:
                return bool(_hook(pid, pgid, token))
            except Exception:
                return False

        buf = RollingBuffer(str(work), session_id=sid,
                            probe=ffprobe_probe(ffprobe,
                                                pre_spawn=_pre_spawn,
                                                on_spawn=_on_spawn))
        buf.bind_db(db_path)
        buf.mark_drained()
        buf.reconcile(db_path)
        if clipper_factory is not None:
            clipper = clipper_factory(buf, db_path, _pre_spawn, _on_spawn)
        else:
            clipper = AutoClipper(
                buf, db_path, HeuristicProvider(), ffmpeg, str(work),
                event_kinds=(MANUAL_MARK_KIND,),
                extract_fn=functools.partial(_extract,
                                             pre_spawn=_pre_spawn,
                                             on_spawn=_on_spawn))
        produced += clipper.process_pending()
    notify_mark_outcomes(state_dir)
    return produced


def finalize_interrupted_sessions(state_dir, ffmpeg, ffprobe,
                                  should_continue=None,
                                  on_spawn=None,
                                  outcome_cb=None,
                                  proc: str = "/proc",
                                  manual_clipper_factory=None) -> int:
    """Sealed-input recovery for sessions left INTERRUPTED: finish the
    clips whose raw input carries durable producer seal evidence so the
    work is not stranded invisible. Narrow and fail-closed on purpose:

    - ELIGIBILITY: PENDING rows with producer-committed seal evidence
      (sealed_sha256/sealed_bytes), plus RECOVERING rows whose claim
      owner is reclaimed via reclaim_dead_claims - and only on positive
      proof the exact recorded owner process is dead (clip_claims birth
      record, or for pre-registry live claims the full session death
      proof: leader + every recorded writer + process groups, with
      complete process visibility). Legacy/unsealed rows and
      unprovable claims stay fenced forever - never reset. Old session
      fences, capture state and delivery state are untouched; nothing
      here resumes capture.
    - INPUT: recovery never consumes the mutable source. It takes a
      private snapshot and verifies it against the producer's
      PRE-EXISTING committed digest; a mismatch fences the clip.
    - OWNERSHIP: every clip is claimed with an owner-token CAS
      (rowcount 1 wins); the final HELD write re-checks CURRENT record
      authorization inside the same transaction. The token is released
      only back to PENDING while still owned.
    - OUTPUT: unique never-reused recovery paths; nothing a previous
      writer could still touch is overwritten. Terminal state is HELD
      with ZERO admission jobs; HELD clips never auto-release.

    KNOWN NARROWING vs pre-registry releases: the recovery edit child
    is durably registered in the session writer manifest (intent
    before spawn, bind-or-kill), so a session WITHOUT an initialized
    manifest - legacy or hand-built rows that never went through
    service session setup - can no longer be finished by this path:
    the spawn is refused, the clip returns to PENDING (never burned
    to FAILED), no manifest is created here (a missing manifest is
    lost history; creating an empty one would forge it), and raw and
    partial files stay untouched. Recovering such sessions needs a
    separately reviewed import path, not a silent bootstrap. Every
    session created by the current service has a manifest, so this
    affects no session the current code produced.

    This function is a library entry point only: no boot or service
    wiring calls it. Returns the number of clips moved to HELD."""
    from ..bot import source_auth
    from ..clipping import seal as _seal
    state_dir = Path(state_dir)
    db = state_dir / "nova.db"
    if not db.exists():
        return 0
    # First turn durable owner marks in interrupted sessions into sealed
    # raw clips. The existing recovery below then edits/QCs only those
    # sealed inputs; heuristic candidates are not replayed.
    extract_pending_manual_marks(
        state_dir, ffmpeg, ffprobe, should_continue=should_continue,
        on_spawn=on_spawn, proc=proc, clipper_factory=manual_clipper_factory)

    conn = connect(str(db))
    migrate(conn)
    run_token = f"recovery-{uuid.uuid4().hex[:12]}"
    try:
        rows = conn.execute(
            "SELECT s.id, t.username FROM stream_sessions s"
            " JOIN twitch_sources t ON t.id = s.source_id"
            " WHERE s.state='INTERRUPTED' AND EXISTS (SELECT 1 FROM clips c"
            "  WHERE c.session_id=s.id"
            "  AND c.status IN ('PENDING','RECOVERING')"
            "  AND c.sealed_sha256 IS NOT NULL)").fetchall()
        total = 0
        for sid, username in rows:
            if should_continue is not None and not should_continue():
                # shutdown requested: stop between sessions; unfinished
                # clips stay PENDING for a later run
                log.warning("recovery stopped early (shutdown requested)")
                break
            if not source_auth.allowed(state_dir, username, "record"):
                log.warning("recovery skipped for session %s (%s): "
                            "record authorization is off", sid, username)
                continue
            # Reclaim provably-dead RECOVERING claims (e.g. a finalizer
            # that died mid-export) so they re-enter as PENDING below.
            # Claims that cannot be proven dead stay fenced forever.
            try:
                reclaim_dead_claims(conn, sid, username, state_dir)
            except Exception as exc:
                log.warning("claim reclaim failed for session %s: %s; "
                            "claims stay fenced", sid, exc)
                continue
            work = state_dir / "sessions" / username
            work.mkdir(parents=True, exist_ok=True)
            snap_dir = state_dir / "recovery" / f"session-{sid}"
            snapshots = []

            def snapshot_resolver(clip_id, path, seal_sha, seal_bytes,
                                  _sd=snap_dir, _track=snapshots):
                # bounded private copy: capped at the sealed byte count
                # and a time budget, copied size AND digest verified
                # against the producer's pre-existing committed
                # evidence, snapshot directory fsynced
                snap = _seal.take_verified_snapshot_bounded(
                    path, _sd, seal_sha, seal_bytes or -1)
                if snap is None:
                    log.warning(
                        "recovery fenced clip %s: input does not match "
                        "the producer's sealed digest", clip_id)
                else:
                    _track.append(snap)
                return snap

            def auth_now(c, u=username):
                r = c.execute("SELECT value FROM settings WHERE key=?",
                              (f"srcauth:{u}:record",)).fetchone()
                return bool(r and r[0] == "1")

            # Recovery edit children are durably registered in the
            # session's writer manifest, exactly like capture and live
            # finalize writers: intent BEFORE the child exists (None
            # forbids the spawn), identity bound right after (a bind
            # failure kills the child group). Without this a recovery
            # exporter would be an unrecorded writer its own later
            # reclaim proof could never account for.
            registry = work / f"writers-{sid}.json"

            def _rec_pre_spawn(_p=registry):
                return record_spawn_intent(_p)

            def _rec_on_spawn(pid, pgid, token,
                              _p=registry, _hook=on_spawn):
                bound = record_spawned(_p, token, pid, pgid)
                if not bound:
                    return False
                if _hook is None:
                    return True
                # the caller's hook may REFUSE the child (the service
                # tracker refuses a child bound after cancellation so
                # edit_clip tears down that exact child): propagate the
                # refusal, never report a successful bind
                try:
                    ok = _hook(pid, pgid, token)
                except Exception:
                    ok = False
                return bool(ok)

            try:
                held = finish_pending_clips(
                    conn, state_dir=state_dir, session_id=sid,
                    source_label=username, out_dir=work,
                    runtime_dir=state_dir / "runtime",
                    ffmpeg=ffmpeg, ffprobe=ffprobe, auto_deliver=False,
                    enqueue_admission=False,
                    claim_token=run_token,
                    output_name=lambda cid, _t=run_token:
                        f"clip-recovery-{cid}-{_t}.mp4",
                    input_resolver=snapshot_resolver,
                    auth_on_conn=auth_now,
                    per_clip_authorization=should_continue,
                    pre_spawn=_rec_pre_spawn,
                    on_spawn=_rec_on_spawn,
                    outcome_cb=outcome_cb,
                    require_sealed=True)
            finally:
                # snapshots are private working copies: removed on every
                # terminal path, success or failure
                for snap in snapshots:
                    snap.unlink(missing_ok=True)
            total += len(held)
        return total
    finally:
        conn.close()
