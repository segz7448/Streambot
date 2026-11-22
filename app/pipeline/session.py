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
import json
import re
import threading
import time
from pathlib import Path

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
                 playback: dict | None = None):
        # playback: None for mode A feeds. For public playback: a dict with
        # optional initial_resolved and test/owned-fixture hooks
        # (resolve_fn, capture_fn, opener, host_ok, worker_kwargs).
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
        held = finish_pending_clips(
            conn, state_dir=self.state_dir, session_id=session_id,
            source_label=self.source_label, out_dir=out_dir,
            runtime_dir=runtime_dir, ffmpeg=self.ffmpeg,
            ffprobe=self.ffprobe, auto_deliver=True,
            pre_spawn=getattr(self, "_pre_spawn", None),
            on_spawn=getattr(self, "_on_spawn_writer", None))
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
        the worker attests no process/thread of its own remains, and no
        session-owned executor work is outstanding."""
        w = getattr(self, "_worker", None)
        if w is None or not hasattr(w, "drained"):
            return False
        try:
            if task is not None and not task.done():
                return False
            with self._inflight_lock:
                if self._inflight:
                    return False
            return bool(w.drained())
        except Exception:
            return False

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
        from ..recording.buffer import RollingBuffer
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
                **pb)
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
                await self._owned(clipper.process_pending)
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
                    # during the live loop as possibly-still-written)
                    await self._owned(detector.process_new_segments, True)
                    await self._owned(clipper.process_pending)
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


def finish_pending_clips(conn, *, state_dir, session_id: int, source_label,
                         out_dir: Path, runtime_dir: Path, ffmpeg, ffprobe,
                         auto_deliver: bool = False, pre_spawn=None,
                         on_spawn=None, enqueue_admission: bool = True,
                         per_clip_authorization=None) -> list:
    """Edit + QC every PENDING clip row of one session: plan, edit,
    ffprobe-measured final duration, atomic HELD + metadata + admission
    outbox per clip. Per-clip isolated and idempotent: only PENDING rows
    are touched, so HELD/admitted clips and their delivery state are
    never re-processed. The repost_allowed provenance gate decides the
    admission enqueue, exactly as in the live finalize path. auto_deliver
    (owner-preference immediate delivery) is used ONLY by the live
    finalize path; boot recovery passes False so nothing is flushed."""
    from ..editing.editor import edit_clip
    from ..editing.presets import PRESETS
    from ..intelligence.memory import Memory
    from ..intelligence.planner import plan_clip
    from ..qc.verify import check_media, probe
    state_dir = Path(state_dir)
    mem = Memory(str(state_dir / "memory.json"))
    rows = conn.execute(
        "SELECT id, file_path, duration_seconds FROM clips"
        " WHERE session_id=? AND status='PENDING'",
        (session_id,)).fetchall()
    held_ids = []
    finished = 0
    for clip_id, path, dur in rows:
        # a long edit can span an authorization change: re-check before
        # EVERY clip when the caller supplies a live check (boot recovery
        # does; the live finalize path keeps its existing behavior)
        if per_clip_authorization is not None and \
                not per_clip_authorization():
            log.warning("authorization changed; leaving %d clip(s) PENDING",
                        len(rows) - len(held_ids))
            break
        try:
            plan = plan_clip("auto", source_label, mem,
                             content_seconds=float(dur or 0))
            edited = Path(out_dir) / f"clip-edited-{clip_id}.mp4"
            edit_clip(ffmpeg, path, str(edited),
                      PRESETS.get(plan.preset, PRESETS["vertical"]),
                      None, 0.5, str(runtime_dir),
                      watermark=plan.watermark or None,
                      pre_spawn=pre_spawn, on_spawn=on_spawn)
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
                from ..bot.handoff import ADMIT_JOB_KIND
                from ..core.jobs import JobQueue
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute(
                        "UPDATE clips SET file_path=?, preset=?,"
                        " duration_seconds=?, status='HELD',"
                        " qc_report_json=? WHERE id=? AND status='PENDING'",
                        (str(edited), plan.preset, final_dur,
                         json.dumps({"qc_passed": True,
                                     "plan_reasons": plan.reasons}),
                         clip_id))
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
                mem.remember(
                    "clip", f"session:{session_id}:{clip_id}",
                    f"{final_dur:.0f}s {plan.preset} clip from "
                    f"{source_label}", "mode A session",
                    confidence="observed")
                finished += 1
                held_ids.append(clip_id)
            else:
                conn.execute(
                    "UPDATE clips SET status='QC_FAILED',"
                    " qc_report_json=? WHERE id=?",
                    (json.dumps({"qc_passed": False}), clip_id))
        except Exception as exc:
            log.warning("finish failed for clip %s: %s", clip_id, exc)
            conn.execute(
                "UPDATE clips SET status='FAILED' WHERE id=?",
                (clip_id,))
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


def finalize_interrupted_sessions(state_dir, ffmpeg, ffprobe, *,
                                  writer_proof=None) -> int:
    """Boot recovery for sessions a dead process left INTERRUPTED: finish
    the clips they had ALREADY extracted (PENDING rows) so the work is
    not stranded invisible. Narrow on purpose:
    - existing PENDING clip rows only; the detector and clipper are NOT
      re-run, so no new output is generated from old media;
    - per session, positive writer-death proof is required (default:
      restartproof against the session's recorded leader + writer
      registry); without proof the session is left untouched;
    - the source's CURRENT record authorization is re-read; a revocation
      since capture leaves the clips PENDING, never admitted;
    - provenance/rights gates inside the finish path (repost_allowed)
      still decide each admission; delivery goes through the normal
      outbox, no boot-time flush (auto_deliver=False).
    Returns the number of clips moved to HELD."""
    from ..bot import restartproof as _rp
    from ..bot import source_auth
    state_dir = Path(state_dir)
    db = state_dir / "nova.db"
    if not db.exists():
        return 0
    conn = connect(str(db))
    migrate(conn)
    try:
        rows = conn.execute(
            "SELECT s.id, t.username FROM stream_sessions s"
            " JOIN twitch_sources t ON t.id = s.source_id"
            " WHERE s.state='INTERRUPTED' AND EXISTS (SELECT 1 FROM clips c"
            "  WHERE c.session_id=s.id AND c.status='PENDING')").fetchall()
        total = 0
        for sid, username in rows:
            if writer_proof is not None:
                ok, why = writer_proof(state_dir, username, sid)
            else:
                ok, why = _rp.proof_for_session(state_dir, username, sid)
            if not ok:
                log.warning("boot finalize skipped for session %s (%s): %s",
                            sid, username, why)
                continue
            if not source_auth.allowed(state_dir, username, "record"):
                log.warning("boot finalize skipped for session %s (%s): "
                            "record authorization is off", sid, username)
                continue
            work = state_dir / "sessions" / username
            work.mkdir(parents=True, exist_ok=True)
            registry = work / f"writers-{sid}.json"
            held = finish_pending_clips(
                conn, state_dir=state_dir, session_id=sid,
                source_label=username, out_dir=work,
                runtime_dir=state_dir / "runtime",
                ffmpeg=ffmpeg, ffprobe=ffprobe, auto_deliver=False,
                pre_spawn=lambda _r=registry: _rp.record_spawn_intent(_r),
                on_spawn=lambda pid, pgid, tok, _r=registry:
                    _rp.record_spawned(_r, tok, pid, pgid),
                enqueue_admission=False,
                per_clip_authorization=lambda u=username:
                    source_auth.allowed(state_dir, u, "record"))
            total += len(held)
        return total
    finally:
        conn.close()
