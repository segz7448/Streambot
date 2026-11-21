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

    @property
    def terminal_kind(self):
        """Typed reason the playback worker stopped for good (restricted,
        challenge, blocked, unauthorized, quality, invalid), 'unauthorized'
        when recording authorization was revoked mid-run, else None."""
        w = getattr(self, "_worker", None)
        if w is None:
            return None
        kind = getattr(w, "terminal_kind", None)
        if kind:
            return kind
        if getattr(w, "end_reason", None) == "revoked":
            return "unauthorized"
        return None

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
        if self.playback is None:
            if attempt is None:
                return "mode A session"
            return json.dumps({"mode": "feed", "sched_attempt": attempt})
        from ..bot import source_auth
        return json.dumps({
            "sched_attempt": attempt,
            "origin": "twitch_public_playback", "mode": "playback",
            "source": self.source_label,
            "record_authorized_at_start": source_auth.allowed(
                self.state_dir, self.source_label, "record"),
            "repost_authorized_at_start": source_auth.allowed(
                self.state_dir, self.source_label, "repost"),
            "started_at": now,
            "note": "authorization is re-read at every publish and capture "
                    "check; this snapshot is provenance only"})

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
        from ..editing.editor import edit_clip
        from ..editing.presets import PRESETS
        from ..intelligence.memory import Memory
        from ..intelligence.planner import plan_clip
        from ..qc.verify import check_media
        mem = Memory(str(self.state_dir / "memory.json"))
        rows = conn.execute(
            "SELECT id, file_path, duration_seconds FROM clips"
            " WHERE session_id=? AND status='PENDING'",
            (session_id,)).fetchall()
        held_ids = []
        for clip_id, path, dur in rows:
            try:
                plan = plan_clip("auto", self.source_label, mem,
                                 content_seconds=float(dur or 0))
                edited = out_dir / f"clip-edited-{clip_id}.mp4"
                edit_clip(self.ffmpeg, path, str(edited),
                          PRESETS.get(plan.preset, PRESETS["vertical"]),
                          None, 0.5, str(runtime_dir),
                          watermark=plan.watermark or None)
                qc = check_media(self.ffprobe, self.ffmpeg, str(edited),
                                 1.0, 100, 100, True)
                if qc.ok:
                    # Atomic HELD-to-admission outbox: the HELD update,
                    # the stored-metadata update and the durable
                    # admission job commit in ONE transaction, so a
                    # failed outbox write rolls the clip back instead
                    # of silently stranding it with no admission. The
                    # outer per-clip handler then marks it FAILED and
                    # visible, and other clips are unaffected.
                    from ..bot.handoff import ADMIT_JOB_KIND
                    from ..core.jobs import JobQueue
                    conn.execute("BEGIN IMMEDIATE")
                    try:
                        conn.execute(
                            "UPDATE clips SET file_path=?, preset=?,"
                            " status='HELD', qc_report_json=? WHERE id=?",
                            (str(edited), plan.preset,
                             json.dumps({"qc_passed": True,
                                         "plan_reasons": plan.reasons}),
                             clip_id))
                        # Stored pipeline metadata the admission flow
                        # reads: the planner's title on the clip's own
                        # candidate event (streamer comes from the
                        # session's registered source row). Durable, no
                        # schema change, never a job-payload claim.
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
                        if repost_allowed(conn, clip_id)[0]:
                            JobQueue(conn).enqueue(
                                ADMIT_JOB_KIND, {"clip_row_id": int(clip_id)})
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise
                    mem.remember(
                        "clip", f"session:{session_id}:{clip_id}",
                        f"{dur:.0f}s {plan.preset} clip from "
                        f"{self.source_label}", "mode A session",
                        confidence="observed")
                    self.clips_finished += 1
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
        if held_ids:
            # automatic delivery ergonomics: when the owner preference
            # is on (default), QC-passed clips go to the paired admin
            # right away instead of waiting for /deliveries flush.
            # Per-clip journaled in ops.deliver_one_clip; a delivery
            # failure leaves the clip HELD, never fails the finish.
            try:
                from ..bot import ops
                outcome = ops.auto_deliver_clips(self.state_dir,
                                                 held_ids)
                if outcome["enabled"]:
                    log.info("auto-delivery for session %s: %d sent, "
                             "%d uncertain", session_id,
                             outcome["delivered"], outcome["uncertain"])
            except Exception as exc:
                log.warning("auto-delivery skipped for session %s: %s",
                            session_id, exc)

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
        runner = FFmpegRunner(self.ffmpeg, self.ffprobe, max_concurrent=1)
        if self.playback is not None:
            from ..recording.playback_worker import PlaybackWorker
            pb = dict(self.playback)
            worker = PlaybackWorker(
                buf, self.source_label, db_path, self.state_dir, self.ffmpeg,
                initial_resolved=pb.pop("initial_resolved", None),
                on_recording=self._mark_recording,
                ffprobe=self.ffprobe, **pb)
        else:
            worker = RecordingWorker(runner, buf, self.feed, db_path,
                                     sweep_seconds=max(1, self.segment_seconds))
        self._worker = worker
        detector = CandidateDetector(self.ffmpeg, buf, db_path)
        clipper = AutoClipper(buf, db_path, HeuristicProvider(),
                              self.ffmpeg, str(work),
                              pre_seconds=self.clip_pre_seconds,
                              post_seconds=self.clip_post_seconds)
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
                await self._owned(detector.process_new_segments)
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
                    await self._owned(detector.process_new_segments)
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
