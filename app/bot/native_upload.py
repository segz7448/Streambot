"""Native upload consumer: drains durable handoff records by uploading
the admitted clip to YouTube as PRIVATE, on-device, with no n8n Cloud
dependency.

Gate: environment NOVA_NATIVE_UPLOAD=1 (process env only, injected at
deploy time like every other secret). Default OFF: with the variable
absent the handoff consumer keeps its receipt-only webhook behavior and
this module is never entered. Activation is a deliberate, separately
reviewed step; this module changes nothing while disabled.

Safety contract (same machinery as /publish, never weakened here):
- Admission provenance: the claimed job must BE the single durable
  record of an ACCEPTED clip_intake admission for a canonical
  cliprow:<id> clip, matching clip id, intake date and handoff job id.
  Arbitrary namespaces, copied, stale, replayed or hand-written jobs
  complete without effect. Opaque external admit() ids are never
  reinterpreted as bot clips. A missing registry fails closed.
- QC gate: only HELD or DELIVERED clips upload; anything else is
  completed without effect, never retried.
- Path gate: the clip file must resolve under the session or
  retrospective clip roots; an escape is recorded and completed without
  effect, never retried.
- Per-clip ownership/dedup: publish_records reservation under the
  kernel-held ClipLease. CONFIRMED clips complete idempotently with
  zero network calls; UNCERTAIN clips fail the job terminally and wait
  for Studio reconciliation; a live publisher's lease defers the job.
- Metadata honesty: title and streamer come from the immutable
  ACCEPTED admission (and the clip's registered source when one
  exists), never a fresh generic plan; tags are never fabricated; a
  source-less clip is described as what it is.
- Singleton lifecycle: every cycle runs under an flock on
  the handoff-consumer lock, so exactly one live consumer owns a state dir.
  Chosen-entry preparation (legacy publication-evidence promotion,
  stranded-reservation recovery, crash-leftover job requeue) runs once
  per state dir per process under that lock before the first claim -
  never overlapping a live worker.
- Retry honesty: only definitive pre-upload failures are retried
  (JobQueue backoff, bounded by max_attempts). Once the upload PUT is
  in flight the outcome is ambiguous: marked UNCERTAIN, job failed
  terminally, never blind-retried.
- Confirmation honesty: a failed durable confirmation after YouTube
  returned a video id keeps the id in the UNCERTAIN record and fails
  the job terminally; it is never reported as success.
- privacy is hardcoded "private".
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Callable, Optional

from ..core.database import connect
from ..core.intake import (HANDOFF_JOB_KIND, RESERVED_CLIP_ID_PREFIX)
from ..core.jobs import JobQueue
from .handoff import (_default_probe, _measure_clip,
                      _record_terminal_failure, _terminal_error)

log = logging.getLogger(__name__)

ENV_NATIVE = "NOVA_NATIVE_UPLOAD"
WORKER_NAME = "native-upload-consumer"

PUBLISHABLE_STATUSES = ("HELD", "DELIVERED")


def enabled() -> bool:
    return os.environ.get(ENV_NATIVE, "").strip().lower() in (
        "1", "true", "yes")


def _fail_permanent(queue: JobQueue, job_id: int, error: str, *,
                    notice: str | None = None,
                    expected_attempts: int | None = None) -> None:
    """Terminal failure with no further retry, for outcomes whose retry
    is unsafe (uncertain upload, confirmation-write failure, uncertain
    prior state). The FAILED row stays for reconciliation. The flip is
    a guarded CAS fenced to the CAPTURED claim identity (active
    ANALYZING state, this worker's lock, and the claim's attempt count
    when given): a duplicate, stale-callback or missing job is a no-op
    - never a state overwrite, never an orphan notice. When notice is
    given, it commits in the SAME transaction as a successful flip:
    outbox-idempotent, exactly one row per terminal outcome, and a
    crash persists neither half."""
    guard = " WHERE id = ? AND state = 'ANALYZING' AND locked_by = ?"
    params: list = [error[:2000], time.time(), job_id, WORKER_NAME]
    if expected_attempts is not None:
        guard += " AND attempts = ?"
        params.append(expected_attempts)
    if notice is not None:
        from ..core import schedule
        queue.conn.executescript(schedule.DDL)
        queue.conn.execute("BEGIN IMMEDIATE")
        try:
            cur = queue.conn.execute(
                "UPDATE job_queue SET state = 'FAILED', locked_by = NULL,"
                " locked_at = NULL, last_error = ?, updated_at = ?"
                + guard, params)
            if cur.rowcount == 1:
                schedule.notice_on_conn(queue.conn, notice)
        except BaseException:
            queue.conn.execute("ROLLBACK")
            raise
        queue.conn.execute("COMMIT")
        return
    with queue.conn:
        queue.conn.execute(
            "UPDATE job_queue SET state = 'FAILED', locked_by = NULL,"
            " locked_at = NULL, last_error = ?, updated_at = ?"
            + guard, params)


_STAGE_LABELS = {"auth": "auth", "session": "session",
                 "upload": "upload"}


def _stage_label(exc: Exception) -> str:
    """Allowlisted stage token for owner-facing text; anything else,
    including provider-shaped strings, collapses to 'unknown'."""
    return _STAGE_LABELS.get(getattr(exc, "stage", None), "unknown")


_VID_TEXT_RE = re.compile(r"[A-Za-z0-9_-]{6,64}")


def _confirmed_notice(row_id: int, vid: str) -> str:
    """Owner text for a confirmed upload. The returned video id is only
    rendered into the URL when it has the expected YouTube id charset;
    an unexpected provider string is never interpolated."""
    if _VID_TEXT_RE.fullmatch(vid):
        return (f"Clip #{row_id} uploaded to YouTube as PRIVATE: "
                f"https://www.youtube.com/watch?v={vid}")
    return (f"Clip #{row_id} uploaded to YouTube as PRIVATE; the returned "
            f"video id had an unexpected format - see /publishhistory")


LOCK_NAME = "native-upload.lock"


def _any_live_lease(state_dir: Path) -> bool:
    """True when any publish-locks/clip-*.lock is currently flock-held
    (a live publisher owns that clip). flock conflicts across open
    file descriptions, including within one process, so a successful
    NB acquire proves the lease is NOT held."""
    import fcntl
    root = Path(state_dir) / "publish-locks"
    if not root.is_dir():
        return False
    for lock in root.glob("clip-*.lock"):
        try:
            fd = os.open(str(lock), os.O_RDWR)
        except OSError:
            continue
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return True  # held by a live publisher
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
    return False


_LEGACY_PROMOTED: set = set()


def _prepare(state_dir: Path, conn) -> None:
    """Per-cycle preparation, run under the exclusive consumer lock and
    before the first claim. There is NO owner-token shortcut: every
    step is idempotent and cheap, and a step that was deferred or failed
    simply runs again next cycle, so recovery can never be silently
    skipped or marked done without having happened.

    - Legacy publication evidence is promoted (insert-only) once per
      process per state dir, marked done ONLY after it succeeded.
    - Stranded RESERVED publish records recover to UNCERTAIN every
      cycle; a record whose clip lease is held by a live publisher is
      skipped by recover_interrupted itself (PublishBusy), never
      invalidated, and is retried next cycle.
    - Under the consumer lock no handoff job can legitimately be
      ANALYZING at cycle start (claim + handling are synchronous in the
      lock), so any such job is crash-leftover and is requeued every
      cycle, independent of live publisher leases.
    Exceptions propagate: the caller skips the cycle without claiming."""
    from ..distribution import publish_records
    key = str(Path(state_dir).resolve())
    if key not in _LEGACY_PROMOTED:
        publish_records.promote_legacy(state_dir)
        _LEGACY_PROMOTED.add(key)
    stranded = publish_records.recover_interrupted(state_dir)
    if stranded:
        log.warning("native upload: marked %d stranded publish "
                    "reservation(s) UNCERTAIN", stranded)
    with conn:
        cur = conn.execute(
            "UPDATE job_queue SET state = 'PENDING', locked_by = NULL,"
            " locked_at = NULL, updated_at = ? WHERE kind = ? AND state"
            " NOT IN ('PENDING', 'COMPLETED', 'FAILED', 'CANCELLED')",
            (time.time(), HANDOFF_JOB_KIND))
    if cur.rowcount:
        log.warning("native upload: requeued %d crash-leftover handoff "
                    "job(s)", cur.rowcount)


def process_one(conn, state_dir: Path, *,
                publisher_factory: Optional[Callable] = None,
                probe_fn: Optional[Callable[[str], dict]] = None,
                ffprobe: str = "ffprobe") -> bool:
    """Claim and natively upload one pending handoff job. Returns True
    when a job was handled (whatever its outcome). Synchronous; call
    from a thread via the handoff consumer loop.

    Singleton: every cycle runs under the handoff-consumer flock;
    a second live consumer skips the cycle instead of overlapping, so
    queue recovery and clip-lease transitions never race."""
    from .handoff import _consumer_lock
    state_dir = Path(state_dir)
    # ONE lock for every consumer entry path: this is the same
    # handoff-consumer flock run_once holds, so a direct caller of
    # process_one can never overlap a live run_once consumer (or
    # another direct caller) in any process.
    with _consumer_lock(state_dir) as acquired:
        if not acquired:
            log.warning("native upload: another live consumer holds "
                        "the state lock; skipping cycle")
            return False
        return process_one_locked(
            conn, state_dir, publisher_factory=publisher_factory,
            probe_fn=probe_fn, ffprobe=ffprobe)


def process_one_locked(conn, state_dir: Path, *,
                       publisher_factory: Optional[Callable] = None,
                       probe_fn: Optional[Callable[[str], dict]] = None,
                       ffprobe: str = "ffprobe") -> bool:
    """Caller MUST hold the handoff-consumer lock (run_once does; the
    public process_one takes it). Crash recovery runs here, under that
    lock, so it can never requeue a job a live worker is processing."""
    state_dir = Path(state_dir)
    try:
        _prepare(state_dir, conn)
    except Exception:
        log.exception("native upload: preparation failed; skipping the "
                      "cycle without claiming (retried next cycle)")
        return False
    return _process_one_locked(
        conn, state_dir, publisher_factory=publisher_factory,
        probe_fn=probe_fn, ffprobe=ffprobe)


def _process_one_locked(conn, state_dir: Path, *,
                        publisher_factory: Optional[Callable] = None,
                        probe_fn: Optional[Callable[[str], dict]] = None,
                        ffprobe: str = "ffprobe") -> bool:
    state_dir = Path(state_dir)
    from .pause import is_paused
    if is_paused(state_dir):
        return False  # paused or pause unreadable: claim nothing
    queue = JobQueue(conn)
    job = queue.claim(WORKER_NAME, kinds=[HANDOFF_JOB_KIND])
    if job is None:
        return False
    try:
        return _handle_claimed(conn, queue, state_dir, job,
                               publisher_factory=publisher_factory,
                               probe_fn=probe_fn, ffprobe=ffprobe)
    except Exception as exc:
        # Stuck-job repair: an unexpected error anywhere before or
        # after the publisher call must never leave a job ANALYZING.
        # Bounded retry carries a truthful error; terminal failure
        # lands in the errors ring that /logs reads.
        log.exception("native upload job %s failed unexpectedly",
                      job["id"])
        try:
            queue.fail(int(job["id"]),
                       f"unexpected error: {exc.__class__.__name__}",
                       terminal_notice="A clip upload job ended "
                                       "unexpectedly; no further "
                                       "automatic retry. See /publishqueue",
                       expected_worker=WORKER_NAME,
                       expected_attempts=int(job["attempts"]))
            err = _terminal_error(queue, int(job["id"]))
            if err is not None:
                _record_terminal_failure(state_dir, "native upload",
                                         int(job["id"]), err)
        except Exception:
            log.exception("native upload job %s error handling failed",
                          job["id"])
        return True


def _handle_claimed(conn, queue, state_dir: Path, job, *,
                    publisher_factory: Optional[Callable] = None,
                    probe_fn: Optional[Callable[[str], dict]] = None,
                    ffprobe: str = "ffprobe") -> bool:
    job_id = int(job["id"])
    try:
        payload = json.loads(job["payload_json"])
        clip_id_str = str(payload["clip_id"])
        intake_date = str(payload["intake_date"])
    except Exception:
        queue.complete(job_id)
        log.warning("native upload job %s had an undecodable payload; "
                    "completed without effect", job_id)
        return True
    # Admission provenance gate: only the pipeline's own canonical
    # namespace may name an uploadable clip. Opaque external admission
    # ids (admit()) have no bot clips row and are never reinterpreted.
    if not clip_id_str.startswith(RESERVED_CLIP_ID_PREFIX):
        queue.complete(job_id)
        log.warning("native upload job %s: clip_id %r is outside the "
                    "canonical bot-row namespace; completed without "
                    "effect", job_id, clip_id_str[:40])
        return True
    try:
        row_id = int(clip_id_str[len(RESERVED_CLIP_ID_PREFIX):])
        if row_id < 1:
            raise ValueError
    except ValueError:
        queue.complete(job_id)
        log.warning("native upload job %s: malformed canonical clip id; "
                    "completed without effect", job_id)
        return True
    # Authoritative binding: this claimed job must BE the single durable
    # record of an ACCEPTED registry admission for this exact clip, on
    # the same intake date. A copied, stale, replayed or hand-written
    # job fails this check and is completed without effect - never
    # uploaded. A missing registry table fails closed.
    try:
        bind = conn.execute(
            "SELECT status, intake_date, handoff_job_id, title, streamer"
            " FROM clip_intake WHERE clip_id = ?",
            (clip_id_str,)).fetchone()
    except sqlite3.OperationalError:
        bind = None
    if (bind is None or bind["status"] != "ACCEPTED"
            or bind["handoff_job_id"] is None
            or int(bind["handoff_job_id"]) != job_id
            or str(bind["intake_date"]) != intake_date):
        queue.complete(job_id)
        log.warning("native upload job %s: no authoritative ACCEPTED "
                    "admission binding for this clip/job/date; completed "
                    "without effect", job_id)
        return True
    row = conn.execute(
        "SELECT c.file_path, c.status, c.session_id, t.username"
        " FROM clips c"
        " LEFT JOIN stream_sessions s ON c.session_id = s.id"
        " LEFT JOIN twitch_sources t ON s.source_id = t.id"
        " WHERE c.id = ?", (row_id,)).fetchone()
    if row is None:
        queue.complete(job_id)
        log.warning("native upload job %s: clips row %s missing; "
                    "completed without effect", job_id, row_id)
        return True
    path, status, session_id, source = (row["file_path"], row["status"],
                                        row["session_id"], row["username"])
    if status not in PUBLISHABLE_STATUSES:
        queue.complete(job_id)
        log.info("native upload job %s: clip %s is %s; completed without"
                 " effect", job_id, row_id, status)
        return True
    try:
        resolved = Path(path).resolve(strict=True)
    except (OSError, TypeError):
        queue.complete(job_id)
        log.warning("native upload job %s: clip %s has no file on disk;"
                    " completed without effect", job_id, row_id)
        return True
    clip_roots = [(state_dir / "sessions").resolve(),
                  (state_dir / "retrospective").resolve()]
    if not any(root in resolved.parents for root in clip_roots):
        from .commands_full import record_error
        record_error(state_dir,
                     f"native upload refused path escape clip #{row_id}")
        queue.complete(job_id)
        log.warning("native upload job %s: path escape refused for clip "
                    "%s", job_id, row_id)
        return True
    if probe_fn is None:
        probe_fn = _default_probe(ffprobe)
    problem = _measure_clip(probe_fn, str(resolved))
    if problem is not None:
        queue.fail(job_id, f"media check: {problem}",
                   terminal_notice=f"Clip #{row_id} failed the media check; "
                                   f"no further automatic retry",
                   expected_worker=WORKER_NAME,
                   expected_attempts=int(job["attempts"]))
        log.warning("native upload job %s media check failed: %s",
                    job_id, problem)
        err = _terminal_error(queue, job_id)
        if err is not None:
            _record_terminal_failure(state_dir, "native upload", job_id,
                                     err)
        return True
    from ..distribution import publish_records
    from ..distribution.youtube import (YouTubeNotConfigured,
                                        YouTubeUploadFailed,
                                        publisher_from_env_or_state)
    if publisher_factory is None:
        publisher_factory = lambda: publisher_from_env_or_state(
            str(state_dir))
    try:
        pub = publisher_factory()
    except YouTubeNotConfigured:
        queue.fail(job_id, "YouTube credentials not configured",
                   terminal_notice=f"Clip #{row_id} upload failed: YouTube "
                                   f"credentials not configured; no "
                                   f"further automatic retry",
                   expected_worker=WORKER_NAME,
                   expected_attempts=int(job["attempts"]))
        log.warning("native upload job %s: publisher not configured",
                    job_id)
        err = _terminal_error(queue, job_id)
        if err is not None:
            _record_terminal_failure(state_dir, "native upload", job_id,
                                     err)
        return True
    # Metadata comes from the immutable ACCEPTED admission (and the
    # clip's registered source when one exists), never a fresh generic
    # plan. A source-less clip is described as what it is; tags are
    # never fabricated.
    from ..intelligence.planner import TITLE_MAX_CHARS
    admitted_title = (bind["title"] or "").strip()
    admitted_streamer = (bind["streamer"] or "").strip()
    title = (admitted_title or f"Clip #{row_id}")[:TITLE_MAX_CHARS]
    origin = admitted_streamer or (source or "").strip()
    if origin:
        description = (f"Clipped from {origin} by NOVA Clip Engine "
                       f"(clip #{row_id}, session "
                       f"{session_id or '?'}).")
    else:
        description = (f"NOVA Clip Engine clip #{row_id} "
                       f"(session {session_id or '?'}).")
    reserved_by = f"native-upload:{job_id}"
    from . import ops
    from .commands_full import record_error
    from . import pause as _pause
    try:
        with publish_records.ClipLease(state_dir, row_id) as lease:
            decision, rec = publish_records.reserve(
                state_dir, row_id, reserved_by, lease=lease,
                gate=_pause.gate_in_tx)
            if decision == "gated":
                # Paused (or pause unreadable) at the committed boundary:
                # nothing reserved, no provider call. The job goes back
                # unspent.
                with conn:
                    conn.execute(
                        "UPDATE job_queue SET state='PENDING', locked_by=NULL,"
                        " locked_at=NULL, attempts=MAX(attempts-1,0),"
                        " run_after=?, updated_at=? WHERE id=? AND"
                        " state='ANALYZING'", (time.time() + 30, time.time(),
                                               job_id))
                log.info("native upload job %s: paused at reserve; returned "
                         "to the queue unspent", job_id)
                return True
            if decision == "repost_denied":
                from ..core.provenance import DENIED_PREFIX, UNREADABLE_PREFIX
                _fail_permanent(
                    queue, job_id,
                    (DENIED_PREFIX if rec.get("retryable") else UNREADABLE_PREFIX)
                    + str(rec.get("reason"))
                    + " (clip stays HELD, nothing uploaded)",
                    notice=f"Clip #{row_id} upload not authorized; nothing "
                           f"was uploaded and the clip stays HELD",
                    expected_attempts=int(job["attempts"]))
                log.info("native upload job %s: repost not authorized; "
                         "no upload", job_id)
                return True
            if decision == "blocked":
                if rec["state"] == "CONFIRMED":
                    # Already uploaded: idempotent success, zero network.
                    queue.complete(job_id)
                    log.info("native upload job %s: clip %s already "
                             "confirmed yt:%s", job_id, row_id,
                             rec["video_id"])
                    return True
                if rec["state"] == "UNCERTAIN":
                    _fail_permanent(
                        queue, job_id,
                        "prior upload outcome uncertain; reconcile in "
                        "YouTube Studio before any retry",
                        notice=f"Clip #{row_id} upload outcome UNKNOWN: an "
                               f"earlier attempt may already have uploaded "
                               f"it. No automatic retry. Reconcile in "
                               f"YouTube Studio, then see /publishqueue",
                        expected_attempts=int(job["attempts"]))
                    _record_terminal_failure(
                        state_dir, "native upload", job_id,
                        f"clip #{row_id} uncertain; reconcile in Studio")
                    return True
                # RESERVED by a crashed attempt: recovery marks it
                # UNCERTAIN at startup; a live one holds the lease (and
                # we would have raised PublishBusy). Defer.
                queue.fail(job_id, "upload already in progress "
                           "(lease contention)",
                           expected_worker=WORKER_NAME,
                           expected_attempts=int(job["attempts"]))
                err = _terminal_error(queue, job_id)
                if err is not None:
                    _record_terminal_failure(state_dir, "native upload",
                                             job_id, err)
                return True
            token = rec.get("attempt_token")
            try:
                vid = pub.publish(str(resolved), title,
                                  description=description,
                                  privacy="private", tags=None)
            except YouTubeUploadFailed as exc:
                if exc.uncertain:
                    publish_records.mark_uncertain(
                        state_dir, row_id, str(exc), lease=lease,
                        attempt_token=token)
                    _fail_permanent(
                        queue, job_id,
                        f"upload outcome uncertain ({exc.stage}); "
                        "reconcile in YouTube Studio",
                        notice=f"Clip #{row_id} upload outcome UNKNOWN: the "
                               f"video may already exist on YouTube. No "
                               f"automatic retry. Reconcile in YouTube "
                               f"Studio, then see /publishqueue",
                        expected_attempts=int(job["attempts"]))
                    record_error(
                        state_dir,
                        f"native upload clip #{row_id} uncertain "
                        f"({exc.stage}): {exc}")
                    return True
                publish_records.fail_definitive(
                    state_dir, row_id, str(exc), lease=lease,
                    attempt_token=token)
                queue.fail(job_id,
                           f"upload failed pre-flight ({exc.stage})",
                           terminal_notice=f"Clip #{row_id} YouTube upload "
                                           f"failed ({_stage_label(exc)}); "
                                           f"no further automatic retry",
                           expected_worker=WORKER_NAME,
                           expected_attempts=int(job["attempts"]))
                log.warning("native upload job %s definitive failure: %s",
                            job_id, exc.stage)
                err = _terminal_error(queue, job_id)
                if err is not None:
                    _record_terminal_failure(state_dir, "native upload",
                                             job_id, err)
                return True
            except Exception as exc:
                publish_records.mark_uncertain(
                    state_dir, row_id, str(exc), lease=lease,
                    attempt_token=token)
                _fail_permanent(
                    queue, job_id,
                    "upload outcome uncertain (unexpected error); "
                    "reconcile in YouTube Studio",
                    notice=f"Clip #{row_id} upload outcome UNKNOWN: the "
                           f"video may already exist on YouTube. No "
                           f"automatic retry. Reconcile in YouTube "
                           f"Studio, then see /publishqueue",
                    expected_attempts=int(job["attempts"]))
                record_error(
                    state_dir,
                    f"native upload clip #{row_id} outcome uncertain: "
                    f"{exc}")
                return True
            try:
                publish_records.confirm(state_dir, row_id, vid,
                                        lease=lease, attempt_token=token,
                                        notice=_confirmed_notice(row_id,
                                                                 vid))
            except Exception:
                # YouTube answered; local confirmation did not persist.
                # Never report success, never retry. Keep the id.
                try:
                    publish_records.mark_uncertain(
                        state_dir, row_id,
                        "confirmation write failed; returned video id "
                        + vid, lease=lease, attempt_token=token)
                except Exception:
                    pass  # RESERVED recovers to UNCERTAIN at startup
                _fail_permanent(
                    queue, job_id,
                    "confirmation write failed after upload; reconcile "
                    "in YouTube Studio",
                    notice=f"Clip #{row_id} upload outcome UNKNOWN: YouTube "
                           f"answered but the local confirmation failed, "
                           f"so the video may already exist. No automatic "
                           f"retry. Reconcile in YouTube Studio, then see "
                           f"/publishqueue",
                    expected_attempts=int(job["attempts"]))
                record_error(
                    state_dir,
                    f"native upload clip #{row_id} confirmation write "
                    f"failed; returned video id {vid}")
                return True
            ops.journal_record_ns(state_dir, "publish", row_id,
                                  "publish", f"yt:{vid}")
            queue.complete(job_id)
            log.info("native upload job %s: clip %s uploaded PRIVATE "
                     "yt:%s", job_id, row_id, vid)
            return True
    except publish_records.PublishBusy:
        # A live publisher owns this clip; try again later.
        queue.fail(job_id, "a live publisher holds this clip lease",
                   expected_worker=WORKER_NAME,
                   expected_attempts=int(job["attempts"]))
        err = _terminal_error(queue, job_id)
        if err is not None:
            _record_terminal_failure(state_dir, "native upload", job_id,
                                     err)
        return True
