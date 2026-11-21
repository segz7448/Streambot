"""Handoff consumer: drives durable pending admission and handoff
records (job_queue kinds 'clip_intake_admit' and 'clip_intake_handoff')
toward the external distribution workflow.

Flow: the pipeline enqueues one durable admission job per newly HELD
clip (per-clip isolated). This consumer performs the admission locally
via the authoritative ClipIntakeAuthority (bounded scheduled retry via
JobQueue.fail; terminal failure is appended to the errors ring that
/logs reads, and the FAILED row stays for reconciliation, so nothing
drops silently). A recorded admission writes the durable handoff job;
the consumer then POSTs the admission notice to the configured n8n
webhook.

Scope honesty:
- Default mode: one authenticated HTTP POST of the admission notice.
  With NOVA_NATIVE_UPLOAD=1 (default off) the consumer instead uploads
  the admitted clip to YouTube as PRIVATE on-device via
  app.bot.native_upload; the webhook is not used in that mode.
- Exactly-once external effect is NOT claimed. A crash between a
  successful POST and the local job completion re-POSTs on reclaim.
  Every POST carries the durable handoff_job_id as the X-Handoff-Id
  header and in the payload so that a FUTURE receiver can dedupe on it;
  no integrated receiver exists yet, so dedupe is a documented
  requirement for the n8n side, not an assumption this module makes.
- The consumer never touches Telegram-to-owner delivery and never
  touches clips that have no admission record: pre-existing HELD clips
  are invisible to it.

Credential policy: the webhook URL and secret come ONLY from the
NOVA_N8N_WEBHOOK_URL / NOVA_N8N_WEBHOOK_SECRET environment variables,
injected at service start. They are never read from the database,
settings, job payloads or files: the user mandates that our credentials
live in the AGT Vault, and the vault-to-process route is environment
injection at deploy time. When either variable is absent the consumer
fails closed (UNCONFIGURED) and leaves every handoff job pending. The
POST itself is HTTPS-only and never follows redirects, so the Bearer
secret cannot leak to a cleartext or redirected host; errors are
recorded redacted (class name only, never the URL or response body).
"""
from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Optional

from ..core.database import connect
from ..core.intake import HANDOFF_JOB_KIND, MAX_DURATION_SEC
from ..core.jobs import JobQueue

log = logging.getLogger(__name__)

WORKER_NAME = "handoff-consumer"
ENV_URL = "NOVA_N8N_WEBHOOK_URL"
ENV_SECRET = "NOVA_N8N_WEBHOOK_SECRET"

ADMIT_JOB_KIND = "clip_intake_admit"

# status strings returned by run_once
UNCONFIGURED = "unconfigured"
IDLE = "idle"
PROCESSED = "processed"


def _fail_blocked(queue, job_id, error):
    """Visible terminal FAILED row (not silently completed)."""
    import time as _t
    with queue.conn:
        queue.conn.execute(
            "UPDATE job_queue SET state='FAILED', locked_by=NULL,"
            " locked_at=NULL, last_error=?, updated_at=? WHERE id=?",
            (error[:2000], _t.time(), job_id))

def _check_webhook_url(url: str) -> None:
    # The POST carries a Bearer secret: cleartext transport is refused
    # outright. The error never embeds the URL (it can carry path
    # secrets), only the scheme violation.
    scheme = urllib.parse.urlsplit(url).scheme.lower()
    if scheme != "https":
        raise RuntimeError("webhook URL must use https")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never follow: a redirect would forward the Authorization
        # header to a host we did not configure.
        return None


def _no_redirect_opener():
    return urllib.request.build_opener(_NoRedirect)


def _webhook_config() -> Optional[tuple[str, str]]:
    # Environment injection only (AGT Vault at deploy time); never the
    # database, settings, job payloads or files. Fail closed.
    url = os.environ.get(ENV_URL, "").strip()
    secret = os.environ.get(ENV_SECRET, "").strip()
    if not url or not secret:
        return None
    return url, secret


def _default_probe(ffprobe: str):
    def _probe(path: str) -> dict:
        from ..qc.verify import probe
        return probe(ffprobe, path)
    return _probe


def _measure_clip(probe_fn: Callable[[str], dict], path: str) -> Optional[str]:
    """Measured media check before any external effect: the file must
    exist, be non-empty, and probe to a duration within the brief's
    limit. Returns None when acceptable, else a short reason string."""
    if not os.path.isfile(path):
        return "file missing"
    if os.path.getsize(path) == 0:
        return "file empty"
    try:
        meta = probe_fn(path)
        duration = float(meta.get("format", {}).get("duration", 0))
    except Exception as exc:
        return f"probe failed: {exc.__class__.__name__}"
    if not (0 < duration <= MAX_DURATION_SEC):
        return f"measured duration {duration:.1f}s out of range"
    return None


def _default_post(url: str, headers: dict, payload: dict) -> None:
    _check_webhook_url(url)
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    for name, value in headers.items():
        req.add_header(name, value)
    try:
        with _no_redirect_opener().open(req, timeout=30) as resp:
            if not (200 <= resp.status < 300):
                raise RuntimeError(f"webhook returned HTTP {resp.status}")
    except urllib.error.HTTPError as exc:
        # Raised for non-2xx including refused 3xx redirects. Re-raise
        # without the URL or response detail: no secret, no URL leak.
        raise RuntimeError(f"webhook returned HTTP {exc.code}") from None
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"webhook unreachable: "
            f"{exc.reason.__class__.__name__}") from None


def _terminal_error(queue: JobQueue, job_id: int) -> Optional[str]:
    row = queue.conn.execute(
        "SELECT state, last_error FROM job_queue WHERE id = ?",
        (job_id,)).fetchone()
    if row and row["state"] == "FAILED":
        return str(row["last_error"] or "")
    return None


def _record_terminal_failure(state_dir: Path, what: str, job_id: int,
                             error: str) -> None:
    """Owner-visible terminal failure: append to the errors ring that
    /logs reads. The FAILED job row stays in job_queue for
    reconciliation; nothing is silently dropped."""
    try:
        from .commands_full import record_error
        record_error(Path(state_dir),
                     f"{what} job {job_id} permanently failed: "
                     f"{error[:200]}")
    except Exception:  # visibility must never crash the consumer
        log.exception("could not record terminal failure for job %s",
                      job_id)


def _process_one_admission(conn, state_dir: Path) -> bool:
    """Claim and perform one pending admission (durable jobs written by
    the pipeline for newly HELD clips). Local-only: needs no webhook
    configuration and no network. Bounded scheduled retry comes from
    JobQueue.fail; terminal failure is owner-visible. Returns True when
    a job was handled."""
    queue = JobQueue(conn)
    job = queue.claim(WORKER_NAME, kinds=[ADMIT_JOB_KIND])
    if job is None:
        return False
    job_id = int(job["id"])
    try:
        payload = json.loads(job["payload_json"])
        clip_row_id = int(payload["clip_row_id"])
    except Exception:
        queue.complete(job_id)
        log.warning("admission job %s had an undecodable payload; "
                    "completed without effect", job_id)
        return True
    from ..core.provenance import repost_allowed
    _o = repost_allowed(conn, clip_row_id)
    if not _o.allowed:
        from ..core.provenance import prefixed
        _fail_blocked(queue, job_id, prefixed(_o))
        log.info("admission job %s: clip %s not admitted (%s); stays HELD",
                 job_id, clip_row_id, _o.reason)
        return True
    try:
        from ..core.intake import ClipIntakeAuthority
        # Admitted metadata comes from the pipeline's own durable rows
        # (the clip's candidate-event title, the session's registered
        # source), never from claims inside the job payload.
        title = streamer = None
        try:
            mrow = conn.execute(
                "SELECT e.metadata_json AS meta, t.username AS source"
                " FROM clips c"
                " LEFT JOIN candidate_events e ON c.event_id = e.id"
                " LEFT JOIN stream_sessions s ON c.session_id = s.id"
                " LEFT JOIN twitch_sources t ON s.source_id = t.id"
                " WHERE c.id = ?", (clip_row_id,)).fetchone()
            if mrow is not None:
                streamer = mrow["source"] or None
                if mrow["meta"]:
                    title = (json.loads(mrow["meta"]) or {}).get("title")
        except Exception:
            log.warning("admission job %s: stored metadata unreadable;"
                        " admitting with registry fallbacks", job_id)
            title = streamer = None
        verdict = ClipIntakeAuthority(
            str(Path(state_dir) / "nova.db")).admit_clip(
            clip_row_id, title=title, streamer=streamer)
    except Exception as exc:
        queue.fail(job_id, f"admission: {exc.__class__.__name__}")
        log.warning("admission job %s failed: %s",
                    job_id, exc.__class__.__name__)
        err = _terminal_error(queue, job_id)
        if err is not None:
            _record_terminal_failure(state_dir, "admission", job_id, err)
        return True
    queue.complete(job_id)
    if not verdict.get("accepted"):
        log.info("admission job %s: clip %s not admitted: %s",
                 job_id, clip_row_id, verdict.get("reason"))
    return True


CONSUMER_LOCK_NAME = "handoff-consumer.lock"


@contextmanager
def _consumer_lock(state_dir: Path):
    """Single live consumer per state dir, across EVERY mode: admission
    processing, the native upload branch, the webhook branch and crash
    recovery all run under this flock. A second live consumer (any
    mode, any process) skips instead of overlapping, so queue
    transitions never race a live worker."""
    fd = os.open(str(Path(state_dir) / CONSUMER_LOCK_NAME),
                 os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def run_once(state_dir: Path, *,
             ffprobe: str = "ffprobe",
             probe_fn: Optional[Callable[[str], dict]] = None,
             post_fn: Optional[Callable[[str, dict, dict], None]] = None,
             publisher_factory: Optional[Callable] = None,
             ) -> str:
    """One claim-and-process cycle. Synchronous; call from a thread.

    Pending admissions are processed first (local-only, no webhook
    needed). Returns PROCESSED when one job was handled (whatever its
    outcome), UNCONFIGURED when only handoff work remains but no webhook
    is configured, IDLE when nothing is pending or another live
    consumer holds the state-dir lock.
    """
    state_dir = Path(state_dir)
    with _consumer_lock(state_dir) as acquired:
        if not acquired:
            log.warning("handoff consumer: another live consumer holds "
                        "the state lock; skipping cycle")
            return IDLE
        return _run_once_locked(state_dir, ffprobe=ffprobe,
                                probe_fn=probe_fn, post_fn=post_fn,
                                publisher_factory=publisher_factory)


def _run_once_locked(state_dir: Path, *,
                     ffprobe: str = "ffprobe",
                     probe_fn: Optional[Callable[[str], dict]] = None,
                     post_fn: Optional[Callable[[str, dict, dict], None]] = None,
                     publisher_factory: Optional[Callable] = None,
                     ) -> str:
    state_dir = Path(state_dir)
    conn = connect(str(state_dir / "nova.db"))
    try:
        if _process_one_admission(conn, state_dir):
            return PROCESSED
        from . import native_upload
        if native_upload.enabled():
            # Native mode (NOVA_NATIVE_UPLOAD=1, default off): upload the
            # admitted clip directly as PRIVATE. No webhook, no n8n.
            return PROCESSED if native_upload.process_one_locked(
                conn, state_dir, publisher_factory=publisher_factory,
                probe_fn=probe_fn, ffprobe=ffprobe) else IDLE
        config = _webhook_config()
        if config is None:
            return UNCONFIGURED
        url, secret = config
        if probe_fn is None:
            probe_fn = _default_probe(ffprobe)
        if post_fn is None:
            post_fn = _default_post
        queue = JobQueue(conn)
        job = queue.claim(WORKER_NAME, kinds=[HANDOFF_JOB_KIND])
        if job is None:
            return IDLE
        job_id = int(job["id"])
        try:
            payload = json.loads(job["payload_json"])
            clip_id = str(payload["clip_id"])
            intake_date = str(payload["intake_date"])
            row_id = int(clip_id.split(":", 1)[1])
        except Exception:
            queue.complete(job_id)
            log.warning("handoff job %s had an undecodable payload; "
                        "completed without effect", job_id)
            return PROCESSED
        from ..core.provenance import repost_allowed as _ra
        _o = _ra(conn, row_id)
        if not _o.allowed:
            from ..core.provenance import prefixed
            _fail_blocked(queue, job_id, prefixed(_o))
            log.info("handoff job %s: clip %s not posted (%s)", job_id,
                     row_id, _o.reason)
            return PROCESSED
        row = conn.execute(
            "SELECT file_path FROM clips WHERE id = ?", (row_id,)).fetchone()
        if row is None:
            # The admission is stable; the media row is gone. Nothing can
            # be posted about media, so the notice is pointless; complete.
            queue.complete(job_id)
            log.warning("handoff job %s: clips row %s missing; completed "
                        "without effect", job_id, row_id)
            return PROCESSED
        problem = _measure_clip(probe_fn, str(row["file_path"]))
        if problem is not None:
            queue.fail(job_id, f"media check: {problem}")
            log.warning("handoff job %s media check failed: %s",
                        job_id, problem)
            err = _terminal_error(queue, job_id)
            if err is not None:
                _record_terminal_failure(state_dir, "handoff", job_id,
                                         err)
            return PROCESSED
        notice = {"handoff_job_id": job_id, "clip_id": clip_id,
                  "intake_date": intake_date}
        headers = {"Authorization": f"Bearer {secret}",
                   "X-Handoff-Id": str(job_id)}
        try:
            post_fn(url, headers, notice)
        except Exception as exc:
            # Redacted: class name only. Exception text can embed the
            # URL (path secrets) or transport detail.
            queue.fail(job_id, f"webhook post: {exc.__class__.__name__}")
            log.warning("handoff job %s post failed: %s",
                        job_id, exc.__class__.__name__)
            err = _terminal_error(queue, job_id)
            if err is not None:
                _record_terminal_failure(state_dir, "handoff", job_id,
                                         err)
            return PROCESSED
        # Crash point: if the process dies between here and complete(),
        # recover() requeues the job and the notice is POSTed again with
        # the SAME X-Handoff-Id, the idempotency key a future receiver
        # must dedupe on.
        queue.complete(job_id)
        return PROCESSED
    finally:
        conn.close()


def recover(state_dir: Path) -> int:
    """Requeue interrupted admission and handoff jobs after a crash.
    Scoped to this consumer's own two kinds: unrelated job kinds and
    their external effects are never replayed by this path. Call once at
    service startup before the consumer loop starts. Runs under the
    shared consumer lock: with a live consumer present it skips rather
    than requeueing jobs a live worker may hold."""
    with _consumer_lock(Path(state_dir)) as acquired:
        if not acquired:
            log.warning("handoff recover: a live consumer holds the "
                        "state lock; skipping recovery")
            return 0
        conn = connect(str(Path(state_dir) / "nova.db"))
        try:
            with conn:
                cur = conn.execute(
                    "UPDATE job_queue SET state = 'PENDING',"
                    " locked_by = NULL, locked_at = NULL, updated_at = ?"
                    " WHERE kind IN (?, ?) AND state NOT IN ('PENDING',"
                    " 'COMPLETED', 'FAILED', 'CANCELLED')",
                    (time.time(), ADMIT_JOB_KIND, HANDOFF_JOB_KIND))
                return int(cur.rowcount)
        finally:
            conn.close()


async def run_consumer(state_dir: Path, *, poll_interval: float = 5.0,
                       ffprobe: str = "ffprobe") -> None:
    """Long-running service loop; cancel to stop."""
    while True:
        try:
            status = await asyncio.to_thread(
                run_once, state_dir, ffprobe=ffprobe)
        except Exception:
            log.exception("handoff consumer cycle failed")
            status = IDLE
        await asyncio.sleep(0.2 if status == PROCESSED else poll_interval)
