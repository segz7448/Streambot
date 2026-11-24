"""Durable death-diagnostics for the service process.

The bot has died silently (no traceback, no exit status on any accessible
surface) and respawned via the host supervisor. This module installs the
smallest durable instrumentation that can narrow the cause of the NEXT death:

- heartbeat.json rewritten atomically every few seconds;
- append-only exit-breadcrumbs.jsonl: start, SIGHUP, unhandled-exception
  and atexit-interpreter-exit events;
- faulthandler output to faulthandler.log for native fatal faults
  (SIGSEGV/SIGABRT class).

What the evidence proves, exactly:

- A heartbeat row proves only that the heartbeat thread completed a write
  at that time. Death time is BOUNDED between the last successful write
  and now; it is not measured. If the heartbeat thread itself stalls, the
  file goes stale while the service may still be alive - staleness is
  suspicion, not proof of death.
- "atexit_interpreter_exit" means the interpreter reached normal teardown
  (atexit handlers ran), including after an unhandled exception or
  sys.exit(nonzero). It does NOT mean the service shutdown was clean or
  successful. An unhandled exception produces both an
  "unhandled_exception" and an "atexit_interpreter_exit" breadcrumb.
- SIGKILL and host-side kills (OOM killer, watchdog, supervisor stop)
  deliver nothing to the process, so no in-process instrumentation can
  record them. That case looks like: start breadcrumb, no exit
  breadcrumb, heartbeat simply stops.

Storage and privacy contract (matches app.core.logging): files 0600,
directory 0700. Breadcrumbs and the fault log are capped with a single
preserved .1 backup, rotated when the active file would exceed its cap;
the newest evidence is always kept. Writes are flushed and fsynced, and
the directory is fsynced after creation/replace, so records survive
process death and, on a filesystem honoring fsync, power loss.
"""
from __future__ import annotations

import atexit
import faulthandler
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger(__name__)

HEARTBEAT_SECONDS = 10
_BREADCRUMB_CAP_BYTES = 256 * 1024   # ~1500 breadcrumbs of headroom
_FAULTLOG_CAP_BYTES = 1024 * 1024    # rotated at install; faults are rare
_HEARTBEAT_NAME = "heartbeat.json"
_BREADCRUMB_NAME = "exit-breadcrumbs.jsonl"
_FAULTLOG_NAME = "faulthandler.log"

_stop = threading.Event()
_thread: Optional[threading.Thread] = None
_faultlog_handle = None


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _open_private_append(path: str):
    """Open for append with the service's private-file contract (0600)."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.fchmod(fd, 0o600)
    return os.fdopen(fd, "a", encoding="utf-8")


def _prepare_private_dir(path: str) -> None:
    os.makedirs(path, mode=0o700, exist_ok=True)
    os.chmod(path, 0o700)


def _rotate_if_oversize(path: str, cap: int) -> None:
    """Keep one .1 backup; never delete the newest evidence outright."""
    try:
        if os.path.getsize(path) <= cap:
            return
        backup = path + ".1"
        os.replace(path, backup)
        os.chmod(backup, 0o600)
    except OSError:
        return


def _atomic_write_json(path: str, payload: dict[str, Any]) -> None:
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, sort_keys=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        os.chmod(path, 0o600)
        _fsync_dir(os.path.dirname(path) or ".")
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _breadcrumb(log_dir: str, event: str, **extra: Any) -> None:
    record: dict[str, Any] = {
        "event": event,
        "pid": os.getpid(),
        "ts": time.time(),
        "iso": _iso(time.time()),
        "monotonic": time.monotonic(),
    }
    record.update(extra)
    path = os.path.join(log_dir, _BREADCRUMB_NAME)
    try:
        _rotate_if_oversize(path, _BREADCRUMB_CAP_BYTES)
        with _open_private_append(path) as fh:
            json.dump(record, fh, sort_keys=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        _fsync_dir(log_dir)
    except OSError:
        # breadcrumbs must never take the service down
        log.warning("could not write %s breadcrumb", event, exc_info=True)


def _heartbeat_payload() -> dict[str, Any]:
    return {
        "pid": os.getpid(),
        "ts": time.time(),
        "iso": _iso(time.time()),
        "monotonic": time.monotonic(),
    }


def _heartbeat_loop(log_dir: str) -> None:
    path = os.path.join(log_dir, _HEARTBEAT_NAME)
    while not _stop.is_set():
        try:
            _atomic_write_json(path, _heartbeat_payload())
        except Exception:  # noqa: BLE001 - one bad write must not kill the
            # heartbeat; a dead thread would make a stale file masquerade
            # as a dead service
            log.warning("heartbeat write failed", exc_info=True)
        _stop.wait(HEARTBEAT_SECONDS)


def _install_sighup(log_dir: str) -> None:
    # Only take SIGHUP if nothing else owns it; never clobber an existing
    # handler (PTB manages SIGINT/SIGTERM itself and is left untouched).
    try:
        current = signal.getsignal(signal.SIGHUP)
    except (ValueError, AttributeError):
        return  # no SIGHUP on this platform / not main thread
    if current not in (signal.SIG_DFL, None):
        log.info("SIGHUP already handled elsewhere; breadcrumb not installed")
        return

    def _on_sighup(signum: int, _frame: Any) -> None:
        _breadcrumb(log_dir, "signal", signum=signum, signame="SIGHUP")
        # default SIGHUP semantics: terminate
        os._exit(128 + signum)

    signal.signal(signal.SIGHUP, _on_sighup)


def _install_excepthook(log_dir: str) -> None:
    previous = sys.excepthook

    def _hook(exc_type: type, exc: BaseException, tb: Any) -> None:
        # exception type name only - never the message, which can carry data
        _breadcrumb(
            log_dir,
            "unhandled_exception",
            exc_type=f"{exc_type.__module__}.{exc_type.__qualname__}",
        )
        previous(exc_type, exc, tb)

    sys.excepthook = _hook


def install(state_dir: str) -> None:
    """Install death diagnostics. Call once at service start, on the main
    thread, after logging is configured. Never raises."""
    global _thread, _faultlog_handle
    log_dir = os.path.join(state_dir, "logs")
    try:
        _prepare_private_dir(log_dir)
        _breadcrumb(log_dir, "start")
        _install_sighup(log_dir)
        _install_excepthook(log_dir)
        atexit.register(_breadcrumb, log_dir, "atexit_interpreter_exit")
        if _faultlog_handle is None:
            faultlog = os.path.join(log_dir, _FAULTLOG_NAME)
            # faults are rare and immediately fatal; rotating the previous
            # process's log at install bounds growth without losing it
            _rotate_if_oversize(faultlog, _FAULTLOG_CAP_BYTES)
            _faultlog_handle = os.fdopen(
                os.open(faultlog, os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                        0o600), "ab", buffering=0)
            os.fchmod(_faultlog_handle.fileno(), 0o600)
            faulthandler.enable(file=_faultlog_handle)
        if _thread is None or not _thread.is_alive():
            _stop.clear()
            # first heartbeat immediately, so a death inside the first
            # interval still leaves a bounded window
            try:
                _atomic_write_json(
                    os.path.join(log_dir, _HEARTBEAT_NAME),
                    _heartbeat_payload())
            except Exception:  # noqa: BLE001
                log.warning("initial heartbeat write failed", exc_info=True)
            _thread = threading.Thread(
                target=_heartbeat_loop, args=(log_dir,),
                name="nova-diagnostics-heartbeat", daemon=True)
            _thread.start()
        log.info("death diagnostics installed (heartbeat %ds, breadcrumbs, faulthandler)",
                 HEARTBEAT_SECONDS)
    except Exception:  # noqa: BLE001 - diagnostics must never break startup
        log.warning("death diagnostics failed to install", exc_info=True)


def _shutdown_for_tests() -> None:
    """Reset all module state; test helper only."""
    global _thread, _faultlog_handle
    _stop.set()
    if _thread is not None:
        _thread.join(timeout=5)
    _thread = None
    if _faultlog_handle is not None:
        faulthandler.disable()
        _faultlog_handle.close()
        _faultlog_handle = None
