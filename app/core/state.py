"""Shared atomic JSON state files.

Every JSON state file (memory, pairing, inbox, journal, watermark,
destinations, relay state) goes through this one helper:

- one process-wide threading.Lock per resolved path, so threads and
  separate instances of the owning class serialize (the audit's
  4x40 write test lost 87% of writes without it)
- an flock on a sibling .lock file, so separate PROCESSES serialize
  too (the service and on-device tools both write the same state)
- writes go to a temp file in the same directory, flush + fsync,
  then os.replace: a crash mid-write leaves the previous file
  intact, and the directory is fsynced so the rename is durable
- a corrupt file FAILS CLOSED with StateCorruptError instead of
  being silently reset to empty (which would destroy the admin
  pairing, the inbox backlog or the delivery journal)
"""
from __future__ import annotations

import fcntl
import json
import os
import threading
from pathlib import Path


class StateCorruptError(Exception):
    pass


_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _locks_guard:
        return _locks.setdefault(key, threading.Lock())


def _read_unlocked(path: Path, default):
    try:
        text = path.read_text()
    except FileNotFoundError:
        return default
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError) as exc:
        raise StateCorruptError(f"{path}: corrupt JSON state: {exc}")


def locked_read(path, default):
    path = Path(path)
    with _lock_for(path):
        fd = os.open(str(path) + ".lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_SH)
            return _read_unlocked(path, default)
        finally:
            os.close(fd)


def locked_update(path, default, fn):
    """Atomic read-modify-write: fn(data) -> (new_data, result)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock_for(path):
        fd = os.open(str(path) + ".lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            data = _read_unlocked(path, default)
            new_data, result = fn(data)
            tmp = path.parent / (
                f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}")
            fdw = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                          0o600)
            with os.fdopen(fdw, "w") as fh:
                json.dump(new_data, fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            dfd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
            return result
        finally:
            os.close(fd)


def locked_write(path, data) -> None:
    locked_update(path, None, lambda _old: (data, None))


def locked_update_text(path, default: str, fn):
    """Same guarantees as locked_update for non-JSON text state
    (the relay PID file): per-path thread lock, cross-process flock,
    temp file + fsync + os.replace + directory fsync."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock_for(path):
        fd = os.open(str(path) + ".lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                data = path.read_text()
            except FileNotFoundError:
                data = default
            new_data, result = fn(data)
            tmp = path.parent / (
                f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}")
            fdw = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                          0o600)
            with os.fdopen(fdw, "w") as fh:
                fh.write(new_data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            dfd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
            return result
        finally:
            os.close(fd)
