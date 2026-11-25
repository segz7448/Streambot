"""Cross-process exclusive ownership of a source's session lifetime.

sessions._sessions is in-memory per process: the service and a separate
runner (acceptance, CLI) cannot see each other's sessions, so two
processes could start sessions on the same source and collide on the
shared workdir, output paths and writer registry. This module adds a
durable per-source lock file with grounded owner identity, acquired
atomically, released on normal stop, and recovered only on positive
proof that the recorded owner is dead.

Identity is pid + /proc start-time + process group (the restartproof
standard): never a stale PID alone. Recovery is fail-closed: a lock
whose owner cannot be PROVEN dead (unreadable, malformed, incomplete
process visibility, live group member) is treated as held and is never
deleted. No path deletes a lock blindly.

OPERATOR RECOVERY - malformed/identity-less lock file:
A hard kill (SIGKILL, power loss) between the O_EXCL create and the
write can leave an EMPTY or partial lock file carrying no owner
identity. Every acquire refuses it fail-closed, forever: there is no
recorded identity whose death could ever be proven. Recovery for that
case is MANUAL and controlled: an operator inspects the exact lock
file, proves no live session or writer owns the source (restartproof
writer/session death proofs and stream_sessions rows), and only then
removes the file by hand. NEVER delete by age or any other heuristic:
a lock whose owner cannot be proven dead stays.
"""
from __future__ import annotations

import errno
import fcntl
import json
import os
import secrets
import threading
import time
from pathlib import Path

from . import restartproof as _rp

# Test seam: invoked inside the reclaim mutex after the dead-owner proof
# and token re-compare, immediately before the unlink. Production: None.
_reclaim_pre_unlink = None

_lock = threading.Lock()
# (state_dir, username) -> SourceLock owned by THIS process. Prevents one
# process double-acquiring its own source while a session lives.
_held: dict[tuple[str, str], "SourceLock"] = {}


def _lock_dir(state_dir) -> Path:
    return Path(state_dir) / "source-locks"


def _lock_path(state_dir, username: str) -> Path:
    return _lock_dir(state_dir) / f"{username}.lock"


def _reclaim_mutex_path(state_dir, username: str) -> Path:
    """Sidecar flock file serializing dead-owner reclaim per source. Never
    deleted: unlinking a flock file lets two openers serialize on
    different inodes. A holder's crash releases the flock automatically."""
    return _lock_dir(state_dir) / f"{username}.reclaim"


def _mutex_acquire(state_dir, username: str, budget_s: float,
                   clock=time.monotonic, sleep=time.sleep):
    """Open the reclaim mutex and flock it exclusively, NON-blocking with
    a bounded budget: command dispatch runs on a shared thread pool, so a
    wedged or slow contender must never park a worker indefinitely.
    Returns the held file object, or None on timeout/unusable."""
    try:
        mh = open(_reclaim_mutex_path(state_dir, username), "a")
    except OSError:
        return None
    deadline = clock() + max(budget_s, 0.0)
    try:
        while True:
            try:
                fcntl.flock(mh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return mh
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
                if clock() >= deadline:
                    mh.close()
                    return None
                sleep(0.05)
    except Exception:
        mh.close()
        raise


def _read_lock(path: Path) -> dict | None:
    """Parsed lock content, or None when absent, unreadable or malformed.
    None conflates those on purpose: every one of them fails closed."""
    try:
        with open(path, "rb") as fh:
            data = json.loads(fh.read().decode("utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _own_identity(proc: str) -> dict | None:
    """This process's grounded identity, or None when /proc cannot
    attest our start-time. Without a recorded start there is no later
    death proof, so acquiring without it is refused (fail closed)."""
    pid = os.getpid()
    start = _rp.read_start_time(pid, proc)
    if start is None:
        return None
    return {"pid": pid, "start": start, "pgid": os.getpgrp()}


class SourceLock:
    """This process's ownership of one source. release() removes the lock
    file ONLY while it still carries our token: a file another owner has
    since created is never deleted."""

    def __init__(self, state_dir, username: str, token: str):
        self.state_dir = Path(state_dir)
        self.username = username
        self.token = token
        self.path = _lock_path(state_dir, username)
        self._key = (str(self.state_dir), username)
        self._release_pending = False   # set when a release failed retryably

    def release(self) -> bool:
        """Remove the lock file ONLY while it still carries our token,
        under the same reclaim mutex that serializes dead-owner reclaim,
        so compare and unlink cannot straddle another transition. The
        mutex being unusable fails closed: the lock is left for
        death-proof recovery rather than deleted unguarded. A FAILED
        release (mutex contention, I/O error) RETAINS this process's
        ownership tracking so the next acquire can retry the release;
        tracking is dropped only on terminal outcomes (file gone, our
        token removed, or the file verifiably carries another token)."""
        mh = _mutex_acquire(self.state_dir, self.username, 2.0)
        if mh is None:
            self._release_pending = True     # retryable: keep _held entry
            return False
        try:
            cur = _read_lock(self.path)
            if cur is None:
                if not self.path.exists():
                    with _lock:
                        _held.pop(self._key, None)
                    return True              # already recovered/released
                self._release_pending = True  # unreadable: not ours to remove
                return False
            if cur.get("token") != self.token:
                with _lock:
                    _held.pop(self._key, None)
                return False                 # another owner: tracking is stale
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                self._release_pending = True  # retryable: keep _held entry
                return False
            with _lock:
                _held.pop(self._key, None)
            return True
        finally:
            fcntl.flock(mh, fcntl.LOCK_UN)
            mh.close()


def _try_create(path: Path, payload: dict) -> bool:
    """Atomic create-with-content: O_CREAT|O_EXCL either creates the file
    or raises FileExistsError, with no clobbering either way (os.link is
    NOT available on the production Android interpreter, so the
    link-candidate pattern is not usable there). The payload is one
    small JSON document written in a single write before fsync; a
    concurrent reader that catches the sub-millisecond empty window
    fails closed on malformed content (a refused start, never a wrong
    one) and the start is retryable."""
    text = json.dumps(payload)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return False
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
    except BaseException:
        try:
            path.unlink()
        except OSError:
            pass
        raise
    return True


def _fsync_dir(path: Path) -> None:
    try:
        dfd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass


def acquire(state_dir, username: str, *, proc: str = "/proc",
            token_fn=None, clock=time.time, mutex_budget_s: float = 2.0):
    """(SourceLock, None) on exclusive ownership, else (None, reason).

    reason names why ownership was refused: held by a live (or not
    provably dead) owner, unverifiable identity, or contention. Every
    refusal is fail-closed: nothing is deleted except a lock whose
    recorded owner was PROVEN dead by restartproof.claim_owner_dead
    (pid absent or verifiably reused, group verifiably empty, complete
    process visibility), and reclaim (proof, token re-compare, unlink,
    re-acquire) runs under the per-source reclaim mutex, so competing
    reclaimers cannot interleave a fresh lock into a stale unlink.
    """
    key = (str(Path(state_dir)), username)
    with _lock:
        prior = _held.get(key)
    if prior is not None:
        if not prior._release_pending:
            return None, (f"this process already holds the source lock "
                          f"for {username}")
        # A previous release failed under contention and ownership
        # tracking was retained. Retry THAT release (it only ever
        # removes our own token under the mutex - not a same-pid
        # bypass); refuse while it still cannot complete.
        if not prior.release():
            return None, (f"this process still holds the source lock "
                          f"for {username} (a previous release did not "
                          "complete; retry when contention clears)")
    ident = _own_identity(proc)
    if ident is None:
        return None, ("cannot record this process's /proc identity; "
                      "source lock not acquired (fail closed)")
    token = (token_fn or (lambda: secrets.token_hex(8)))()
    payload = {**ident, "token": token, "ts": clock()}
    path = _lock_path(state_dir, username)
    try:
        _lock_dir(state_dir).mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return None, f"source lock directory unusable ({type(exc).__name__})"
    for _attempt in range(2):
        if _try_create(path, payload):
            _fsync_dir(path)
            lock = SourceLock(state_dir, username, token)
            with _lock:
                _held[key] = lock
            return lock, None
        # Lock file exists. Death-proof + removal + re-acquire MUST be
        # serialized cross-process: without it two reclaimers can both
        # prove the same dead token, and the delayed one's unlink would
        # delete the FIRST reclaimer's fresh live lock (compare-then-
        # unlink is not atomic). The sidecar flock file is never deleted,
        # so all openers serialize on one inode; a holder's crash drops
        # the flock automatically.
        mh = _mutex_acquire(state_dir, username, mutex_budget_s)
        if mh is None:
            return None, (f"reclaim mutex for {username} busy or unusable "
                          f"(bounded {mutex_budget_s}s wait); not acquired")
        try:
            try:
                owner = _read_lock(path)
                if owner is None:
                    if not path.exists():
                        continue    # gone under us: retry fast path
                    return None, (f"existing source lock for {username} is "
                                  "unreadable or malformed; not "
                                  "overriding (fail closed)")
                pid, start, pgid = (owner.get("pid"), owner.get("start"),
                                    owner.get("pgid"))
                if not (_rp._valid_pos_int(pid) and _rp._valid_pos_int(start)
                        and _rp._valid_pos_int(pgid)):
                    return None, (f"existing source lock for {username} has "
                                  "no verifiable owner identity; not "
                                  "overriding (fail closed)")
                dead, why = _rp.claim_owner_dead(pid, start, pgid, proc)
                if not dead:
                    return None, f"source lock for {username} is held: {why}"
                # Owner PROVEN dead under the reclaim mutex. Remove only
                # the exact file proved dead: token re-compare, then
                # unlink, then acquire - all before any other reclaimer
                # can enter.
                cur = _read_lock(path)
                if cur is None or cur.get("token") != owner.get("token"):
                    return None, (f"source lock for {username} changed "
                                  "during recovery; not acquired")
                if _reclaim_pre_unlink is not None:
                    _reclaim_pre_unlink()
                try:
                    path.unlink()
                except OSError:
                    return None, (f"could not remove the dead source lock "
                                  f"for {username}")
                if not _try_create(path, payload):
                    return None, (f"source lock for {username} reappeared "
                                  "during reclaim; not acquired")
                _fsync_dir(path)
                lock = SourceLock(state_dir, username, token)
                with _lock:
                    _held[key] = lock
                return lock, None
            finally:
                fcntl.flock(mh, fcntl.LOCK_UN)
        finally:
            mh.close()
    return None, f"source lock for {username} still contended after recovery"
