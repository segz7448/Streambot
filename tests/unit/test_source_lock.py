"""Cross-process source-lifetime lock: exclusive acquisition across
processes, release on normal stop, and fail-closed recovery only on
grounded proof of owner death (restartproof identity: pid + /proc
start-time + process group), never stale-PID-alone or blind deletion."""
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from app.bot import restartproof as _rp
from app.bot import source_lock
from app.pipeline.session import ClipSession


@pytest.fixture(autouse=True)
def _clear_held():
    source_lock._held.clear()
    yield
    source_lock._held.clear()


def _lock_path(sd, username="lacy"):
    return Path(sd) / "source-locks" / f"{username}.lock"


def _write_lock(sd, payload, username="lacy"):
    p = _lock_path(sd, username)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload))
    return p


def test_acquire_release_cycle(tmp_path):
    lock, why = source_lock.acquire(tmp_path, "lacy")
    assert why is None and lock is not None
    data = json.loads(_lock_path(tmp_path).read_text())
    assert data["pid"] == os.getpid()
    assert data["start"] == _rp.read_start_time(os.getpid())
    assert data["pgid"] == os.getpgrp()
    assert data["token"] == lock.token
    # same process cannot double-own the source while it holds the lock
    lock2, why2 = source_lock.acquire(tmp_path, "lacy")
    assert lock2 is None and "already holds" in why2
    # release on normal stop removes the file and frees the source
    assert lock.release() is True
    assert not _lock_path(tmp_path).exists()
    lock3, why3 = source_lock.acquire(tmp_path, "lacy")
    assert lock3 is not None and why3 is None
    lock3.release()


def test_release_never_deletes_another_owners_lock(tmp_path):
    lock, _ = source_lock.acquire(tmp_path, "lacy")
    # someone else's fresh lock replaces ours on disk
    _write_lock(tmp_path, {"pid": 1, "start": 1, "pgid": 1,
                           "token": "not-ours", "ts": 0})
    assert lock.release() is False
    assert json.loads(_lock_path(tmp_path).read_text())["token"] == "not-ours"


def test_concurrent_starts_across_processes_exactly_one_wins(tmp_path):
    script = (
        "import sys;"
        "from app.bot import source_lock as s;"
        "lock, why = s.acquire(sys.argv[1], 'lacy');"
        "print('OK' if lock else 'REFUSED')"
    )
    procs = [subprocess.Popen([sys.executable, "-c", script, str(tmp_path)],
                              stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                              text=True, cwd=Path(__file__).parents[2])
             for _ in range(8)]
    outs = [p.communicate(timeout=60)[0].strip() for p in procs]
    assert outs.count("OK") == 1, outs
    assert outs.count("REFUSED") == 7, outs
    for p in procs:
        assert p.returncode == 0


def test_recovery_after_owner_death(tmp_path):
    script = (
        "import sys, time;"
        "from app.bot import source_lock as s;"
        "lock, why = s.acquire(sys.argv[1], 'lacy');"
        "assert lock, why;"
        "print('READY', flush=True);"
        "time.sleep(60)"
    )
    child = subprocess.Popen([sys.executable, "-c", script, str(tmp_path)],
                             stdout=subprocess.PIPE, text=True,
                             cwd=Path(__file__).parents[2],
                             start_new_session=True)
    try:
        assert child.stdout.readline().strip() == "READY"
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=30)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        child.stdout.close()
    # owner verifiably dead (pid absent, group empty): recovery allowed
    lock, why = source_lock.acquire(tmp_path, "lacy")
    assert lock is not None, why
    data = json.loads(_lock_path(tmp_path).read_text())
    assert data["pid"] == os.getpid()
    lock.release()


def test_competing_dead_owner_reclaimers_exactly_one_wins(tmp_path):
    """Regression for the competing-reclaimer race: two processes prove
    the same dead token; the delayed one's unlink must NOT delete the
    first reclaimer's fresh live lock. The pre-unlink hook widens the
    interleaving deterministically (both reclaimers pass the token
    compare before either unlinks)."""
    _write_lock(tmp_path, {"pid": 999999, "start": 1, "pgid": 999998,
                           "token": "dead", "ts": 0})
    barrier = tmp_path / "go"
    script = (
        "import sys, time, os\n"
        "import pathlib\n"
        "_real_unlink = pathlib.Path.unlink\n"
        "def _slow_unlink(self, *a, **k):\n"
        # stagger the LOCK-FILE pre-unlink window per child (tmp cleanup\n"
        # stays fast so both children read the dead token up front):\n"
        # child0 unlinks and re-acquires, then child1's delayed unlink\n"
        # lands on child0's FRESH lock - the exact interleaving that\n"
        # made compare-then-unlink delete a live lock\n"
        "    if self.name.endswith('.lock'):\n"
        "        time.sleep(0.3 + 0.6 * int(sys.argv[3]))\n"
        "    return _real_unlink(self, *a, **k)\n"
        "pathlib.Path.unlink = _slow_unlink\n"
        "from app.bot import source_lock as s\n"
        "while not os.path.exists(sys.argv[2]):\n"
        "    time.sleep(0.01)\n"
        "lock, why = s.acquire(sys.argv[1], 'lacy')\n"
        "print(('OK ' + lock.token) if lock else ('REFUSED ' + why),\n"
        "      flush=True)\n"
        "time.sleep(1.0)\n"
    )
    procs = [subprocess.Popen([sys.executable, "-c", script, str(tmp_path),
                               str(barrier), str(i)],
                              stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                              text=True, cwd=Path(__file__).parents[2])
             for i in range(2)]
    barrier.touch()
    outs = [p.communicate(timeout=60)[0].strip() for p in procs]
    winners = [o.split()[1] for o in outs if o.startswith("OK ")]
    refused = [o for o in outs if o.startswith("REFUSED")]
    assert len(winners) == 1 and len(refused) == 1, outs
    final = json.loads(_lock_path(tmp_path).read_text())
    assert final["token"] == winners[0], (outs, final)
    assert final["pid"] != 999999            # the fresh live lock survived


def test_reclaim_mutex_busy_bounded_refusal(tmp_path):
    """A wedged contender holding the reclaim mutex must not park the
    acquirer indefinitely (command dispatch runs on a shared thread
    pool): acquisition refuses within its budget and deletes nothing."""
    _write_lock(tmp_path, {"pid": 999999, "start": 1, "pgid": 999998,
                           "token": "dead", "ts": 0})
    script = (
        "import fcntl, sys, time\n"
        "mh = open(sys.argv[1], 'a')\n"
        "fcntl.flock(mh, fcntl.LOCK_EX)\n"
        "print('READY', flush=True)\n"
        "time.sleep(30)\n"
    )
    mutex = source_lock._reclaim_mutex_path(tmp_path, "lacy")
    mutex.parent.mkdir(parents=True, exist_ok=True)
    child = subprocess.Popen([sys.executable, "-c", script, str(mutex)],
                             stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "READY"
        t0 = time.monotonic()
        lock, why = source_lock.acquire(tmp_path, "lacy",
                                        mutex_budget_s=0.6)
        elapsed = time.monotonic() - t0
        assert lock is None and "busy or unusable" in why, (lock, why)
        assert elapsed < 2.0, elapsed
        # nothing deleted while the mutex was held
        assert json.loads(_lock_path(tmp_path).read_text())["token"] == "dead"
    finally:
        child.kill()
        child.wait()
        child.stdout.close()
    # mutex free again: reclaim proceeds normally
    lock, why = source_lock.acquire(tmp_path, "lacy")
    assert lock is not None, why
    lock.release()


def test_failed_release_retains_ownership_and_recovers(tmp_path):
    """Busy mutex at finalize/release: release fails but ownership
    tracking is RETAINED (the file names this live process, so death
    proof can never clear it). After contention clears, a new start
    retries the pending release and succeeds - no process restart, no
    same-pid exclusivity bypass."""
    lock, why = source_lock.acquire(tmp_path, "lacy")
    assert lock is not None, why
    script = (
        "import fcntl, sys, time\n"
        "mh = open(sys.argv[1], 'a')\n"
        "fcntl.flock(mh, fcntl.LOCK_EX)\n"
        "print('READY', flush=True)\n"
        "time.sleep(30)\n"
    )
    mutex = source_lock._reclaim_mutex_path(tmp_path, "lacy")
    child = subprocess.Popen([sys.executable, "-c", script, str(mutex)],
                             stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "READY"
        # monkeypatch the release budget down so the test is fast
        orig = source_lock._mutex_acquire
        source_lock._mutex_acquire = lambda sd, u, budget, **k: orig(
            sd, u, 0.5, **k)
        try:
            assert lock.release() is False
        finally:
            source_lock._mutex_acquire = orig
        assert lock._release_pending is True
        key = (str(tmp_path), "lacy")
        assert key in source_lock._held      # ownership retained
        # while held, a new acquire must refuse (live hold, no bypass)
        lock2, why2 = source_lock.acquire(tmp_path, "lacy",
                                          mutex_budget_s=0.5)
        assert lock2 is None and "previous release" in why2
    finally:
        child.kill()
        child.wait()
        child.stdout.close()
    # contention cleared: the same process retries the pending release
    # inside acquire and starts a new session without any restart
    lock3, why3 = source_lock.acquire(tmp_path, "lacy")
    assert lock3 is not None, why3
    data = json.loads(_lock_path(tmp_path).read_text())
    assert data["token"] == lock3.token != lock.token
    assert lock3.release() is True


def test_live_owner_refused_and_pid_reuse_recovered(tmp_path):
    pid = os.getpid()
    start = _rp.read_start_time(pid)
    # exact live identity (this process): held, never deleted
    _write_lock(tmp_path, {"pid": pid, "start": start,
                           "pgid": os.getpgrp(), "token": "t", "ts": 0})
    lock, why = source_lock.acquire(tmp_path, "lacy")
    assert lock is None and "still alive" in why
    assert _lock_path(tmp_path).exists()
    # same pid with a DIFFERENT start tick = verifiable PID reuse:
    # the recorded owner is dead, recovery is allowed
    _write_lock(tmp_path, {"pid": pid, "start": start + 1,
                           "pgid": 999999999, "token": "t", "ts": 0})
    lock, why = source_lock.acquire(tmp_path, "lacy")
    assert lock is not None, why
    lock.release()


def test_malformed_lock_fail_closed_never_deleted(tmp_path):
    p = _lock_path(tmp_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"\x00garbage{")
    lock, why = source_lock.acquire(tmp_path, "lacy")
    assert lock is None and "unreadable or malformed" in why
    assert p.read_bytes() == b"\x00garbage{"        # untouched


def test_unverifiable_owner_identity_fail_closed(tmp_path):
    _write_lock(tmp_path, {"pid": "x", "start": None, "pgid": 1,
                           "token": "t", "ts": 0})
    lock, why = source_lock.acquire(tmp_path, "lacy")
    assert lock is None and "no verifiable owner identity" in why
    assert _lock_path(tmp_path).exists()


def test_incomplete_process_visibility_fail_closed(tmp_path, monkeypatch):
    # owner pid absent but /proc itself unlistable: absence is not proof
    monkeypatch.setattr(source_lock, "_own_identity",
                        lambda proc: {"pid": os.getpid(), "start": 1,
                                      "pgid": os.getpgrp()})
    _write_lock(tmp_path, {"pid": 999999, "start": 1, "pgid": 999998,
                           "token": "t", "ts": 0})
    lock, why = source_lock.acquire(tmp_path, "lacy",
                                    proc="/nonexistent-proc-dir")
    assert lock is None and "no process visibility" in why
    assert _lock_path(tmp_path).exists()


def test_no_proc_identity_not_acquired(tmp_path):
    # own start-time unreadable: no grounded identity, fail closed
    lock, why = source_lock.acquire(tmp_path, "lacy",
                                    proc="/nonexistent-proc-dir")
    assert lock is None and "cannot record this process" in why


def test_finalized_hook_runs_only_when_proven():
    calls = []
    sess = object.__new__(ClipSession)
    sess._owners_drained = True
    sess._session_id = None
    sess._task = None
    sess._owner_drained = lambda task=None: True
    sess._on_finalized = lambda: calls.append(1)
    ClipSession._mark_drained(sess)
    assert sess._finalized is True
    assert calls == [1]

    calls2 = []
    sess2 = object.__new__(ClipSession)
    sess2._owners_drained = False
    sess2._session_id = None
    sess2._task = None
    sess2._owner_drained = lambda task=None: False
    sess2._on_finalized = lambda: calls2.append(1)
    ClipSession._mark_drained(sess2)
    assert not getattr(sess2, "_finalized", False)
    assert calls2 == []
