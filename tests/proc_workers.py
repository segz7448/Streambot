"""Real separate worker processes for cross-process safety tests.

Run as `python -m tests.proc_workers <mode> ...` through subprocess.Popen.
Uses only pipes and sqlite/file locks, never multiprocessing semaphores, so it
works where sem_open is denied (Android) while still crossing a real process
boundary.
"""
import json
import os
import select
import subprocess
import sys
import time
from pathlib import Path


def _close_stdio(proc):
    for stream in (proc.stdin, proc.stdout, proc.stderr):
        try:
            if stream is not None:
                stream.close()
        except OSError:
            pass


def spawn_ready(argv, cwd, timeout=10.0, expect='READY'):
    """Start a worker and wait a bounded time for its readiness line.

    On timeout, early exit or a wrong line the child is killed, reaped and its
    pipes closed before AssertionError is raised, so nothing is left behind.
    """
    proc = subprocess.Popen(argv, cwd=cwd, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    fd = proc.stdout.fileno()
    deadline = time.monotonic() + timeout
    buf = b''
    problem = None
    while b'\n' not in buf:
        left = deadline - time.monotonic()
        if left <= 0:
            problem = 'timed out before readiness'
            break
        ready, _, _ = select.select([fd], [], [], left)
        if not ready:
            continue
        chunk = os.read(fd, 4096)
        if not chunk:
            problem = 'exited before readiness (rc=%r)' % proc.poll()
            break
        buf += chunk
    if problem is None and buf.split(b'\n', 1)[0].decode(
            'utf-8', 'replace').strip() != expect:
        problem = 'unexpected readiness line'
    if problem is not None:
        proc.kill()
        proc.wait(5)
        _close_stdio(proc)
        raise AssertionError('worker ' + problem)
    return proc


def _admit(db, clip_id):
    from tests.unit.test_intake import _admit_one
    print(json.dumps(_admit_one(db, clip_id)), flush=True)


def _hold(state):
    from app.distribution import publish_records as pr
    from tests.unit.test_publish_records import _reserve
    with pr.ClipLease(Path(state), 1) as lease:
        _, row = _reserve(Path(state), 1, 'child', lease=lease)
        if pr.get(Path(state), 1)['state'] != 'RESERVED':
            raise SystemExit('not reserved before READY')
        print('READY', flush=True)
        sys.stdin.readline()  # parent releases by writing a line; kill = EOF-less death
        pr.confirm(Path(state), 1, 'child-video', lease=lease,
                   attempt_token=row['attempt_token'])


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'admit':
        _admit(sys.argv[2], sys.argv[3])
    elif mode == 'hold':
        _hold(sys.argv[2])
    else:
        raise SystemExit(2)
