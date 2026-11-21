"""Real separate worker processes for cross-process safety tests.

Run as `python -m tests.proc_workers <mode> ...` through subprocess.Popen.
Uses only pipes and sqlite/file locks, never multiprocessing semaphores, so it
works where sem_open is denied (Android) while still crossing a real process
boundary.
"""
import json
import sys
from pathlib import Path


def _admit(db, clip_id):
    from tests.unit.test_intake import _admit_one
    print(json.dumps(_admit_one(db, clip_id)), flush=True)


def _hold(state):
    from app.distribution import publish_records as pr
    from tests.unit.test_publish_records import _reserve
    with pr.ClipLease(Path(state), 1) as lease:
        _, row = _reserve(Path(state), 1, 'child', lease=lease)
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
