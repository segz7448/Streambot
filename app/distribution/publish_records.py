"""Durable per-clip ownership, guarded by a kernel-held cross-process lease.

A lease is held across reserve/upload/confirm. Recovery and reconciliation
must obtain the same lease, so neither can invalidate a living uploader.
Attempt tokens fence transitions from stale callers. Crash releases the
kernel lease but leaves RESERVED evidence, recovered only to UNCERTAIN.
"""
from __future__ import annotations

from contextlib import contextmanager, closing
import fcntl
import os
from pathlib import Path
import secrets
import sqlite3
import time

DDL = """
CREATE TABLE IF NOT EXISTS publish_records (
    clip_id INTEGER PRIMARY KEY,
    state TEXT NOT NULL,
    video_id TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    reserved_by TEXT,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    attempt_token TEXT
);
"""

class PublishBusy(RuntimeError):
    pass

class PublishStateError(RuntimeError):
    pass


class ClipLease:
    def __init__(self, state_dir: Path, clip_id: int):
        self.state_dir = Path(state_dir).resolve()
        self.clip_id = int(clip_id)
        self.fd = None
        self.pid = os.getpid()

    def __enter__(self):
        if self.fd is not None:
            raise PublishStateError('lease already entered')
        root = self.state_dir / 'publish-locks'
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(str(root / f'clip-{self.clip_id}.lock'),
                     os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise PublishBusy('a live publisher holds this clip lease') from None
        except OSError:
            os.close(fd)
            raise
        self.fd = fd
        return self

    def require(self, state_dir, clip_id):
        if (self.fd is None or self.pid != os.getpid() or
                self.state_dir != Path(state_dir).resolve() or
                self.clip_id != int(clip_id)):
            raise PublishStateError('valid matching clip lease required')

    def __exit__(self, *args):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def _connect(state_dir):
    from ..core.database import connect
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    # Serializes initial DDL/upgrade too, including cross-process callers.
    fd = os.open(str(state_dir / 'publish-schema.lock'), os.O_CREAT | os.O_RDWR, 0o600)
    conn = None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        conn = connect(str(state_dir / 'nova.db'))
        conn.executescript(DDL)
        columns = {r['name'] for r in conn.execute('PRAGMA table_info(publish_records)')}
        if 'attempt_token' not in columns:
            conn.execute('ALTER TABLE publish_records ADD COLUMN attempt_token TEXT')
        return conn
    except Exception:
        if conn is not None:
            conn.close()
        raise
    finally:
        os.close(fd)


def get(state_dir, clip_id):
    with closing(_connect(state_dir)) as conn:
        row = conn.execute('SELECT * FROM publish_records WHERE clip_id=?',
                           (int(clip_id),)).fetchone()
        return dict(row) if row else None


def reserve(state_dir, clip_id, reserved_by, *, lease):
    lease.require(state_dir, clip_id)
    now, token = time.time(), secrets.token_hex(16)
    with closing(_connect(state_dir)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        row = conn.execute('SELECT * FROM publish_records WHERE clip_id=?',
                           (clip_id,)).fetchone()
        if row is not None and row['state'] != 'FAILED':
            conn.rollback()
            return 'blocked', dict(row)
        if row is None:
            conn.execute("INSERT INTO publish_records (clip_id,state,attempts,reserved_by,"
                         "created_at,updated_at,attempt_token) VALUES (?,'RESERVED',1,?,?,?,?)",
                         (clip_id, reserved_by[:128], now, now, token))
        else:
            conn.execute("UPDATE publish_records SET state='RESERVED',attempts=attempts+1,"
                         "reserved_by=?,last_error=NULL,updated_at=?,attempt_token=? WHERE clip_id=?",
                         (reserved_by[:128], now, token, clip_id))
        conn.commit()
    return 'reserved', get(state_dir, clip_id)


def _transition(state_dir, clip_id, from_state, to_state, *, lease,
                attempt_token, video_id=None, error=None):
    lease.require(state_dir, clip_id)
    if not attempt_token:
        raise PublishStateError('nonempty attempt token required')
    with closing(_connect(state_dir)) as conn:
        cur = conn.execute('UPDATE publish_records SET state=?,video_id=COALESCE(?,video_id),'
                           'last_error=?,updated_at=? WHERE clip_id=? AND state=? AND attempt_token=?',
                           (to_state, video_id, (error or '')[:2000] or None,
                            time.time(), clip_id, from_state, attempt_token))
        if cur.rowcount != 1:
            raise PublishStateError('publish transition rejected: stale attempt or state mismatch')


def confirm(state_dir, clip_id, video_id, *, lease, attempt_token):
    if not isinstance(video_id, str) or not video_id.strip():
        raise PublishStateError('nonempty confirmed video id required')
    _transition(state_dir, clip_id, 'RESERVED', 'CONFIRMED', lease=lease,
                attempt_token=attempt_token, video_id=video_id)


def fail_definitive(state_dir, clip_id, error, *, lease, attempt_token):
    _transition(state_dir, clip_id, 'RESERVED', 'FAILED', lease=lease,
                attempt_token=attempt_token, error=error)


def mark_uncertain(state_dir, clip_id, error, *, lease, attempt_token):
    _transition(state_dir, clip_id, 'RESERVED', 'UNCERTAIN', lease=lease,
                attempt_token=attempt_token, error=error)


def recover_interrupted(state_dir):
    with closing(_connect(state_dir)) as conn:
        ids = [r[0] for r in conn.execute("SELECT clip_id FROM publish_records WHERE state='RESERVED'")]
    changed = 0
    for clip_id in ids:
        try:
            with ClipLease(state_dir, clip_id) as lease:
                row = get(state_dir, clip_id)
                if row and row['state'] == 'RESERVED':
                    # Old reservations may predate attempt tokens.
                    with closing(_connect(state_dir)) as conn:
                        cur = conn.execute("UPDATE publish_records SET state='UNCERTAIN',updated_at=?,"
                            "last_error='owner lease released before confirmation; reconcile in Studio' "
                            "WHERE clip_id=? AND state='RESERVED'", (time.time(), clip_id))
                        changed += cur.rowcount
        except PublishBusy:
            continue  # living uploader, never invalidate it
    return changed


def reconcile(state_dir, clip_id, *, expected_attempt_token, video_id='', absent_verified=False):
    if not video_id and not absent_verified:
        raise PublishStateError('explicit verified absence required to allow retry')
    with ClipLease(state_dir, clip_id) as lease:
        row = get(state_dir, clip_id)
        if not row or row['state'] != 'UNCERTAIN' or row['attempt_token'] != expected_attempt_token:
            raise PublishStateError('reconciliation attempt/state changed')
        with closing(_connect(state_dir)) as conn:
            cur = conn.execute("UPDATE publish_records SET state=?,video_id=?,updated_at=?,last_error=? "
                "WHERE clip_id=? AND state='UNCERTAIN' AND attempt_token IS ?",
                ('CONFIRMED' if video_id else 'FAILED', video_id or None, time.time(),
                 None if video_id else 'operator verified no uploaded video', clip_id, expected_attempt_token))
            if cur.rowcount != 1:
                raise PublishStateError('reconciliation did not persist')


def promote_legacy_entries(state_dir, entries):
    """Persist all surviving old evidence BEFORE journal overwrite/prune.
    Insert-only, never changes a live reservation. Uncertain wins when old
    success and unresolved uncertainty both exist for the same clip.
    No clip FK: tombstones must survive clip deletion and preserve ids.
    """
    evidence = {}
    for key, entry in entries.items():
        if not isinstance(entry, dict) or entry.get('command') != 'publish':
            continue
        result = entry.get('result')
        if key.startswith('publish:') and key[8:].isdigit() and isinstance(result, str) and result.startswith('yt:') and result[3:]:
            evidence.setdefault(int(key[8:]), ('CONFIRMED', result[3:]))
        if entry.get('status') == 'uncertain':
            parts = entry.get('text', '').split()
            if len(parts) >= 2 and parts[0].split('@')[0] == '/publish':
                try:
                    evidence[int(parts[1])] = ('UNCERTAIN', None)
                except ValueError:
                    pass
    if not evidence:
        return 0
    now, inserted = time.time(), 0
    with closing(_connect(state_dir)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        for clip_id, (state, video_id) in evidence.items():
            cur = conn.execute('INSERT OR IGNORE INTO publish_records '
                '(clip_id,state,video_id,attempts,reserved_by,created_at,updated_at) '
                "VALUES (?,?,?,0,'legacy-journal',?,?)", (clip_id,state,video_id,now,now))
            inserted += cur.rowcount
        conn.commit()
    return inserted


def promote_legacy(state_dir):
    from ..core.state import locked_update
    from ..bot.ops import JOURNAL
    def apply(data):
        count = promote_legacy_entries(state_dir, data)
        return data, count
    return locked_update(Path(state_dir) / JOURNAL, {}, apply)
