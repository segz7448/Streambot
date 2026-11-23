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
-- Daily upload-START budget ledger: one durable row per (local day,
-- clip). This is a conservative count of distinct clip upload STARTS
-- per reserve-day, NOT proven successful publications per finish-day;
-- a midnight-spanning upload is charged to its start day. source_key
-- is written once at first charge and never rewritten, so a later
-- source-identity change cannot mint a second allowance.
CREATE TABLE IF NOT EXISTS publication_budget (
    day TEXT NOT NULL,
    clip_id INTEGER NOT NULL,
    source_key TEXT NOT NULL,
    created_at REAL NOT NULL,
    UNIQUE(day, clip_id)
);
-- Evidence that the conservative same-day bootstrap ran (once per day).
CREATE TABLE IF NOT EXISTS publication_budget_meta (
    day TEXT PRIMARY KEY,
    bootstrapped_at REAL NOT NULL,
    rows_charged INTEGER NOT NULL
);
"""

# (global, per-source) distinct clip upload starts per local day. The
# per-source rule is redundant while both equal the retained global
# ceiling of 8; it exists for deliberate future policy, and only tests
# inject different (smaller) values via reserve(budget_limits=...).
DEFAULT_BUDGET_LIMITS = (8, 8)
_UNKNOWN_SOURCE = "unknown"

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


def _budget_tz():
    from ..core.intake import resolve_timezone, DEFAULT_TIMEZONE
    return resolve_timezone(DEFAULT_TIMEZONE)


def _local_day(ts, tz) -> str:
    from datetime import datetime
    return datetime.fromtimestamp(float(ts), tz).date().isoformat()


def _valid_budget_ts(ts, now) -> bool:
    """A bootstrap timestamp must be a finite real number, not in the
    future, and convertible by the local-day function; anything else
    leaves the day UNRESOLVED (fail closed)."""
    import math
    if (ts is None or isinstance(ts, bool)
            or not isinstance(ts, (int, float)) or not math.isfinite(ts)
            or ts > float(now)):
        return False
    try:
        _local_day(ts, _budget_tz())
    except (OverflowError, OSError, ValueError):
        return False
    return True


def _budget_source_key(conn, clip_id) -> str:
    """Resolve a clip's budget identity from provenance TYPE, never from
    the human source label. Registered feed/playback sources use the
    canonical broadcaster id when present, else the stable source row id;
    owner-uploaded (retrospective) ingest and anything unresolvable share
    the 'unknown' bucket, which has its own per-source allowance and is
    never identified with a registered source. Existing budget rows are
    never treated as proof of a live identity. Unreadable rows degrade
    to 'unknown' rather than failing the reservation.
    """
    try:
        c = conn.execute('SELECT session_id FROM clips WHERE id=?',
                         (int(clip_id),)).fetchone()
        if c is None or c[0] is None:
            return _UNKNOWN_SOURCE
        s = conn.execute('SELECT source_id, ingest_mode FROM stream_sessions'
                         ' WHERE id=?', (c[0],)).fetchone()
        if s is None:
            return _UNKNOWN_SOURCE
        if s[1] not in ('feed', 'playback'):
            return _UNKNOWN_SOURCE
        t = conn.execute('SELECT id, broadcaster_id FROM twitch_sources'
                         ' WHERE id=?', (s[0],)).fetchone()
        if t is None:
            return _UNKNOWN_SOURCE
        if t[1]:
            return 'twitch:' + str(t[1])
        return 'source-row:' + str(t[0])
    except sqlite3.Error:
        return _UNKNOWN_SOURCE


def _bootstrap_budget(conn, today, now, tz) -> str | None:
    """Charge every existing publish-records row whose created_at OR
    updated_at falls on today. Overcount is deliberate and documented:
    same-day confirm/recovery/reconciliation updates charge a row whose
    upload actually started earlier; a charged row that was never truly
    started today consumes budget anyway (conservative). A timestamp
    that is missing, non-numeric or in the future leaves the day
    UNRESOLVED, which fails the reservation closed (never silently
    skipped). Runs once per day; the meta row is written in the same
    transaction as the charges.
    """
    if conn.execute('SELECT 1 FROM publication_budget_meta WHERE day=?',
                    (today,)).fetchone():
        return None
    rows = conn.execute(
        'SELECT clip_id, created_at, updated_at FROM publish_records').fetchall()
    charged = 0
    for r in rows:
        days = set()
        for ts in (r['created_at'], r['updated_at']):
            if not _valid_budget_ts(ts, now):
                return ('unresolved timestamp on publish-records clip %s'
                        % r['clip_id'])
            days.add(_local_day(ts, tz))
        if today in days:
            conn.execute(
                'INSERT OR IGNORE INTO publication_budget '
                '(day, clip_id, source_key, created_at) VALUES (?,?,?,?)',
                (today, int(r['clip_id']),
                 _budget_source_key(conn, r['clip_id']), float(r['created_at'])))
            charged += 1
    conn.execute(
        'INSERT INTO publication_budget_meta (day, bootstrapped_at, rows_charged)'
        ' VALUES (?,?,?)', (today, float(now), charged))
    return None


def get(state_dir, clip_id):
    with closing(_connect(state_dir)) as conn:
        row = conn.execute('SELECT * FROM publish_records WHERE clip_id=?',
                           (int(clip_id),)).fetchone()
        return dict(row) if row else None


def reserve(state_dir, clip_id, reserved_by, *, lease, gate=None,
            budget_limits=None):
    """gate(conn)->bool is evaluated inside BEGIN IMMEDIATE; False returns
    ('gated', None) with nothing written.

    budget_limits is (global, per-source) distinct-clip upload STARTS per
    local day; None means DEFAULT_BUDGET_LIMITS. A reservation past a
    limit returns ('budget_capped', {'scope','limit','day'}), or scope
    'bootstrap' when conservative same-day reconstruction could not be
    resolved, with nothing written. Production callers never pass this:
    the retained global ceiling of 8 applies.
    """
    lease.require(state_dir, clip_id)
    now, token = time.time(), secrets.token_hex(16)
    with closing(_connect(state_dir)) as conn:
        conn.execute('BEGIN IMMEDIATE')
        if gate is not None and not gate(conn):
            conn.rollback()
            return 'gated', None
        # playback-origin clips need CURRENT repost authorization at the
        # single reservation boundary shared by native, manual and
        # operator paths (provenance read from durable rows)
        from ..core.provenance import repost_allowed
        origin = repost_allowed(conn, clip_id)
        if not origin.allowed:
            conn.rollback()
            return 'repost_denied', {'reason': origin.reason,
                                     'retryable': origin.retryable}
        row = conn.execute('SELECT * FROM publish_records WHERE clip_id=?',
                           (clip_id,)).fetchone()
        if row is not None and row['state'] != 'FAILED':
            conn.rollback()
            return 'blocked', dict(row)
        # Authoritative reservation time is captured HERE, after the
        # schema lock, BEGIN IMMEDIATE and the gate/provenance reads: a
        # wait that crosses local midnight must charge the day the
        # reservation actually starts on, never the day the caller
        # arrived. The row timestamps below use this same value.
        now = time.time()
        # Existing publication state is settled above; only a genuinely
        # new reservation consumes daily budget. Bootstrap of same-day
        # legacy evidence runs first, in this same transaction.
        if budget_limits is None:
            budget_limits = DEFAULT_BUDGET_LIMITS
        tz = _budget_tz()
        today = _local_day(now, tz)
        err = _bootstrap_budget(conn, today, now, tz)
        if err is not None:
            conn.rollback()
            return 'budget_capped', {'scope': 'bootstrap', 'day': today,
                                     'reason': err}
        key = _budget_source_key(conn, clip_id)
        conn.execute('INSERT OR IGNORE INTO publication_budget '
                     '(day, clip_id, source_key, created_at) VALUES (?,?,?,?)',
                     (today, int(clip_id), key, now))
        glim, slim = (int(budget_limits[0]), int(budget_limits[1]))
        n_global = conn.execute(
            'SELECT COUNT(DISTINCT clip_id) FROM publication_budget WHERE day=?',
            (today,)).fetchone()[0]
        if n_global > glim:
            conn.rollback()
            return 'budget_capped', {'scope': 'global', 'limit': glim,
                                     'day': today}
        n_src = conn.execute(
            'SELECT COUNT(DISTINCT clip_id) FROM publication_budget'
            ' WHERE day=? AND source_key=?', (today, key)).fetchone()[0]
        if n_src > slim:
            conn.rollback()
            return 'budget_capped', {'scope': 'source', 'limit': slim,
                                     'day': today, 'source_key': key}
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
                attempt_token, video_id=None, error=None, notice=None):
    lease.require(state_dir, clip_id)
    if not attempt_token:
        raise PublishStateError('nonempty attempt token required')
    with closing(_connect(state_dir)) as conn:
        if notice is not None:
            # The owner outcome notice commits in the SAME transaction as
            # the terminal transition: a crash can never persist one
            # without the other, and the (state, attempt_token) guard
            # makes the pair exactly-once per attempt - a retried job
            # finds the row already transitioned and cannot re-notify.
            from ..core import schedule
            conn.executescript(schedule.DDL)
            conn.execute('BEGIN IMMEDIATE')
            try:
                cur = conn.execute('UPDATE publish_records SET state=?,video_id=COALESCE(?,video_id),'
                                   'last_error=?,updated_at=? WHERE clip_id=? AND state=? AND attempt_token=?',
                                   (to_state, video_id, (error or '')[:2000] or None,
                                    time.time(), clip_id, from_state, attempt_token))
                if cur.rowcount != 1:
                    raise PublishStateError('publish transition rejected: stale attempt or state mismatch')
                schedule.notice_on_conn(conn, notice)
            except BaseException:
                conn.execute('ROLLBACK')
                raise
            conn.execute('COMMIT')
            return
        cur = conn.execute('UPDATE publish_records SET state=?,video_id=COALESCE(?,video_id),'
                           'last_error=?,updated_at=? WHERE clip_id=? AND state=? AND attempt_token=?',
                           (to_state, video_id, (error or '')[:2000] or None,
                            time.time(), clip_id, from_state, attempt_token))
        if cur.rowcount != 1:
            raise PublishStateError('publish transition rejected: stale attempt or state mismatch')


def confirm(state_dir, clip_id, video_id, *, lease, attempt_token, notice=None):
    if not isinstance(video_id, str) or not video_id.strip():
        raise PublishStateError('nonempty confirmed video id required')
    _transition(state_dir, clip_id, 'RESERVED', 'CONFIRMED', lease=lease,
                attempt_token=attempt_token, video_id=video_id, notice=notice)


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
