from contextlib import closing
from tests.unit import _prov
from datetime import datetime, timedelta
import os
import subprocess
import sys
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest import mock
from app.core import database
from app.core.database import migrate
from app.distribution import publish_records as pr


def _reserve(sd, cid, *a, **k):
    _prov.seed_clip(sd, cid)
    return pr.reserve(sd, cid, *a, **k)


class DailyBudgetTests(unittest.TestCase):
    """Daily upload-START budget. reserve(budget_limits=...) is a TEST
    injection point for small ceilings; nothing here asserts production
    behavior - production keeps the retained global ceiling (8,8) via
    the reserve default and never passes budget_limits.
    """

    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        with closing(database.connect(str(self.d / 'nova.db'))) as conn, conn:
            migrate(conn)

    def _seed(self, cid, user='fixture-feed', mode='feed',
              broadcaster_id=None, d=None):
        d = d or self.d
        conn = database.connect(str(d / 'nova.db'))
        try:
            n = time.time()
            with conn:
                conn.execute('INSERT OR IGNORE INTO twitch_sources'
                             ' (username,created_at,updated_at) VALUES (?,?,?)',
                             (user, n, n))
                if broadcaster_id is not None:
                    conn.execute('UPDATE twitch_sources SET broadcaster_id=?'
                                 ' WHERE username=?', (broadcaster_id, user))
                sid = conn.execute('SELECT id FROM twitch_sources'
                                   ' WHERE username=?', (user,)).fetchone()[0]
                sess = conn.execute("INSERT INTO stream_sessions"
                                    " (source_id,state,ingest_mode)"
                                    " VALUES (?,'ENDED',?)",
                                    (sid, mode)).lastrowid
                conn.execute("INSERT INTO clips (id,session_id,status,created_at)"
                             " VALUES (?,?,'HELD',?)", (int(cid), sess, n))
        finally:
            conn.close()

    def _reserve(self, cid, limits=(2, 2)):
        with pr.ClipLease(self.d, cid) as lease:
            return pr.reserve(self.d, cid, 'test', lease=lease,
                              budget_limits=limits)

    def _budget_rows(self):
        with closing(database.connect(str(self.d / 'nova.db'))) as conn:
            return conn.execute(
                'SELECT day, clip_id, source_key FROM publication_budget'
                ' ORDER BY clip_id').fetchall()

    def test_global_cap_on_distinct_clip_starts(self):
        for cid in (1, 2):
            self._seed(cid)
            self.assertEqual(self._reserve(cid)[0], 'reserved')
        self._seed(3)
        decision, info = self._reserve(3)
        self.assertEqual(decision, 'budget_capped')
        self.assertEqual(info['scope'], 'global')
        self.assertEqual(info['limit'], 2)
        # nothing reserved and the capped charge rolled back
        self.assertIsNone(pr.get(self.d, 3))
        self.assertEqual([r['clip_id'] for r in self._budget_rows()], [1, 2])

    def test_per_source_cap_and_disjoint_namespaces(self):
        self._seed(1, 'alice'); self._seed(2, 'alice'); self._seed(3, 'alice')
        self._seed(4, 'bob', broadcaster_id='b-77')
        for cid in (1, 2):
            self.assertEqual(self._reserve(cid, (10, 2))[0], 'reserved')
        decision, info = self._reserve(3, (10, 2))
        self.assertEqual((decision, info['scope']), ('budget_capped', 'source'))
        self.assertTrue(info['source_key'].startswith('source-row:'))
        # a different source keeps its own allowance
        self.assertEqual(self._reserve(4, (10, 2))[0], 'reserved')
        keys = {r['source_key'] for r in self._budget_rows()}
        self.assertEqual(len(keys), 2)
        self.assertIn('twitch:b-77', keys)
        self.assertTrue(any(k.startswith('source-row:') for k in keys))

    def test_failed_retry_same_day_is_free(self):
        self._seed(1)
        decision, row = self._reserve(1, (1, 1))
        self.assertEqual(decision, 'reserved')
        with pr.ClipLease(self.d, 1) as lease:
            pr.fail_definitive(self.d, 1, 'nope', lease=lease,
                               attempt_token=row['attempt_token'])
        # retrying the same clip the same day consumes nothing new
        self.assertEqual(self._reserve(1, (1, 1))[0], 'reserved')
        self._seed(2)
        decision, info = self._reserve(2, (1, 1))
        self.assertEqual((decision, info['scope']), ('budget_capped', 'global'))
        self.assertEqual(len(self._budget_rows()), 1)

    def test_in_flight_state_settles_before_budget(self):
        self._seed(1)
        self.assertEqual(self._reserve(1, (1, 1))[0], 'reserved')
        # an in-flight reservation is blocked by state, not by budget,
        # and never double-charged
        self.assertEqual(self._reserve(1, (1, 1))[0], 'blocked')
        self.assertEqual(len(self._budget_rows()), 1)

    def test_repost_denied_consumes_no_budget(self):
        self._seed(1, mode='playback')  # no repost grant
        decision, _ = self._reserve(1, (8, 8))
        self.assertEqual(decision, 'repost_denied')
        self.assertEqual(self._budget_rows(), [])

    def test_upload_ingest_clips_share_the_unknown_bucket(self):
        self._seed(1, 'alice', mode='upload')
        self._seed(2, 'bob', mode='upload')
        self.assertEqual(self._reserve(1, (10, 1))[0], 'reserved')
        decision, info = self._reserve(2, (10, 1))
        self.assertEqual((decision, info['scope']), ('budget_capped', 'source'))
        self.assertEqual(info['source_key'], 'unknown')

    def test_bootstrap_charges_same_day_legacy_rows(self):
        tz = pr._budget_tz()
        noon = datetime.fromtimestamp(time.time(), tz).replace(
            hour=12, minute=0, second=0, microsecond=0)
        y = (noon - timedelta(days=1)).timestamp()
        now = time.time()
        with closing(database.connect(str(self.d / 'nova.db'))) as conn, conn:
            # clip 9: created yesterday but touched today -> charged
            # clip 10: entirely yesterday -> never charged
            for cid, ca, ua in ((9, y, now), (10, y, y + 60)):
                conn.execute("INSERT INTO publish_records"
                             " (clip_id,state,attempts,created_at,updated_at)"
                             " VALUES (?,'CONFIRMED',1,?,?)", (cid, ca, ua))
        self._seed(1); self._seed(2)
        self.assertEqual(self._reserve(1, (2, 2))[0], 'reserved')
        decision, info = self._reserve(2, (2, 2))
        self.assertEqual((decision, info['scope']), ('budget_capped', 'global'))
        with closing(database.connect(str(self.d / 'nova.db'))) as conn:
            meta = conn.execute('SELECT rows_charged FROM'
                                ' publication_budget_meta').fetchone()
        self.assertIsNotNone(meta)
        self.assertEqual(meta[0], 1)  # exactly one legacy row charged
        self.assertEqual({r['clip_id'] for r in self._budget_rows()}, {1, 9})

    def test_bootstrap_fails_closed_on_unresolved_timestamp(self):
        now = time.time()
        with closing(database.connect(str(self.d / 'nova.db'))) as conn, conn:
            conn.execute("INSERT INTO publish_records"
                         " (clip_id,state,attempts,created_at,updated_at)"
                         " VALUES (9,'RESERVED',1,?,?)", (now, now + 90000))
        self._seed(1)
        decision, info = self._reserve(1, (8, 8))
        self.assertEqual((decision, info['scope']),
                         ('budget_capped', 'bootstrap'))
        self.assertIsNone(pr.get(self.d, 1))
        with closing(database.connect(str(self.d / 'nova.db'))) as conn:
            self.assertIsNone(conn.execute(
                'SELECT 1 FROM publication_budget_meta').fetchone())
            self.assertEqual(conn.execute(
                'SELECT COUNT(*) FROM publication_budget').fetchone()[0], 0)

    def test_reserve_time_recaptured_after_lock_across_midnight(self):
        # A caller arriving at 23:59:59 local whose reservation actually
        # starts at 00:00:01 the next day is charged to the NEW day; the
        # prior day's ceiling cannot be bypassed by schema-lock wait.
        tz = pr._budget_tz()
        before = datetime(2026, 11, 23, 23, 59, 59, tzinfo=tz).timestamp()
        after = datetime(2026, 11, 24, 0, 0, 1, tzinfo=tz).timestamp()
        day1, day2 = pr._local_day(before, tz), pr._local_day(after, tz)
        self.assertNotEqual(day1, day2)
        pr.get(self.d, 0)  # create budget tables via publish_records DDL
        with closing(database.connect(str(self.d / 'nova.db'))) as conn, conn:
            conn.execute('INSERT INTO publication_budget_meta'
                         ' (day,bootstrapped_at,rows_charged) VALUES (?,?,?)',
                         (day1, before, 2))
            for cid in (11, 12):
                conn.execute('INSERT INTO publication_budget'
                             ' (day,clip_id,source_key,created_at)'
                             ' VALUES (?,?,?,?)', (day1, cid, 'unknown', before))
        self._seed(1)

        class _Clock:
            val = before

        def gate(conn):  # runs inside BEGIN IMMEDIATE, after the lock
            _Clock.val = after  # the lock wait crossed local midnight
            return True

        with mock.patch.object(pr.time, 'time',
                               side_effect=lambda: _Clock.val):
            with pr.ClipLease(self.d, 1) as lease:
                decision, row = pr.reserve(self.d, 1, 'test', lease=lease,
                                           gate=gate, budget_limits=(2, 2))
        self.assertEqual(decision, 'reserved')
        days = [r['day'] for r in self._budget_rows() if r['clip_id'] == 1]
        self.assertEqual(days, [day2])

    def test_bootstrap_fails_closed_on_nonfinite_or_out_of_range(self):
        # (NULL is impossible: columns are NOT NULL and sqlite maps NaN
        # to NULL, so both are excluded by schema; the remaining
        # unverifiable classes are nonfinite and out-of-range values.)
        for bad in (float('-inf'), -1e20):
            d = Path(tempfile.mkdtemp())
            with closing(database.connect(str(d / 'nova.db'))) as conn, conn:
                migrate(conn)
                now = time.time()
                # one valid same-day legacy row, then one unverifiable:
                # the failure must roll back the partial bootstrap too
                conn.execute("INSERT INTO publish_records"
                             " (clip_id,state,attempts,created_at,updated_at)"
                             " VALUES (8,'CONFIRMED',1,?,?)", (now, now))
                conn.execute("INSERT INTO publish_records"
                             " (clip_id,state,attempts,created_at,updated_at)"
                             " VALUES (9,'CONFIRMED',1,?,?)", (now, bad))
            self._seed(1, d=d)
            with pr.ClipLease(d, 1) as lease:
                decision, info = pr.reserve(d, 1, 'test', lease=lease,
                                            budget_limits=(8, 8))
            self.assertEqual((decision, info['scope']),
                             ('budget_capped', 'bootstrap'), repr(bad))
            self.assertIsNone(pr.get(d, 1))
            with closing(database.connect(str(d / 'nova.db'))) as conn:
                self.assertEqual(conn.execute(
                    'SELECT COUNT(*) FROM publication_budget').fetchone()[0], 0)
                self.assertIsNone(conn.execute(
                    'SELECT 1 FROM publication_budget_meta').fetchone())

    def test_default_limits_are_the_retained_ceiling(self):
        # documents the default constant; not a production-behavior claim
        self.assertEqual(pr.DEFAULT_BUDGET_LIMITS, (8, 8))


class PublishRecordsTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        with closing(sqlite3.connect(str(self.d / 'nova.db'))) as conn, conn:
            migrate(conn)
            for n in range(1, 5):
                conn.execute('INSERT INTO clips(id,created_at) VALUES (?,0)', (n,))

    def test_confirmed_blocks_and_stale_token_rejected(self):
        with pr.ClipLease(self.d, 1) as lease:
            decision, row = _reserve(self.d, 1, 'test', lease=lease)
            self.assertEqual(decision, 'reserved')
            token = row['attempt_token']
            with self.assertRaises(pr.PublishStateError):
                pr.confirm(self.d, 1, 'bad', lease=lease, attempt_token='wrong')
            pr.confirm(self.d, 1, 'video-id', lease=lease, attempt_token=token)
            with self.assertRaises(pr.PublishStateError):
                pr.confirm(self.d, 1, 'other', lease=lease, attempt_token=token)
            self.assertEqual(_reserve(self.d, 1, 'repeat', lease=lease)[0], 'blocked')
        self.assertEqual(pr.get(self.d, 1)['video_id'], 'video-id')
        with self.assertRaises(pr.PublishStateError):
            pr.confirm(self.d, 1, 'closed', lease=lease, attempt_token=token)

    def test_definitive_failure_new_token_fences_old_attempt(self):
        with pr.ClipLease(self.d, 1) as lease:
            _, first = _reserve(self.d, 1, 'test', lease=lease)
            pr.fail_definitive(self.d, 1, 'auth', lease=lease, attempt_token=first['attempt_token'])
            _, second = _reserve(self.d, 1, 'retry', lease=lease)
            self.assertEqual(second['attempts'], 2)
            self.assertNotEqual(first['attempt_token'], second['attempt_token'])
            with self.assertRaises(pr.PublishStateError):
                pr.mark_uncertain(self.d, 1, 'late', lease=lease, attempt_token=first['attempt_token'])

    def test_uncertainty_requires_exact_reconciliation(self):
        with pr.ClipLease(self.d, 1) as lease:
            _, row = _reserve(self.d, 1, 'test', lease=lease)
            token = row['attempt_token']
            pr.mark_uncertain(self.d, 1, 'timeout', lease=lease, attempt_token=token)
            with self.assertRaises(pr.PublishBusy):
                pr.reconcile(self.d, 1, expected_attempt_token=token, video_id='live')
        with self.assertRaises(pr.PublishStateError):
            pr.reconcile(self.d, 1, expected_attempt_token='wrong', video_id='found')
        with self.assertRaises(pr.PublishStateError):
            pr.reconcile(self.d, 1, expected_attempt_token=token)
        pr.reconcile(self.d, 1, expected_attempt_token=token, absent_verified=True)
        with pr.ClipLease(self.d, 1) as lease:
            self.assertEqual(_reserve(self.d, 1, 'retry', lease=lease)[0], 'reserved')

    def test_recovery_skips_live_then_recovers_after_lease_release(self):
        with pr.ClipLease(self.d, 1) as lease:
            _reserve(self.d, 1, 'test', lease=lease)
            self.assertEqual(pr.recover_interrupted(self.d), 0)
            self.assertEqual(pr.get(self.d, 1)['state'], 'RESERVED')
        self.assertEqual(pr.recover_interrupted(self.d), 1)
        self.assertEqual(pr.get(self.d, 1)['state'], 'UNCERTAIN')
        self.assertEqual(pr.recover_interrupted(self.d), 0)

    def _notices(self):
        with closing(sqlite3.connect(str(self.d / 'nova.db'))) as conn:
            conn.row_factory = sqlite3.Row
            if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                "AND name='schedule_notices'").fetchone():
                return []
            return [dict(r) for r in conn.execute(
                'SELECT * FROM schedule_notices ORDER BY id').fetchall()]

    def test_confirm_notice_commits_atomically_exactly_once(self):
        with pr.ClipLease(self.d, 1) as lease:
            _, row = _reserve(self.d, 1, 'test', lease=lease)
            token = row['attempt_token']
            pr.confirm(self.d, 1, 'video-id', lease=lease,
                       attempt_token=token, notice='uploaded yt:video-id')
            # stale retry of the same attempt cannot duplicate the notice
            with self.assertRaises(pr.PublishStateError):
                pr.confirm(self.d, 1, 'video-id', lease=lease,
                           attempt_token=token, notice='uploaded yt:video-id')
        notices = self._notices()
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0]['text'], 'uploaded yt:video-id')
        self.assertIsNone(notices[0]['sent_at'])
        self.assertEqual(pr.get(self.d, 1)['state'], 'CONFIRMED')

    def test_notice_rolls_back_with_failed_transition(self):
        from app.core import schedule
        original = schedule.notice_on_conn
        def boom(c, text, schedule_id=None, now=None):
            raise RuntimeError('simulated crash at enqueue')
        schedule.notice_on_conn = boom
        try:
            with pr.ClipLease(self.d, 1) as lease:
                _, row = _reserve(self.d, 1, 'test', lease=lease)
                with self.assertRaises(RuntimeError):
                    pr.confirm(self.d, 1, 'video-id', lease=lease,
                               attempt_token=row['attempt_token'],
                               notice='uploaded yt:video-id')
        finally:
            schedule.notice_on_conn = original
        # neither half persisted: still RESERVED, zero notices
        self.assertEqual(pr.get(self.d, 1)['state'], 'RESERVED')
        self.assertEqual(self._notices(), [])

    def test_transitions_without_notice_stay_silent(self):
        with pr.ClipLease(self.d, 1) as lease:
            _, row = _reserve(self.d, 1, 'test', lease=lease)
            pr.confirm(self.d, 1, 'video-id', lease=lease,
                       attempt_token=row['attempt_token'])
        self.assertEqual(self._notices(), [])

    def _start_holder(self):
        from tests.process_helpers import track_children
        from tests.proc_workers import spawn_ready
        track_children(self)
        root = str(Path(__file__).resolve().parents[2])
        proc = spawn_ready(
            [sys.executable, '-m', 'tests.proc_workers', 'hold', str(self.d)],
            root, timeout=30)
        self.assertEqual(pr.get(self.d, 1)['state'], 'RESERVED')
        return proc

    def test_stalled_or_wrong_worker_is_killed_and_reaped(self):
        from tests.proc_workers import spawn_ready
        root = str(Path(__file__).resolve().parents[2])
        made = []
        real = subprocess.Popen

        def tracking(*a, **k):
            proc = real(*a, **k)
            made.append(proc)
            return proc
        cases = [
            ([sys.executable, '-c', 'import time; time.sleep(60)'], 'timed out'),
            ([sys.executable, '-c', 'print("NOPE", flush=True); import time; time.sleep(60)'], 'unexpected'),
            ([sys.executable, '-c', 'raise SystemExit(3)'], 'exited'),
        ]
        from unittest import mock
        with mock.patch('subprocess.Popen', side_effect=tracking):
            for argv, word in cases:
                with self.assertRaises(AssertionError) as cm:
                    spawn_ready(argv, root, timeout=1.0)
                self.assertIn(word, str(cm.exception))
        self.assertEqual(len(made), 3)
        for proc in made:
            self.assertIsNotNone(proc.poll())
            self.assertTrue(proc.stdout.closed and proc.stdin.closed and proc.stderr.closed)

    def test_process_recovery_cannot_invalidate_live_uploader(self):
        proc = self._start_holder()
        try:
            self.assertEqual(pr.recover_interrupted(self.d), 0)
            with self.assertRaises(pr.PublishBusy):
                with pr.ClipLease(self.d, 1):
                    self.fail('duplicate worker acquired live lease')
        finally:
            proc.stdin.write(b'go\n')
            proc.stdin.flush()
            proc.stdin.close()
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.terminate()
                proc.wait(5)
            proc.stdout.close()
            proc.stderr.close()
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(pr.get(self.d, 1)['state'], 'CONFIRMED')

    def test_process_death_releases_lease_but_never_allows_retry(self):
        proc = self._start_holder()
        proc.kill()
        proc.wait(5)
        proc.stdin.close()
        proc.stdout.close()
        proc.stderr.close()
        self.assertEqual(pr.recover_interrupted(self.d), 1)
        with pr.ClipLease(self.d, 1) as lease:
            self.assertEqual(_reserve(self.d, 1, 'retry', lease=lease)[0], 'blocked')

    def test_bulk_promotion_preserves_old_evidence_without_clip_row(self):
        entries = {'publish:99': {'command': 'publish', 'result': 'yt:old'},
                   'tg:3': {'command': 'publish', 'status': 'uncertain', 'text': '/publish 2 title'}}
        self.assertEqual(pr.promote_legacy_entries(self.d, entries), 2)
        self.assertEqual(pr.get(self.d, 99)['video_id'], 'old')
        self.assertEqual(pr.get(self.d, 2)['state'], 'UNCERTAIN')
        self.assertEqual(pr.promote_legacy_entries(self.d, entries), 0)

    def test_bulk_promotion_before_count_prune(self):
        import json
        from app.bot import ops
        entries = {f'publish:{n}': {'command': 'publish', 'result': f'yt:v{n}', 'ts': 1}
                   for n in range(600)}
        (self.d / ops.JOURNAL).write_text(json.dumps(entries))
        from unittest import mock
        with mock.patch('time.time', return_value=1):
            ops.journal_record(self.d, 700, 'other', 'done')
        self.assertLessEqual(len(json.loads((self.d / ops.JOURNAL).read_text())), ops.JOURNAL_CAP)
        with closing(sqlite3.connect(str(self.d / 'nova.db'))) as conn, conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM publish_records').fetchone()[0], 600)
        self.assertEqual(pr.get(self.d, 599)['video_id'], 'v599')
