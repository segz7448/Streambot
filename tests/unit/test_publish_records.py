from contextlib import closing
from tests.unit import _prov
import os
import subprocess
import sys
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from app.core.database import migrate
from app.distribution import publish_records as pr


def _reserve(sd, cid, *a, **k):
    _prov.seed_clip(sd, cid)
    return pr.reserve(sd, cid, *a, **k)


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

    def _start_holder(self):
        from tests.process_helpers import track_children
        track_children(self)
        root = str(Path(__file__).resolve().parents[2])
        proc = subprocess.Popen(
            [sys.executable, '-m', 'tests.proc_workers', 'hold', str(self.d)],
            cwd=root, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        line = proc.stdout.readline()
        if line.strip() != 'READY':
            proc.kill()
            self.fail('holder did not become ready: ' + proc.stderr.read())
        return proc

    def test_process_recovery_cannot_invalidate_live_uploader(self):
        proc = self._start_holder()
        try:
            self.assertEqual(pr.recover_interrupted(self.d), 0)
            with self.assertRaises(pr.PublishBusy):
                with pr.ClipLease(self.d, 1):
                    self.fail('duplicate worker acquired live lease')
        finally:
            proc.stdin.write('go\n')
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
