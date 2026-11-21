"""Synthetic end-to-end validation of the caller/consumer integration:
QC-HELD clip -> authoritative admission -> durable handoff record ->
consumer -> authenticated webhook notice. No network, no Telegram, no
YouTube: the webhook is a fake callable and media probing is injected.
"""
import json
import os
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from app.core import database
from tests.unit import _prov
from app.core.intake import ClipIntakeAuthority, HANDOFF_JOB_KIND
from app.core.jobs import JobQueue
from app.bot import handoff

TZ = ZoneInfo("America/Los_Angeles")
FIXED_EPOCH = datetime(2026, 11, 21, 12, 0, 0, tzinfo=TZ).timestamp()


def fixed_clock():
    return FIXED_EPOCH


class HandoffIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.tmp.name)
        self.db = str(self.state_dir / "nova.db")
        conn = database.connect(self.db)
        database.migrate(conn)
        conn.close()
        # Credentials arrive only via env injection (AGT Vault at deploy);
        # never the database/settings.
        os.environ[handoff.ENV_URL] = "https://n8n.example.test/webhook/x"
        os.environ[handoff.ENV_SECRET] = "unit-test-secret"
        self.addCleanup(self._clear_env)
        self.auth = ClipIntakeAuthority(self.db, _clock=fixed_clock)
        self.posts = []

        def fake_post(url, headers, payload):
            self.posts.append({"url": url, "headers": dict(headers),
                               "payload": dict(payload)})
        self.fake_post = fake_post
        self.probe_fn = lambda path: {"format": {"duration": "30.0"},
                                      "streams": []}

    def tearDown(self):
        self.tmp.cleanup()

    def _add_held_clip(self, duration=120.0):
        media = self.state_dir / "clip.mp4"
        media.write_bytes(b"\x00" * 1024)
        conn = database.connect(self.db)
        with conn:
            cur = conn.execute(
                "INSERT INTO clips (duration_seconds, file_path, status,"
                " created_at, session_id) VALUES (?, ?, 'HELD', 0, ?)",
                (duration, str(media), _prov.feed_session_id(conn)))
            cid = int(cur.lastrowid)
        conn.close()
        return cid

    def _job_rows(self):
        conn = database.connect(self.db)
        rows = conn.execute(
            "SELECT id, state, attempts, last_error, payload_json"
            " FROM job_queue WHERE kind = ?",
            (HANDOFF_JOB_KIND,)).fetchall()
        conn.close()
        return rows

    def _run_once(self):
        return handoff.run_once(self.state_dir, probe_fn=self.probe_fn,
                                post_fn=self.fake_post)

    def test_end_to_end_admission_to_authenticated_notice(self):
        cid = self._add_held_clip()
        verdict = self.auth.admit_clip(cid)
        self.assertTrue(verdict["accepted"], verdict)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.posts), 1)
        post = self.posts[0]
        self.assertEqual(post["url"],
                         "https://n8n.example.test/webhook/x")
        self.assertEqual(post["headers"]["Authorization"],
                         "Bearer unit-test-secret")
        job = self._job_rows()[0]
        self.assertEqual(post["headers"]["X-Handoff-Id"], str(job["id"]))
        self.assertEqual(post["payload"],
                         {"handoff_job_id": job["id"],
                          "clip_id": f"cliprow:{cid}",
                          "intake_date": "2026-11-21"})
        self.assertEqual(job["state"], "COMPLETED")
        self.assertEqual(self._run_once(), handoff.IDLE)

    def test_crash_between_post_and_complete_reposts_with_same_id(self):
        cid = self._add_held_clip()
        self.auth.admit_clip(cid)
        job_id = self._job_rows()[0]["id"]
        # Simulate the crash: POST succeeds, completion never happens.
        # Do it by hand against the real primitives rather than by
        # patching, so the scoped requeue path (handoff.recover) is real.
        conn = database.connect(self.db)
        queue = JobQueue(conn)
        job = queue.claim(handoff.WORKER_NAME, kinds=[HANDOFF_JOB_KIND])
        self.assertIsNotNone(job)
        self.fake_post("https://n8n.example.test/webhook/x",
                       {"Authorization": "Bearer unit-test-secret",
                        "X-Handoff-Id": str(job_id)},
                       {"handoff_job_id": job_id,
                        "clip_id": f"cliprow:{cid}",
                        "intake_date": "2026-11-21"})
        conn.close()  # "crash": job left ANALYZING, notice already sent
        self.assertEqual(handoff.recover(self.state_dir), 1)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        # The notice was POSTed twice with the SAME idempotency key; a
        # future receiver must dedupe on it. No exactly-once claim: this
        # is the documented reconciliation contract.
        self.assertEqual(len(self.posts), 2)
        self.assertEqual(self.posts[0]["headers"]["X-Handoff-Id"],
                         self.posts[1]["headers"]["X-Handoff-Id"])
        self.assertEqual(self._job_rows()[0]["state"], "COMPLETED")

    def test_webhook_failure_retries_then_fails_visibly(self):
        cid = self._add_held_clip()
        self.auth.admit_clip(cid)

        def always_fail(url, headers, payload):
            raise OSError("connection refused")
        for expected_attempts in (1, 2, 3):
            self.assertEqual(
                handoff.run_once(self.state_dir, probe_fn=self.probe_fn,
                                 post_fn=always_fail), handoff.PROCESSED)
            job = self._job_rows()[0]
            self.assertEqual(job["attempts"], expected_attempts)
            # Skip the backoff wait.
            conn = database.connect(self.db)
            conn.execute("UPDATE job_queue SET run_after = 0 WHERE id = ?",
                         (job["id"],))
            conn.close()
        job = self._job_rows()[0]
        self.assertEqual(job["state"], "FAILED")
        self.assertIn("webhook post", job["last_error"])

    def test_media_check_failure_fails_the_job(self):
        cid = self._add_held_clip()
        self.auth.admit_clip(cid)
        bad_probe = lambda path: {"format": {"duration": "500.0"},
                                  "streams": []}
        self.assertEqual(
            handoff.run_once(self.state_dir, probe_fn=bad_probe,
                             post_fn=self.fake_post), handoff.PROCESSED)
        job = self._job_rows()[0]
        self.assertIn("media check", job["last_error"])
        self.assertEqual(len(self.posts), 0)

    def test_missing_file_fails_the_job(self):
        cid = self._add_held_clip()
        conn = database.connect(self.db)
        conn.execute("UPDATE clips SET file_path = ? WHERE id = ?",
                     (str(self.state_dir / "gone.mp4"), cid))
        conn.close()
        self.auth.admit_clip(cid)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertIn("file missing", self._job_rows()[0]["last_error"])
        self.assertEqual(len(self.posts), 0)

    def test_unconfigured_leaves_job_pending(self):
        self._clear_env()
        cid = self._add_held_clip()
        self.auth.admit_clip(cid)
        self.assertEqual(self._run_once(), handoff.UNCONFIGURED)
        self.assertEqual(self._job_rows()[0]["state"], "PENDING")
        self.assertEqual(len(self.posts), 0)

    def test_settings_db_secret_is_never_used(self):
        # Credential policy: a URL/secret sitting in the settings table
        # must NOT configure the consumer; env injection (AGT Vault at
        # deploy time) is the only source. Env cleared -> fail closed.
        self._clear_env()
        conn = database.connect(self.db)
        conn.execute(
            "INSERT INTO settings(key, value) VALUES"
            " ('n8n_webhook_url', 'https://n8n.example.test/webhook/x'),"
            " ('n8n_webhook_secret', 'unit-test-secret')")
        conn.close()
        cid = self._add_held_clip()
        self.auth.admit_clip(cid)
        self.assertEqual(self._run_once(), handoff.UNCONFIGURED)
        self.assertEqual(self._job_rows()[0]["state"], "PENDING")
        self.assertEqual(len(self.posts), 0)

    def test_recovery_is_scoped_to_handoff_jobs(self):
        # A crashed unrelated job kind must NOT be replayed by the
        # handoff consumer's scoped recovery.
        conn = database.connect(self.db)
        queue = JobQueue(conn)
        other_id = queue.enqueue("telegram_delivery", payload={})
        handoff_id = queue.enqueue(HANDOFF_JOB_KIND, payload={})
        for jid in (other_id, handoff_id):
            conn.execute(
                "UPDATE job_queue SET state = 'ANALYZING', locked_by = 'x'"
                " WHERE id = ?", (jid,))
        conn.commit()
        conn.close()
        self.assertEqual(handoff.recover(self.state_dir), 1)
        conn = database.connect(self.db)
        states = dict(conn.execute(
            "SELECT id, state FROM job_queue").fetchall())
        conn.close()
        self.assertEqual(states[other_id], "ANALYZING")  # untouched
        self.assertEqual(states[handoff_id], "PENDING")   # requeued

    def test_admission_job_flows_without_webhook_config(self):
        # The pipeline path: a durable admission job, not a direct call.
        # Admission is local-only and must complete with no webhook
        # configured; the notice waits for configuration.
        self._clear_env()
        cid = self._add_held_clip()
        conn = database.connect(self.db)
        JobQueue(conn).enqueue(handoff.ADMIT_JOB_KIND,
                               {"clip_row_id": cid})
        conn.close()
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        # admission recorded + durable handoff job written
        self.assertEqual(len(self._job_rows()), 1)
        self.assertEqual(self._job_rows()[0]["state"], "PENDING")
        self.assertEqual(len(self.posts), 0)
        # admission done, handoff pending, no webhook -> UNCONFIGURED
        self.assertEqual(self._run_once(), handoff.UNCONFIGURED)
        # configure: the notice now flows
        os.environ[handoff.ENV_URL] = "https://n8n.example.test/webhook/x"
        os.environ[handoff.ENV_SECRET] = "unit-test-secret"
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(self._job_rows()[0]["state"], "COMPLETED")

    def test_admission_per_clip_isolation(self):
        # One clip's undecodable admission job never blocks the others.
        cids = [self._add_held_clip() for _ in range(3)]
        conn = database.connect(self.db)
        queue = JobQueue(conn)
        job_ids = [queue.enqueue(handoff.ADMIT_JOB_KIND,
                                 {"clip_row_id": c}) for c in cids]
        conn.execute("UPDATE job_queue SET payload_json = '{broken'"
                     " WHERE id = ?", (job_ids[0],))
        conn.commit()
        conn.close()
        for _ in range(3):
            self.assertEqual(self._run_once(), handoff.PROCESSED)
        # every unobstructed clip admitted: the corrupted job completed
        # without effect, the other two wrote one handoff job each
        self.assertEqual(len(self._job_rows()), 2)
        for _ in range(2):
            self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.posts), 2)

    def test_http_webhook_url_rejected_before_any_request(self):
        import threading
        from http.server import BaseHTTPRequestHandler, HTTPServer
        hits = []

        class Sink(BaseHTTPRequestHandler):
            def do_POST(self):
                hits.append(self.path)
                self.send_response(200)
                self.end_headers()

            def log_message(self, *a):
                pass

        server = HTTPServer(("127.0.0.1", 0), Sink)
        thread = threading.Thread(target=server.serve_forever,
                                  daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_address[1]}/webhook"
            with self.assertRaises(RuntimeError):
                handoff._default_post(
                    url, {"Authorization": "Bearer x"}, {"a": 1})
            self.assertEqual(hits, [])  # nothing was ever sent
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_redirect_is_never_followed_and_secret_never_forwarded(self):
        # The reviewer's probe scenario: a 302 to another host must not
        # receive the Authorization header.
        import threading
        import urllib.error
        from http.server import BaseHTTPRequestHandler, HTTPServer
        received = {}

        class Redirector(BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(302)
                self.send_header(
                    "Location",
                    f"http://127.0.0.1:{landing.server_address[1]}/landing")
                self.end_headers()

            def log_message(self, *a):
                pass

        class Landing(BaseHTTPRequestHandler):
            def do_POST(self):
                received["authorization"] = self.headers.get(
                    "Authorization")
                self.send_response(200)
                self.end_headers()

            do_GET = do_POST

            def log_message(self, *a):
                pass

        landing = HTTPServer(("127.0.0.1", 0), Landing)
        redirector = HTTPServer(("127.0.0.1", 0), Redirector)
        threads = [
            threading.Thread(target=s.serve_forever, daemon=True)
            for s in (landing, redirector)]
        for t in threads:
            t.start()
        try:
            url = (f"http://127.0.0.1:{redirector.server_address[1]}"
                   "/webhook")
            import urllib.request
            req = urllib.request.Request(
                url, data=b"{}", method="POST")
            req.add_header("Authorization", "Bearer SECRET-123")
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                handoff._no_redirect_opener().open(req, timeout=5)
            self.assertEqual(ctx.exception.code, 302)
            ctx.exception.close()
            self.assertNotIn("authorization", received)
        finally:
            for s in (redirector, landing):
                s.shutdown()
                s.server_close()
            for t in threads:
                t.join(timeout=5)

    def test_terminal_failure_is_owner_visible(self):
        # Bounded scheduled retry, then a durable FAILED row AND a line
        # in the errors ring that /logs reads: nothing drops silently.
        cid = self._add_held_clip()
        self.auth.admit_clip(cid)
        job_id = self._job_rows()[0]["id"]

        def always_fail(url, headers, payload):
            raise OSError("connection refused")

        for _ in range(3):  # max_attempts
            self.assertEqual(
                handoff.run_once(self.state_dir, probe_fn=self.probe_fn,
                                 post_fn=always_fail), handoff.PROCESSED)
            # Skip the backoff wait (bounded scheduled retry is
            # JobQueue.fail's run_after; covered by test_jobs).
            conn = database.connect(self.db)
            conn.execute("UPDATE job_queue SET run_after = 0 WHERE"
                         " kind = ?", (HANDOFF_JOB_KIND,))
            conn.close()
        row = self._job_rows()[0]
        self.assertEqual(row["state"], "FAILED")
        ring = (self.state_dir / "errors.log").read_text()
        self.assertIn(f"handoff job {job_id} permanently failed", ring)

    def _clear_env(self):
        os.environ.pop(handoff.ENV_URL, None)
        os.environ.pop(handoff.ENV_SECRET, None)

    def test_over_cap_clip_gets_no_job_and_no_notice(self):
        cids = [self._add_held_clip() for _ in range(9)]
        verdicts = [self.auth.admit_clip(c) for c in cids]
        self.assertTrue(all(v["accepted"] for v in verdicts[:8]))
        self.assertFalse(verdicts[8]["accepted"])
        self.assertEqual(verdicts[8]["reason"], "daily_cap")
        self.assertEqual(len(self._job_rows()), 8)
        for _ in range(8):
            self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(self._run_once(), handoff.IDLE)
        self.assertEqual(len(self.posts), 8)
        self.assertEqual(len({p["headers"]["X-Handoff-Id"]
                              for p in self.posts}), 8)

    def test_preexisting_held_clip_is_invisible(self):
        # The 4 pre-existing HELD clips have no admission record; the
        # consumer must never see them.
        self._add_held_clip()
        self.assertEqual(self._run_once(), handoff.IDLE)
        self.assertEqual(len(self.posts), 0)
        conn = database.connect(self.db)
        status = conn.execute("SELECT status FROM clips").fetchone()[0]
        conn.close()
        self.assertEqual(status, "HELD")

    def test_clip_row_deleted_after_admission_fails_visibly_without_effect(self):
        cid = self._add_held_clip()
        self.auth.admit_clip(cid)
        conn = database.connect(self.db)
        conn.execute("DELETE FROM clips WHERE id = ?", (cid,))
        conn.close()
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        row = self._job_rows()[0]
        self.assertEqual(row["state"], "FAILED")     # visible, not silently completed
        self.assertTrue(row["last_error"].startswith("provenance_unreadable:"))
        self.assertEqual(len(self.posts), 0)

    def test_concurrent_admission_and_consumption(self):
        # Producers admit while the consumer drains: every accepted clip
        # yields exactly one completed job and one notice.
        import threading
        cids = [self._add_held_clip() for _ in range(6)]

        def produce(cid):
            ClipIntakeAuthority(self.db, _clock=fixed_clock) \
                .admit_clip(cid)

        threads = [threading.Thread(target=produce, args=(c,))
                   for c in cids for _ in range(2)]  # duplicates included
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        processed = 0
        while self._run_once() == handoff.PROCESSED:
            processed += 1
        self.assertEqual(processed, 6)
        self.assertEqual(len(self.posts), 6)
        states = {r["state"] for r in self._job_rows()}
        self.assertEqual(states, {"COMPLETED"})


if __name__ == "__main__":
    unittest.main()
