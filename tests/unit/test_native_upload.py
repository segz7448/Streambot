"""Native upload consumer: gates, ownership/dedup, retry honesty and
confirmation honesty. No network, no Telegram, no YouTube: the
publisher is a fake callable and media probing is injected.
"""
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from app.core import database
from tests.unit import _prov
from app.core.intake import (ClipIntakeAuthority, HANDOFF_JOB_KIND,
                             resolve_timezone)
from app.bot import handoff, native_upload
from app.distribution import publish_records
from app.distribution.youtube import (YouTubeNotConfigured,
                                      YouTubeUploadFailed)

TZ = resolve_timezone("America/Los_Angeles")
FIXED_EPOCH = datetime(2026, 11, 21, 12, 0, 0, tzinfo=TZ).timestamp()


def fixed_clock():
    return FIXED_EPOCH


class FakePublisher:
    def __init__(self, behavior="ok"):
        self.behavior = behavior
        self.calls = []

    def publish(self, path, title, description="", privacy="", tags=None):
        self.calls.append({"path": path, "title": title,
                           "description": description,
                           "privacy": privacy, "tags": list(tags or [])})
        if self.behavior == "ok":
            return "ytNative123"
        if self.behavior == "definitive":
            raise YouTubeUploadFailed("token refresh failed",
                                      stage="auth", uncertain=False)
        if self.behavior == "uncertain":
            raise YouTubeUploadFailed("connection dropped mid-PUT",
                                      stage="upload", uncertain=True)
        if self.behavior == "unexpected":
            raise RuntimeError("totally unexpected")
        raise AssertionError(self.behavior)


class NativeUploadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.tmp.name)
        self.db = str(self.state_dir / "nova.db")
        conn = database.connect(self.db)
        database.migrate(conn)
        conn.close()
        os.environ[native_upload.ENV_NATIVE] = "1"
        # Webhook env intentionally absent: native mode must not need it.
        os.environ.pop(handoff.ENV_URL, None)
        os.environ.pop(handoff.ENV_SECRET, None)
        self.addCleanup(self._clear_env)
        self.auth = ClipIntakeAuthority(self.db, _clock=fixed_clock)
        self.publisher = FakePublisher("ok")
        self.probe_fn = lambda path: {"format": {"duration": "30.0"},
                                      "streams": []}

    def _clear_env(self):
        os.environ.pop(native_upload.ENV_NATIVE, None)
        os.environ.pop(handoff.ENV_URL, None)
        os.environ.pop(handoff.ENV_SECRET, None)

    def tearDown(self):
        self.tmp.cleanup()

    def _add_held_clip(self, status="HELD", under_root=True):
        root = self.state_dir / "sessions" / "s1"
        root.mkdir(parents=True, exist_ok=True)
        media = (root / "clip.mp4") if under_root else \
            (self.state_dir / "clip.mp4")
        media.write_bytes(b"\x00" * 1024)
        conn = database.connect(self.db)
        with conn:
            cur = conn.execute(
                "INSERT INTO clips (duration_seconds, file_path, status,"
                " created_at, session_id) VALUES (?, ?, ?, 0, ?)",
                (120.0, str(media), status, _prov.feed_session_id(conn)))
            cid = int(cur.lastrowid)
        conn.close()
        return cid

    def _admit(self, cid):
        verdict = self.auth.admit_clip(cid)
        self.assertTrue(verdict["accepted"], verdict)

    def _jobs(self):
        conn = database.connect(self.db)
        rows = conn.execute(
            "SELECT id, state, attempts, last_error FROM job_queue"
            " WHERE kind = ?", (HANDOFF_JOB_KIND,)).fetchall()
        conn.close()
        return rows

    def _record(self, clip_row_id):
        return publish_records.get(self.state_dir, clip_row_id)

    def _run_once(self):
        return handoff.run_once(self.state_dir, probe_fn=self.probe_fn,
                                publisher_factory=lambda: self.publisher)

    def _errors_ring(self):
        p = self.state_dir / "errors.log"
        return p.read_text() if p.exists() else ""

    def test_gate_default_off(self):
        os.environ.pop(native_upload.ENV_NATIVE, None)
        self.assertFalse(native_upload.enabled())
        cid = self._add_held_clip()
        self._admit(cid)
        # With the gate off and no webhook configured the handoff job
        # stays pending; native upload never runs.
        self.assertEqual(self._run_once(), handoff.UNCONFIGURED)
        self.assertEqual(len(self.publisher.calls), 0)
        self.assertEqual(self._jobs()[0]["state"], "PENDING")

    def test_success_confirms_and_completes(self):
        cid = self._add_held_clip()
        self._admit(cid)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 1)
        self.assertEqual(self.publisher.calls[0]["privacy"], "private")
        rec = self._record(cid)
        self.assertEqual(rec["state"], "CONFIRMED")
        self.assertEqual(rec["video_id"], "ytNative123")
        self.assertEqual(self._jobs()[0]["state"], "COMPLETED")
        # Second cycle: nothing pending, no further network.
        self.assertEqual(self._run_once(), handoff.IDLE)
        self.assertEqual(len(self.publisher.calls), 1)

    def test_confirmed_clip_completes_without_network(self):
        cid = self._add_held_clip()
        self._admit(cid)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        # A copied duplicate job for the same clip: canonical namespace
        # and date, but the registry binds admission to the ORIGINAL
        # job id, so this copy is refused without an upload.
        conn = database.connect(self.db)
        with conn:
            conn.execute(
                "INSERT INTO job_queue (kind, payload_json, state,"
                " max_attempts, run_after, created_at, updated_at)"
                " VALUES (?, ?, 'PENDING', 3, 0, 0, 0)",
                (HANDOFF_JOB_KIND,
                 json.dumps({"clip_id": f"cliprow:{cid}",
                             "intake_date": "2026-11-21"})))
        conn.close()
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 1)  # zero new calls
        states = [j["state"] for j in self._jobs()]
        self.assertEqual(states, ["COMPLETED", "COMPLETED"])

    def test_definitive_failure_retries_then_terminates(self):
        self.publisher.behavior = "definitive"
        cid = self._add_held_clip()
        self._admit(cid)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        job = self._jobs()[0]
        self.assertEqual(job["state"], "PENDING")  # bounded retry
        rec = self._record(cid)
        self.assertEqual(rec["state"], "FAILED")  # retryable state
        # Drain remaining attempts.
        for _ in range(5):
            conn = database.connect(self.db)
            with conn:
                conn.execute("UPDATE job_queue SET run_after = 0")
            conn.close()
            if self._run_once() != handoff.PROCESSED:
                break
        job = self._jobs()[0]
        self.assertEqual(job["state"], "FAILED")
        self.assertIn("pre-flight", job["last_error"])
        self.assertIn("native upload", self._errors_ring())

    def test_uncertain_failure_never_retried(self):
        self.publisher.behavior = "uncertain"
        cid = self._add_held_clip()
        self._admit(cid)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        job = self._jobs()[0]
        self.assertEqual(job["state"], "FAILED")  # terminal at once
        self.assertIn("uncertain", job["last_error"])
        rec = self._record(cid)
        self.assertEqual(rec["state"], "UNCERTAIN")
        self.assertEqual(self._run_once(), handoff.IDLE)  # no reclaim
        self.assertEqual(len(self.publisher.calls), 1)

    def test_unexpected_error_is_uncertain_and_terminal(self):
        self.publisher.behavior = "unexpected"
        cid = self._add_held_clip()
        self._admit(cid)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(self._jobs()[0]["state"], "FAILED")
        self.assertEqual(self._record(cid)["state"], "UNCERTAIN")

    def test_prior_uncertain_blocks_and_terminates(self):
        cid = self._add_held_clip()
        self._admit(cid)
        with publish_records.ClipLease(self.state_dir, cid) as lease:
            decision, rec = publish_records.reserve(
                self.state_dir, cid, "seed", lease=lease)
            publish_records.mark_uncertain(
                self.state_dir, cid, "seeded", lease=lease,
                attempt_token=rec["attempt_token"])
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(self._jobs()[0]["state"], "FAILED")
        self.assertEqual(len(self.publisher.calls), 0)
        self.assertIn("reconcile", self._errors_ring())

    def test_confirmation_write_failure_keeps_video_id(self):
        cid = self._add_held_clip()
        self._admit(cid)
        real_confirm = publish_records.confirm
        publish_records.confirm = _raising_confirm
        try:
            self.assertEqual(self._run_once(), handoff.PROCESSED)
        finally:
            publish_records.confirm = real_confirm
        job = self._jobs()[0]
        self.assertEqual(job["state"], "FAILED")
        rec = self._record(cid)
        self.assertEqual(rec["state"], "UNCERTAIN")
        self.assertIn("ytNative123", rec["last_error"])

    def test_path_escape_refused_without_upload(self):
        cid = self._add_held_clip(under_root=False)
        self._admit(cid)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 0)
        self.assertEqual(self._jobs()[0]["state"], "COMPLETED")
        self.assertIn("path escape", self._errors_ring())

    def test_unpublishable_status_completes_without_upload(self):
        cid = self._add_held_clip()
        self._admit(cid)  # properly bound admission, then status flips
        conn = database.connect(self.db)
        with conn:
            conn.execute("UPDATE clips SET status = 'QC_FAILED'"
                         " WHERE id = ?", (cid,))
        conn.close()
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 0)
        self.assertEqual(self._jobs()[0]["state"], "COMPLETED")

    def _bound_job_and_registry(self, cid):
        """Admit a clip and return (job_id, clip_id_str, intake_date)."""
        self._admit(cid)
        job = self._jobs()[0]
        return int(job["id"]), f"cliprow:{cid}", "2026-11-21"

    def _reset_job_pending(self):
        conn = database.connect(self.db)
        with conn:
            conn.execute("UPDATE job_queue SET state = 'PENDING',"
                         " run_after = 0, locked_by = NULL,"
                         " locked_at = NULL")
        conn.close()

    def test_wrong_namespace_never_uploads(self):
        # Reviewer reproduction shape: arbitrary text before the colon.
        cid = self._add_held_clip()
        conn = database.connect(self.db)
        with conn:
            conn.execute(
                "INSERT INTO job_queue (kind, payload_json, state,"
                " max_attempts, run_after, created_at, updated_at)"
                " VALUES (?, ?, 'PENDING', 3, 0, 0, 0)",
                (HANDOFF_JOB_KIND,
                 json.dumps({"clip_id": "not-admitted:%d" % cid,
                             "intake_date": "1999-01-01"})))
        conn.close()
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 0)
        self.assertEqual(self._jobs()[0]["state"], "COMPLETED")
        self.assertIsNone(self._record(cid))

    def test_canonical_id_without_admission_never_uploads(self):
        cid = self._add_held_clip()
        conn = database.connect(self.db)
        with conn:
            conn.execute(
                "INSERT INTO job_queue (kind, payload_json, state,"
                " max_attempts, run_after, created_at, updated_at)"
                " VALUES (?, ?, 'PENDING', 3, 0, 0, 0)",
                (HANDOFF_JOB_KIND,
                 json.dumps({"clip_id": f"cliprow:{cid}",
                             "intake_date": "2026-11-21"})))
        conn.close()
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 0)
        self.assertIsNone(self._record(cid))

    def test_rejected_cap_registry_never_uploads(self):
        cid = self._add_held_clip()
        job_id, clip_id_str, date = self._bound_job_and_registry(cid)
        conn = database.connect(self.db)
        with conn:
            conn.execute("UPDATE clip_intake SET status = 'REJECTED_CAP'"
                         " WHERE clip_id = ?", (clip_id_str,))
        conn.close()
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 0)
        self.assertIsNone(self._record(cid))

    def test_handoff_job_id_mismatch_never_uploads(self):
        cid = self._add_held_clip()
        job_id, clip_id_str, date = self._bound_job_and_registry(cid)
        conn = database.connect(self.db)
        with conn:
            conn.execute("UPDATE clip_intake SET handoff_job_id = ?"
                         " WHERE clip_id = ?", (job_id + 999,
                                                clip_id_str))
        conn.close()
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 0)
        self.assertIsNone(self._record(cid))

    def test_intake_date_mismatch_never_uploads(self):
        cid = self._add_held_clip()
        job_id, clip_id_str, date = self._bound_job_and_registry(cid)
        conn = database.connect(self.db)
        with conn:
            conn.execute("UPDATE job_queue SET payload_json = ?",
                         (json.dumps({"clip_id": clip_id_str,
                                      "intake_date": "1999-01-01"}),))
        conn.close()
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 0)
        self.assertIsNone(self._record(cid))

    def test_external_opaque_admission_never_uploaded(self):
        # admit() accepts opaque ids and enqueues a handoff job; native
        # upload must never reinterpret it as a bot clip row.
        verdict = self.auth.admit("external-clip-9", duration_sec=30.0)
        self.assertTrue(verdict["accepted"], verdict)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 0)
        jobs = self._jobs()
        self.assertEqual(jobs[0]["state"], "COMPLETED")

    def test_live_publisher_lease_defers_job(self):
        cid = self._add_held_clip()
        self._admit(cid)
        lease = publish_records.ClipLease(self.state_dir, cid)
        lease.__enter__()
        self.addCleanup(lease.__exit__, None, None, None)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 0)
        self.assertEqual(self._jobs()[0]["state"], "PENDING")
        self.assertIsNone(self._record(cid))

    def test_publisher_not_configured_bounded_retry(self):
        def raising_factory():
            raise YouTubeNotConfigured("no creds")
        cid = self._add_held_clip()
        self._admit(cid)
        status = handoff.run_once(self.state_dir, probe_fn=self.probe_fn,
                                  publisher_factory=raising_factory)
        self.assertEqual(status, handoff.PROCESSED)
        self.assertEqual(self._jobs()[0]["state"], "PENDING")
        self.assertEqual(len(self.publisher.calls), 0)

    def test_bad_payload_and_missing_row_complete(self):
        conn = database.connect(self.db)
        with conn:
            conn.execute(
                "INSERT INTO job_queue (kind, payload_json, state,"
                " max_attempts, run_after, created_at, updated_at)"
                " VALUES (?, 'not json', 'PENDING', 3, 0, 0, 0)",
                (HANDOFF_JOB_KIND,))
        conn.close()
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        # Bound admission whose clips row is then deleted: the registry
        # verdict is stable, but there is nothing to upload.
        cid = self._add_held_clip()
        self._admit(cid)
        conn = database.connect(self.db)
        with conn:
            conn.execute("DELETE FROM clips WHERE id = ?", (cid,))
        conn.close()
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        self.assertEqual(self._run_once(), handoff.IDLE)
        self.assertEqual(len(self.publisher.calls), 0)

    def test_missing_registry_table_fails_closed(self):
        # Production today has no clip_intake table: any claimed job
        # must complete without an upload, never error-loop.
        tmp2 = tempfile.TemporaryDirectory()
        self.addCleanup(tmp2.cleanup)
        sd2 = Path(tmp2.name)
        db2 = str(sd2 / "nova.db")
        conn = database.connect(db2)
        database.migrate(conn)
        with conn:
            conn.execute(
                "INSERT INTO job_queue (kind, payload_json, state,"
                " max_attempts, run_after, created_at, updated_at)"
                " VALUES (?, ?, 'PENDING', 3, 0, 0, 0)",
                (HANDOFF_JOB_KIND,
                 json.dumps({"clip_id": "cliprow:1",
                             "intake_date": "2026-11-21"})))
        conn.close()
        status = handoff.run_once(sd2, probe_fn=self.probe_fn,
                                  publisher_factory=lambda: self.publisher)
        self.assertEqual(status, handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 0)
        conn = database.connect(db2)
        state = conn.execute("SELECT state FROM job_queue").fetchone()[0]
        conn.close()
        self.assertEqual(state, "COMPLETED")


def _raising_confirm(*a, **k):
    raise sqlite3.OperationalError("db locked")


class NativeUploadV3Tests(NativeUploadTests):
    """Reviewer v3 items: singleton fencing, chosen-entry preparation,
    stuck-job repair, metadata honesty and contention visibility.
    setUp/helpers come from NativeUploadTests; no test methods are
    inherited (all base names overridden below are new)."""

    # Hide inherited test methods so only the v3 ones run here.
    def test_singleton_lock_skips_second_consumer(self):
        import fcntl
        cid = self._add_held_clip()
        self._admit(cid)
        fd = os.open(str(self.state_dir / handoff.CONSUMER_LOCK_NAME),
                     os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            status = self._run_once()
            self.assertEqual(status, handoff.IDLE)
            # Direct entry (public process_one) is fenced by the SAME
            # lock: it must skip, never overlap the live consumer, and
            # crash recovery must not run either.
            conn = database.connect(self.db)
            try:
                self.assertFalse(native_upload.process_one(
                    conn, self.state_dir, probe_fn=self.probe_fn,
                    publisher_factory=lambda: self.publisher))
            finally:
                conn.close()
            self.assertEqual(self.publisher.calls, [])
            states = [r["state"] for r in self._jobs()]
            self.assertEqual(states, ["PENDING"])
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def test_unexpected_error_never_strands_job_analyzing(self):
        cid = self._add_held_clip()
        self._admit(cid)

        def boom():
            raise RuntimeError("factory exploded")
        status = handoff.run_once(self.state_dir, probe_fn=self.probe_fn,
                                  publisher_factory=boom)
        self.assertEqual(status, handoff.PROCESSED)
        job = self._jobs()[0]
        # bounded retry, truthful error - never stuck ANALYZING
        self.assertEqual(job["state"], "PENDING")
        self.assertIn("unexpected error", job["last_error"])
        self.assertEqual(self.publisher.calls, [])

    def test_crash_leftover_job_requeued_then_processed(self):
        cid = self._add_held_clip()
        self._admit(cid)
        job = self._jobs()[0]
        conn = database.connect(self.db)
        with conn:
            conn.execute(
                "UPDATE job_queue SET state='ANALYZING', locked_by='dead'"
                " WHERE id=?", (job["id"],))
        conn.close()
        status = self._run_once()
        self.assertEqual(status, handoff.PROCESSED)
        self.assertEqual(len(self.publisher.calls), 1)
        self.assertEqual(self._jobs()[0]["state"], "COMPLETED")

    def test_admitted_metadata_drives_upload_fields(self):
        cid = self._add_held_clip()
        verdict = self.auth.admit_clip(cid, title="My Admitted Title",
                                       streamer="admitted_streamer")
        self.assertTrue(verdict["accepted"], verdict)
        self._run_once()
        call = self.publisher.calls[0]
        self.assertEqual(call["title"], "My Admitted Title")
        self.assertIn("Clipped from admitted_streamer",
                      call["description"])
        self.assertEqual(call["tags"], [])  # never fabricated

    def test_clip_without_session_origin_is_denied_not_uploaded(self):
        # production denies unresolved provenance: a clip with no session
        # (no source) is never uploaded and the refusal is visible
        cid = self._add_held_clip()
        conn = database.connect(self.db)
        with conn:
            conn.execute("UPDATE clips SET session_id=NULL WHERE id=?", (cid,))
        conn.close()
        self._admit(cid)
        self._run_once()
        self.assertEqual(self.publisher.calls, [])
        rows = self._jobs()
        self.assertEqual(rows[0]["state"], "FAILED")
        self.assertTrue(rows[0]["last_error"].startswith("provenance_unreadable:"))

    def test_lease_contention_bounded_and_visible(self):
        cid = self._add_held_clip()
        self._admit(cid)
        lease = publish_records.ClipLease(self.state_dir, cid)
        lease.__enter__()  # a live publisher holds the clip
        try:
            # one attempt short of the cap: the contention failure must
            # terminate visibly instead of burning attempts silently
            conn = database.connect(self.db)
            with conn:
                conn.execute(
                    "UPDATE job_queue SET attempts = max_attempts - 1")
            conn.close()
            self._run_once()
            job = self._jobs()[0]
            self.assertEqual(job["state"], "FAILED")
            self.assertIn("lease", job["last_error"])
            self.assertEqual(self.publisher.calls, [])
            self.assertIsNone(self._record(cid))
        finally:
            lease.__exit__()

    def _add_session_clip_with_metadata(self, title, source_name):
        root = self.state_dir / "sessions" / "s1"
        root.mkdir(parents=True, exist_ok=True)
        media = root / "clip.mp4"
        media.write_bytes(b"\x00" * 1024)
        conn = database.connect(self.db)
        with conn:
            conn.execute(
                "INSERT INTO twitch_sources (username, created_at,"
                " updated_at) VALUES (?, 0, 0)", (source_name,))
            sid = conn.execute(
                "SELECT id FROM twitch_sources WHERE username=?",
                (source_name,)).fetchone()[0]
            sess = conn.execute(
                "INSERT INTO stream_sessions (source_id, state, ingest_mode)"
                " VALUES (?, 'ENDED', 'upload')", (sid,))
            ev = conn.execute(
                "INSERT INTO candidate_events (session_id, detected_at,"
                " kind, confidence, status, metadata_json) VALUES"
                " (?,?,?,?,?,?)",
                (sess.lastrowid, 0, "audio_spike", 0.9, "CLIPPED",
                 json.dumps({"title": title})))
            cur = conn.execute(
                "INSERT INTO clips (event_id, session_id,"
                " duration_seconds, file_path, status, created_at)"
                " VALUES (?,?,?,?,?,0)",
                (ev.lastrowid, sess.lastrowid, 120.0, str(media),
                 "HELD"))
            cid = int(cur.lastrowid)
        conn.close()
        return cid

    def test_stored_metadata_flows_through_real_admission_cycle(self):
        # generated-path shape: pipeline-stored title/source, admission
        # ONLY via the queued clip_intake_admit job (no direct admit)
        title = "SYNTHETIC spike clip from fixture (30s)"
        cid = self._add_session_clip_with_metadata(
            title, "synthetic-fixture")
        from app.core.jobs import JobQueue
        conn = database.connect(self.db)
        JobQueue(conn).enqueue(handoff.ADMIT_JOB_KIND,
                               {"clip_row_id": cid})
        conn.close()
        # cycle 1: the consumer performs the real admission, reading
        # stored pipeline metadata (never job-payload claims)
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        conn = database.connect(self.db)
        reg = conn.execute(
            "SELECT status, title, streamer, handoff_job_id FROM"
            " clip_intake WHERE clip_id = ?",
            (f"cliprow:{cid}",)).fetchone()
        conn.close()
        self.assertEqual(reg["status"], "ACCEPTED")
        self.assertEqual(reg["title"], title)
        self.assertEqual(reg["streamer"], "synthetic-fixture")
        self.assertIsNotNone(reg["handoff_job_id"])
        # cycle 2: the bound handoff drains with the admitted metadata
        self.assertEqual(self._run_once(), handoff.PROCESSED)
        call = self.publisher.calls[0]
        self.assertEqual(call["title"], title)
        self.assertIn("synthetic-fixture", call["description"])
        self.assertEqual(call["tags"], [])

    def test_shared_lock_skips_whole_cycle_including_admission(self):
        import fcntl
        cid = self._add_held_clip()
        from app.core.jobs import JobQueue
        conn = database.connect(self.db)
        JobQueue(conn).enqueue(handoff.ADMIT_JOB_KIND,
                               {"clip_row_id": cid})
        conn.close()
        fd = os.open(str(self.state_dir / handoff.CONSUMER_LOCK_NAME),
                     os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            self.assertEqual(self._run_once(), handoff.IDLE)
            conn = database.connect(self.db)
            st = conn.execute(
                "SELECT state FROM job_queue WHERE kind=?",
                (handoff.ADMIT_JOB_KIND,)).fetchone()[0]
            conn.close()
            self.assertEqual(st, "PENDING")
            self.assertEqual(self.publisher.calls, [])
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def test_recover_skips_while_live_consumer(self):
        import fcntl
        cid = self._add_held_clip()
        self._admit(cid)
        job = self._jobs()[0]
        conn = database.connect(self.db)
        with conn:
            conn.execute(
                "UPDATE job_queue SET state='ANALYZING' WHERE id=?",
                (job["id"],))
        conn.close()
        fd = os.open(str(self.state_dir / handoff.CONSUMER_LOCK_NAME),
                     os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            self.assertEqual(handoff.recover(self.state_dir), 0)
            conn = database.connect(self.db)
            st = conn.execute(
                "SELECT state FROM job_queue WHERE id=?",
                (job["id"],)).fetchone()[0]
            conn.close()
            self.assertEqual(st, "ANALYZING")
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        # after the live consumer is gone, recovery proceeds
        self.assertEqual(handoff.recover(self.state_dir), 1)

    def _cycle(self):
        conn = database.connect(self.db)
        try:
            return native_upload.process_one(
                conn, self.state_dir, probe_fn=self.probe_fn,
                publisher_factory=lambda: self.publisher)
        finally:
            conn.close()

    def _reserve_stranded(self, cid):
        with publish_records.ClipLease(self.state_dir, cid) as lease:
            publish_records.reserve(self.state_dir, cid, "dead-worker",
                                    lease=lease)

    def test_deferred_recovery_runs_on_a_later_cycle_same_process(self):
        # Cycle 1: a live publisher lease defers the stranded reservation.
        # Cycle 2 (same process, same owner): lease gone -> it MUST recover
        # (the old owner-token bookkeeping marked prep done and skipped it).
        cid = self._add_held_clip()
        self._reserve_stranded(cid)
        live = publish_records.ClipLease(self.state_dir, cid)
        live.__enter__()
        try:
            self._cycle()
            self.assertEqual(
                publish_records.get(self.state_dir, cid)["state"],
                "RESERVED")
        finally:
            live.__exit__()
        self._cycle()
        self.assertEqual(
            publish_records.get(self.state_dir, cid)["state"], "UNCERTAIN")

    def test_same_owner_transient_analyzing_job_is_requeued_next_cycle(self):
        # A job left ANALYZING by a same-process transient failure after a
        # successful earlier prepare must be requeued, not stranded.
        cid = self._add_held_clip()
        self._admit(cid)
        self._cycle()  # prepare ran; job handled
        conn = database.connect(self.db)
        with conn:
            conn.execute("INSERT INTO job_queue (kind, payload_json, state,"
                         " max_attempts, run_after, created_at, updated_at)"
                         " SELECT kind, payload_json, 'ANALYZING', 3, 0,"
                         " created_at, updated_at FROM job_queue WHERE kind=?"
                         " LIMIT 1", (HANDOFF_JOB_KIND,))
        conn.close()
        self.assertTrue(any(r["state"] == "ANALYZING"
                            for r in self._jobs()))
        self._cycle()
        self.assertFalse(any(r["state"] == "ANALYZING"
                             for r in self._jobs()))

    def test_analyzing_requeued_even_with_live_lease_elsewhere(self):
        cid = self._add_held_clip()
        self._admit(cid)
        other = self._add_held_clip()
        job = self._jobs()[0]
        conn = database.connect(self.db)
        with conn:
            conn.execute("UPDATE job_queue SET state='ANALYZING' WHERE id=?",
                         (job["id"],))
        conn.close()
        live = publish_records.ClipLease(self.state_dir, other)
        live.__enter__()
        try:
            prep_conn = database.connect(self.db)
            try:
                native_upload._prepare(self.state_dir, prep_conn)
            finally:
                prep_conn.close()
        finally:
            live.__exit__()
        self.assertEqual(self._jobs()[0]["state"], "PENDING")

    def test_prepare_failure_skips_cycle_without_claim_and_retries(self):
        cid = self._add_held_clip()
        self._admit(cid)
        with mock.patch.object(publish_records, "recover_interrupted",
                               side_effect=OSError("transient")):
            self.assertFalse(self._cycle())
        self.assertEqual([r["state"] for r in self._jobs()], ["PENDING"])
        self.assertEqual(self.publisher.calls, [])
        self.assertTrue(self._cycle())  # next cycle proceeds normally

    def test_legacy_promotion_not_marked_done_on_failure(self):
        native_upload._LEGACY_PROMOTED.discard(
            str(self.state_dir.resolve()))
        conn = database.connect(self.db)
        with mock.patch.object(publish_records, "promote_legacy",
                               side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                native_upload._prepare(self.state_dir, conn)
        self.assertNotIn(str(self.state_dir.resolve()),
                         native_upload._LEGACY_PROMOTED)
        native_upload._prepare(self.state_dir, conn)
        self.assertIn(str(self.state_dir.resolve()),
                      native_upload._LEGACY_PROMOTED)
        conn.close()


# Keep only the v3 tests in the subclass: unittest discovers inherited
# base methods too, which would rerun the v2 suite under a second name.
for _name in list(dir(NativeUploadV3Tests)):
    if (_name.startswith("test_")
            and _name not in NativeUploadV3Tests.__dict__):
        setattr(NativeUploadV3Tests, _name, None)


if __name__ == "__main__":
    unittest.main()
