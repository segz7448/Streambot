import json
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from app.bot import pause
from app.bot import youtube_views as yv
from app.core.database import migrate
from app.core.jobs import JobQueue
from app.distribution import publish_records as pr


class Base(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        c = sqlite3.connect(str(self.d / "nova.db"))
        migrate(c)
        c.close()

    def conn(self):
        c = sqlite3.connect(str(self.d / "nova.db"))
        c.row_factory = sqlite3.Row
        return c


class ViewsTests(Base):
    def test_empty_and_read_only(self):
        before = (self.d / "nova.db").stat().st_mtime_ns
        self.assertIn("empty", yv.publishqueue_reply(self.d))
        self.assertIn("No confirmed", yv.publishhistory_reply(self.d))
        self.assertEqual(before, (self.d / "nova.db").stat().st_mtime_ns)

    def test_unavailable_is_not_empty_or_zero(self):
        for fn in (yv.publishqueue_reply, yv.publishhistory_reply):
            self.assertIn("unavailable", fn(self.d / "nope"))
        self.assertIn("unavailable", yv.youtube_reply(self.d / "nope"))
        # broken schema (table missing) is also unavailable, not empty
        bad = Path(tempfile.mkdtemp())
        sqlite3.connect(str(bad / "nova.db")).execute("create table x(a)").connection.commit()
        self.assertIn("unavailable", yv.publishqueue_reply(bad))

    def test_queue_shows_jobs_retry_pause_and_records(self):
        c = self.conn()
        q = JobQueue(c)
        jid = q.enqueue("clip_intake_handoff", {"clip_id": "cliprow:7", "intake_date": "d"})
        job = q.claim("w", kinds=["clip_intake_handoff"])
        q.fail(jid, "network blip")             # PENDING with retry backoff
        c.execute("INSERT INTO publish_records (clip_id,state,attempts,last_error,created_at,updated_at)"
                  " VALUES (2,'UNCERTAIN',1,'timeout',?,?)", (time.time(), time.time()))
        c.execute("INSERT INTO publish_records (clip_id,state,video_id,attempts,created_at,updated_at)"
                  " VALUES (1,'CONFIRMED','vid123',1,?,?)", (time.time(), time.time()))
        c.commit(); c.close()
        pause.set_paused(self.d, True)
        out = yv.publishqueue_reply(self.d)
        self.assertIn("cliprow:7", out)
        self.assertIn("PENDING attempt 1/", out)
        self.assertIn("retry after", out)
        self.assertIn("network blip", out)
        self.assertIn("pause: PAUSED", out)
        self.assertIn("#2 UNCERTAIN", out)
        self.assertNotIn("vid123", out)
        self.assertIn("vid123", yv.publishhistory_reply(self.d))
        self.assertIn("pause: PAUSED", yv.youtube_reply(self.d))

    def test_counts_uncapped_and_localized(self):
        c = self.conn()
        now = time.time()
        c.executemany("INSERT INTO publish_records (clip_id,state,video_id,attempts,created_at,updated_at)"
                      " VALUES (?,'CONFIRMED','v',1,?,?)", [(i, now, now) for i in range(1, 1201)])
        c.commit(); c.close()
        self.assertIn("confirmed uploads: 1200", yv.youtube_reply(self.d))
        self.assertIn("1200 total, newest 15", yv.publishhistory_reply(self.d))
        self.assertRegex(yv.publishhistory_reply(self.d), r"P[SD]T")  # vendored zone, not host

    def test_credentials_never_leak(self):
        env = {"NOVA_YOUTUBE_CLIENT_ID": "SECRETID", "NOVA_YOUTUBE_CLIENT_SECRET": "SECRETSEC",
               "NOVA_YOUTUBE_REFRESH_TOKEN": "SECRETTOK", "NOVA_NATIVE_UPLOAD": "1"}
        with mock.patch.dict(os.environ, env, clear=False):
            r = yv.youtube_reply(self.d) + yv.youtubestatus_reply(self.d)
        self.assertIn("present", r)
        self.assertIn("ENABLED", r)
        self.assertNotIn("SECRET", r)

    def test_registered(self):
        import inspect
        from app.bot.service import BotService
        src = inspect.getsource(BotService)
        for n in ("youtubestatus", "publishqueue", "publishhistory", "autopublish"):
            self.assertIn(f'"{n}"', src)


class PauseTests(Base):
    def test_state_tristate_and_fail_closed(self):
        self.assertEqual(pause.state(self.d), "running")
        pause.set_paused(self.d, True)
        self.assertEqual(pause.state(self.d), "paused")
        self.assertEqual(pause.state(self.d / "nope"), "unreadable")
        self.assertTrue(pause.is_paused(self.d / "nope"))
        self.assertIn("UNREADABLE", pause.autopublish_reply(self.d / "nope", "").upper())
        self.assertIn("Could NOT change", pause.autopublish_reply(self.d / "nope", "pause"))

    def test_gate_inside_reserve_blocks_and_writes_nothing(self):
        with pr.ClipLease(self.d, 5) as lease:
            pause.set_paused(self.d, True)
            dec, rec = pr.reserve(self.d, 5, "t", lease=lease, gate=pause.gate_in_tx)
            self.assertEqual((dec, rec), ("gated", None))
            self.assertIsNone(pr.get(self.d, 5))
            pause.set_paused(self.d, False)
            dec, rec = pr.reserve(self.d, 5, "t", lease=lease, gate=pause.gate_in_tx)
            self.assertEqual(dec, "reserved")

    def test_pause_after_reserve_is_reported_in_flight(self):
        with pr.ClipLease(self.d, 6) as lease:
            pr.reserve(self.d, 6, "t", lease=lease, gate=pause.gate_in_tx)
            out = pause.autopublish_reply(self.d, "pause")
            self.assertIn("1 upload(s) already reserved will still finish", out)
        self.assertIn("reserved/in-flight 1", pause.autopublish_reply(self.d, ""))

    def test_pause_commits_serialize_with_reserve(self):
        # pause is a BEGIN IMMEDIATE writer: while reserve holds its
        # transaction, pause waits; so exactly one ordering is observable.
        import threading
        with pr.ClipLease(self.d, 8) as lease:
            seen = []
            def gate(conn):
                t = threading.Thread(target=lambda: pause.set_paused(self.d, True))
                t.start(); time.sleep(0.3)
                seen.append(t.is_alive())     # pause is blocked behind our tx
                r = pause.gate_in_tx(conn)
                self._t = t
                return r
            dec, _ = pr.reserve(self.d, 8, "t", lease=lease, gate=gate)
            self._t.join(10)
        self.assertEqual(dec, "reserved")
        self.assertEqual(seen, [True])
        self.assertEqual(pause.state(self.d), "paused")
        self.assertIn("1 upload(s) already reserved", pause.autopublish_reply(self.d, "pause"))

    def test_native_upload_pause_before_claim_and_at_reserve(self):
        from app.bot import native_upload as nu
        from app.core.database import connect
        from app.core.intake import ClipIntakeAuthority
        conn = connect(str(self.d / "nova.db"))
        q = JobQueue(conn)
        jid = q.enqueue("clip_intake_handoff", {"clip_id": "cliprow:1", "intake_date": "2026-11-21"})
        pause.set_paused(self.d, True)
        self.assertFalse(nu._process_one_locked(conn, self.d))   # nothing claimed
        self.assertEqual(conn.execute("select state from job_queue").fetchone()[0], "PENDING")
        pause.set_paused(self.d, False)
        job = q.claim(nu.WORKER_NAME, kinds=["clip_intake_handoff"])
        ClipIntakeAuthority(str(self.d / "nova.db"))
        conn.execute("INSERT INTO clip_intake (clip_id,intake_date,status,daily_count,handoff_job_id,created_at,updated_at,title,streamer)"
                     " VALUES ('cliprow:1','2026-11-21','ACCEPTED',1,?,0,0,'t','s')", (jid,))
        sd = self.d / "sessions" / "s"; sd.mkdir(parents=True)
        (sd / "c.mp4").write_bytes(b"x")
        conn.execute("INSERT INTO clips (id,status,file_path,created_at) VALUES (1,'HELD',?,0)", (str(sd / "c.mp4"),))
        conn.commit()
        calls = []
        class P:
            def publish(self, *a, **k):
                calls.append(1); return "vid"
        def probe(p):
            pause.set_paused(self.d, True)      # pause lands DURING the media probe
            return {"format": {"duration": "30"},
                    "streams": [{"codec_type": "video", "width": 1080, "height": 1920}, {"codec_type": "audio"}]}
        self.assertTrue(nu._handle_claimed(conn, q, self.d, job, publisher_factory=lambda: P(), probe_fn=probe))
        self.assertEqual(calls, [])
        row = conn.execute("SELECT state,attempts FROM job_queue WHERE id=?", (jid,)).fetchone()
        self.assertEqual((row["state"], row["attempts"]), ("PENDING", 0))
        self.assertIsNone(conn.execute("SELECT 1 FROM publish_records").fetchone())


class TokenSourceTests(unittest.TestCase):
    def test_env_first_file_fallback_and_fail_closed(self):
        from app.core import tokensrc as ts
        f = Path(tempfile.mkdtemp()) / "t"
        f.write_text("FILETOKEN\n")
        with mock.patch.dict(os.environ, {"NOVA_TOKEN_FILE": str(f)}, clear=False):
            os.environ.pop("NOVA_TELEGRAM_TOKEN", None)
            self.assertEqual((ts.source(), ts.read_token()), ("file", "FILETOKEN"))
            os.environ["NOVA_TELEGRAM_TOKEN"] = " ENVTOKEN "
            self.assertEqual((ts.source(), ts.read_token()), ("env", "ENVTOKEN"))
            os.environ["NOVA_TELEGRAM_TOKEN"] = "  "
            with self.assertRaises(ts.TokenUnavailable):
                ts.read_token()
            os.environ.pop("NOVA_TELEGRAM_TOKEN")
            f.unlink()
            self.assertEqual(ts.source(), "none")
            with self.assertRaises(Exception):
                ts.read_token()

    def test_status_never_shows_value(self):
        from app.bot import service
        with mock.patch.dict(os.environ, {"NOVA_TELEGRAM_TOKEN": "SECRETVALUE"}, clear=False):
            self.assertEqual(service._tok_src(), "env")

    def test_delivery_uses_env_token(self):
        from app.distribution import telegram_delivery as td
        got = {}
        async def fake(token, store, video, caption):
            got["t"] = token; return 1
        with mock.patch.object(td, "deliver_clip", fake), \
             mock.patch.dict(os.environ, {"NOVA_TELEGRAM_TOKEN": "ENVT"}, clear=False):
            self.assertEqual(td.deliver_clip_sync("/nonexistent", str(tempfile.mkdtemp()), "v.mp4"), 1)
        self.assertEqual(got["t"], "ENVT")


if __name__ == "__main__":
    unittest.main()
