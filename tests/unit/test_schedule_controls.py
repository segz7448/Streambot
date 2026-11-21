import os
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path

from app.bot import schedule_cmds as sc
from app.bot import commands_full as cf
from app.core import schedule as sched
from app.core.database import migrate
from app.core.intake import resolve_timezone


class ScheduleTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        c = sqlite3.connect(str(self.d / "nova.db"))
        migrate(c)
        c.close()
        cf.addsource_reply(self.d, "kaicenat")
        self.now = datetime(2026, 11, 21, 9, 0, tzinfo=resolve_timezone("America/Los_Angeles")).timestamp()

    def test_parse_pst(self):
        t = sched.parse_when("2026-11-22 10:30", now=self.now)
        self.assertEqual(datetime.fromtimestamp(t, resolve_timezone("America/Los_Angeles")).strftime("%H:%M %Z"), "10:30 PST")
        self.assertEqual(sched.parse_when("tomorrow 10:30", now=self.now), t)
        for bad in ("tomorrow", "2026-13-01 10:00", "today 25:00", "x y"):
            with self.assertRaises(ValueError):
                sched.parse_when(bad, now=self.now)

    def test_add_validation(self):
        t = self.now + 3600
        with self.assertRaises(ValueError): sched.add(self.d, "ghostuser", t, 3600, now=self.now)
        with self.assertRaises(ValueError): sched.add(self.d, "kaicenat", self.now - 7200, 3600, now=self.now)
        with self.assertRaises(ValueError): sched.add(self.d, "kaicenat", t, 60, now=self.now)
        with self.assertRaises(ValueError): sched.add(self.d, "bad name!", t, 3600, now=self.now)
        sid = sched.add(self.d, "KaiCenat", t, 3600, now=self.now)
        with self.assertRaises(ValueError): sched.add(self.d, "kaicenat", t + 1800, 3600, now=self.now)  # overlap
        self.assertTrue(sched.cancel(self.d, sid))
        self.assertFalse(sched.cancel(self.d, sid))

    def test_dst_gap_and_fold_rejected(self):
        with self.assertRaises(ValueError): sched.parse_when("2027-03-14 02:30", now=self.now)  # gap
        with self.assertRaises(ValueError): sched.parse_when("2026-11-01 01:30", now=self.now)  # fold
        sched.parse_when("2026-11-22 02:30", now=self.now)

    def test_day_bounds_dst_exact(self):
        a, b = sched.parse_day("2026-11-01", now=self.now)
        self.assertEqual(round((b - a) / 3600), 25)
        a, b = sched.parse_day("2027-03-14", now=self.now)
        self.assertEqual(round((b - a) / 3600), 23)

    def _cbs(self, **kw):
        log = {"start": [], "stop": []}
        d = dict(alive=True, confirm=False, token="tok1", err="")
        d.update(kw)
        def start(u):
            log["start"].append(u)
            if d["token"] is None:
                return "No authorized feed for x. Nothing is recording.", None
            return "Clipping session started", d["token"]
        def stop(u, t):
            log["stop"].append((u, t))
            return d.get("stopok", True)
        return log, d, dict(start_fn=start, stop_fn=stop,
                            alive_fn=lambda u, t: d["alive"],
                            confirm_fn=lambda u, since: d["confirm"],
                            error_fn=lambda u, t: d["err"])

    def _tick(self, cb, now):
        return sched.tick(self.d, cb["start_fn"], cb["stop_fn"], alive_fn=cb["alive_fn"],
                          confirm_fn=cb["confirm_fn"], error_fn=cb["error_fn"], now=now)

    def test_launch_is_not_recording_until_segment(self):
        t = self.now + 60
        sched.add(self.d, "kaicenat", t, 3600, now=self.now)
        log, st, cb = self._cbs()
        self.assertEqual(self._tick(cb, self.now), [])
        ev = self._tick(cb, t + 5)
        self.assertEqual([e[1] for e in ev], ["LAUNCHED"])
        self.assertNotIn("recording confirmed", ev[0][2])
        self.assertEqual(self._tick(cb, t + 10), [])      # no double launch, no confirm yet
        st["confirm"] = True
        ev = self._tick(cb, t + 40)
        self.assertEqual([e[1] for e in ev], ["RECORDING"])
        self.assertEqual(len(log["start"]), 1)
        ev = self._tick(cb, t + 3601)
        self.assertEqual([e[1] for e in ev], ["DONE"])
        self.assertEqual(log["stop"], [("kaicenat", "tok1")])
        self.assertIn("session stopped", ev[0][2])

    def test_async_failure_never_reported_as_recording(self):
        t = self.now + 60
        sched.add(self.d, "kaicenat", t, 3600, now=self.now)
        log, st, cb = self._cbs()
        self._tick(cb, t + 1)
        st["alive"] = False
        st["err"] = "FFmpegError: input invalid"
        ev = self._tick(cb, t + 20)
        self.assertEqual(ev[0][1], "MISSED")
        self.assertIn("input invalid", ev[0][2])

    def test_unconfirmed_launch_times_out(self):
        t = self.now + 60
        sched.add(self.d, "kaicenat", t, 3600, now=self.now)
        log, st, cb = self._cbs()
        self._tick(cb, t + 1)
        ev = self._tick(cb, t + 1 + sched.CONFIRM_TIMEOUT_S + 5)
        self.assertEqual(ev[0][1], "MISSED")

    def test_stop_false_reported_truthfully(self):
        t = self.now + 60
        sched.add(self.d, "kaicenat", t, 600, now=self.now)
        log, st, cb = self._cbs(confirm=True, stopok=False)
        self._tick(cb, t + 1); self._tick(cb, t + 40)
        ev = self._tick(cb, t + 700)
        self.assertIn("no longer running", ev[0][2])
        self.assertNotIn("session stopped", ev[0][2])

    def test_launch_exception_commits_claim_no_repeat(self):
        t = self.now + 60
        sched.add(self.d, "kaicenat", t, 3600, now=self.now)
        n = []
        def boom(u):
            n.append(1); raise RuntimeError("x")
        cb = dict(start_fn=boom, stop_fn=lambda u, t: True, alive_fn=lambda u, t: False,
                  confirm_fn=lambda u, s: False, error_fn=lambda u, t: "")
        self._tick(cb, t + 1); self._tick(cb, t + 40)
        self.assertEqual(len(n), 1)
        self.assertEqual(sched.list_open(self.d), [])

    def test_restart_reconcile_never_relaunches(self):
        t = self.now + 60
        sched.add(self.d, "kaicenat", t, 3600, now=self.now)
        log, st, cb = self._cbs()
        self._tick(cb, t + 1)
        self.assertEqual(sched.reconcile(self.d), 1)
        self._tick(cb, t + 100)
        self.assertEqual(len(log["start"]), 1)
        self.assertEqual(sched.list_open(self.d), [])

    def test_day_mark_retries_then_launches(self):
        a, b = sched.parse_day("2026-11-22", now=self.now)
        sched.add(self.d, "kaicenat", a, None, now=self.now, day_end=b)
        log, st, cb = self._cbs(token=None)
        ev = self._tick(cb, a + 5)
        self.assertEqual([e[1] for e in ev], ["WAITING"])
        self.assertEqual(self._tick(cb, a + 60), [])           # throttled
        self.assertEqual(len(log["start"]), 1)
        self._tick(cb, a + 400)
        self.assertEqual(len(log["start"]), 2)
        st["token"] = "tokD"
        ev = self._tick(cb, a + 800)
        self.assertEqual([e[1] for e in ev], ["LAUNCHED"])

    def test_day_mark_ends_missed_truthfully(self):
        a, b = sched.parse_day("2026-11-22", now=self.now)
        sched.add(self.d, "kaicenat", a, None, now=self.now, day_end=b)
        log, st, cb = self._cbs(token=None)
        self._tick(cb, a + 5)
        ev = self._tick(cb, b + 1)
        self.assertEqual(ev[0][1], "MISSED")
        self.assertIn("no recording started", ev[0][2])

    def test_stale_window_missed_not_started(self):
        t = self.now + 60
        sched.add(self.d, "kaicenat", t, 3600, now=self.now)
        log, st, cb = self._cbs()
        ev = self._tick(cb, t + 3600 - 5 + 3000)
        self.assertEqual(ev[0][1], "MISSED")
        self.assertEqual(log["start"], [])

    def test_outbox_durable(self):
        sched.enqueue_notices(self.d, [(1, "X", "hello")])
        n = sched.pending_notices(self.d)
        self.assertEqual([t for _, t in n], ["hello"])
        sched.mark_notice_sent(self.d, n[0][0])
        self.assertEqual(sched.pending_notices(self.d), [])

    def test_commands_need_timezone_then_work(self):
        self.assertIn("NOT set", sc.schedule_reply(self.d, "kaicenat 2099-01-01"))
        self.assertIn("NOT set", sc.timezone_reply(self.d, ""))
        self.assertIn("Unknown", sc.timezone_reply(self.d, "Mars/Base"))
        self.assertIn("set to", sc.timezone_reply(self.d, "America/Los_Angeles"))
        self.assertIn("Usage", sc.schedule_reply(self.d, ""))
        self.assertIn("Not scheduled", sc.schedule_reply(self.d, "kaicenat 2020-01-01"))
        r = sc.schedule_reply(self.d, "kaicenat 2099-01-01")
        self.assertIn("Scheduled #1", r)
        self.assertIn("cannot see when the streamer goes live", r)
        self.assertIn("Scheduled #2", sc.schedule_reply(self.d, "kaicenat 2099-01-02 10:00 3"))
        out = sc.schedules_reply(self.d)
        self.assertIn("day", out); self.assertIn("3h", out)
        self.assertIn("cancelled", sc.unschedule_reply(self.d, "1"))


class _FakeSess:
    def __init__(self, tok, alive=True):
        import threading
        self._sched_token = tok
        self._stopping = threading.Event()
        self._alive = alive
        self.error = "boom"
        self.stopped = 0

    def alive(self):
        return self._alive

    def stop(self):
        self.stopped += 1
        self._stopping.set()


class SessionBridgeTests(unittest.TestCase):
    def setUp(self):
        from app.bot import sessions as ss
        self.ss = ss
        self.d = Path(tempfile.mkdtemp())
        c = sqlite3.connect(str(self.d / "nova.db"))
        migrate(c)
        c.close()
        cf.addsource_reply(self.d, "kaicenat")
        ss._sessions.clear()
        self.addCleanup(ss._sessions.clear)

    def test_no_feed_no_record_auth_starts_nothing(self):
        reply, tok = self.ss.start_scheduled(self.d, "kaicenat", "ff", "fp")
        self.assertIsNone(tok)
        self.assertIn("not authorized", reply)
        self.assertEqual(self.ss._sessions, {})

    def test_token_exactness(self):
        a, b = _FakeSess("a"), _FakeSess("b")
        self.ss._sessions["kaicenat"] = a
        self.assertTrue(self.ss.session_alive("kaicenat", "a"))
        self.assertFalse(self.ss.session_alive("kaicenat", "b"))
        self.assertFalse(self.ss.session_alive("kaicenat", None))
        self.assertEqual(self.ss.session_error("kaicenat", "a"), "boom")
        self.assertEqual(self.ss.session_error("kaicenat", "b"), "")
        # a later session replaced ours: stopping our token must not touch it
        self.ss._sessions["kaicenat"] = b
        self.assertFalse(self.ss.stop_session("kaicenat", "a"))
        self.assertEqual(b.stopped, 0)
        self.assertTrue(self.ss.stop_session("kaicenat", "b"))
        self.assertFalse(self.ss.stop_session("kaicenat", "b"))  # already stopping
        self.assertEqual(b.stopped, 1)

    def test_feed_start_binds_token(self):
        (self.d / "feeds").mkdir()
        (self.d / "feeds" / "kaicenat.url").write_text("https://relay.example/x.m3u8")
        made = []

        def factory(*a, **k):
            s = _FakeSess(None)
            s.start = lambda: None
            made.append(s)
            return s
        import unittest.mock as m
        orig = self.ss.startclip_reply
        with m.patch.object(self.ss, "validate_feed", lambda f, d: f):
            reply = orig(self.d, "kaicenat", "ff", "fp", session_factory=factory)
            tok = getattr(made[0], "_sched_token", None)
        self.assertIn("started", reply)
        self.assertTrue(tok)
        self.assertTrue(self.ss.session_alive("kaicenat", tok))

    def test_confirm_recording_needs_segment_after_claim(self):
        t = 1000.0
        self.assertFalse(sched.confirm_recording(self.d, "kaicenat", t))
        c = sqlite3.connect(str(self.d / "nova.db"))
        try:
            sid = c.execute("SELECT id FROM twitch_sources WHERE username='kaicenat'").fetchone()[0]
            c.execute("INSERT INTO stream_sessions (source_id,state,started_at) VALUES (?,?,?)",
                      (sid, "RECORDING", 500.0))
            c.execute("INSERT INTO recording_segments (session_id,file_path,sequence,created_at) VALUES (1,'x',0,501)")
            c.commit()
            self.assertFalse(sched.confirm_recording(self.d, "kaicenat", t))  # older session
            c.execute("INSERT INTO stream_sessions (source_id,state,started_at) VALUES (?,?,?)",
                      (sid, "RECORDING", 1001.0))
            c.commit()
            self.assertFalse(sched.confirm_recording(self.d, "kaicenat", t))  # no segment yet
            c.execute("INSERT INTO recording_segments (session_id,file_path,sequence,created_at) VALUES (2,'y',0,1002)")
            c.commit()
        finally:
            c.close()
        self.assertTrue(sched.confirm_recording(self.d, "kaicenat", t))
        self.assertFalse(sched.confirm_recording(Path(tempfile.mkdtemp()), "kaicenat", t))
