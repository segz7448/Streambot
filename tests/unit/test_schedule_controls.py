"""Scheduler core: durable attempt ownership, exact-session confirmation on
real finalized media, atomic state+notice, day eligibility, stop/cancel,
restart, DST budgets. No Telegram transport and no network."""
import json
import os
import sqlite3
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from app.bot import commands_full as cf
from app.bot import schedule_cmds as sc
from app.clipping.retrospective import validate_recording
from app.core import schedule as sched
from app.core.database import migrate
from app.core.intake import resolve_timezone

LA = resolve_timezone("America/Los_Angeles")


def ts(y, mo, d, h=0, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=LA).timestamp()


class World:
    """Stand-in for the live session registry, keyed by attempt id."""
    def __init__(self, d):
        self.d = d
        self.s = {}              # aid -> dict(running, stopping, error)
        self.starts = []
        self.mode = "start"      # start | not_started | raise_registered | raise_ghost
        self.stops = []
        self.stop_raises = 0
        self.confirmed = set()
        self.user_before = None
        self.writer = False
        self.terminal = {}
        self.record_ok = False

    def start(self, user, aid):
        self.starts.append((user, aid))
        if self.user_before:
            self.user_before(aid)
        if self.mode == "not_started":
            return "not_started", "Not started: the channel is offline or unknown."
        if self.mode in ("blocked", "unauthorized"):
            return self.mode, f"refused ({self.mode})"
        if self.mode == "raise_registered":
            self.s[aid] = dict(running=True, stopping=False, error="")
            raise RuntimeError("secret-looking text must not leak")
        if self.mode == "raise_ghost":
            raise RuntimeError("boom")
        self.s[aid] = dict(running=True, stopping=False, error="")
        return "started", "ok"

    def stop(self, user, aid):
        if self.stop_raises:
            self.stop_raises -= 1
            raise RuntimeError("stop failed")
        s = self.s.get(aid)
        self.stops.append(aid)
        if s and not s["stopping"]:
            s["stopping"] = True
            return True
        return False

    def finish(self, aid, error=""):
        self.s[aid]["running"] = False
        self.s[aid]["error"] = error

    def ops(self):
        g = lambda aid, k: bool(self.s.get(aid, {}).get(k))
        return sched.Ops(
            start=self.start, stop=self.stop,
            running=lambda u, a: g(a, "running"),
            alive=lambda u, a: g(a, "running") and not g(a, "stopping"),
            stopping=lambda u, a: g(a, "stopping"),
            confirm=lambda a: a in self.confirmed,
            error=lambda u, a: self.s.get(a, {}).get("error", ""),
            writer_active=lambda u: self.writer,
            terminal=lambda u, a: self.terminal.get(a, ""),
            record_allowed=lambda u: self.record_ok)


class Base(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        c = sqlite3.connect(str(self.d / "nova.db"))
        migrate(c)
        c.close()
        cf.addsource_reply(self.d, "kaicenat")
        cf.addsource_reply(self.d, "ishowspeed")
        self.w = World(self.d)
        self.now = ts(2026, 11, 21, 9)

    def tick(self, t):
        sched.tick(self.d, self.w.ops(), now=t)

    def rows(self, sql, args=()):
        c = sqlite3.connect(str(self.d / "nova.db"))
        try:
            return c.execute(sql, args).fetchall()
        finally:
            c.close()

    def plan(self, sid):
        return self.rows("SELECT state,note FROM clip_schedules WHERE id=?", (sid,))[0]

    def attempts(self, sid=None):
        return self.rows("SELECT id,state,stop_by FROM schedule_attempts"
                         + (" WHERE schedule_id=%d" % sid if sid else "") + " ORDER BY created_at")

    def notices(self):
        return [r[0] for r in self.rows("SELECT text FROM schedule_notices ORDER BY id")]

    def day(self, user="kaicenat", day="2026-11-21"):
        t0, t1 = sched.parse_day(day, tzname="America/Los_Angeles")
        return sched.add(self.d, user, t0, None, now=self.now, day_end=t1), t0, t1


class OwnershipTests(Base):
    def test_attempt_committed_before_launch(self):
        sid, t0, _ = self.day()
        seen = {}

        def probe(aid):
            seen["attempt"] = self.rows("SELECT state FROM schedule_attempts WHERE id=?", (aid,))
            seen["plan"] = self.plan(sid)[0]
        self.w.user_before = probe
        self.tick(self.now)
        self.assertEqual(seen["attempt"], [("INTENT",)])     # durable before any launch
        self.assertEqual(seen["plan"], "ACTIVE")
        self.assertEqual(self.attempts(sid)[0][1], "LAUNCHED")

    def test_exception_with_running_session_binds_not_duplicates(self):
        sid, _, _ = self.day()
        self.w.mode = "raise_registered"
        self.tick(self.now)
        self.assertEqual(self.attempts(sid)[0][1], "LAUNCHED")
        for k in range(1, 6):
            self.tick(self.now + 300 * k)
        self.assertEqual(len(self.w.starts), 1)              # no duplicate launch
        self.assertFalse(any("secret-looking" in n for n in self.notices()))

    def test_exception_with_active_writer_holds_until_verified_quiet(self):
        sid, _, _ = self.day()
        self.w.mode, self.w.writer = "raise_ghost", True
        self.tick(self.now)
        self.assertEqual(self.plan(sid)[0], "UNCERTAIN")
        self.assertEqual(self.attempts(sid)[0][1], "UNCERTAIN")
        for k in range(1, 10):
            self.tick(self.now + 300 * k)               # writer still active: never rearmed
        self.assertEqual(len(self.w.starts), 1)
        self.assertEqual(self.plan(sid)[0], "UNCERTAIN")
        self.assertEqual(sched.cancel(self.d, sid, now=self.now + 3000), "cancelled_running")
        self.tick(self.now + 3100)
        self.assertEqual(self.attempts(sid)[0][1], "UNCERTAIN")   # cancel does not free the writer
        sid2 = None
        t0, t1 = sched.parse_day("2026-11-21", tzname="America/Los_Angeles")
        sid2 = sched.add(self.d, "kaicenat", t0, None, now=self.now, day_end=t1)
        self.w.mode = "start"
        self.tick(self.now + 3200)
        self.assertEqual(len(self.w.starts), 1)           # blocked: open attempt still owns the streamer
        self.w.writer = False                              # actual cleanup verified
        self.tick(self.now + 3300)
        self.assertEqual(self.attempts(sid)[0][1], "INTERRUPTED")
        self.tick(self.now + 3400)
        self.assertEqual(len(self.w.starts), 2)

    def test_exception_with_nothing_running_or_writing_retries_not_uncertain(self):
        sid, _, _ = self.day()
        self.w.mode = "raise_ghost"
        self.tick(self.now)
        self.assertEqual(self.attempts(sid)[0][1], "NOT_STARTED")
        self.assertEqual(self.plan(sid)[0], "SCHEDULED")

    def test_definitive_not_started_retries_with_backoff_inside_end(self):
        sid, t0, t1 = self.day()
        self.w.mode = "not_started"
        t = self.now
        while t < t1 + 3600:
            self.tick(t)
            t += 30
        n = len(self.w.starts)
        self.assertTrue(n < 120, n)                         # not 288 fixed attempts, backoff bound
        self.assertEqual(self.plan(sid)[0], "MISSED")
        starts_after_end = [a for a in self.rows("SELECT created_at FROM schedule_attempts") if a[0] > t1]
        self.assertEqual(starts_after_end, [])
        self.assertEqual(sum("not recording yet" in n for n in self.notices()), 1)

    def test_one_owner_per_streamer_and_overlap_is_atomic(self):
        self.day()
        with self.assertRaises(ValueError):
            self.day()
        errs, ok = [], []
        t0, t1 = sched.parse_day("2026-11-22", tzname="America/Los_Angeles")

        def go():
            try:
                ok.append(sched.add(self.d, "ishowspeed", t0, None, now=self.now, day_end=t1))
            except ValueError as e:
                errs.append(e)
        th = [threading.Thread(target=go) for _ in range(6)]
        [x.start() for x in th]; [x.join() for x in th]
        self.assertEqual((len(ok), len(errs)), (1, 5))


class DayAndStopTests(Base):
    def test_day_stays_eligible_after_early_end_and_records_again(self):
        sid, t0, t1 = self.day()
        self.tick(self.now)
        a1 = self.w.starts[0][1]
        self.w.confirmed.add(a1)
        self.tick(self.now + 30)
        self.assertEqual(self.attempts(sid)[0][1], "RECORDING")
        self.w.finish(a1, "SomeError: text")
        self.tick(self.now + 60)
        self.assertEqual(self.plan(sid)[0], "SCHEDULED")     # whole-day eligibility survives
        self.tick(self.now + 200)
        self.assertEqual(len(self.w.starts), 2)
        self.assertNotEqual(self.w.starts[1][1], a1)
        self.assertTrue(any("try again" in n for n in self.notices()))
        self.assertFalse(any("text" in n for n in self.notices()))  # class-name only
        self.tick(t1 + 1)
        self.assertEqual(self.w.s[self.w.starts[1][1]]["stopping"], True)
        self.w.finish(self.w.starts[1][1])
        self.tick(t1 + 31)
        self.assertEqual(self.plan(sid)[0], "DONE")

    def test_user_stop_cancels_plan_never_relaunches(self):
        sid, _, _ = self.day()
        self.tick(self.now)
        aid = self.w.starts[0][1]
        self.w.s[aid]["stopping"] = True        # /stopclip signalled the session
        self.assertTrue(sched.user_stopped(self.d, aid, now=self.now + 5))
        self.assertEqual(self.plan(sid)[0], "CANCELLED")
        self.w.finish(aid)
        for k in range(1, 6):
            self.tick(self.now + 100 * k)
        self.assertEqual(len(self.w.starts), 1)
        self.assertEqual(self.attempts(sid)[0][1], "ENDED")

    def test_user_stop_detected_even_if_hook_failed(self):
        sid, _, _ = self.day()
        self.tick(self.now)
        aid = self.w.starts[0][1]
        self.w.s[aid]["stopping"] = True        # stopped, but user_stopped never ran
        self.tick(self.now + 30)
        self.assertEqual(self.plan(sid)[0], "CANCELLED")
        self.w.finish(aid)
        self.tick(self.now + 400)
        self.assertEqual(len(self.w.starts), 1)

    def test_unschedule_active_stops_exact_session_no_relaunch(self):
        sid, _, _ = self.day()
        self.tick(self.now)
        aid = self.w.starts[0][1]
        self.assertEqual(sched.cancel(self.d, sid, now=self.now + 5), "cancelled_running")
        self.tick(self.now + 30)
        self.assertEqual(self.w.stops, [aid])
        self.w.finish(aid)
        self.tick(self.now + 60)
        self.assertEqual(self.attempts(sid)[0][1], "ENDED")
        self.tick(self.now + 600)
        self.assertEqual(len(self.w.starts), 1)
        self.assertEqual(sched.cancel(self.d, sid), "none")

    def test_stop_exception_stays_stopping_and_retries(self):
        sid, _, t1 = self.day()
        self.tick(self.now)
        aid = self.w.starts[0][1]
        sched.cancel(self.d, sid, now=self.now + 1)
        self.w.stop_raises = 2
        self.tick(self.now + 30)
        self.assertEqual(self.attempts(sid)[0][1], "STOPPING")
        self.tick(self.now + 60)
        self.assertEqual(self.attempts(sid)[0][1], "STOPPING")
        self.tick(self.now + 90)                  # third call works
        self.assertEqual(self.w.stops, [aid])
        self.w.finish(aid)
        self.tick(self.now + 120)
        self.assertEqual(self.attempts(sid)[0][1], "ENDED")
        self.assertEqual(sum("has not stopped" in n for n in self.notices()), 1)

    def test_unconfirmed_launch_times_out_and_retries(self):
        sid, _, _ = self.day()
        self.tick(self.now)
        self.tick(self.now + 100)
        self.assertEqual(self.attempts(sid)[0][1], "LAUNCHED")
        self.assertFalse(any("recording confirmed" in n for n in self.notices()))
        self.tick(self.now + sched.CONFIRM_TIMEOUT_S + 30)
        self.assertEqual(self.attempts(sid)[0][1], "STOPPING")
        self.w.finish(self.w.starts[0][1])
        self.tick(self.now + sched.CONFIRM_TIMEOUT_S + 60)
        self.assertEqual(self.plan(sid)[0], "SCHEDULED")

    def test_fixed_window_only_when_given_and_missed_if_late(self):
        t = self.now + 3600
        sid = sched.add(self.d, "kaicenat", t, 3600, now=self.now)
        self.tick(t + sched.GRACE_S + 60)
        self.assertEqual(self.plan(sid)[0], "MISSED")
        self.assertEqual(self.w.starts, [])
        with self.assertRaises(ValueError):
            sched.add(self.d, "ishowspeed", t, 60, now=self.now)


class PolicyTests(Base):
    def test_cancel_during_launch_is_not_overwritten_and_new_session_stopped(self):
        sid, _, _ = self.day()
        self.w.user_before = lambda aid: sched.cancel(self.d, sid, now=self.now + 1)
        self.tick(self.now)
        self.assertEqual(self.plan(sid)[0], "CANCELLED")
        aid = self.w.starts[0][1]
        self.assertEqual(self.attempts(sid)[0][1], "STOPPING")
        self.assertEqual(self.w.stops, [aid])              # exact new session stopped
        self.w.finish(aid)
        self.tick(self.now + 60)
        self.tick(self.now + 600)
        self.assertEqual(len(self.w.starts), 1)
        self.assertEqual(self.plan(sid)[0], "CANCELLED")

    def test_cancel_during_not_started_not_overwritten(self):
        sid, _, _ = self.day()
        self.w.mode = "not_started"
        self.w.user_before = lambda aid: sched.cancel(self.d, sid, now=self.now + 1)
        self.tick(self.now)
        self.assertEqual(self.plan(sid)[0], "CANCELLED")
        self.tick(self.now + 1000)
        self.assertEqual(len(self.w.starts), 1)

    def test_blocked_preflight_stops_and_is_visible_never_retried(self):
        sid, _, t1 = self.day()
        self.w.mode = "blocked"
        t = self.now
        while t < t1:
            self.tick(t)
            t += 600
        self.assertEqual(len(self.w.starts), 1)
        self.assertEqual(self.plan(sid)[0], "BLOCKED")
        self.assertIn(sid, [r["id"] for r in sched.list_visible(self.d)])
        self.assertNotIn(sid, [r["id"] for r in sched.list_open(self.d)])
        self.assertTrue(any("/checklive" in n and "stopped trying" in n for n in self.notices()))
        t0, _ = sched.parse_day("2026-11-21", tzname="America/Los_Angeles")
        sched.add(self.d, "kaicenat", t0, None, now=self.now, day_end=t1)   # owner re-creates: allowed

    def test_missing_grant_waits_for_grant_not_clock(self):
        sid, _, t1 = self.day()
        self.w.mode = "unauthorized"
        t = self.now
        while t < self.now + 6 * 3600:
            self.tick(t)
            t += 300
        self.assertEqual(len(self.w.starts), 1)            # no churn
        self.assertEqual(self.plan(sid)[0], "AWAITING_AUTH")
        self.w.mode, self.w.record_ok = "start", True       # the grant actually turned on
        self.tick(t)
        self.tick(t + 30)
        self.assertEqual(len(self.w.starts), 2)
        self.assertTrue(any("now authorized" in n for n in self.notices()))

    def test_terminal_outcome_after_launch_survives_to_plan(self):
        for kind, want in (("restricted", "BLOCKED"), ("challenge", "BLOCKED"),
                           ("blocked", "BLOCKED"), ("quality", "BLOCKED"),
                           ("invalid", "BLOCKED"), ("unauthorized", "AWAITING_AUTH")):
            self.setUp()
            sid, _, _ = self.day()
            self.tick(self.now)
            aid = self.w.starts[0][1]
            self.w.confirmed.add(aid)
            self.tick(self.now + 30)
            self.w.terminal[aid] = kind
            self.w.finish(aid, "RuntimeError: playback refused")
            self.tick(self.now + 60)
            self.assertEqual(self.plan(sid)[0], want, kind)
            for k in range(1, 8):
                self.tick(self.now + 60 + 120 * k)
            self.assertEqual(len(self.w.starts), 1, kind)  # no fresh session after 60 s

    def test_ordinary_early_end_still_retries(self):
        sid, _, _ = self.day()
        self.tick(self.now)
        aid = self.w.starts[0][1]
        self.w.finish(aid)
        self.tick(self.now + 30)
        self.tick(self.now + 200)
        self.assertEqual(len(self.w.starts), 2)


class RestartAndNoticeTests(Base):
    def test_restart_interrupts_attempt_keeps_day_eligible_and_notifies(self):
        sid, t0, t1 = self.day()
        self.tick(self.now)
        self.assertEqual(sched.reconcile(self.d, now=self.now + 50), 1)
        self.assertEqual(self.attempts(sid)[0][1], "INTERRUPTED")
        self.assertEqual(self.plan(sid)[0], "SCHEDULED")
        self.assertTrue(any("restarted" in n for n in self.notices()))
        self.tick(self.now + 60)
        self.assertEqual(len(self.w.starts), 2)
        # past the end: a restart finishes it, never relaunches
        sid2 = sched.add(self.d, "ishowspeed", t0, None, now=self.now, day_end=t1)
        self.tick(self.now + 70)
        sched.reconcile(self.d, now=t1 + 5)
        self.assertEqual(self.plan(sid2)[0], "MISSED")

    def test_restart_with_live_writer_holds_until_verified(self):
        sid, t0, t1 = self.day()
        self.tick(self.now)
        self.w.s.clear()                                   # process restarted: registry is empty
        n = sched.reconcile(self.d, now=self.now + 50, writer_active=lambda u: True)
        self.assertEqual(n, 1)
        self.assertEqual(self.attempts(sid)[0][1], "UNRESOLVED")
        self.assertEqual(self.plan(sid)[0], "UNCERTAIN")
        self.w.writer = True
        for k in range(1, 6):
            self.tick(self.now + 100 * k)
        self.assertEqual(len(self.w.starts), 1)
        self.w.writer = False
        self.tick(self.now + 700)
        self.assertEqual(self.attempts(sid)[0][1], "INTERRUPTED")
        self.assertEqual(self.plan(sid)[0], "SCHEDULED")
        self.tick(self.now + 800)
        self.assertEqual(len(self.w.starts), 2)

    def test_state_and_notice_commit_together(self):
        sid, _, _ = self.day()
        with mock.patch.object(sched, "_notice", side_effect=RuntimeError("db")):
            with self.assertRaises(RuntimeError):
                self.tick(self.now)       # not_started/started path writes a notice
        # nothing half-written: the plan did not move on without its notice
        self.assertEqual(self.notices(), [])
        st = self.attempts(sid)
        self.assertTrue(all(s[1] == "INTENT" for s in st))   # intent was durable, outcome rolled back
        self.assertEqual(len(self.w.starts), 1)
        self.tick(self.now + 30)                              # the live session is adopted, not relaunched
        self.assertEqual(self.attempts(sid)[0][1], "LAUNCHED")
        self.assertEqual(len(self.w.starts), 1)

    def test_notice_send_crash_is_labelled_possible_repeat(self):
        sid, _, _ = self.day()
        self.tick(self.now)
        nid, text = sched.claim_notice(self.d)
        self.assertNotIn("possible repeat", text)
        self.assertEqual(sched.claim_notice(self.d), None)    # only one in flight
        # crash before mark_sent: after restart it is resent, labelled
        sched.reconcile(self.d, now=self.now + 10)
        got = sched.claim_notice(self.d)
        self.assertEqual(got[0], nid)
        self.assertIn("(possible repeat)", got[1])
        sched.mark_notice_sent(self.d, nid)
        self.assertNotEqual(sched.claim_notice(self.d), None)  # the restart notice, once
        # failed send is released and labelled
        sched.release_notice(self.d, nid + 1)
        self.assertIn("possible repeat", sched.claim_notice(self.d)[1])

    def test_startup_notices_survive_until_admin_exists(self):
        self.day()
        self.tick(self.now)
        sched.reconcile(self.d, now=self.now + 5)
        self.assertGreaterEqual(len(sched.pending_notices(self.d)), 2)


class ConfirmTests(Base):
    def _media(self, name, ok=True):
        p = Path(tempfile.mkdtemp()) / name
        if ok:
            r = subprocess.run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                                "testsrc=size=160x120:rate=10", "-t", "2", "-pix_fmt",
                                "yuv420p", str(p)], capture_output=True)
            assert r.returncode == 0
        else:
            p.write_bytes(b"not media at all" * 50)
        return str(p)

    def _session(self, user, attempt, seg_path, state="RECORDING"):
        c = sqlite3.connect(str(self.d / "nova.db"))
        try:
            sid = c.execute("SELECT id FROM twitch_sources WHERE username=?", (user,)).fetchone()[0]
            notes = json.dumps({"mode": "feed", "sched_attempt": attempt}) if attempt else "mode A session"
            cur = c.execute("INSERT INTO stream_sessions (source_id,state,started_at,notes) "
                            "VALUES (?,?,?,?)", (sid, state, 1.0, notes))
            if seg_path:
                c.execute("INSERT INTO recording_segments (session_id,file_path,sequence,created_at)"
                          " VALUES (?,?,?,?)", (cur.lastrowid, seg_path, 0, 1.0))
            c.commit()
        finally:
            c.close()

    def conf(self, attempt):
        return sched.confirm_recording(self.d, attempt,
                                       lambda p: _valid(p))

    def test_only_own_session_with_real_valid_media_confirms(self):
        good, bad = self._media("a.mp4"), self._media("b.mp4", ok=False)
        aid = "a" * 32
        self.assertFalse(self.conf(aid))                              # no session row
        self._session("kaicenat", None, good, state="ENDED")          # unrelated ended session
        self.assertFalse(self.conf(aid))
        self._session("kaicenat", aid, "/nonexistent/seg.mp4")
        self.assertFalse(self.conf(aid))                              # missing file
        bid = "b" * 32
        self._session("kaicenat", bid, bad)
        self.assertFalse(self.conf(bid))                              # unreadable file
        cid = "c" * 32
        self._session("kaicenat", cid, good)
        self.assertTrue(self.conf(cid))                               # real, probe-valid
        self.assertFalse(self.conf(aid))                              # another attempt still no

    def test_tick_uses_confirm_for_recording_state(self):
        sid, _, _ = self.day()
        self.tick(self.now)
        aid = self.w.starts[0][1]
        self.tick(self.now + 30)
        self.assertEqual(self.attempts(sid)[0][1], "LAUNCHED")
        self.w.confirmed.add(aid)
        self.tick(self.now + 60)
        self.assertEqual(self.attempts(sid)[0][1], "RECORDING")


def _valid(p):
    try:
        validate_recording("ffprobe", p)
        return True
    except Exception:
        return False


class DstBudgetTests(Base):
    def _run_day(self, day):
        t0, t1 = sched.parse_day(day, tzname="America/Los_Angeles")
        self.now = t0 - 10
        sid = sched.add(self.d, "kaicenat", t0, None, now=self.now, day_end=t1)
        self.w.mode = "not_started"
        t = t0
        while t < t1 + 120:
            self.tick(t)
            t += 30
        return sid, t0, t1, len(self.w.starts)

    def test_25h_and_23h_days_use_end_at_not_attempt_count(self):
        sid, t0, t1, n25 = self._run_day("2026-11-01")
        self.assertEqual((t1 - t0) / 3600, 25)
        self.assertEqual(self.plan(sid)[0], "MISSED")
        self.assertEqual(len(self.w.starts), n25)
        self.setUp()
        sid, t0, t1, n23 = self._run_day("2026-03-08")
        self.assertEqual((t1 - t0) / 3600, 23)
        self.assertEqual(self.plan(sid)[0], "MISSED")
        self.assertGreater(n25, n23)                    # more hours, more tries: end_at bound

    def test_parse_and_dst_validation(self):
        with self.assertRaises(ValueError):
            sched.parse_when("2026-03-08 02:30", tzname="America/Los_Angeles")   # gap
        with self.assertRaises(ValueError):
            sched.parse_when("2026-11-01 01:30", tzname="America/Los_Angeles")   # fold
        self.assertEqual(datetime.fromtimestamp(sched.parse_when("2026-11-21 10:00", tzname="America/Los_Angeles"), LA).hour, 10)


class CommandTests(Base):
    def test_commands(self):
        self.assertIn("NOT set", sc.schedule_reply(self.d, "kaicenat 2099-01-01"))
        self.assertIn("set to", sc.timezone_reply(self.d, "America/Los_Angeles"))
        self.assertIn("Unknown", sc.timezone_reply(self.d, "Mars/Base"))
        u = sc.schedule_reply(self.d, "kaicenat 2099-01-01 10:00")
        self.assertIn("No time or duration is ever assumed", u)
        self.assertIn("Not scheduled", sc.schedule_reply(self.d, "kaicenat 2020-01-01"))
        r = sc.schedule_reply(self.d, "kaicenat 2099-01-01")
        self.assertIn("Scheduled #1", r)
        self.assertNotIn("/setfeed", r)
        self.assertIn("records nothing", r)
        self.assertIn("Scheduled #2", sc.schedule_reply(self.d, "kaicenat 2099-01-02 10:00 3"))
        out = sc.schedules_reply(self.d)
        self.assertIn("whole day", out); self.assertIn("3h", out)
        self.assertIn("cancelled", sc.unschedule_reply(self.d, "1"))
        self.assertIn("No open schedule", sc.unschedule_reply(self.d, "1"))
        self.assertIn("Usage", sc.unschedule_reply(self.d, "x"))


if __name__ == "__main__":
    unittest.main()
