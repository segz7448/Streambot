"""Schedule commands, the schedule loop and /stopclip through the real
BotService / sessions module: admin auth, router, durable outbox, shutdown
cancellation. Boundary: no Telegram network transport; the PTB application
object is built but no update is fetched."""
import asyncio
import sqlite3
import threading
import time
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from tests.unit.test_pair_handler import _Msg
from app.bot.pairing import PairingStore
from app.bot.service import BotService
from app.bot.watermark import UpdateWatermark
from app.bot import sessions as ss
from app.core import schedule as sched
from app.core.database import migrate


def _update(update_id, text, user_id):
    m = _Msg()
    m.text = text
    return types.SimpleNamespace(update_id=update_id,
                                 effective_user=types.SimpleNamespace(id=user_id), message=m)


class _Fake:
    def __init__(self, tok):
        self._sched_token = tok
        self._stopping = threading.Event()
        self.error = None
        self.n = 0

    def alive(self):
        return True

    def stop(self):
        self.n += 1
        self._stopping.set()


class Base(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        from app.core import content_gate as _cg
        import time as _t
        _cg.set_provider((lambda u: ('GTA 6 live', _t.time())) if getattr(self, 'qualified', True) else None)
        self.addCleanup(_cg.set_provider, None)
        self.dir = Path(self.td.name)
        env = mock.patch.dict("os.environ", {"NOVA_STATE_DIR": str(self.dir)})
        env.start(); self.addCleanup(env.stop)
        c = sqlite3.connect(str(self.dir / "nova.db")); migrate(c); c.close()
        self.store = PairingStore(str(self.dir / "admin.json"))
        self.store.begin_challenge("abcdefgh")
        self.svc = BotService("token", self.store,
                              watermark=UpdateWatermark(str(self.dir / "updates.json")))
        asyncio.run(self.svc._pair(_update(1, "/pair abcdefgh", 12345),
                                   types.SimpleNamespace(args=["abcdefgh"])))
        self.n = 10
        self.addCleanup(self.td.cleanup)
        ss._sessions.clear(); self.addCleanup(ss._sessions.clear)

    def cmd(self, text, user_id=12345):
        self.n += 1
        u = _update(self.n, text, user_id)
        asyncio.run(self.svc._command(u, types.SimpleNamespace(args=[])))
        return u.message.replies

    def rows(self, sql, args=()):
        c = sqlite3.connect(str(self.dir / "nova.db"))
        try:
            return c.execute(sql, args).fetchall()
        finally:
            c.close()


class CommandTests(Base):
    def test_commands_through_service_no_invented_time(self):
        self.cmd("/addsource ishowspeed")
        self.assertIn("NOT set", " ".join(self.cmd("/schedule ishowspeed 2099-01-01")))
        self.assertIn("set to", " ".join(self.cmd("/timezone America/Los_Angeles")))
        r3 = " ".join(self.cmd("/schedule ishowspeed 2099-01-01 10:00"))
        self.assertIn("No time or duration is ever assumed", r3)
        self.assertEqual(sched.list_open(self.dir), [])
        r = " ".join(self.cmd("/schedule ishowspeed 2099-01-01"))
        self.assertIn("Scheduled #1", r); self.assertIn("records nothing", r)
        self.assertIn("whole day", " ".join(self.cmd("/schedules")))
        self.assertIn("cancelled", " ".join(self.cmd("/unschedule 1")))

    def test_non_admin_no_reply_no_row(self):
        self.cmd("/addsource ishowspeed"); self.cmd("/timezone America/Los_Angeles")
        self.assertEqual(self.cmd("/schedule ishowspeed 2099-01-01", user_id=999), [])
        self.assertEqual(sched.list_open(self.dir), [])


class LoopTests(Base):
    def _plan(self):
        self.cmd("/addsource ishowspeed")
        now = time.time()
        return sched.add(self.dir, "ishowspeed", now - 100, None, now=now - 200,
                         day_end=now + 86400)

    def _run_loop_once(self, send):
        if getattr(self, "qualified", True):
            from app.core import content_gate as _cg; _cg.set_provider(lambda u: ("GTA 6 live", time.time()))
        self.svc.app = types.SimpleNamespace(bot=types.SimpleNamespace(send_message=send))

        async def stop(_s):
            raise asyncio.CancelledError
        with mock.patch("app.bot.service.asyncio.sleep", stop), \
                mock.patch("app.bot.media.ffmpeg_paths", return_value=("ff", "fp")):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(self.svc._schedule_loop())

    def test_unauthorized_day_mark_starts_nothing_and_notifies_once(self):
        self._plan()
        sent = []

        async def send(chat, text):
            sent.append((chat, text))
        self._run_loop_once(send)
        self._run_loop_once(send)
        self.assertEqual(ss._sessions, {})
        self.assertEqual(len(sent), 1)                      # one message, not one per try
        self.assertEqual(sent[0][0], 12345)
        self.assertIn("not authorized", sent[0][1])
        self.assertEqual(sched.pending_notices(self.dir), [])
        self.assertEqual([r[0] for r in self.rows("SELECT state FROM schedule_attempts")], ["NOT_STARTED"])

    def test_send_failure_keeps_notice_and_labels_repeat(self):
        self._plan()

        async def bad(chat, text):
            raise RuntimeError("net")
        self._run_loop_once(bad)
        self.assertEqual(len(sched.pending_notices(self.dir)), 1)
        sent = []

        async def ok(chat, text):
            sent.append(text)
        self._run_loop_once(ok)
        self.assertEqual(len(sent), 1)
        self.assertTrue(sent[0].startswith("(possible repeat)"))

    def test_cancel_during_slow_tick_waits_for_the_thread(self):
        self._plan()
        started, release, done = threading.Event(), threading.Event(), threading.Event()
        order = []

        def slow_tick(sd, ops):
            started.set(); release.wait(5); done.set()

        async def main():
            self.svc.app = types.SimpleNamespace(bot=types.SimpleNamespace(
                send_message=lambda *a: None))
            t = asyncio.create_task(self.svc._schedule_loop())
            while not started.is_set():
                await asyncio.sleep(0.01)
            t.cancel()
            asyncio.get_running_loop().call_later(0.2, release.set)
            try:
                await t
            except asyncio.CancelledError:
                order.append(("cancelled", done.is_set()))
        with mock.patch.object(sched, "tick", slow_tick), \
                mock.patch("app.bot.media.ffmpeg_paths", return_value=("ff", "fp")):
            asyncio.run(main())
        self.assertEqual(order, [("cancelled", True)])       # thread finished before shutdown returned

    def test_cancel_during_send_leaves_sending_then_repeat_after_restart(self):
        self._plan()
        gate = asyncio.Event

        async def hang(chat, text):
            await gate().wait()
        self.svc.app = types.SimpleNamespace(bot=types.SimpleNamespace(send_message=hang))

        async def main():
            with mock.patch("app.bot.media.ffmpeg_paths", return_value=("ff", "fp")):
                t = asyncio.create_task(self.svc._schedule_loop())
                for _ in range(300):
                    await asyncio.sleep(0.02)
                    if self.rows("SELECT 1 FROM schedule_notices WHERE sending_at IS NOT NULL"):
                        break
                t.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await t
        asyncio.run(main())
        self.assertEqual(len(self.rows("SELECT 1 FROM schedule_notices WHERE sending_at IS NOT NULL AND sent_at IS NULL")), 1)
        sched.reconcile(self.dir)
        got = sched.claim_notice(self.dir)
        self.assertTrue(got[1].startswith("(possible repeat)"))


class StopclipHookTests(Base):
    def test_stopclip_cancels_the_schedule_of_that_session(self):
        self.cmd("/addsource ishowspeed")
        now = time.time()
        sid = sched.add(self.dir, "ishowspeed", now - 100, None, now=now - 200,
                        day_end=now + 86400)
        seen = {}
        ops = sched.Ops(
            start=lambda u, a: (seen.setdefault("aid", a), ss._sessions.__setitem__(u, _Fake(a)),
                                ("started", "ok"))[2],
            stop=ss.stop_session, running=ss.session_running, alive=ss.session_alive,
            stopping=ss.session_stopping, confirm=lambda a: False, error=ss.session_error,
            content_qualified=lambda u: True)
        sched.tick(self.dir, ops)
        self.assertEqual(self.rows("SELECT state FROM clip_schedules")[0][0], "ACTIVE")
        reply = ss.stopclip_reply(self.dir)
        self.assertIn("Stopping 1", reply)
        self.assertEqual(self.rows("SELECT state FROM clip_schedules")[0][0], "CANCELLED")
        # manual session (no attempt id) is stopped without touching schedules
        ss._sessions["x"] = _Fake(None)
        self.assertIn("Stopping 1", ss.stopclip_reply(self.dir))


class BindingTests(Base):
    def test_attempt_id_is_stamped_before_launch_on_object_and_durable_notes(self):
        import json
        from app.pipeline.session import ClipSession
        (self.dir / "feeds").mkdir()
        (self.dir / "feeds" / "ishowspeed.url").write_text("https://relay.example/x.m3u8")
        self.cmd("/addsource ishowspeed")
        order = []

        class F(_Fake):
            def __init__(self, *a, **k):
                super().__init__(None)

            def start(self):
                order.append(("start", self.attempt_id, self._sched_token))
        with mock.patch.object(ss, "validate_feed", lambda f, d: f):
            ss._tl.attempt, ss._tl.started = "a" * 32, False
            try:
                reply = ss.startclip_reply(self.dir, "ishowspeed", "ff", "fp", session_factory=F)
                started = ss._tl.started
            finally:
                ss._tl.attempt, ss._tl.started = None, False
        self.assertIn("started", reply)
        self.assertTrue(started)
        self.assertEqual(order, [("start", "a" * 32, "a" * 32)])   # stamped BEFORE start()
        self.assertTrue(ss.session_alive("ishowspeed", "a" * 32))
        self.assertFalse(ss.session_alive("ishowspeed", "b" * 32))
        # the durable session row carries the attempt for BOTH ingest modes
        cs = ClipSession("ff", "fp", "x", "ishowspeed", self.dir)
        self.assertEqual(cs._notes(1.0), "mode A session")
        cs.attempt_id = "c" * 32
        self.assertEqual(json.loads(cs._notes(1.0))["sched_attempt"], "c" * 32)
        pb = ClipSession("ff", "fp", "", "ishowspeed", self.dir, playback={})
        pb.attempt_id = "d" * 32
        self.assertEqual(json.loads(pb._notes(1.0))["sched_attempt"], "d" * 32)

    def test_not_started_when_authorization_off(self):
        self.cmd("/addsource ishowspeed")
        kind, reply = ss.start_scheduled(self.dir, "ishowspeed", "ff", "fp", "e" * 32)
        self.assertEqual(kind, "unauthorized")
        self.assertIn("not authorized", reply)
        self.assertEqual(ss._sessions, {})


class OwnershipRegistryTests(Base):
    def _tl(self, aid):
        ss._tl.attempt, ss._tl.started, ss._tl.refusal = aid, False, None
        self.addCleanup(lambda: setattr(ss._tl, "attempt", None))

    def test_registered_before_start_and_raise_policy(self):
        self.cmd("/addsource ishowspeed")
        (self.dir / "feeds").mkdir()
        (self.dir / "feeds" / "ishowspeed.url").write_text("https://relay.example/x.m3u8")
        seen = {}

        class Dead(_Fake):
            def __init__(self, *a, **k):
                super().__init__(None)

            def start(self):
                seen["registered"] = ss._sessions.get("ishowspeed") is self
                raise RuntimeError("thread start failed")

            def alive(self):
                return False
        self._tl("a" * 32)
        with mock.patch.object(ss, "validate_feed", lambda f, d: f):
            with self.assertRaises(RuntimeError):
                ss.startclip_reply(self.dir, "ishowspeed", "ff", "fp", session_factory=Dead)
        self.assertTrue(seen["registered"])                  # tracked BEFORE the effect
        self.assertNotIn("ishowspeed", ss._sessions)         # dead start holds nothing

        class Live(Dead):
            def start(self):
                seen["live_registered"] = ss._sessions.get("ishowspeed") is self
                raise RuntimeError("raised after the thread started")

            def alive(self):
                return True
        self._tl("b" * 32)
        with mock.patch.object(ss, "validate_feed", lambda f, d: f):
            with self.assertRaises(RuntimeError):
                ss.startclip_reply(self.dir, "ishowspeed", "ff", "fp", session_factory=Live)
        self.assertTrue(seen["live_registered"])
        self.assertTrue(ss.session_running("ishowspeed", "b" * 32))   # adoptable, still owned
        self.assertFalse(ss._tl.started)

    def test_stopclip_during_reservation_cancels_and_does_not_retry(self):
        from app.bot import source_auth
        self.cmd("/addsource ishowspeed")
        source_auth.set_scope(self.dir, "ishowspeed", "record", True)
        now = time.time()
        sid = sched.add(self.dir, "ishowspeed", now - 100, None, now=now - 200,
                        day_end=now + 86400)
        calls = []

        def resolve(username, authorized, should_stop=None, **k):
            calls.append(username)
            ss.stopclip_reply(self.dir)          # /stopclip arrives during preflight
            return {"x": 1}
        ops = sched.Ops(
            start=lambda u, a: ss.start_scheduled(self.dir, u, "ff", "fp", a, resolve_fn=resolve),
            stop=ss.stop_session, running=ss.session_running, alive=ss.session_alive,
            stopping=ss.session_stopping, confirm=lambda a: False, error=ss.session_error,
            terminal=ss.session_terminal, content_qualified=lambda u: True)
        sched.tick(self.dir, ops, now=now)
        self.assertEqual(self.rows("SELECT state FROM clip_schedules")[0][0], "CANCELLED")
        for k in range(1, 6):
            sched.tick(self.dir, ops, now=now + 400 * k)
        self.assertEqual(len(calls), 1)          # no second resolve
        self.assertEqual(ss._sessions, {})

    def test_worker_and_session_expose_typed_terminal(self):
        from app.recording.playback_worker import PlaybackWorker
        from app.pipeline.session import ClipSession
        w = PlaybackWorker.__new__(PlaybackWorker)
        w.captured_segments = 0
        for kind in ("restricted", "challenge", "blocked", "unauthorized", "quality", "invalid"):
            w.terminal_kind = None
            with self.assertRaises(RuntimeError):
                w._refused(kind)
            self.assertEqual(w.terminal_kind, kind)
        w.terminal_kind = None
        with self.assertRaises(RuntimeError):
            w._refused("offline")
        self.assertIsNone(w.terminal_kind)               # offline stays retryable
        cs = ClipSession("ff", "fp", "", "ishowspeed", self.dir, playback={})
        self.assertIsNone(cs.terminal_kind)
        cs._worker = types.SimpleNamespace(terminal_kind=None, end_reason="revoked")
        self.assertEqual(cs.terminal_kind, "unauthorized")
        cs._worker = types.SimpleNamespace(terminal_kind="challenge", end_reason="failed")
        self.assertEqual(cs.terminal_kind, "challenge")
        ss._sessions["ishowspeed"] = cs
        cs._sched_token = "t" * 32
        self.assertEqual(ss.session_terminal("ishowspeed", "t" * 32), "challenge")
        self.assertEqual(ss.session_terminal("ishowspeed", "u" * 32), "")


class TerminationProofTests(Base):
    def test_only_drained_session_is_proof(self):
        import json
        from app.core.database import connect
        c = connect(str(self.dir / "nova.db"))
        src = c.execute("SELECT id FROM twitch_sources LIMIT 1").fetchone()
        if src is None:
            c.execute("INSERT INTO twitch_sources (username,created_at,updated_at) VALUES ('ishowspeed',0,0)")
            src = c.execute("SELECT id FROM twitch_sources LIMIT 1").fetchone()
        aid = "c" * 32
        note = lambda **k: json.dumps({"mode": "feed", "sched_attempt": aid, **k})
        c.execute("INSERT INTO stream_sessions (source_id,state,notes) VALUES (?,?,?)",
                  (src[0], "INTERRUPTED", note()))
        c.commit()
        self.assertFalse(ss.session_terminated(self.dir, "ishowspeed", aid))   # crash marker is no proof
        c.execute("UPDATE stream_sessions SET notes=?", (note(drained=True),))
        c.commit(); c.close()
        self.assertTrue(ss.session_terminated(self.dir, "ishowspeed", aid))
        self.assertFalse(ss.session_terminated(self.dir, "ishowspeed", "d" * 32))
        # silence and an empty process scan are not proof either
        self.assertFalse(ss.session_terminated(self.dir, "ishowspeed", "e" * 32))


class WriterActiveTests(Base):
    def test_unverifiable_inspection_fails_closed(self):
        work = self.dir / "sessions" / "ishowspeed"
        work.mkdir(parents=True)
        with mock.patch("os.listdir", side_effect=OSError("no proc")):
            self.assertTrue(ss.writer_active(self.dir, "ishowspeed"))
        real_open = open

        def deny(path, *a, **k):
            if str(path).startswith("/proc/") and str(path).endswith("/cmdline"):
                raise PermissionError("denied")
            return real_open(path, *a, **k)
        with mock.patch("builtins.open", deny):
            self.assertTrue(ss.writer_active(self.dir, "ishowspeed"))

    def test_recent_file_and_orphan_process_both_count(self):
        import os, subprocess, sys
        work = self.dir / "sessions" / "ishowspeed"
        work.mkdir(parents=True)
        self.assertFalse(ss.writer_active(self.dir, "ishowspeed"))
        f = work / "seg.ts"
        f.write_bytes(b"x")
        self.assertTrue(ss.writer_active(self.dir, "ishowspeed"))
        old = time.time() - 600
        os.utime(f, (old, old))
        self.assertFalse(ss.writer_active(self.dir, "ishowspeed"))
        p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)", str(work / "out.ts")])
        try:
            time.sleep(0.3)
            self.assertTrue(ss.writer_active(self.dir, "ishowspeed"))    # orphan child by cmdline
        finally:
            p.kill(); p.wait()
        self.assertFalse(ss.writer_active(self.dir, "ishowspeed"))


class ContentGateServiceTests(LoopTests):
    """The real service has no GTA6 verifier: automatic capture never starts."""
    qualified = False

    def test_default_service_gate_is_closed(self):
        self._plan()
        sent = []

        async def send(chat, text):
            sent.append((chat, text))
        self._run_loop_once(send)
        self._run_loop_once(send)
        self.assertEqual(ss._sessions, {})
        self.assertEqual(self.rows("SELECT state FROM clip_schedules")[0][0],
                         "AWAITING_QUALIFICATION")
        self.assertEqual(len(sent), 1)
        self.assertIn("GTA6", sent[0][1])

    test_unauthorized_day_mark_starts_nothing_and_notifies_once = None


if __name__ == "__main__":
    unittest.main()
