"""Schedule-loop composition: the REAL BotService._schedule_loop -
its actual Ops construction, tick invocation, notice claim/send/mark
cycle - drives intended scheduled starts and both owner-cancel paths
under local adapters. Only the session/network boundary
(app.bot.sessions functions), the GTA6 title gate, and the Telegram
notice send are faked; core.schedule.tick, the Ops wiring, the
scheduler DB and the notice pipeline are production code.
test_schedule_controls exercises sched.tick with a hand-built Ops;
this file proves the service wires that Ops correctly. No network,
no real session, no real send.

Two truthful cancel shapes, matching production behaviour exactly:
- /unschedule of a running plan (sched.cancel -> "cancelled_running"):
  tick stops the exact session (stop_by='cancel') and the SYNCHRONOUS
  reply is the owner's settlement message - by design no async notice
  is generated on this path (verified against app.core.schedule).
- /stopclip observed out-of-band (session already stopping without a
  recorded stop_by): tick settles the plan CANCELLED and generates the
  "cancelled because its session was stopped" notice, which must be
  delivered AND durably marked sent (sent_at) in the notice DB.

Determinism, per review:
- The REAL BotService._blocking is wrapped by a TEST-ONLY tracked
  delegator that counts active calls and exposes a completion event.
  Production _blocking only returns after its thread finished (even
  under cancellation, via the shield), so active==0 means no owned
  task/thread work is pending. The production shield is never changed.
- The wake Event is a tracked subclass: the "idle point" is the loop
  PARKED in its wake-wait with active blocking count 0, not a delay.
- Cancellation uses asyncio.wait (a real bound: it never waits for a
  shielded task past its timeout). A task still pending after the
  bound FAILS with the stack and retained state; any held fixture
  worker is released and the actual drain is awaited BEFORE any
  cleanup may delete the disposable state. If a task is truly stuck,
  the state dir is retained for inspection instead of being deleted
  under a live owned thread.
- A deliberately held tick proves the harness observes deferred
  cancellation (pending while the worker is held) and only settles
  after the worker is released - the timeout diagnostic cannot
  silently pass.
- Offline PTB request objects are shut down explicitly.

Runs under the repo's -W error::ResourceWarning gate like every other
suite.
"""
import asyncio
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from tests.unit.test_media_wiring import HAS_PTB, _service


def _close_ptb_requests(bot):
    """Shut the offline Application's request objects down explicitly:
    they hold httpx clients whose sockets would otherwise linger past
    the test (unclosed-resource warnings are a fixture defect here,
    not production behaviour)."""
    reqs = getattr(bot, "_request", None)
    if not reqs:
        return
    if not isinstance(reqs, (tuple, list)):
        reqs = (reqs,)

    async def close_all():
        for r in reqs:
            shutdown = getattr(r, "shutdown", None)
            if shutdown is not None:
                await shutdown()

    asyncio.run(close_all())


class _TrackedWake(asyncio.Event):
    """A notice-wake Event that reports whether the loop is parked in
    its wake-wait right now (the known idle point)."""

    def __init__(self):
        super().__init__()
        self.waiting = False

    async def wait(self):
        self.waiting = True
        try:
            return await super().wait()
        finally:
            self.waiting = False


@unittest.skipUnless(HAS_PTB, "python-telegram-bot not installed")
class ScheduleLoopOpsTests(unittest.TestCase):
    def setUp(self):
        self._retain_state = False
        self.d = Path(tempfile.mkdtemp())
        self.addCleanup(self._cleanup_state)
        conn = sqlite3.connect(str(self.d / "nova.db"))
        from app.core.database import migrate
        migrate(conn)
        conn.close()
        self.svc = _service(self.d)
        self.addCleanup(_close_ptb_requests, self.svc.app.bot)
        from app.bot import source_auth, commands_full as cf
        cf.addsource_reply(self.d, "testchan")
        source_auth.set_scope(self.d, "testchan", "record", True)
        # world: the session registry stand-in behind the real seams.
        # self.state models the session's drained/stopping attestation
        # ONLY; it is not a live worker and proves nothing about real
        # capture.
        self.starts, self.stops = [], []
        self.state = {}  # aid -> dict(running, stopping)
        self.terminal = {}
        self.sent = []
        self._blocking_active = 0
        self._blocking_done = threading.Event()
        self._blocking_done.set()
        self._patch_sender()

    def _cleanup_state(self):
        # runs LAST (registered first): never delete the disposable DB
        # while an owned thread could still reference it
        if self._retain_state:
            return
        shutil.rmtree(self.d, True)

    def _patch_sender(self):
        async def fake_send(_bot, chat_id, text):
            self.sent.append(text)
        p = mock.patch.object(type(self.svc.app.bot), "send_message",
                              fake_send)
        p.start()
        self.addCleanup(p.stop)

    def _track_blocking(self):
        """Wrap the REAL BotService._blocking with a TEST-ONLY
        delegator: active-call count plus a thread-completion event.
        The real _blocking only returns after its thread finished, so
        active==0 attests no owned thread work is pending."""
        cls = type(self.svc)
        real = cls._blocking  # staticmethod -> plain function
        tracker = self

        async def tracked(fn, *args):
            tracker._blocking_active += 1
            tracker._blocking_done.clear()
            try:
                return await real(fn, *args)
            finally:
                tracker._blocking_active -= 1
                if tracker._blocking_active == 0:
                    tracker._blocking_done.set()

        p = mock.patch.object(cls, "_blocking", staticmethod(tracked))
        p.start()
        self.addCleanup(p.stop)

    def _adapters(self):
        from app.bot import sessions
        from app.core import content_gate

        def start_scheduled(sd, u, ff, fp, aid, resolve_fn=None):
            self.starts.append((u, aid))
            self.state[aid] = dict(running=True, stopping=False)
            return "started", "ok"

        def stop_session(u, aid):
            self.stops.append(aid)
            s = self.state.get(aid)
            if s and not s["stopping"]:
                s["stopping"] = True
                return True
            return False

        def g(aid, k):
            return bool(self.state.get(aid, {}).get(k))

        patches = [
            mock.patch.object(sessions, "start_scheduled", start_scheduled),
            mock.patch.object(sessions, "stop_session", stop_session),
            mock.patch.object(sessions, "session_running",
                              lambda u, a: g(a, "running")),
            mock.patch.object(sessions, "session_alive",
                              lambda u, a: g(a, "running")
                              and not g(a, "stopping")),
            mock.patch.object(sessions, "session_stopping",
                              lambda u, a: g(a, "stopping")),
            mock.patch.object(sessions, "session_error",
                              lambda u, a: ""),
            mock.patch.object(sessions, "writer_active",
                              lambda sd, u: False),
            mock.patch.object(sessions, "session_terminated",
                              lambda sd, u, a: a in self.state
                              and not g(a, "running")),
            mock.patch.object(sessions, "session_terminal",
                              lambda u, a, sd=None: self.terminal.get(a, "")),
            mock.patch.object(content_gate, "qualified", lambda u: True),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def _rows(self, sql, args=()):
        conn = sqlite3.connect(str(self.d / "nova.db"))
        rows = conn.execute(sql, args).fetchall()
        conn.close()
        return rows

    def _notices(self):
        return self._rows(
            "SELECT text, sent_at FROM schedule_notices ORDER BY id")

    def _plan(self, sid):
        return self._rows(
            "SELECT state, note FROM clip_schedules WHERE id=?", (sid,))[0]

    def _attempt(self, aid):
        return self._rows(
            "SELECT state, stop_by, stop_calls FROM schedule_attempts "
            "WHERE id=?", (aid,))[0]

    async def _wait(self, pred, what, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = pred()
            if value:
                return value
            await asyncio.sleep(0.05)
        self.fail("barrier failed: %s; notices=%r active_blocking=%r"
                  % (what, self._notices(), self._blocking_active))

    def _start_loop(self, extra_adapters=None):
        """Install adapters, the tracked _blocking delegator and the
        tracked wake, then start the REAL _schedule_loop task."""
        self._adapters()
        if extra_adapters is not None:
            extra_adapters()
        self._track_blocking()
        self.wake = _TrackedWake()
        self.svc._notice_wake = self.wake
        return asyncio.create_task(self.svc._schedule_loop())

    async def _idle_barrier(self):
        """Known idle point: the loop is PARKED in its wake-wait and no
        _blocking work is active (active==0 means no owned thread is
        running, per the production _blocking contract)."""
        await self._wait(
            lambda: self.wake.waiting and self._blocking_active == 0,
            "loop parked at wake-wait with no active blocking work")

    async def _cancel_and_settle(self, task, bound=5, grace=10):
        """Cancel at the idle point, then observe termination under a
        REAL bound (asyncio.wait never waits past its timeout for a
        shielded task). Pending past the bound FAILS with diagnostics;
        any held fixture worker is released and the actual drain is
        awaited BEFORE cleanup may delete state; a truly stuck task
        retains the state dir instead of deleting it under live work.
        """
        task.cancel()
        done, pending = await asyncio.wait({task}, timeout=bound)
        if pending:
            stack = task.get_stack()
            active = self._blocking_active
            # release any held fixture worker, then await the real drain
            self._release_held()
            done2, pending2 = await asyncio.wait({task}, timeout=grace)
            if pending2:
                self._retain_state = True
                self.fail("loop task still pending %ss after cancel + "
                          "release; state retained at %s; stack=%r "
                          "active_blocking=%r"
                          % (bound + grace, self.d, stack, active))
            # drained only after release: consume the result too, then
            # fail with the recorded diagnostics
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self.fail("loop task outlived the %ss cancel bound "
                      "(cancellation deferred by shielded _blocking); "
                      "drained only after release; stack=%r "
                      "active_blocking=%r" % (bound, stack, active))
        # termination observed: consume the result explicitly
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.fail("loop task raised %r; stack=%r"
                      % (exc, task.get_stack()))
        self.assertTrue(task.done())

    def _release_held(self):
        hold = getattr(self, "tick_hold", None)
        if hold is not None:
            hold.set()

    async def _run_loop(self, phases):
        """Run the real loop, execute the phase callbacks in order,
        then cancel at the known idle point and settle under a bound.
        The cancel/settle is in finally: a failed phase, barrier or
        assertion still releases held work and drains the loop BEFORE
        fixture cleanup may delete the disposable state."""
        task = self._start_loop()
        try:
            for phase in phases:
                await phase()
            await self._idle_barrier()
        finally:
            self._release_held()
            await self._cancel_and_settle(task)

    def _schedule(self):
        from app.core import schedule as sched
        now = time.time()
        return sched, sched.add(self.d, "testchan", now - 30, 3600)

    async def _start_barrier(self):
        await self._wait(lambda: self.starts, "one intended start")
        self.assertEqual(len(self.starts), 1)
        user, aid = self.starts[0]
        self.assertEqual(user, "testchan")
        return aid

    def test_unschedule_running_stops_exact_session_no_relaunch(self):
        sid_holder = {}

        async def phase_start():
            aid = await self._start_barrier()
            sid_holder["aid"] = aid
            from app.core import schedule as sched
            self.assertEqual(sched.cancel(self.d, sid_holder["sid"]),
                             "cancelled_running")
            self.svc._notice_wake.set()

        async def phase_stop():
            await self._wait(lambda: self.stops, "exact session stop")
            self.assertEqual(self.stops, [sid_holder["aid"]])
            # drained attestation model (fixture stand-in): the exact
            # session reports itself finished
            self.state[sid_holder["aid"]]["running"] = False
            self.terminal[sid_holder["aid"]] = "ended"
            self.svc._notice_wake.set()

        async def phase_settle():
            await self._wait(
                lambda: self._attempt(sid_holder["aid"])[0] == "ENDED",
                "attempt settles ENDED")

        sched, sid = self._schedule()
        sid_holder["sid"] = sid
        asyncio.run(self._run_loop([phase_start, phase_stop,
                                    phase_settle]))
        aid = sid_holder["aid"]
        state, stop_by, stop_calls = self._attempt(aid)
        self.assertEqual((state, stop_by, stop_calls), ("ENDED", "cancel", 1))
        self.assertEqual(self._plan(sid)[0], "CANCELLED")
        self.assertEqual(len(self.starts), 1, "no relaunch after cancel")
        self.assertEqual(len(self.stops), 1, "exact-once stop")
        # truthful notice discipline on this path: the synchronous
        # /unschedule reply is the settlement message; the only async
        # notice is the start notice, and it is durably marked sent
        notices = self._notices()
        self.assertEqual(len(notices), 1, notices)
        self.assertIn("started testchan", notices[0][0])
        self.assertIsNotNone(notices[0][1], "start notice must be sent")
        self.assertTrue(any("started testchan" in t for t in self.sent))

    def test_stopclip_observed_settles_with_notice_marked_sent(self):
        sid_holder = {}

        async def phase_start():
            aid = await self._start_barrier()
            sid_holder["aid"] = aid
            # /stopclip path: the session's own hook stopped it; the
            # next tick observes a stopping session with no stop_by
            self.state[aid]["stopping"] = True
            self.svc._notice_wake.set()

        async def phase_settle_notice():
            await self._wait(
                lambda: [r for r in self._notices()
                         if "cancelled because its session was stopped"
                         in r[0] and r[1] is not None],
                "cancel notice durably marked sent")
            self.assertEqual(self._plan(sid_holder["sid"])[0],
                             "CANCELLED")

        async def phase_drained():
            aid = sid_holder["aid"]
            self.state[aid]["running"] = False
            self.terminal[aid] = "ended"
            self.svc._notice_wake.set()
            await self._wait(
                lambda: self._attempt(aid)[0] == "ENDED",
                "attempt settles ENDED")

        sched, sid = self._schedule()
        sid_holder["sid"] = sid
        asyncio.run(self._run_loop(
            [phase_start, phase_settle_notice, phase_drained]))
        aid = sid_holder["aid"]
        state, stop_by, _ = self._attempt(aid)
        self.assertEqual(state, "ENDED")
        self.assertEqual(stop_by, "user")
        self.assertEqual(len(self.starts), 1, "no relaunch after cancel")
        self.assertEqual(self.stops, [aid],
                         "tick asks the exact session to stop once "
                         "(stop_calls tracked, no blind repeats)")
        self.assertTrue(
            any("cancelled because its session was stopped" in t
                for t in self.sent),
            "settlement notice must reach the admin: %r" % self.sent)

    def test_cancel_defers_until_shielded_worker_released(self):
        """Held-tick case: with a tick worker deliberately held inside
        the REAL _blocking thread, cancelling the loop must NOT
        complete while the worker is held (the production shield
        defers it), the harness must OBSERVE the task pending at the
        bound instead of silently passing, and termination must land
        only after the worker is released and actually drained."""
        from app.bot import sessions
        self.tick_hold = threading.Event()
        self.tick_hold.set()  # released until armed
        self.tick_entered = threading.Event()
        self.addCleanup(self._release_held)

        def held_running(u, a):
            self.tick_entered.set()
            self.tick_hold.wait(30)
            return bool(self.state.get(a, {}).get("running"))

        def extra():
            p = mock.patch.object(sessions, "session_running",
                                  held_running)
            p.start()
            self.addCleanup(p.stop)

        sid_holder = {}

        async def run():
            task = self._start_loop(extra_adapters=extra)
            try:
                aid = await self._start_barrier()
                sid_holder["aid"] = aid
                # arm the hold: the NEXT tick's worker blocks inside
                self.tick_entered.clear()
                self.tick_hold.clear()
                self.svc._notice_wake.set()
                # worker is inside the held call, tracked as active
                await self._wait(
                    lambda: self.tick_entered.is_set()
                    and self._blocking_active == 1,
                    "held tick worker active inside _blocking")
                task.cancel()
                # the shield defers cancellation: still pending at 1s
                done, pending = await asyncio.wait({task}, timeout=1.0)
                self.assertEqual(done, set(),
                                 "cancel must not complete while the "
                                 "shielded worker is held")
                self.assertEqual(pending, {task})
                self.assertEqual(self._blocking_active, 1,
                                 "held worker still tracked as active")
                # release the worker; the drain must now complete
                self.tick_hold.set()
                done, pending = await asyncio.wait({task}, timeout=5)
                self.assertEqual(pending, set(),
                                 "released worker must drain within 5s")
                try:
                    task.result()
                except asyncio.CancelledError:
                    pass
                self.assertTrue(task.done())
                self.assertEqual(self._blocking_active, 0)
            finally:
                # a failed assertion above still releases the held
                # worker and explicitly settles the loop BEFORE
                # fixture cleanup, via the bounded diagnostic path
                self._release_held()
                if not task.done():
                    await self._cancel_and_settle(task)

        sched, sid = self._schedule()
        sid_holder["sid"] = sid
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
