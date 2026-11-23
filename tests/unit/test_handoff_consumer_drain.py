"""run_consumer cancellation-drain: a cancel while a cycle's worker
thread is in flight must not return until the worker has finished;
normal cycles and failure cycles keep their prior behavior."""
import asyncio
import threading
import unittest
from unittest import mock

from app.bot import handoff


class RunConsumerDrainTests(unittest.IsolatedAsyncioTestCase):

    async def _drain(self, task, release):
        # deterministic cleanup: always release a held worker, cancel the
        # consumer if still running, observe completion within a bound,
        # consume the result, and fail if anything is left pending
        release.set()
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            done, pending = await asyncio.wait([task], timeout=5)
            if pending:
                self.fail("consumer task still pending after drain")
            for t in done:
                try:
                    t.result()
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

    async def test_cancel_drains_inflight_cycle(self):
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def slow_run_once(state_dir, *, ffprobe="ffprobe"):
            started.set()
            release.wait(10)
            finished.set()
            return handoff.IDLE

        task = None
        try:
            with mock.patch.object(handoff, "run_once", slow_run_once):
                task = asyncio.create_task(
                    handoff.run_consumer("/nonexistent"))
                await asyncio.to_thread(started.wait, 5)
                self.assertTrue(started.is_set())
                task.cancel()
                await asyncio.sleep(0.1)
                # cancellation must NOT have returned while the worker runs
                self.assertFalse(task.done())
                self.assertFalse(finished.is_set())
                # a repeated cancel while held changes nothing: the same
                # shielded future is still being drained
                task.cancel()
                await asyncio.sleep(0.1)
                self.assertFalse(task.done())
                self.assertFalse(finished.is_set())
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                # the worker genuinely completed before cancellation surfaced
                self.assertTrue(finished.is_set())
        finally:
            await self._drain(task, release)

    async def test_normal_cycles_then_cancel_during_sleep(self):
        cycles = []

        def quick_run_once(state_dir, *, ffprobe="ffprobe"):
            cycles.append(1)
            return handoff.IDLE

        task = None
        try:
            with mock.patch.object(handoff, "run_once", quick_run_once):
                task = asyncio.create_task(
                    handoff.run_consumer("/nonexistent", poll_interval=0.05))
                await asyncio.sleep(0.2)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertGreaterEqual(len(cycles), 1)
        finally:
            await self._drain(task, threading.Event())

    async def test_cycle_failure_keeps_idle_behavior(self):
        calls = []

        def failing_run_once(state_dir, *, ffprobe="ffprobe"):
            calls.append(1)
            raise RuntimeError("transient")

        task = None
        try:
            with mock.patch.object(handoff, "run_once", failing_run_once):
                with self.assertLogs(handoff.log, level="ERROR"):
                    task = asyncio.create_task(
                        handoff.run_consumer("/nonexistent",
                                             poll_interval=0.05))
                    await asyncio.sleep(0.2)
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
            # the loop survived the failure and kept cycling until cancelled
            self.assertGreaterEqual(len(calls), 1)
        finally:
            await self._drain(task, threading.Event())

    async def test_cancel_after_cycle_error_still_raises_cancelled(self):
        started = threading.Event()
        release = threading.Event()

        def failing_slow(state_dir, *, ffprobe="ffprobe"):
            started.set()
            release.wait(10)
            raise RuntimeError("cycle blew up")

        task = None
        try:
            with mock.patch.object(handoff, "run_once", failing_slow):
                task = asyncio.create_task(
                    handoff.run_consumer("/nonexistent"))
                # barrier: the intended worker is actually in flight
                await asyncio.to_thread(started.wait, 5)
                self.assertTrue(started.is_set())
                task.cancel()
                release.set()
                # a remembered cancellation wins over the retained cycle error
                with self.assertRaises(asyncio.CancelledError):
                    await task
        finally:
            await self._drain(task, release)


if __name__ == "__main__":
    unittest.main()
