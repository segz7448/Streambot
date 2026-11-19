import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.core.database import connect, migrate
from app.core.lifecycle import HealthRegistry, ServiceContext, Service, Supervisor
from app.recording.buffer import RollingBuffer
from app.recording.worker import RecordingWorker


class FakeProc:
    def __init__(self):
        self.pid = 4242
        self.running = True
        self.stopped = False

    async def wait(self):
        return 0, b""

    async def stop(self, grace=5.0):
        self.stopped = True
        self.running = False
        return 0


class FakeRunner:
    def __init__(self, proc):
        self.proc = proc
        self.started = []

    async def run(self, args, session_id, timeout=0.0):
        self.started.append((args, session_id))
        return self.proc

    async def stop_session(self, session_id):
        await self.proc.stop()


class RecordingWorkerTests(unittest.TestCase):
    def test_records_reconciles_and_stops_cleanly(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "t.db")
            conn = connect(db)
            migrate(conn)
            conn.execute(
                "INSERT INTO twitch_sources (username, created_at, updated_at)"
                " VALUES ('chan', 0, 0)")
            conn.execute(
                "INSERT INTO stream_sessions (source_id, state)"
                " VALUES (1, 'RECORDING')")
            buf = RollingBuffer(tmp, session_id=1, segment_seconds=5)
            (buf.dir).mkdir(parents=True, exist_ok=True)
            (buf.dir / "seg-000000.ts").write_bytes(b"x" * 100)

            proc = FakeProc()
            worker = RecordingWorker(FakeRunner(proc), buf, "fake://src", db,
                                     sweep_seconds=0.05)
            registry = HealthRegistry()
            svc = Service("record", worker.run, heartbeat_seconds=0.05)
            sup = Supervisor([svc], registry)

            async def drive():
                task = asyncio.create_task(sup.run())
                await asyncio.sleep(0.2)
                sup.stopping.set()
                await task

            asyncio.run(drive())
            self.assertTrue(proc.stopped)
            rows = conn.execute("SELECT COUNT(*) AS n FROM recording_segments"
                                " WHERE session_id = 1").fetchone()
            self.assertGreaterEqual(rows["n"], 1)


if __name__ == "__main__":
    unittest.main()


class StderrDrainTests(unittest.TestCase):
    """A >64KiB stderr line without a newline must not kill the drain:
    readline() raises LimitOverrunError there; chunk reads survive."""

    def test_huge_line_drain_completes(self):
        import asyncio
        import sys
        from app.recording.ffmpeg import FFmpegRunner

        async def go():
            runner = FFmpegRunner(sys.executable)
            proc = await runner.run(
                ["-c", "import sys; sys.stderr.write('x'*300000)"],
                session_id="drain-test")
            rc, tail = await asyncio.wait_for(proc.wait(), timeout=30)
            return rc, tail

        rc, tail = asyncio.run(go())
        self.assertEqual(rc, 0)
        self.assertGreater(len(tail), 0)
        self.assertLessEqual(len(tail), 100 * 512)


class FakeCtx:
    def __init__(self):
        self.stopping = False

    def heartbeat(self):
        pass


class CrashProc:
    """Dies abnormally after one sweep."""
    def __init__(self, pid=4300, rc=1):
        self.pid = pid
        self.rc = rc
        self.running = True
        self.stopped = False

    async def wait(self):
        return self.rc, b"connection reset"

    async def stop(self, grace=5.0):
        self.stopped = True
        self.running = False
        return self.rc


class SeqRunner:
    """Returns one proc per start; records the args of every start."""
    def __init__(self, procs):
        self.procs = list(procs)
        self.starts = []

    async def run(self, args, session_id, timeout=0.0):
        self.starts.append(args)
        return self.procs.pop(0)

    async def stop_session(self, session_id):
        for p in self.procs:
            await p.stop()


class ReconnectTests(unittest.TestCase):
    def _worker(self, tmp, runner, **kw):
        db = str(Path(tmp) / "t.db")
        conn = connect(db)
        migrate(conn)
        conn.execute(
            "INSERT INTO twitch_sources (username, created_at, updated_at)"
            " VALUES ('chan', 0, 0)")
        conn.execute(
            "INSERT INTO stream_sessions (source_id, state)"
            " VALUES (1, 'RECORDING')")
        conn.close()
        buf = RollingBuffer(tmp, session_id=1, segment_seconds=5)
        buf.dir.mkdir(parents=True, exist_ok=True)
        kw.setdefault("sweep_seconds", 0.02)
        kw.setdefault("reconnect_base_seconds", 0.01)
        return RecordingWorker(runner, buf, "fake://src", db, **kw), buf

    def test_abnormal_exit_reconnects_with_continuous_numbering(self):
        with tempfile.TemporaryDirectory() as tmp:
            dying = CrashProc()
            alive = FakeProc()
            runner = SeqRunner([dying, alive])
            worker, buf = self._worker(tmp, runner)
            ctx = FakeCtx()

            async def drive():
                task = asyncio.create_task(worker.run(ctx))
                await asyncio.sleep(0.1)
                # the first capture produced one segment before dropping
                (buf.dir / "seg-000000.ts").write_bytes(b"x")
                dying.running = False  # network drop after first sweep
                await asyncio.sleep(0.3)
                ctx.stopping = True
                await task

            asyncio.run(drive())
            self.assertEqual(worker.reconnects, 1)
            self.assertEqual(len(runner.starts), 2)
            self.assertNotIn("-start_number", runner.starts[0])
            # second start continues numbering after on-disk segments
            args = runner.starts[1]
            self.assertIn("-start_number", args)

    def test_reconnect_budget_exhausted_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            procs = [CrashProc(pid=4300 + i) for i in range(3)]
            runner = SeqRunner(procs)
            worker, _buf = self._worker(tmp, runner, max_reconnects=2)
            ctx = FakeCtx()

            async def drive():
                task = asyncio.create_task(worker.run(ctx))
                for p in procs:
                    await asyncio.sleep(0.06)
                    p.running = False
                try:
                    await task
                except RuntimeError:
                    raise

            with self.assertRaisesRegex(RuntimeError, "reconnects"):
                asyncio.run(drive())
            self.assertEqual(worker.reconnects, 2)

    def test_clean_exit_not_retried(self):
        with tempfile.TemporaryDirectory() as tmp:
            proc = CrashProc(rc=0)
            runner = SeqRunner([proc])
            worker, _buf = self._worker(tmp, runner)
            ctx = FakeCtx()

            async def drive():
                task = asyncio.create_task(worker.run(ctx))
                await asyncio.sleep(0.06)
                proc.running = False
                await task

            asyncio.run(drive())
            self.assertEqual(worker.reconnects, 0)
            self.assertEqual(len(runner.starts), 1)

    def test_next_seq_continues_after_existing(self):
        with tempfile.TemporaryDirectory() as tmp:
            buf = RollingBuffer(tmp, session_id=1, segment_seconds=5)
            buf.dir.mkdir(parents=True, exist_ok=True)
            self.assertEqual(buf.next_seq(), 0)
            (buf.dir / "seg-000004.ts").write_bytes(b"x")
            (buf.dir / "seg-000002.ts").write_bytes(b"x")
            self.assertEqual(buf.next_seq(), 5)
            args = buf.segment_args("file:///f.mp4", start_number=5)
            i = args.index("-start_number")
            self.assertEqual(args[i + 1], "5")
