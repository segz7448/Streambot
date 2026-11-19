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
