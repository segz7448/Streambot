"""Cross-layer ownership contract: ClipSession + real worker + real Supervisor
+ real FFmpegRunner/FFmpegProcess (a synthetic live recorder process) and real
executor threads. A session may persist drained=true only when EVERY owner
attested it is drained."""
import asyncio
import json
import os
import signal
import sqlite3
import stat
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from app.bot import sessions as ss
from app.core import database
from app.detection.detector import CandidateDetector
from app.pipeline.session import ClipSession
from app.recording.ffmpeg import FFmpegProcess


def _alive(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    try:
        with open(f"/proc/{pid}/stat") as fh:
            return fh.read().split(")")[-1].split()[0] != "Z"
    except OSError:
        return False


class Base(unittest.TestCase):
    def setUp(self):
        self._t = tempfile.TemporaryDirectory()
        self.sd = Path(self._t.name)
        c = database.connect(str(self.sd / "nova.db"))
        database.migrate(c)
        c.close()
        self.addCleanup(self._t.cleanup)

    def row(self):
        with closing(sqlite3.connect(str(self.sd / "nova.db"))) as c:
            r = c.execute("SELECT state,notes FROM stream_sessions ORDER BY id DESC LIMIT 1").fetchone()
        return r[0], (json.loads(r[1]) if r and r[1] else {})


class RecorderStopFailure(Base):
    def test_failed_recorder_stop_keeps_ownership_and_never_attests(self):
        fake = self.sd / "fakeffmpeg"
        fake.write_text("#!/bin/sh\nexec sleep 300\n")
        fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
        killed = []
        real_stop = FFmpegProcess.stop
        fail = {"on": True}

        async def stop(self_, grace=5.0):
            if fail["on"]:
                raise OSError("stop failed")
            return await real_stop(self_, grace)

        def cleanup():
            for p in list(ClipSession._procs_for_test):
                if _alive(p):
                    os.kill(p, signal.SIGKILL)
        ClipSession._procs_for_test = []
        self.addCleanup(cleanup)
        s = ClipSession(str(fake), str(fake), "http://x.invalid/live.m3u8", "streamerx", self.sd,
                        segment_seconds=2, tick_seconds=0.3)
        s.attempt_id = s._sched_token = "a" * 32
        with mock.patch.object(FFmpegProcess, "stop", stop):
            s.start()
            time.sleep(2.0)
            pids = [p.pid for p in s._worker.runner.processes.values()]
            ClipSession._procs_for_test += pids
            self.assertTrue(pids and all(_alive(p) for p in pids))
            s.stop()
            time.sleep(4.0)                       # many drain rounds, every stop raises
            self.assertTrue(s.alive(), "session released ownership with a live recorder")
            self.assertTrue(all(_alive(p) for p in pids))
            state, notes = self.row()
            self.assertNotIn("drained", notes)    # no proof from a failed teardown
            self.assertFalse(ss.session_terminated(self.sd, "streamerx", "a" * 32))
            self.assertTrue(s._worker.runner.live_processes() >= 1)
            fail["on"] = False                    # the stop finally works
            s.join(40)
        self.assertFalse(s.alive())
        self.assertFalse(any(_alive(p) for p in pids))
        state, notes = self.row()
        self.assertIs(notes.get("drained"), True)
        self.assertTrue(ss.session_terminated(self.sd, "streamerx", "a" * 32))


class ExecutorWorkOwned(Base):
    def test_cancelled_session_waits_for_blocked_detector_and_never_runs_it_twice(self):
        started = threading.Event()
        release = threading.Event()
        calls = {"now": 0, "max": 0, "n": 0}
        lock = threading.Lock()

        def blocked(self_, *a, **k):
            with lock:
                calls["now"] += 1
                calls["n"] += 1
                calls["max"] = max(calls["max"], calls["now"])
            started.set()
            try:
                release.wait(30)
            finally:
                with lock:
                    calls["now"] -= 1

        def cap(*a, should_stop=None, **k):
            while not should_stop():
                time.sleep(0.05)
            return mock.Mock(reason="stopped", segments=0, ffmpeg_rc=0)
        from app.bot import source_auth
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        s = ClipSession("ffmpeg", "ffprobe", "", "streamerx", self.sd, segment_seconds=2,
                        tick_seconds=0.2,
                        playback={"initial_resolved": object(), "capture_fn": cap,
                                  "resolve_interval": 0.0, "auth_poll_seconds": 0.0})
        s.DRAIN_WAIT_SECONDS = 1.0

        async def go():
            t = asyncio.create_task(s._run())
            for _ in range(100):
                if started.is_set():
                    break
                await asyncio.sleep(0.1)
            self.assertTrue(started.is_set())
            for _ in range(4):                       # repeated cancellation
                t.cancel()
                await asyncio.sleep(0.4)
            self.assertFalse(t.done(), "session finished under a blocked executor thread")
            self.assertEqual(calls["n"], 1)          # cleanup did not re-run the detector
            self.assertNotIn("drained", self.row()[1])
            self.assertFalse(s._owner_drained(getattr(s, "_task", None)))
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(t, 30)
        with mock.patch.object(CandidateDetector, "process_new_segments", blocked):
            asyncio.run(go())
        self.assertEqual(calls["max"], 1)
        self.assertEqual(s._inflight, 0)


class DurableTerminal(Base):
    def test_terminal_and_drained_are_persisted_by_the_session_not_a_live_property(self):
        s = ClipSession("ffmpeg", "ffprobe", "", "streamerx", self.sd, playback={})
        c = database.connect(str(self.sd / "nova.db"))
        now = time.time()
        c.execute("INSERT OR IGNORE INTO twitch_sources (username,created_at,updated_at) VALUES ('streamerx',?,?)", (now, now))
        sid = c.execute("INSERT INTO stream_sessions (source_id,state,started_at,notes) VALUES "
                        "((SELECT id FROM twitch_sources LIMIT 1),'INTERRUPTED',?,?)",
                        (now, json.dumps({"mode": "playback", "sched_attempt": "b" * 32}))).lastrowid
        c.commit(); c.close()
        import types
        s._session_id = sid
        s._worker = types.SimpleNamespace(terminal_kind="challenge", end_reason="failed",
                                          drained=lambda: True)
        s._owners_drained = True
        s._task = types.SimpleNamespace(done=lambda: True)
        s._mark_drained()
        # no live object registered: a crash-restart sees only the durable row
        self.assertEqual(ss.session_terminal("streamerx", "b" * 32, self.sd), "challenge")
        self.assertTrue(ss.session_terminated(self.sd, "streamerx", "b" * 32))
        # an unattested owner must not persist proof, but still persists the outcome
        s._worker = types.SimpleNamespace(terminal_kind="unauthorized", end_reason="failed",
                                          drained=lambda: False)
        c = database.connect(str(self.sd / "nova.db"))
        c.execute("UPDATE stream_sessions SET notes=?", (json.dumps({"sched_attempt": "b" * 32}),))
        c.commit(); c.close()
        s._mark_drained()
        self.assertEqual(ss.session_terminal("streamerx", "b" * 32, self.sd), "unauthorized")
        self.assertFalse(ss.session_terminated(self.sd, "streamerx", "b" * 32))


if __name__ == "__main__":
    unittest.main()
