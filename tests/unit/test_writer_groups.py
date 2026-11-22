"""Writer-spawn gating on durable registration, and process-group
containment for registered writers.

Every writer subprocess (edit, extract, capture) must either be recorded
through the two-phase intent/bind protocol or never survive: an
unrecordable writer is never spawned, an unbindable one is killed with
its whole group, and a registered writer leads its own process group so
the boot-time proof sweep can find surviving descendants.
"""
import asyncio
import os
import subprocess
import sys
import time
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.editing import editor
from app.editing.editor import edit_clip
from app.editing.presets import HORIZONTAL
from app.clipping import extractor


class EditSpawnGatingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.out = str(self.tmp / "out.mp4")
        self.src = str(self.tmp / "src.mp4")
        Path(self.src).write_bytes(b"\x00" * 128)

    def test_unrecordable_intent_never_spawns(self):
        with mock.patch.object(editor.subprocess, "Popen") as pop:
            with self.assertRaises(RuntimeError):
                edit_clip("ffmpeg", self.src, self.out, HORIZONTAL,
                          0, 10, str(self.tmp),
                          pre_spawn=lambda: None, on_spawn=lambda *a: True)
        pop.assert_not_called()

    def test_unbindable_writer_killed_with_group(self):
        # a real sleeping child in its own session; binding fails, so the
        # child and its whole group must be dead afterwards
        fake_ff = self.tmp / "fake-ffmpeg"
        fake_ff.write_text("#!/bin/sh\nsleep 300\n")
        fake_ff.chmod(0o755)
        seen = {}

        def fail_bind(pid, pgid, token):
            seen["pid"] = pid
            return False

        with self.assertRaises(RuntimeError):
            edit_clip(str(fake_ff), self.src, self.out, HORIZONTAL,
                      0, 10, str(self.tmp),
                      pre_spawn=lambda: "tok", on_spawn=fail_bind)
        with self.assertRaises(ProcessLookupError):
            os.kill(seen["pid"], 0)

    def test_bind_receives_pid_and_group(self):
        fake_ff = self.tmp / "fake-ffmpeg"
        fake_ff.write_text("#!/bin/sh\nexit 0\n")
        fake_ff.chmod(0o755)
        seen = {}

        def bind(pid, pgid, token):
            seen.update(pid=pid, pgid=pgid, token=token)
            return True

        edit_clip(str(fake_ff), self.src, self.out, HORIZONTAL,
                  0, 10, str(self.tmp),
                  pre_spawn=lambda: "tok", on_spawn=bind)
        self.assertEqual(seen["token"], "tok")
        self.assertEqual(seen["pid"], seen["pgid"])  # leads its own group
        self.assertEqual(os.getpgid(seen["pid"]) if _alive(seen["pid"])
                         else seen["pgid"], seen["pid"])


def _alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _fake_plan():
    seg = mock.Mock()
    seg.path = "/nonexistent/seg.mp4"
    plan = mock.Mock()
    plan.segments = [seg]
    plan.start_offset = 0.0
    plan.duration = 20.0
    return plan


class ExtractSpawnGatingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.out = str(self.tmp / "out.mp4")

    def test_unrecordable_intent_never_spawns(self):
        with mock.patch.object(extractor, "plan_clip",
                               lambda *a, **k: _fake_plan()), \
             mock.patch.object(extractor.subprocess, "Popen") as pop:
            with self.assertRaises(RuntimeError):
                extractor.extract_clip("ffmpeg", mock.Mock(), 1.0, self.out,
                                       pre_spawn=lambda: None,
                                       on_spawn=lambda *a: True)
        pop.assert_not_called()

    def test_unbindable_writer_killed_with_group(self):
        proc = mock.Mock()
        proc.pid = 4321
        with mock.patch.object(extractor, "plan_clip",
                               lambda *a, **k: _fake_plan()), \
             mock.patch.object(extractor.subprocess, "Popen",
                               return_value=proc), \
             mock.patch.object(editor, "_kill_group") as kill:
            with self.assertRaises(RuntimeError):
                extractor.extract_clip("ffmpeg", mock.Mock(), 1.0, self.out,
                                       pre_spawn=lambda: "tok",
                                       on_spawn=lambda *a: False)
        kill.assert_called_once_with(proc)


class ProcessGroupTests(unittest.TestCase):
    def test_own_group_leads_and_stop_takes_group(self):
        from app.recording.ffmpeg import FFmpegProcess

        async def go():
            p = FFmpegProcess(args=["sh", "-c", "sleep 300 & exec sleep 300"],
                              session_id="t")
            await p.start(own_group=True)
            pid = p.pid
            await asyncio.sleep(0.5)  # let the grandchild spawn
            kids = _children_of(pid)
            await p.stop(grace=0.3)
            return pid, kids

        pid, kids = asyncio.run(go())
        self.assertTrue(kids, "expected a grandchild process")
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        for kid in kids:
            with self.assertRaises(ProcessLookupError):
                os.kill(kid, 0)

    def test_default_stays_in_caller_group(self):
        from app.recording.ffmpeg import FFmpegProcess

        async def go():
            p = FFmpegProcess(args=["sh", "-c", "exec sleep 300"],
                              session_id="t")
            await p.start()  # own_group=False default
            grp = os.getpgid(p.pid)
            await p.stop(grace=0.3)
            return grp

        grp = asyncio.run(go())
        self.assertEqual(grp, os.getpgrp())

    def test_runner_spawns_registered_writers_in_own_group(self):
        from app.recording import ffmpeg as ff

        captured = {}

        async def fake_start(self, own_group=False):
            captured["own_group"] = own_group
            self.pid = None
            self._proc = None

        async def go():
            runner = ff.FFmpegRunner("ffmpeg", on_spawn=lambda *a: True,
                                     pre_spawn=lambda: "tok")
            with mock.patch.object(ff.FFmpegProcess, "start", fake_start):
                await runner.run(["-x"], session_id="t")

        asyncio.run(go())
        self.assertTrue(captured["own_group"])

    def test_runner_refuses_unrecordable_writer(self):
        from app.recording import ffmpeg as ff

        async def go():
            runner = ff.FFmpegRunner("ffmpeg", pre_spawn=lambda: None,
                                     on_spawn=lambda *a: True)
            with mock.patch.object(ff.FFmpegProcess, "start") as start:
                with self.assertRaises(ff.FFmpegError):
                    await runner.run(["-x"], session_id="t")
                start.assert_not_called()

        asyncio.run(go())

    def test_runner_kills_unbindable_writer(self):
        from app.recording import ffmpeg as ff

        async def go():
            runner = ff.FFmpegRunner("ffmpeg", pre_spawn=lambda: "tok",
                                     on_spawn=lambda *a: False)
            proc_holder = {}
            real_start = ff.FFmpegProcess.start

            async def start_spy(self, own_group=False):
                await real_start(self, own_group=own_group)
                proc_holder["proc"] = self

            with mock.patch.object(ff.FFmpegProcess, "start", start_spy):
                with self.assertRaises(ff.FFmpegError):
                    await runner.run([sys.executable, "-c",
                                      "import time; time.sleep(300)"],
                                     session_id="t")
            return proc_holder["proc"].pid

        pid = asyncio.run(go())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)


def _children_of(pid):
    kids = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as fh:
                parts = fh.read().rsplit(")", 1)[1].split()
            if int(parts[1]) == pid:  # ppid
                kids.append(int(entry))
        except (FileNotFoundError, ProcessLookupError, ValueError, IndexError):
            continue
    return kids


def _group_members(pgid):
    out = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as fh:
                parts = fh.read().rsplit(")", 1)[1].split()
            if int(parts[2]) == pgid and parts[0] not in ("Z", "X"):
                out.append(int(entry))
        except Exception:
            continue
    return out


_TERM_IGNORING_CHILD = ("import signal,time;"
                        "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
                        "time.sleep(300)")


class KillGroupRegressionTests(unittest.TestCase):
    """Reviewer-owned regression (r2): a TERM-resistant descendant must
    not survive _kill_group. A dead leader is not a dead process group."""

    def test_term_resistant_descendant_must_not_survive(self):
        from app.editing.editor import _kill_group
        parent = ("import subprocess,sys,time;"
                  "p=subprocess.Popen([sys.executable,'-c',sys.argv[1]]);"
                  "print(p.pid,flush=True);time.sleep(300)")
        p = subprocess.Popen([sys.executable, "-c", parent,
                              _TERM_IGNORING_CHILD],
                             start_new_session=True, stdout=subprocess.PIPE,
                             text=True)
        try:
            kid = int(p.stdout.readline())
            _kill_group(p)
            self.assertFalse(_group_members(p.pid),
                             "a dead leader is not a dead process group")
            self.assertFalse(_alive(kid))
        finally:
            import signal
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                p.wait(timeout=5)
            except Exception:
                pass
            p.stdout.close()


class StopGroupCleanupTests(unittest.TestCase):
    """FFmpegProcess.stop must clean the child's own group on BOTH paths:
    a leader that already exited, and a successful leader wait that left
    TERM-resistant members behind."""

    def test_stop_with_already_dead_leader_kills_group(self):
        from app.recording.ffmpeg import FFmpegProcess

        async def go():
            p = FFmpegProcess(
                args=["sh", "-c",
                      f"{sys.executable} -c '{_TERM_IGNORING_CHILD}' & exit 0"],
                session_id="t")
            await p.start(own_group=True)
            await asyncio.wait_for(p.wait(), timeout=10)  # leader exits
            members = _group_members(p.pid)
            await p.stop(grace=0.3)
            return p.pid, members

        pid, members = asyncio.run(go())
        self.assertTrue(members, "expected a surviving descendant")
        for m in members:
            with self.assertRaises(ProcessLookupError):
                os.kill(m, 0)
        self.assertFalse(_group_members(pid))

    def test_stop_after_successful_leader_wait_kills_survivors(self):
        from app.recording.ffmpeg import FFmpegProcess

        async def go():
            p = FFmpegProcess(
                args=["sh", "-c",
                      f"{sys.executable} -c '{_TERM_IGNORING_CHILD}' & "
                      "exec sleep 300"],
                session_id="t")
            await p.start(own_group=True)
            await asyncio.sleep(0.5)
            members = _group_members(p.pid)
            await p.stop(grace=0.3)
            return p.pid, members

        pid, members = asyncio.run(go())
        self.assertTrue(members, "expected leader plus descendant")
        for m in members:
            with self.assertRaises(ProcessLookupError):
                os.kill(m, 0)
        self.assertFalse(_group_members(pid))


class PipeClosureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.out = str(self.tmp / "out.mp4")
        self.src = str(self.tmp / "src.mp4")
        Path(self.src).write_bytes(b"\x00" * 128)
        fake_ff = self.tmp / "fake-ffmpeg"
        fake_ff.write_text("#!/bin/sh\nexit 0\n")
        fake_ff.chmod(0o755)
        self.ff = str(fake_ff)

    def _capture_popen(self, captured):
        real_popen = editor.subprocess.Popen

        def spy(*a, **k):
            proc = real_popen(*a, **k)
            captured["proc"] = proc
            return proc
        return mock.patch.object(editor.subprocess, "Popen",
                                 side_effect=spy)

    def test_pipes_closed_on_success(self):
        captured = {}
        with self._capture_popen(captured):
            edit_clip(self.ff, self.src, self.out, HORIZONTAL, 0, 10,
                      str(self.tmp), pre_spawn=lambda: "t",
                      on_spawn=lambda *a: True)
        self.assertTrue(captured["proc"].stdout.closed)
        self.assertTrue(captured["proc"].stderr.closed)

    def test_pipes_closed_on_bind_failure_kill(self):
        captured = {}
        with self._capture_popen(captured):
            with self.assertRaises(RuntimeError):
                edit_clip(self.ff, self.src, self.out, HORIZONTAL, 0, 10,
                          str(self.tmp), pre_spawn=lambda: "t",
                          on_spawn=lambda *a: False)
        self.assertTrue(captured["proc"].stdout.closed)
        self.assertTrue(captured["proc"].stderr.closed)


class OwnerDrainedFailClosedTests(unittest.TestCase):
    """Executor completion is not ownership release: _owner_drained must
    stay False while any registered writer is unverifiable or alive."""

    def setUp(self):
        from app.pipeline.session import ClipSession
        self.tmp = Path(tempfile.mkdtemp())
        inst = ClipSession.__new__(ClipSession)
        inst._session_id = 42
        inst.state_dir = self.tmp
        inst.source_label = "someuser"
        inst._worker = mock.Mock()
        inst._worker.drained = lambda: True
        inst._inflight = 0
        self.inst = inst
        self.reg_dir = self.tmp / "sessions" / "someuser"
        self.reg_dir.mkdir(parents=True)
        self.registry = self.reg_dir / "writers-42.json"

    def test_missing_registry_blocks_drained(self):
        # the manifest is initialized before any session work, so a
        # missing file is a LOST manifest, indistinguishable from lost
        # writer evidence: it must block, not pass as "no writers"
        self.assertFalse(self.inst._owner_drained())

    def test_empty_manifest_is_drained(self):
        from app.bot import restartproof as rp
        self.assertTrue(rp.init_registry(self.registry))
        self.assertTrue(self.inst._owner_drained())

    def test_live_registered_writer_blocks_drained(self):
        from app.bot import restartproof as rp
        self.assertTrue(rp.init_registry(self.registry))
        proc = subprocess.Popen(["sleep", "300"], start_new_session=True)
        try:
            token = rp.record_spawn_intent(self.registry)
            self.assertIsNotNone(token)
            self.assertTrue(rp.record_spawned(self.registry, token,
                                              proc.pid, proc.pid))
            self.assertFalse(self.inst._owner_drained())
            proc.terminate()
            proc.wait(timeout=5)
            deadline = time.monotonic() + 5
            while _group_members(proc.pid) and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertTrue(self.inst._owner_drained())
        finally:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            proc.wait()

    def test_unreadable_registry_blocks_drained(self):
        self.registry.write_bytes(b"not json at all")
        self.assertFalse(self.inst._owner_drained())

    def test_unresolved_intent_blocks_drained(self):
        from app.bot import restartproof as rp
        self.assertTrue(rp.init_registry(self.registry))
        token = rp.record_spawn_intent(self.registry)
        self.assertIsNotNone(token)
        self.assertFalse(self.inst._owner_drained())

    def test_missing_manifest_blocks_intent_creation(self):
        # lost history must never be silently recreated: only explicit
        # before-work initialization may create the manifest
        from app.bot import restartproof as rp
        self.assertIsNone(rp.record_spawn_intent(self.registry))
        self.assertFalse(self.registry.exists())
        self.assertTrue(rp.init_registry(self.registry))
        self.assertIsNotNone(rp.record_spawn_intent(self.registry))

    def _entry(self, **over):
        import json as _json
        entry = {"intent": "t", "ts": 1.0, "pid": 424242,
                 "start": 1000, "pgid": 424242}
        entry.update(over)
        self.registry.write_text(_json.dumps(
            {"v": 1, "writers": [entry]}))

    def test_live_pid_with_missing_start_blocks_drained(self):
        # the r3 gap: a live pid whose start is missing must NOT be
        # mistaken for a reused (dead) pid
        live = os.getpid()
        self._entry(pid=live, start=None, pgid=live)
        self.assertFalse(self.inst._owner_drained())
        self._entry(pid=live, pgid=live)
        import json as _json
        self.registry.write_text(_json.dumps(
            {"v": 1, "writers":
             [{"intent": "t", "ts": 1.0, "pid": live, "pgid": live}]}))
        self.assertFalse(self.inst._owner_drained())

    def test_missing_pgid_blocks_drained(self):
        import json as _json
        self.registry.write_text(_json.dumps(
            {"v": 1, "writers":
             [{"intent": "t", "ts": 1.0, "pid": 424242,
               "start": 1000}]}))
        self.assertFalse(self.inst._owner_drained())

    def test_legacy_unversioned_manifest_blocks_drained(self):
        # pre-version layout: never wiped, never read as valid
        import json as _json
        self.registry.write_text(_json.dumps(
            [{"intent": "t", "ts": 1.0, "pid": 424242, "start": 1000,
              "pgid": 424242}]))
        self.assertFalse(self.inst._owner_drained())

    def test_invalid_identities_block_drained(self):
        for bad in (dict(pid=-1), dict(pid=0), dict(pid=True),
                    dict(start=-5), dict(start=0), dict(pgid=-2),
                    dict(pgid="x")):
            self._entry(**bad)
            self.assertFalse(self.inst._owner_drained(),
                             f"entry {bad} must fail closed")


if __name__ == "__main__":
    unittest.main()
