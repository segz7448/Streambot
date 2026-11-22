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


if __name__ == "__main__":
    unittest.main()
