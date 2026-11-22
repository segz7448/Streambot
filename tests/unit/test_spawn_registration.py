"""Spawn gating: a writer that cannot be durably recorded is never
spawned; a spawned writer whose identity cannot be recorded is
terminated and the spawn fails. No swallowed registration failures."""
import unittest
from unittest import mock

from app.ingestion import hls_pump
from app.ingestion import twitch_playback as tp
from app.recording.ffmpeg import FFmpegRunner, FFmpegError


def _resolved() -> tp.Resolved:
    v = tp.Variant("360p", 360, 1_000_000, "https://fake/v.m3u8")
    return tp.Resolved("chan", v, (v,), 0.0)


class CaptureGatingTests(unittest.TestCase):
    def _capture(self, **kw):
        return hls_pump.capture(
            _resolved(), "ffmpeg", "/tmp/out.ts", max_media_seconds=10.0,
            deadline_sec=60.0, opener=lambda *a: (200, b""),
            host_ok=lambda h: True, **kw)

    def test_unrecordable_intent_never_spawns(self):
        with mock.patch.object(hls_pump.subprocess, "Popen") as popen:
            with self.assertRaises(ValueError):
                self._capture(pre_spawn=lambda: None)
        popen.assert_not_called()

    def test_unbindable_writer_is_terminated_and_spawn_fails(self):
        proc = mock.Mock(pid=4242, stdin=None)
        with mock.patch.object(hls_pump.subprocess, "Popen",
                               return_value=proc) as popen, \
             mock.patch.object(hls_pump, "_terminate") as term:
            with self.assertRaises(RuntimeError):
                self._capture(pre_spawn=lambda: "tok-1",
                              on_spawn=lambda pid, pgid, tok: False)
        popen.assert_called_once()
        # the owned child - its whole group - is torn down, nothing else
        term.assert_called_once_with(proc, 10.0)

    def test_spawn_hook_exception_is_a_failed_spawn_not_a_swallow(self):
        proc = mock.Mock(pid=4242, stdin=None)
        def boom(pid, pgid, tok):
            raise OSError("disk full")
        with mock.patch.object(hls_pump.subprocess, "Popen",
                               return_value=proc), \
             mock.patch.object(hls_pump, "_terminate") as term:
            with self.assertRaises(RuntimeError):
                self._capture(on_spawn=boom)
        term.assert_called_once()


class RunnerGatingTests(unittest.IsolatedAsyncioTestCase):
    async def test_unrecordable_intent_never_starts_process(self):
        runner = FFmpegRunner("ffmpeg", pre_spawn=lambda: None)
        with mock.patch("app.recording.ffmpeg.FFmpegProcess.start",
                        ) as start:
            with self.assertRaises(FFmpegError):
                await runner.run(["-v"], "s1")
        start.assert_not_called()


if __name__ == "__main__":
    unittest.main()
