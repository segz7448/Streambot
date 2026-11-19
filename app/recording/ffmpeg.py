"""FFmpeg subprocess lifecycle management.

Every FFmpeg process is tracked with an id, session, start time, timeout and
termination path, and never blocks the asyncio event loop. The binary is
located explicitly (AGT has no system ffmpeg; the engine verifies a
bionic-compatible build during deployment and stores its path).
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from ..core.logging import get_logger

log = get_logger("recording.ffmpeg")


class FFmpegError(Exception):
    pass


@dataclass
class FFmpegProcess:
    args: list[str]
    session_id: str
    timeout_seconds: float = 0.0
    pid: Optional[int] = None
    started_at: float = 0.0
    _proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)

    @property
    def running(self) -> bool:
        return self._proc is not None and self._proc.returncode is None

    async def start(self) -> None:
        self._proc = await asyncio.create_subprocess_exec(
            *self.args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        self.pid = self._proc.pid
        self.started_at = time.time()

    async def stop(self, grace: float = 5.0) -> int:
        if not self._proc:
            return -1
        if self._proc.returncode is not None:
            return self._proc.returncode
        self._proc.terminate()
        try:
            await asyncio.wait_for(self._proc.wait(), timeout=grace)
        except asyncio.TimeoutError:
            self._proc.kill()
            await self._proc.wait()
        return self._proc.returncode or 0

    async def wait(self) -> tuple[int, bytes]:
        assert self._proc is not None
        try:
            _, stderr = await asyncio.wait_for(
                self._proc.communicate(),
                timeout=self.timeout_seconds or None)
            return self._proc.returncode or 0, stderr
        except asyncio.TimeoutError:
            await self.stop(grace=1.0)
            raise FFmpegError(f"ffmpeg timed out after {self.timeout_seconds}s")


class FFmpegRunner:
    """Creates and tracks FFmpeg/FFprobe processes for one installation."""

    def __init__(self, ffmpeg_path: str, ffprobe_path: Optional[str] = None,
                 max_concurrent: int = 1):
        self.ffmpeg_path = ffmpeg_path
        self.ffprobe_path = ffprobe_path or ffmpeg_path.replace(
            "ffmpeg", "ffprobe")
        self._sem = asyncio.Semaphore(max_concurrent)
        self.processes: dict[str, FFmpegProcess] = {}

    async def probe(self, path: str, timeout: float = 30.0) -> dict:
        """Inspect a media file; raises FFmpegError on invalid input."""
        args = [self.ffprobe_path, "-v", "error", "-print_format", "json",
                "-show_format", "-show_streams", path]
        async with self._sem:
            proc = await asyncio.create_subprocess_exec(
                *args, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=timeout)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise FFmpegError(f"ffprobe timed out after {timeout}s")
        if proc.returncode != 0:
            raise FFmpegError(f"ffprobe failed rc={proc.returncode}:"
                              f" {stderr[:500]!r}")
        return json.loads(stdout or b"{}")

    async def run(self, args: list[str], session_id: str,
                  timeout: float = 0.0) -> FFmpegProcess:
        proc = FFmpegProcess(args=[self.ffmpeg_path, *args],
                             session_id=session_id,
                             timeout_seconds=timeout)
        async with self._sem:
            await proc.start()
            self.processes[proc.session_id + ":" + str(proc.pid)] = proc
        return proc

    async def stop_session(self, session_id: str) -> None:
        for key, proc in list(self.processes.items()):
            if proc.session_id == session_id and proc.running:
                await proc.stop()

    async def reconcile(self) -> list[str]:
        """Drop finished processes from tracking (startup recovery)."""
        done = [k for k, p in self.processes.items() if not p.running]
        for k in done:
            del self.processes[k]
        return done
