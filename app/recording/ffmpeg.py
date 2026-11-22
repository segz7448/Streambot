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
from collections import deque
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

    async def start(self, own_group: bool = False) -> None:
        # own_group: the child leads its own process group so a later boot
        # can prove IT (and any descendant) dead by pgid, and stop() can
        # take down the whole group instead of leaking grandchildren
        self._own_group = own_group
        self._proc = await asyncio.create_subprocess_exec(
            *self.args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=own_group)
        self.pid = self._proc.pid
        self.started_at = time.time()
        # Drain stderr continuously: an unconsumed pipe buffer (64 KiB)
        # fills on a long capture and the child blocks on write,
        # silently hanging recording. Keep only a bounded tail for
        # diagnostics.
        self._stderr_tail: "deque[bytes]" = deque(maxlen=100)
        self._drain_task = asyncio.create_task(self._drain_stderr())

    async def _drain_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        try:
            while True:
                # bounded chunk reads: readline() dies with
                # LimitOverrunError on a >64KiB line without a newline
                # and the drain would stop silently, re-creating the
                # pipe-fill hang this exists to prevent
                chunk = await self._proc.stderr.read(4096)
                if not chunk:
                    break
                self._stderr_tail.append(chunk[-512:])
        except Exception:
            pass  # drain must never kill the capture

    def stderr_tail(self) -> bytes:
        return b"".join(list(getattr(self, "_stderr_tail", ()))[-20:])

    async def stop(self, grace: float = 5.0) -> int:
        if not self._proc:
            return -1
        if self._proc.returncode is not None:
            return self._proc.returncode
        import os
        import signal
        pgid = self._proc.pid if getattr(self, "_own_group", False) else None
        def _sig(sig):
            if pgid is not None:
                try:
                    os.killpg(pgid, sig)
                    return
                except ProcessLookupError:
                    return
                except PermissionError:
                    pass          # fall back to the direct child
            try:
                (self._proc.terminate if sig == signal.SIGTERM
                 else self._proc.kill)()
            except ProcessLookupError:
                pass
        _sig(signal.SIGTERM)
        try:
            await asyncio.wait_for(self._proc.wait(), timeout=grace)
        except asyncio.TimeoutError:
            _sig(signal.SIGKILL)
            await self._proc.wait()
        return self._proc.returncode or 0

    async def wait(self) -> tuple[int, bytes]:
        assert self._proc is not None
        try:
            await asyncio.wait_for(
                self._proc.wait(),
                timeout=self.timeout_seconds or None)
            drain = getattr(self, "_drain_task", None)
            if drain is not None:
                try:
                    await asyncio.wait_for(drain, timeout=2.0)
                except Exception:
                    drain.cancel()
            return self._proc.returncode or 0, self.stderr_tail()
        except asyncio.TimeoutError:
            await self.stop(grace=1.0)
            raise FFmpegError(f"ffmpeg timed out after {self.timeout_seconds}s")


class FFmpegRunner:
    """Creates and tracks FFmpeg/FFprobe processes for one installation."""

    def __init__(self, ffmpeg_path: str, ffprobe_path: Optional[str] = None,
                 max_concurrent: int = 1, pre_spawn=None, on_spawn=None):
        self.pre_spawn, self.on_spawn = pre_spawn, on_spawn
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
        self.starting = getattr(self, "starting", 0) + 1
        token = None
        if self.pre_spawn is not None:
            try:
                token = self.pre_spawn()
            except Exception:
                token = None
            if token is None:
                self.starting -= 1
                # a writer that cannot be recorded is never spawned
                raise FFmpegError("writer spawn intent not recordable")
        try:
            async with self._sem:
                try:
                    # registered writers lead their own group: the pgid is
                    # recorded so a boot-time proof sweep catches surviving
                    # descendants, and stop() takes the whole group down
                    await proc.start(own_group=self.on_spawn is not None)
                    if self.on_spawn is not None and proc.pid is not None:
                        try:
                            bound = self.on_spawn(proc.pid, proc.pid, token)
                        except Exception:
                            bound = False
                        if not bound:
                            try:
                                await proc.stop(grace=1.0)
                            except Exception:
                                pass
                            raise FFmpegError(
                                "writer identity not recorded; writer terminated")
                finally:
                    # register even if the caller was cancelled mid-start
                    if proc.pid is not None or proc._proc is not None:
                        self.processes[proc.session_id + ":" + str(proc.pid)] = proc
        finally:
            self.starting -= 1
        return proc

    starting = 0

    def live_processes(self) -> int:
        """Tracked recorder processes still running. Positive-attestation
        input: a stop that raised leaves this above zero."""
        return sum(1 for p in self.processes.values() if p.running)

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
