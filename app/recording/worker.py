"""Recording worker: supervised capture into the rolling buffer.

Runs FFmpeg segmentation as a supervised service so Telegram outages never
interrupt recording, tracks the process lifecycle, reconciles the segment
database on startup and enforces the storage cap continuously.
"""
from __future__ import annotations

import asyncio

from ..core.lifecycle import ServiceContext
from ..core.logging import get_logger
from .buffer import RollingBuffer
from .ffmpeg import FFmpegRunner

log = get_logger("recording.worker")


class RecordingWorker:
    def __init__(self, runner: FFmpegRunner, buffer: RollingBuffer,
                 source_url: str, db_path: str,
                 sweep_seconds: float = 10.0):
        self.runner = runner
        self.buffer = buffer
        self.source_url = source_url
        self.db_path = db_path
        self.sweep_seconds = sweep_seconds
        self.session_name = f"record-{buffer.session_id}"

    async def run(self, ctx: ServiceContext) -> None:
        reconciled = self.buffer.reconcile(self.db_path)
        if reconciled:
            log.info("reconciled %d on-disk segments", reconciled)
        proc = await self.runner.run(
            self.buffer.segment_args(self.source_url),
            session_id=self.session_name)
        log.info("recording started pid=%s session=%s",
                 proc.pid, self.session_name)
        try:
            while not ctx.stopping:
                ctx.heartbeat()
                self.buffer.enforce_storage_cap()
                if not proc.running:
                    rc, stderr = await proc.wait()
                    if rc == 0:
                        # finite feed fully consumed: clean completion,
                        # not a crash - the session finalizes what was
                        # recorded
                        log.info("feed exhausted (clean exit); "
                                 "recording complete")
                        return
                    raise RuntimeError(
                        f"ffmpeg exited rc={rc}: {stderr[:300]!r}")
                await asyncio.sleep(self.sweep_seconds)
        finally:
            await self.runner.stop_session(self.session_name)
            self.buffer.enforce_storage_cap()
            self.buffer.reconcile(self.db_path)
            log.info("recording stopped session=%s", self.session_name)
