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
                 sweep_seconds: float = 10.0,
                 max_reconnects: int = 5,
                 reconnect_base_seconds: float = 5.0):
        self.runner = runner
        self.buffer = buffer
        self.source_url = source_url
        self.db_path = db_path
        self.sweep_seconds = sweep_seconds
        self.max_reconnects = max_reconnects
        self.reconnect_base_seconds = reconnect_base_seconds
        self.reconnects = 0
        self.session_name = f"record-{buffer.session_id}"

    async def _start(self, start_number: int):
        proc = await self.runner.run(
            self.buffer.segment_args(self.source_url, start_number),
            session_id=self.session_name)
        log.info("recording started pid=%s session=%s start_number=%d",
                 proc.pid, self.session_name, start_number)
        return proc

    async def _backoff(self, ctx: ServiceContext, seconds: float) -> bool:
        """Interruptible reconnect wait; True if still good to retry."""
        waited = 0.0
        while waited < seconds and not ctx.stopping:
            ctx.heartbeat()
            step = min(1.0, seconds - waited)
            await asyncio.sleep(step)
            waited += step
        return not ctx.stopping

    async def run(self, ctx: ServiceContext) -> None:
        reconciled = self.buffer.reconcile(self.db_path)
        if reconciled:
            log.info("reconciled %d on-disk segments", reconciled)
        proc = await self._start(self.buffer.next_seq())
        try:
            while not ctx.stopping:
                ctx.heartbeat()
                self.buffer.enforce_storage_cap()
                if not proc.running:
                    rc, stderr = await proc.wait()
                    if rc == 0:
                        # finite feed fully consumed OR the live stream
                        # ended cleanly: completion, not a crash. A clean
                        # end is not auto-retried - a live channel that
                        # went offline would otherwise loop forever.
                        log.info("feed exhausted (clean exit); "
                                 "recording complete")
                        return
                    if self.reconnects >= self.max_reconnects:
                        raise RuntimeError(
                            f"ffmpeg exited rc={rc} after "
                            f"{self.reconnects} reconnects: "
                            f"{stderr[:300]!r}")
                    # abnormal exit (network drop, server disconnect):
                    # bounded exponential backoff, then restart capture
                    # into the SAME buffer with continuous segment
                    # numbering - the session, its events and its
                    # content clock survive the drop.
                    self.reconnects += 1
                    delay = min(60.0, self.reconnect_base_seconds
                                * (2 ** (self.reconnects - 1)))
                    log.warning(
                        "ffmpeg dropped rc=%s; reconnect %d/%d in %.0fs "
                        "(session %s)", rc, self.reconnects,
                        self.max_reconnects, delay, self.session_name)
                    if not await self._backoff(ctx, delay):
                        break
                    proc = await self._start(self.buffer.next_seq())
                    continue
                await asyncio.sleep(self.sweep_seconds)
        finally:
            await self.runner.stop_session(self.session_name)
            self.buffer.enforce_storage_cap()
            self.buffer.reconcile(self.db_path)
            log.info("recording stopped session=%s reconnects=%d",
                     self.session_name, self.reconnects)
