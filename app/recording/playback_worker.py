"""Supervised capture of a public Twitch live playback into the rolling buffer.

Same service interface as RecordingWorker.run(ctx). ffmpeg never touches the
network: app.ingestion.hls_pump downloads through the validated fetch worker
and pipes MPEG-TS into the buffer's segmenter.

Policy (all fail closed, no bypass attempts):
- RECORD authorization (settings srcauth:<user>:record) is checked before
  every cycle and polled (cached ~2s) while capturing; off stops capture at
  the next check (not instant) and the session ends.
- Every cycle re-resolves a fresh playback URL within the resolver's own
  rate bound (a wait of at least min_interval separates resolves).
- offline at start: refused. offline later: clean end of the stream.
- restricted / challenge / blocked (429 or throttle) / unauthorized /
  quality / invalid: TERMINAL (RuntimeError naming only the kind). A
  restriction is never retried through a different route.
- transient 'error' and ffmpeg exits: bounded reconnects with backoff and
  a cumulative-gap limit, like RecordingWorker.
"""
from __future__ import annotations

import asyncio
import time

from ..bot import source_auth
from ..core.logging import get_logger
from ..ingestion import hls_pump, twitch_playback as tp

log = get_logger("recording.playback")

TERMINAL = {"restricted", "challenge", "blocked", "unauthorized", "quality",
            "invalid"}


class PlaybackWorker:
    def __init__(self, buffer, channel: str, db_path: str, state_dir,
                 ffmpeg: str, *, initial_resolved=None, max_height: int = 720,
                 cycle_media_seconds: float = 1500.0,
                 cycle_deadline_seconds: float = 1800.0,
                 max_reconnects: int = 5, reconnect_base_seconds: float = 5.0,
                 max_gap_seconds: float = 300.0,
                 resolve_interval: float = tp.MIN_INTERVAL_SEC,
                 resolve_fn=None, capture_fn=None, opener=None, host_ok=None,
                 auth_poll_seconds: float = 2.0, sleep=asyncio.sleep):
        self.buffer, self.channel = buffer, channel
        self.db_path, self.state_dir, self.ffmpeg = db_path, state_dir, ffmpeg
        self.initial = initial_resolved
        self.max_height = max_height
        self.cycle_media = cycle_media_seconds
        self.cycle_deadline = cycle_deadline_seconds
        self.max_reconnects = max_reconnects
        self.reconnect_base = reconnect_base_seconds
        self.max_gap = max_gap_seconds
        self.resolve_interval = resolve_interval
        self.resolve_fn = resolve_fn or tp.resolve
        self.capture_fn = capture_fn or hls_pump.capture
        self.opener, self.host_ok = opener, host_ok
        self.auth_poll = auth_poll_seconds
        self._sleep = sleep
        self.reconnects = 0
        self.gap_seconds = 0.0
        self.captured_segments = 0
        self.cycles = 0
        self.end_reason = "not_started"
        self.finished = False
        self.error_text = None
        self.session_name = f"playback-{buffer.session_id}"
        self._auth_at = 0.0
        self._auth_ok = False

    # -- authorization -------------------------------------------------
    def record_ok(self) -> bool:
        now = time.monotonic()
        if now - self._auth_at >= self.auth_poll:
            self._auth_ok = source_auth.allowed(self.state_dir, self.channel, "record")
            self._auth_at = now
        return self._auth_ok

    def _recheck(self) -> bool:
        self._auth_at = 0.0
        return self.record_ok()

    # -- one blocking cycle (runs in a thread) -------------------------
    def _cycle(self, stop) -> str:
        """-> 'ended' | 'continue' | 'stopped' | 'revoked' | 'retry'.
        Raises RuntimeError on terminal refusals."""
        stop_or_revoked = lambda: stop() or not self.record_ok()
        if not self._recheck():
            return "revoked"
        if stop():
            return "stopped"
        try:
            if self.initial is not None:
                resolved, self.initial = self.initial, None
            else:
                resolved = self.resolve_fn(
                    self.channel, authorized=True, max_height=self.max_height,
                    opener=self.opener, host_ok=self.host_ok,
                    min_interval=self.resolve_interval, should_stop=stop_or_revoked)
        except tp.PlaybackRefused as e:
            return self._refused(e.kind)
        if not self._recheck():
            return "revoked"
        self.cycles += 1
        try:
            res = self.capture_fn(
                resolved, self.ffmpeg, "", max_media_seconds=self.cycle_media,
                deadline_sec=self.cycle_deadline, opener=self.opener,
                host_ok=self.host_ok, should_stop=stop_or_revoked,
                output_args=self.buffer.segmenter_output_args(self.buffer.next_seq()))
        except tp.PlaybackRefused as e:
            return self._refused(e.kind)
        self.captured_segments += res.segments
        log.info("playback cycle %d channel=%s reason=%s segments=%d",
                 self.cycles, self.channel, res.reason, res.segments)
        if res.reason == "stopped":
            return "revoked" if not self.record_ok() else "stopped"
        if res.reason == "ended":
            return "ended"
        if res.reason in ("media_cap", "deadline"):
            return "continue"
        return "retry"       # ffmpeg_exit

    def _refused(self, kind: str) -> str:
        if kind == "offline":
            if self.captured_segments:
                return "ended"
            raise RuntimeError("playback refused: offline at start")
        if kind in TERMINAL:
            raise RuntimeError(f"playback refused: {kind} (terminal, not retried)")
        return "retry"       # transient 'error'

    # -- service entry -------------------------------------------------
    async def run(self, ctx) -> None:
        reconciled = self.buffer.reconcile(self.db_path)
        if reconciled:
            log.info("reconciled %d on-disk segments", reconciled)
        try:
            while not ctx.stopping:
                ctx.heartbeat()
                t0 = time.monotonic()
                task = asyncio.create_task(asyncio.to_thread(
                    self._cycle, lambda: ctx.stopping))
                while not task.done():
                    ctx.heartbeat()
                    self.buffer.enforce_storage_cap()
                    await asyncio.wait({task}, timeout=1.0)
                outcome = task.result()      # RuntimeError propagates (terminal)
                if outcome in ("ended", "stopped", "revoked"):
                    self.end_reason = outcome
                    return
                if outcome == "continue":
                    # fresh token; keep resolve spacing within the rate bound
                    self.gap_seconds += self.resolve_interval
                    await self._wait(ctx, self.resolve_interval)
                    continue
                # retry: bounded backoff + cumulative gap
                self.reconnects += 1
                if self.reconnects > self.max_reconnects:
                    raise RuntimeError(
                        f"playback unstable: {self.max_reconnects} reconnects exhausted")
                delay = max(min(60.0, self.reconnect_base * 2 ** (self.reconnects - 1)),
                            self.resolve_interval)
                self.gap_seconds += delay
                if self.gap_seconds > self.max_gap:
                    raise RuntimeError("playback unstable: cumulative reconnect gap exceeded")
                await self._wait(ctx, delay)
            self.end_reason = "stopped"
        except RuntimeError as exc:
            self.end_reason = "failed"
            self.error_text = str(exc)
            raise
        finally:
            self.finished = True
            self.buffer.enforce_storage_cap()
            self.buffer.reconcile(self.db_path)
            log.info("playback stopped session=%s reason=%s segments=%d",
                     self.session_name, self.end_reason, self.captured_segments)

    async def _wait(self, ctx, seconds: float) -> None:
        waited = 0.0
        while waited < seconds and not ctx.stopping:
            ctx.heartbeat()
            if not self.record_ok():
                return
            await self._sleep(min(1.0, seconds - waited))
            waited += 1.0
