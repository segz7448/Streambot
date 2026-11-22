"""Playback capture continuity across cycle boundaries.

Two regressions pinned here, with real ffmpeg and a local in-memory opener
(no network, no third-party host):

1. ffmpeg's -t output cap lands INSIDE the last fed segment (its EXTINF
   share is only counted after the write), so ffmpeg can exit cleanly at
   the cap while the final write is still in flight. That boundary is
   media_cap, not ffmpeg_exit: the worker must not burn a reconnect on
   every single cycle boundary of a healthy long capture.
2. Between cycles the worker used to idle a full resolve interval (30 s of
   lost footage every cycle) even though the resolver's own rate window
   was satisfied one whole cycle earlier; it now waits only the true
   remaining window. A healthy cycle re-arms the reconnect/gap budgets,
   like RecordingWorker.
"""
import asyncio
import subprocess
import time
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from app.ingestion import hls_pump
from app.ingestion import twitch_playback as tp
from app.recording.playback_worker import PlaybackWorker


def make_segment(path: Path, seconds: float = 2.4) -> bytes:
    subprocess.run(
        ["ffmpeg", "-hide_banner", "-loglevel", "error",
         "-f", "lavfi", "-i", "testsrc=size=640x360:rate=15",
         "-f", "lavfi", "-i", "sine=frequency=440",
         "-t", str(seconds), "-c:v", "libx264", "-g", "15",
         "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:v", "1M",
         "-f", "mpegts", str(path)], check=True)
    return path.read_bytes()


class _Opener:
    """Fake opener matching tp.default_opener's signature. Serves a live
    media playlist that advances by one segment per fetch; every segment is
    the same real 2.4 s MPEG-TS file whose EXTINF says 2.0 s, so ffmpeg's
    output clock runs ahead of the pump's media counter."""

    def __init__(self, seg_bytes: bytes, junk: bool = False):
        # junk must exceed ffmpeg's 5 MB probe window so the decoder
        # actually exits with an error instead of consuming forever
        self.seg = (b"\x00junk\xff" * (1024 * 1024)) if junk else seg_bytes
        self.seq = 0

    def __call__(self, req, timeout, limit, deadline, should_stop):
        url = req.full_url
        if url.endswith("v.m3u8"):
            seq = self.seq
            self.seq += 1
            text = ("#EXTM3U\n#EXT-X-TARGETDURATION:2\n"
                    f"#EXT-X-MEDIA-SEQUENCE:{seq}\n"
                    "#EXTINF:2.0,\n"
                    f"seg{seq}.ts\n")
            return 200, text.encode()
        return 200, self.seg


def _resolved() -> tp.Resolved:
    v = tp.Variant("360p", 360, 1_000_000, "https://fake/v.m3u8")
    return tp.Resolved("chan", v, (v,), time.time())


class PumpBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.addCleanup(self.td.cleanup)
        self.dir = Path(self.td.name)
        self.seg = make_segment(self.dir / "seg.ts")

    def _capture(self, opener, cap=3.0):
        return hls_pump.capture(
            _resolved(), "ffmpeg", str(self.dir / "out.ts"),
            max_media_seconds=cap, deadline_sec=60.0,
            opener=opener, host_ok=lambda host: True)

    def test_cap_boundary_is_media_cap_not_ffmpeg_exit(self):
        res = self._capture(_Opener(self.seg))
        self.assertEqual(res.reason, "media_cap")
        self.assertEqual(res.ffmpeg_rc, 0)
        self.assertGreaterEqual(res.segments, 1)

    def test_genuine_ffmpeg_failure_is_never_a_clean_cap(self):
        # undecodable input: ffmpeg drains the pipe while probing and exits
        # nonzero at EOF. Whatever the pump reason, the rc must stay nonzero
        # so the worker treats the cycle as failed (retry), never a clean
        # boundary - a misreported rc=0 here would silently accept garbage.
        res = self._capture(_Opener(self.seg, junk=True), cap=50.0)
        self.assertNotEqual(res.ffmpeg_rc, 0)


class _FakeBuffer:
    """Minimal buffer double: no .dir, so media validity passes; segment
    numbering advances like a real segmenter's."""

    session_id = 1

    def __init__(self):
        self._seq = 0

    def next_seq(self):
        return self._seq

    def segmenter_output_args(self, start):
        self._seq = start + 3        # the cycle "wrote" 3 segments
        return ["-f", "null", "-"]

    def reconcile(self, db_path):
        return 0

    def enforce_storage_cap(self, *a, **k):
        return 0


class _Ctx:
    def __init__(self):
        self.stopping = False

    def heartbeat(self):
        pass


class WorkerCycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.addCleanup(self.td.cleanup)
        self.dir = Path(self.td.name)
        self.sleeps = []

        async def rec(seconds):
            self.sleeps.append(seconds)
        self.rec = rec
        from app.core import content_gate as cg
        cg.set_provider(lambda u: cg.Evidence(u, "GTA 6 live",
                                              time.time(), True))
        self.addCleanup(cg.set_provider, None)
        self.auth = mock.patch(
            "app.bot.source_auth.allowed", return_value=True)
        self.auth.start()
        self.addCleanup(self.auth.stop)

    def _worker(self, capture_fn, **kw):
        self.mono = [10_000.0]
        return PlaybackWorker(
            _FakeBuffer(), "chan", str(self.dir / "nova.db"), self.dir,
            "ffmpeg", resolve_fn=lambda channel, **k: object(),
            capture_fn=capture_fn, auth_poll_seconds=0.0,
            sleep=self.rec, mono=lambda: self.mono[0], **kw)

    async def _run(self, worker, ctx):
        await worker.run(ctx)

    async def test_continue_waits_only_the_true_rate_window(self):
        ctx = _Ctx()
        calls = []

        def cap(resolved, ffmpeg, out, **kw):
            calls.append(1)
            self.mono[0] += 1500.0    # the cycle "ran" a full 25 minutes
            if len(calls) >= 3:
                ctx.stopping = True
            return hls_pump.PumpResult("media_cap", 3, 6.0, 1000, 0, 1, "")

        w = self._worker(cap, resolve_interval=30.0)
        await self._run(w, ctx)
        self.assertEqual(len(calls), 3)
        # two boundaries crossed; the old fixed idle would sleep 60 s of
        # 1-second chunks. The true remaining window is ~0 after a cycle.
        self.assertLess(sum(self.sleeps), 5.0)
        self.assertEqual(w.end_reason, "stopped")

    async def test_healthy_cycle_rearms_reconnect_budget(self):
        ctx = _Ctx()
        calls = []

        def cap(resolved, ffmpeg, out, **kw):
            calls.append(1)
            self.mono[0] += 1500.0    # a long, healthy capture cycle
            if len(calls) == 1:
                raise tp.PlaybackRefused("error", "transient blip")
            if len(calls) >= 3:
                ctx.stopping = True
            return hls_pump.PumpResult("media_cap", 3, 6.0, 1000, 0, 1, "")

        w = self._worker(cap, resolve_interval=30.0,
                         healthy_reset_seconds=300.0)
        await self._run(w, ctx)
        self.assertEqual(len(calls), 3)
        # the transient failure spent one reconnect; the healthy cycle
        # after it re-armed the budget instead of letting boundary noise
        # accumulate into a session-killing "playback unstable"
        self.assertEqual(w.reconnects, 0)
        self.assertLess(w.gap_seconds, 2.0)
        self.assertEqual(w.end_reason, "stopped")

    async def test_nonzero_ffmpeg_rc_at_cap_consumes_a_reconnect(self):
        ctx = _Ctx()
        calls = []

        def cap(resolved, ffmpeg, out, **kw):
            calls.append(1)
            ctx.stopping = True     # one cycle is enough
            # rc != 0: the cycle's media is not trustworthy
            return hls_pump.PumpResult("media_cap", 3, 6.0, 1000, 1, 1, "")

        w = self._worker(cap, resolve_interval=30.0)
        await self._run(w, ctx)
        self.assertEqual(w.reconnects, 1)

    async def test_unhealthy_cycles_still_exhaust_the_budget(self):
        ctx = _Ctx()
        calls = []

        def cap(resolved, ffmpeg, out, **kw):
            calls.append(1)
            raise tp.PlaybackRefused("error", "still broken")

        w = self._worker(cap, resolve_interval=30.0, max_reconnects=2,
                         reconnect_base_seconds=1.0,
                         healthy_reset_seconds=0.0)
        with self.assertRaises(RuntimeError):
            await self._run(w, ctx)
        self.assertEqual(w.end_reason, "failed")
        self.assertIn("reconnects exhausted", w.error_text)


if __name__ == "__main__":
    unittest.main()
