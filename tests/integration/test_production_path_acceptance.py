"""Local production-path acceptance: hls_pump capture -> real buffer
segments -> candidate detection -> auto-clip extraction (sealed) ->
finish (edit + QC -> HELD).

Every step runs the REAL deployed code with real ffmpeg media processing:
the only doubles are the in-memory HLS opener (no network, no source
contact) and the auth/gate seams. This proves the automatic
capture-to-QC chain end to end on any machine with ffmpeg; device
viability still needs the operator's bounded phone window.
"""
import asyncio
import json
import subprocess
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from app.core import content_gate as cg
from app.core.database import connect, migrate
from app.ingestion import twitch_playback as tp
from app.recording.buffer import RollingBuffer
from app.recording.playback_worker import PlaybackWorker


def make_segment(path: Path, loud: bool, seconds: float = 2.0) -> bytes:
    """2s testsrc + sine; the loud variant is ~30dB above the quiet ones
    so the real audio analysis sees a genuine loudness spike."""
    vol = "28dB" if loud else "-8dB"
    subprocess.run(
        ["ffmpeg", "-hide_banner", "-loglevel", "error",
         "-f", "lavfi", "-i", "testsrc=size=640x360:rate=15",
         "-f", "lavfi", "-i", "sine=frequency=440",
         "-af", f"volume={vol}", "-t", str(seconds),
         "-c:v", "libx264", "-g", "15", "-pix_fmt", "yuv420p",
         "-c:a", "aac", "-f", "mpegts", str(path)], check=True)
    return path.read_bytes()


class _PlaylistOpener:
    """Serves one finite HLS playlist (#EXT-X-ENDLIST) whose segments are
    real generated MPEG-TS files; per-fetch responses mirror
    tp.default_opener's (status, bytes) contract."""

    def __init__(self, segments: list[bytes]):
        self.segments = segments
        self.fetches = 0

    def __call__(self, req, timeout, limit, deadline, should_stop):
        url = req.full_url
        if url.endswith("v.m3u8"):
            self.fetches += 1
            lines = ["#EXTM3U", "#EXT-X-TARGETDURATION:2",
                     "#EXT-X-MEDIA-SEQUENCE:0"]
            for i in range(len(self.segments)):
                lines.append("#EXTINF:2.0,")
                lines.append(f"seg{i}.ts")
            lines.append("#EXT-X-ENDLIST")
            return 200, "\n".join(lines).encode()
        idx = int(url.rsplit("seg", 1)[1].split(".")[0])
        return 200, self.segments[idx]


def _resolved() -> tp.Resolved:
    v = tp.Variant("360p", 360, 1_000_000, "https://fake/v.m3u8")
    return tp.Resolved("chan", v, (v,), time.time())


class _Ctx:
    stopping = False

    def heartbeat(self):
        pass


class ProductionPathAcceptance(unittest.TestCase):
    def test_capture_to_held_real_media(self):
        td = TemporaryDirectory()
        self.addCleanup(td.cleanup)
        root = Path(td.name)
        state_dir = root / "state"
        work = root / "sessions" / "chan"
        work.mkdir(parents=True)
        state_dir.mkdir()
        (state_dir / "runtime").mkdir()
        db_path = str(state_dir / "nova.db")
        conn = connect(db_path)
        migrate(conn)
        conn.execute(
            "INSERT INTO twitch_sources (username, created_at, updated_at)"
            " VALUES ('chan', 0, 0)")
        cur = conn.execute(
            "INSERT INTO stream_sessions (source_id, state, started_at,"
            " ingest_mode, notes) VALUES (1, 'STARTING', ?, 'playback',"
            " '{}')", (time.time(),))
        sid = cur.lastrowid
        conn.commit()
        conn.close()

        quiet = make_segment(root / "q.ts", loud=False)
        loud = make_segment(root / "l.ts", loud=True)
        # quiet, quiet, LOUD, quiet: a real spike inside captured media
        opener = _PlaylistOpener([quiet, quiet, loud, quiet])

        buf = RollingBuffer(str(work), session_id=sid, segment_seconds=2,
                            pre_event_seconds=2, post_event_seconds=2,
                            max_storage_mb=64)
        cg.set_provider(lambda u: cg.Evidence(u, "GTA 6 live",
                                              time.time(), True))
        self.addCleanup(cg.set_provider, None)
        auth = mock.patch("app.bot.source_auth.allowed", return_value=True)
        auth.start()
        self.addCleanup(auth.stop)

        worker = PlaybackWorker(
            buf, "chan", db_path, state_dir, "ffmpeg",
            resolve_fn=lambda channel, **k: _resolved(),
            opener=opener, host_ok=lambda host: True,
            auth_poll_seconds=0.0, ffprobe="ffprobe")

        async def run():
            await worker.run(_Ctx())
        asyncio.run(run())
        self.assertEqual(worker.end_reason, "ended", worker.error_text)
        self.assertGreaterEqual(worker.captured_segments, 3)
        segs = buf.segments()
        self.assertGreaterEqual(len(segs), 3)
        for s in segs[:3]:
            self.assertGreater(s.path.stat().st_size, 1000)

        # real detection over the captured media: the loud segment must
        # produce at least one audio_spike candidate
        from app.detection.detector import CandidateDetector
        detector = CandidateDetector("ffmpeg", buf, db_path)
        detector.process_new_segments(include_newest=True)
        conn = connect(db_path)
        events = conn.execute(
            "SELECT id, kind, confidence FROM candidate_events"
            " WHERE session_id=? AND status='PENDING'", (sid,)).fetchall()
        conn.close()
        kinds = [e[1] for e in events]
        self.assertIn("audio_spike", kinds,
                      f"no audio spike detected in real captured media: "
                      f"{kinds}")

        # real auto-clip: extraction with durable seal
        from app.clipping.auto import AutoClipper
        from app.intelligence.providers import HeuristicProvider
        clipper = AutoClipper(buf, db_path, HeuristicProvider(), "ffmpeg",
                              str(work), pre_seconds=2, post_seconds=2)
        produced = clipper.process_pending()
        self.assertGreaterEqual(produced, 1, "no clip produced from a "
                                             "real detected candidate")
        conn = connect(db_path)
        rows = conn.execute(
            "SELECT id, file_path, status, sealed_sha256 FROM clips"
            " WHERE session_id=?", (sid,)).fetchall()
        self.assertTrue(rows)
        for clip_id, path, status, seal in rows:
            self.assertEqual(status, "PENDING")
            self.assertIsNotNone(seal, "clip input not sealed")
            self.assertTrue(Path(path).exists())
        conn.close()

        # real finish: plan -> edit -> QC -> durable HELD
        from app.pipeline.session import finish_pending_clips
        conn = connect(db_path)
        held = finish_pending_clips(
            conn, state_dir=state_dir, session_id=sid, source_label="chan",
            out_dir=work / "edited", runtime_dir=state_dir / "runtime",
            ffmpeg="ffmpeg", ffprobe="ffprobe", auto_deliver=False)
        self.assertEqual(sorted(held), sorted(r[0] for r in rows))
        for clip_id in held:
            status, qc_json, edited = conn.execute(
                "SELECT status, qc_report_json, file_path FROM clips"
                " WHERE id=?", (clip_id,)).fetchone()
            self.assertEqual(status, "HELD")
            self.assertTrue(json.loads(qc_json)["qc_passed"])
            self.assertTrue(Path(edited).exists())
            from app.qc.verify import check_media
            qc = check_media("ffprobe", "ffmpeg", edited)
            self.assertTrue(qc.ok, qc.issues)
        conn.close()
        print(f"PRODUCTION_PATH: capture->spike->sealed->edit->QC->HELD ok "
              f"({len(held)} clip(s), end={worker.end_reason})")


if __name__ == "__main__":
    unittest.main()
