"""Owner-media integration: the REAL owner-facing wrapper
(app.bot.media.ingest_media - journal, bounds, download, validate)
drives the REAL retrospective pipeline (process_recording with real
ffmpeg detection -> extraction -> edit -> QC) through BOTH delivery
preference modes, with replay protection at the wrapper level.

Existing coverage deliberately leaves this seam open: test_media_wiring
stubs the pipeline, test_mode_c_command_path calls process_recording
directly without the wrapper. This file closes the wrapper x real
pipeline product locally. Only the Telegram boundary (get_file /
download / deliver_fn) is faked; there is no network, no source
contact, no real send. Skips without ffmpeg.
"""
import asyncio
import shutil
import sqlite3
import subprocess
import tempfile
import types
import unittest
from pathlib import Path

FFMPEG = shutil.which("ffmpeg")
FFPROBE = shutil.which("ffprobe")


def _make_fixture(path: Path) -> None:
    """8s h264/aac: quiet, 2s loud moment at t=4, quiet - the real audio
    analysis sees a genuine spike (same shape as mode_c's fixture)."""
    subprocess.run(
        [FFMPEG, "-y", "-f", "lavfi", "-i",
         "testsrc2=size=320x240:rate=15:duration=8",
         "-f", "lavfi", "-i",
         "sine=frequency=600:duration=8,"
         "volume='if(between(t,4,6),1.0,0.03)':eval=frame",
         "-c:v", "libx264", "-pix_fmt", "yuv420p",
         "-c:a", "aac", "-shortest", str(path)],
        capture_output=True, timeout=120, check=True)


def _fake_update_and_bot(fixture: Path, update_id: int):
    """Minimal stand-ins at the Telegram boundary only. get_file is
    counted so replay tests can prove no second download."""
    calls = {"get_file": 0}

    class _TgFile:
        async def download_to_drive(self, dest):
            shutil.copyfile(fixture, dest)

    class _Bot:
        async def get_file(self, file_id):
            calls["get_file"] += 1
            return _TgFile()

    video = types.SimpleNamespace(
        file_id="fake-file-id", mime_type="video/mp4",
        file_size=fixture.stat().st_size)
    update = types.SimpleNamespace(
        update_id=update_id,
        message=types.SimpleNamespace(video=video, document=None))
    return update, _Bot(), calls


def _probe_streams(path: Path) -> list:
    out = subprocess.run(
        [FFPROBE, "-v", "quiet", "-show_entries", "stream=codec_type",
         "-of", "csv=p=0", str(path)], capture_output=True, text=True,
        timeout=60)
    return [ln.strip() for ln in out.stdout.strip().splitlines() if ln.strip()]


@unittest.skipUnless(FFMPEG and FFPROBE, "ffmpeg not available")
class OwnerMediaPipelineTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.fixture = self.dir / "owner-video.mp4"
        _make_fixture(self.fixture)
        from app.core.database import connect, migrate
        conn = connect(str(self.dir / "nova.db"))
        migrate(conn)
        conn.close()

    def _ingest(self, update_id=424242, deliver_fn=None):
        from app.bot import media
        update, bot, calls = _fake_update_and_bot(self.fixture, update_id)
        # ffmpeg resolution goes through the production helper
        text = asyncio.run(media.ingest_media(
            update, bot, self.dir, deliver_fn=deliver_fn))
        return text, calls

    def _clip_row(self):
        conn = sqlite3.connect(str(self.dir / "nova.db"))
        row = conn.execute(
            "SELECT id, status, delivered_at, file_path FROM clips").fetchone()
        conn.close()
        return row

    def test_auto_deliver_off_holds_and_never_sends(self):
        from app.bot import ops
        ops.set_auto_deliver(self.dir, False)
        sent = []
        text, calls = self._ingest(
            deliver_fn=lambda path, caption: sent.append((path, caption)) or 42)
        self.assertIn("HELD", text)
        self.assertNotIn("Delivered automatically", text)
        self.assertEqual(sent, [], "no send may happen with preference off")
        self.assertEqual(calls["get_file"], 1)
        clip_id, status, delivered_at, file_path = self._clip_row()
        self.assertEqual(status, "HELD")
        self.assertIsNone(delivered_at)
        edited = Path(file_path)
        self.assertTrue(edited.is_file())
        # acceptance limit: NO subtitles - the output must not even
        # carry a subtitle stream
        self.assertNotIn("subtitle", _probe_streams(edited))
        self.assertIn("video", _probe_streams(edited))
        self.assertIn("audio", _probe_streams(edited))
        # journal persisted with the clip binding
        from app.bot import ops as _ops
        cached = _ops.journal_result(self.dir, 424242, "ingest")
        self.assertIsNotNone(cached)
        self.assertIn(f"[clip:{clip_id}]", cached)
        # replay of the same update: no re-download, no reprocess,
        # no send - the recorded result is returned
        sent2 = []
        text2, calls2 = self._ingest(
            deliver_fn=lambda path, caption: sent2.append(path) or 43)
        self.assertIn("Already processed", text2)
        self.assertEqual(calls2["get_file"], 0)
        self.assertEqual(sent2, [])
        conn = sqlite3.connect(str(self.dir / "nova.db"))
        self.assertEqual(conn.execute("SELECT count(*) FROM clips").fetchone()[0], 1)
        conn.close()

    def test_auto_deliver_on_delivers_exactly_once_through_journal(self):
        sent = []
        text, calls = self._ingest(
            deliver_fn=lambda path, caption: sent.append((path, caption)) or 42)
        self.assertIn("Clip ready and sent to you.", text)
        self.assertEqual(len(sent), 1)
        clip_id, status, delivered_at, file_path = self._clip_row()
        self.assertEqual(status, "DELIVERED")
        self.assertIsNotNone(delivered_at)
        # replay: resumes only the journaled delivery path, which is
        # idempotent per clip - no second send, no reprocess
        text2, calls2 = self._ingest(
            deliver_fn=lambda path, caption: sent.append((path, caption)) or 43)
        self.assertIn("Already processed", text2)
        self.assertEqual(calls2["get_file"], 0)
        self.assertEqual(len(sent), 1, "replay must not resend a delivered clip")
        conn = sqlite3.connect(str(self.dir / "nova.db"))
        self.assertEqual(conn.execute("SELECT count(*) FROM clips").fetchone()[0], 1)
        conn.close()

    def test_status_and_deliveries_truthful_about_held_clip(self):
        import types as _t
        from app.bot import ops
        from app.bot.service import BotService
        from app.bot import sessions
        ops.set_auto_deliver(self.dir, False)
        text, _ = self._ingest()
        self.assertIn("HELD", text)
        # /deliveries lists the held clip truthfully
        listing = ops.deliveries_reply(self.dir, "")
        self.assertIn("Held clips", listing)
        # /status pipeline section: no fabricated session, no claim of
        # liveness proof; the held clip is not presented as delivered
        svc = _t.SimpleNamespace(_state_dir=lambda: self.dir)
        import unittest.mock as _m
        with _m.patch.dict(sessions._sessions, {}, clear=True):
            status = BotService._pipeline_status(svc)
        self.assertIn("no in-process session registered", status)
        self.assertNotIn("delivered", status.lower().split("session")[0])
        # flush through the real command path delivers exactly once
        sent = []
        flush = ops.deliveries_reply(
            self.dir, "flush",
            deliver_fn=lambda path, caption: sent.append(path) or 42)
        self.assertIn("Delivered 1", flush)
        self.assertEqual(len(sent), 1)
        conn = sqlite3.connect(str(self.dir / "nova.db"))
        self.assertEqual(conn.execute(
            "SELECT status FROM clips").fetchone()[0], "DELIVERED")
        conn.close()
        # a second flush has nothing to send and says so truthfully
        again = ops.deliveries_reply(
            self.dir, "flush",
            deliver_fn=lambda path, caption: sent.append(path) or 43)
        self.assertNotIn("Delivered 1", again)
        self.assertEqual(len(sent), 1)


if __name__ == "__main__":
    unittest.main()
