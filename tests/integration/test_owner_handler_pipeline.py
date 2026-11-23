"""Owner-handler seam: the REAL BotService._media handler (admin auth
gate, busy flag, pre-ack inbox, watermark) invokes the REAL
ingest_media wrapper and the REAL retrospective pipeline, with the
delivery preference explicitly OFF in disposable state.

test_media_wiring covers this routing with the pipeline stubbed;
test_owner_media_pipeline covers the wrapper x real pipeline without
the handler. This file closes the remaining composition: handler/auth
+ wrapper + real pipeline together. Only the Telegram network boundary
(get_file / download / reply_text) is faked. Skips without ffmpeg or
python-telegram-bot.
"""
import asyncio
import pathlib
import shutil
import sqlite3
import subprocess
import tempfile
import types
import unittest

from tests.unit.test_media_wiring import (
    ADMIN_ID, STRANGER_ID, HAS_PTB, _service, _FakeBot)
from tests.integration.test_schedule_loop_ops import _close_ptb_requests

FFMPEG = shutil.which("ffmpeg")
FFPROBE = shutil.which("ffprobe")


def _make_fixture(path: pathlib.Path) -> None:
    subprocess.run(
        [FFMPEG, "-y", "-f", "lavfi", "-i",
         "testsrc2=size=320x240:rate=15:duration=8",
         "-f", "lavfi", "-i",
         "sine=frequency=600:duration=8,"
         "volume='if(between(t,4,6),1.0,0.03)':eval=frame",
         "-c:v", "libx264", "-pix_fmt", "yuv420p",
         "-c:a", "aac", "-shortest", str(path)],
        capture_output=True, timeout=120, check=True)


def _update_with_real_video(update_id, size, user_id=ADMIN_ID):
    video = types.SimpleNamespace(
        file_id="BAADBQAD_real_file_id", mime_type="video/mp4",
        file_size=size)
    replies = []

    async def reply_text(text, **kw):
        replies.append(text)

    message = types.SimpleNamespace(video=video, document=None,
                                    reply_text=reply_text)
    update = types.SimpleNamespace(
        update_id=update_id,
        effective_user=types.SimpleNamespace(id=user_id),
        message=message)
    return update, replies


@unittest.skipUnless(FFMPEG and FFPROBE and HAS_PTB,
                     "ffmpeg or python-telegram-bot not available")
class OwnerHandlerPipelineTests(unittest.TestCase):
    def setUp(self):
        self.d = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.d, True)
        self.fixture = self.d / "owner-video.mp4"
        _make_fixture(self.fixture)
        from app.core.database import connect, migrate
        conn = connect(str(self.d / "nova.db"))
        migrate(conn)
        conn.close()
        self.svc = _service(self.d)
        self.addCleanup(_close_ptb_requests, self.svc.app.bot)
        # OFF fixture: the owner preference is held, so no delivery
        # attempt may happen even though ingest_media receives no
        # deliver_fn through the production handler (None = real sender)
        from app.bot import ops
        ops.set_auto_deliver(self.d, False)

    def _run(self, update, payload):
        ctx = types.SimpleNamespace(bot=_FakeBot(payload))
        asyncio.run(self.svc._media(update, ctx))
        return ctx.bot

    def test_admin_video_full_chain_held_no_send(self):
        payload = self.fixture.read_bytes()
        update, replies = _update_with_real_video(920001, len(payload))
        bot = self._run(update, payload)
        self.assertEqual(bot.fetched_ids, ["BAADBQAD_real_file_id"])
        self.assertEqual(len(replies), 1)
        self.assertIn("HELD", replies[0])
        self.assertNotIn("Delivered automatically", replies[0])
        conn = sqlite3.connect(str(self.d / "nova.db"))
        rows = conn.execute(
            "SELECT status, delivered_at FROM clips").fetchall()
        conn.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], ("HELD", None))
        self.assertTrue(self.svc.watermark.already_handled(920001))
        self.assertEqual(self.svc.inbox.pending(), [])
        # replay at the handler level: watermark refuses before any work
        update2, replies2 = _update_with_real_video(920001, len(payload))
        bot2 = self._run(update2, payload)
        self.assertEqual(bot2.fetched_ids, [])
        self.assertEqual(replies2, [])
        conn = sqlite3.connect(str(self.d / "nova.db"))
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM clips").fetchone()[0], 1)
        conn.close()

    def test_stranger_refused_before_pipeline_with_real_modules(self):
        payload = self.fixture.read_bytes()
        update, replies = _update_with_real_video(
            920002, len(payload), user_id=STRANGER_ID)
        bot = self._run(update, payload)
        self.assertEqual(bot.fetched_ids, [])
        self.assertEqual(replies, [])
        self.assertTrue(self.svc.watermark.already_handled(920002))
        conn = sqlite3.connect(str(self.d / "nova.db"))
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM clips").fetchone()[0], 0)
        conn.close()

    def test_busy_flag_holds_with_real_pipeline(self):
        self.svc._media_busy = True
        payload = self.fixture.read_bytes()
        update, replies = _update_with_real_video(920003, len(payload))
        bot = self._run(update, payload)
        self.assertEqual(bot.fetched_ids, [])
        self.assertEqual(len(replies), 1)
        self.assertIn("previous video", replies[0])
        conn = sqlite3.connect(str(self.d / "nova.db"))
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM clips").fetchone()[0], 0)
        conn.close()


if __name__ == "__main__":
    unittest.main()
