"""Intake wiring qualification: the real Telegram-facing path from a
deserialized update through the registered handlers, the admin gate,
the pre-ack inbox and the watermark into ingest_media.

These tests use the real python-telegram-bot Update model and the real
handler/filter objects BotService registers, so the wiring actually
exercised is the same code Telegram updates hit. Only the network
boundary is faked (bot.get_file/download_to_drive) and the media
pipeline (validate_recording/process_recording) is stubbed at its
module import point; everything between is production code."""
import asyncio
import os
import pathlib
import tempfile
import types
import unittest

try:
    from telegram import Update
    HAS_PTB = True
except ImportError:
    # python-telegram-bot is vendored on-device; without it the wiring
    # tests cannot run (they exist precisely to exercise the real one)
    HAS_PTB = False

from app.bot.pairing import PairingStore

if HAS_PTB:
    from app.bot.service import BotService

ADMIN_ID = 111000111  # obviously fake; never a real user id
STRANGER_ID = 12345

# Shape of a real Bot API message carrying a video (fields as returned
# by getUpdates for a video upload).
VIDEO_UPDATE_JSON = {
    "update_id": 900001,
    "message": {
        "message_id": 77,
        "date": 1763491200,
        "chat": {"id": ADMIN_ID, "type": "private",
                 "first_name": "T"},
        "from": {"id": ADMIN_ID, "is_bot": False,
                 "first_name": "T"},
        "video": {
            "file_id": "BAADBQAD_real_file_id",
            "file_unique_id": "AQAD_real",
            "width": 1920, "height": 1080, "duration": 42,
            "mime_type": "video/mp4",
            "file_size": 12_000_000,
        },
    },
}


def _service(state_dir: pathlib.Path):
    os.environ["NOVA_STATE_DIR"] = str(state_dir)
    store = PairingStore(str(state_dir / "admin.json"))
    store._write({"admin_user_id": ADMIN_ID})
    return BotService("123:fake-token-offline", store)


@unittest.skipUnless(HAS_PTB, "python-telegram-bot not installed")
class RegisteredFilterTests(unittest.TestCase):
    """The update JSON a real client sends must be claimed by exactly
    the media handler BotService registered."""

    def setUp(self):
        self.d = pathlib.Path(tempfile.mkdtemp())
        self.svc = _service(self.d)
        # the service's own bot object, built offline by the
        # Application constructor. CommandHandler needs
        # message.get_bot().username; initialize() would call getMe over
        # the network, so inject the bot user identity instead.
        from telegram import User
        self.svc.app.bot._bot_user = User(
            id=1, first_name="nova", is_bot=True,
            username="novaclip_test_bot")
        self.update = Update.de_json(VIDEO_UPDATE_JSON,
                                     bot=self.svc.app.bot)

    def _handlers_accepting(self, update):
        accepted = []
        for group in self.svc.app.handlers.values():
            for handler in group:
                result = handler.check_update(update)
                if asyncio.iscoroutine(result):
                    result = asyncio.run(result)
                if result:
                    accepted.append(handler)
        return accepted

    def test_real_video_update_deserializes_with_video_payload(self):
        msg = self.update.message
        self.assertIsNotNone(msg.video)
        self.assertEqual(msg.video.mime_type, "video/mp4")
        self.assertEqual(msg.video.file_size, 12_000_000)

    def test_exactly_one_registered_handler_claims_video_update(self):
        accepted = self._handlers_accepting(self.update)
        self.assertEqual(len(accepted), 1)
        # and it is the media handler, not the pair or command handlers
        self.assertEqual(accepted[0].callback, self.svc._media)

    def test_command_update_still_routes_to_command_handler(self):
        cmd_json = {
            "update_id": 900002,
            "message": {
                "message_id": 78, "date": 1763491200,
                "chat": {"id": ADMIN_ID, "type": "private"},
                "from": {"id": ADMIN_ID, "is_bot": False,
                         "first_name": "T"},
                "text": "/status",
                "entities": [{"type": "bot_command",
                              "offset": 0, "length": 7}],
            },
        }
        update = Update.de_json(cmd_json, bot=self.svc.app.bot)
        accepted = self._handlers_accepting(update)
        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0].callback, self.svc._command)


class _FakeFile:
    def __init__(self, payload):
        self.payload = payload

    async def download_to_drive(self, dest):
        pathlib.Path(dest).write_bytes(self.payload)


class _FakeBot:
    def __init__(self, payload=b"fake-video-bytes"):
        self.payload, self.fetched_ids = payload, []

    async def get_file(self, file_id):
        self.fetched_ids.append(file_id)
        return _FakeFile(self.payload)


def _message_with_video(update_id, user_id=ADMIN_ID):
    video = types.SimpleNamespace(
        file_id="BAADBQAD_real_file_id", mime_type="video/mp4",
        file_size=16)
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


@unittest.skipUnless(HAS_PTB, "python-telegram-bot not installed")
class MediaPathTests(unittest.TestCase):
    """_media -> ingest_media with the production guards around it.
    The media pipeline itself is stubbed at its import point in
    app.clipping.retrospective; the fake result mirrors the real
    process_recording result shape."""

    def setUp(self):
        self.d = pathlib.Path(tempfile.mkdtemp())
        self.svc = _service(self.d)
        from app.clipping import retrospective
        self.retro = retrospective
        self._orig_validate = retrospective.validate_recording
        self._orig_process = retrospective.process_recording
        self.process_calls = []

        def fake_validate(fp, src, **kw):
            assert pathlib.Path(src).exists()
            return {"duration": 42.0, "width": 1920, "height": 1080}

        def fake_process(ff, fp, src, work, db, memory, source_label,
                         **kw):
            self.process_calls.append(
                {"src": src, "work": work, "db": db,
                 "source_label": source_label})
            return {
                "status": "ok", "event_kind": "audio_spike",
                "event_confidence": 0.9,
                "clip_duration_seconds": 24.0,
                "plan": {"preset": "vertical",
                         "title": "Clutch moment",
                         "tags": ["gaming"], "watermark": "",
                         "reasons": ["loudness spike"]},
            }

        retrospective.validate_recording = fake_validate
        retrospective.process_recording = fake_process

    def tearDown(self):
        self.retro.validate_recording = self._orig_validate
        self.retro.process_recording = self._orig_process

    def _run(self, update):
        ctx = types.SimpleNamespace(bot=_FakeBot())
        asyncio.run(self.svc._media(update, ctx))
        return ctx.bot

    def test_admin_video_reaches_pipeline_and_replies(self):
        update, replies = _message_with_video(910001)
        bot = self._run(update)
        self.assertEqual(bot.fetched_ids, ["BAADBQAD_real_file_id"])
        self.assertEqual(len(self.process_calls), 1)
        # the pipeline received the file that was actually downloaded
        src = self.process_calls[0]["src"]
        self.assertTrue(src.endswith("910001.mp4"))
        self.assertEqual(pathlib.Path(src).read_bytes(),
                         b"fake-video-bytes")
        self.assertTrue(self.process_calls[0]["source_label"]
                        .startswith("owner-upload:"))
        self.assertEqual(len(replies), 1)
        self.assertIn("Clip ready", replies[0])
        self.assertIn("HELD", replies[0])
        # watermark advanced: a replayed update_id is ignored
        self.assertTrue(self.svc.watermark.already_handled(910001))
        # pre-ack inbox drained after successful handling
        self.assertEqual(self.svc.inbox.pending(), [])
        # journaled as a side-effecting op
        from app.bot import ops
        self.assertIsNotNone(
            ops.journal_result(self.d, 910001, "ingest"))

    def test_replayed_update_id_never_downloads_again(self):
        update, _ = _message_with_video(910002)
        first = self._run(update)
        self.assertEqual(len(first.fetched_ids), 1)
        update2, replies2 = _message_with_video(910002)
        second = self._run(update2)
        self.assertEqual(second.fetched_ids, [])
        self.assertEqual(len(self.process_calls), 1)
        self.assertEqual(replies2, [])

    def test_stranger_is_refused_fail_closed(self):
        update, replies = _message_with_video(910003,
                                              user_id=STRANGER_ID)
        bot = self._run(update)
        self.assertEqual(bot.fetched_ids, [])
        self.assertEqual(self.process_calls, [])
        self.assertEqual(replies, [])
        self.assertTrue(self.svc.watermark.already_handled(910003))

    def test_busy_flag_rejects_overlap_with_reply(self):
        self.svc._media_busy = True
        update, replies = _message_with_video(910004)
        bot = self._run(update)
        self.assertEqual(bot.fetched_ids, [])
        self.assertEqual(self.process_calls, [])
        self.assertEqual(len(replies), 1)
        self.assertIn("previous video", replies[0])
        self.assertTrue(self.svc.watermark.already_handled(910004))

    def test_pipeline_error_marks_failed_not_handled(self):
        def boom(*a, **kw):
            raise RuntimeError("pipeline exploded")
        self.retro.process_recording = boom
        update, _ = _message_with_video(910005)
        with self.assertRaises(RuntimeError):
            self._run(update)
        # not marked handled: a restart may retry via the inbox/journal
        self.assertFalse(self.svc.watermark.already_handled(910005))


if __name__ == "__main__":
    unittest.main()


class ServiceHandoffFfprobeTests(unittest.TestCase):
    """The service entry must start the handoff consumer with the real
    discovered device ffprobe, never the bare PATH default."""

    def test_service_wires_discovered_ffprobe(self):
        import asyncio
        from unittest import mock
        from app.bot import media as bot_media, handoff, service as svc_mod

        captured = {}

        async def fake_run_consumer(state_dir, *, poll_interval=5.0,
                                    ffprobe="ffprobe"):
            captured["ffprobe"] = ffprobe
            captured["state_dir"] = str(state_dir)
            await asyncio.sleep(3600)

        svc = object.__new__(svc_mod.BotService)
        svc._state_dir = lambda: pathlib.Path("/tmp/nonexistent-nova-state")
        with mock.patch.object(bot_media, "ffmpeg_paths",
                               return_value=("/dev/ffmpeg",
                                             "/dev/discovered-ffprobe")), \
             mock.patch.object(handoff, "run_consumer", fake_run_consumer):
            asyncio.run(svc._start_handoff_consumer(None))
            task = svc._handoff_task
            try:
                self.assertEqual(captured.get("ffprobe"),
                                 "/dev/discovered-ffprobe")
            finally:
                async def _stop():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                asyncio.run(_stop())
