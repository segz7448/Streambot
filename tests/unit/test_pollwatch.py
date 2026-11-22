"""PollWatch / PollWatchedBot: typed, content-free polling liveness.

No Telegram transport: Bot API methods are replaced with AsyncMocks, so the
only code under test is the recording wrapper. Asserts counts/ages only and
that no payload text is ever recorded.
"""
import asyncio
import types
import unittest
from tempfile import TemporaryDirectory
from unittest import mock

import telegram
from telegram.error import TimedOut

from app.bot.pairing import PairingStore
from app.bot.pollwatch import PollWatch, PollWatchedBot
from app.bot.service import BotService
from app.bot.watermark import UpdateWatermark
from tests.unit.test_pair_handler import _Msg


class WatchUnitTests(unittest.TestCase):
    def setUp(self):
        self.t = [1000.0]
        self.w = PollWatch(clock=lambda: self.t[0])

    def test_ok_counts_updates_and_age(self):
        self.w.note_poll_ok(0)          # an EMPTY successful poll is proof
        self.t[0] += 5
        self.w.note_poll_ok(3)
        self.t[0] += 10
        snap = self.w.snapshot()
        self.assertEqual(snap["ok_count"], 2)
        self.assertEqual(snap["updates_received"], 3)
        self.assertEqual(snap["last_ok_age"], 10.0)

    def test_error_records_class_name_only(self):
        self.w.note_poll_error("TimedOut")
        self.w.note_poll_error("NetworkError")
        snap = self.w.snapshot()
        self.assertEqual(snap["error_count"], 2)
        self.assertEqual(snap["last_error_kind"], "NetworkError")
        self.assertIsNone(snap["last_ok_at"])

    def test_outbound_count(self):
        self.w.note_outbound()
        self.w.note_outbound()
        self.assertEqual(self.w.snapshot()["outbound_sent"], 2)


class WatchedBotTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.w = PollWatch()
        self.bot = PollWatchedBot(self.w, token="123:abc")

    async def test_get_updates_success_including_empty(self):
        with mock.patch.object(telegram.Bot, "get_updates",
                               new=mock.AsyncMock(return_value=[])):
            self.assertEqual(await self.bot.get_updates(), [])
        snap = self.w.snapshot()
        self.assertEqual(snap["ok_count"], 1)
        self.assertEqual(snap["updates_received"], 0)

    async def test_get_updates_with_updates(self):
        with mock.patch.object(telegram.Bot, "get_updates",
                               new=mock.AsyncMock(return_value=[1, 2])):
            await self.bot.get_updates()
        self.assertEqual(self.w.snapshot()["updates_received"], 2)

    async def test_get_updates_error_propagates_and_records_kind(self):
        with mock.patch.object(telegram.Bot, "get_updates",
                               new=mock.AsyncMock(side_effect=TimedOut())):
            with self.assertRaises(TimedOut):
                await self.bot.get_updates()
        snap = self.w.snapshot()
        self.assertEqual(snap["error_count"], 1)
        self.assertEqual(snap["last_error_kind"], "TimedOut")
        self.assertIsNone(snap["last_ok_at"])

    async def test_cancellation_is_not_an_error(self):
        with mock.patch.object(telegram.Bot, "get_updates",
                               new=mock.AsyncMock(
                                   side_effect=asyncio.CancelledError())):
            with self.assertRaises(asyncio.CancelledError):
                await self.bot.get_updates()
        snap = self.w.snapshot()
        self.assertEqual(snap["error_count"], 0)
        self.assertEqual(snap["ok_count"], 0)

    async def test_send_message_and_video_counted(self):
        msg = types.SimpleNamespace(message_id=7)
        with mock.patch.object(telegram.Bot, "send_message",
                               new=mock.AsyncMock(return_value=msg)), \
                mock.patch.object(telegram.Bot, "send_video",
                                  new=mock.AsyncMock(return_value=msg)):
            await self.bot.send_message(1, "x")
            await self.bot.send_video(1, b"v")
        self.assertEqual(self.w.snapshot()["outbound_sent"], 2)


class ServiceWiringTests(unittest.TestCase):
    """The real BotService polls through a PollWatchedBot and surfaces the
    watch in /status and the heartbeat log. No network."""

    def setUp(self):
        self.td = TemporaryDirectory()
        from pathlib import Path
        self.dir = Path(self.td.name)
        self.addCleanup(self.td.cleanup)
        self.store = PairingStore(str(self.dir / "admin.json"))
        self.svc = BotService("token", self.store,
                              watermark=UpdateWatermark(
                                  str(self.dir / "updates.json")))

    def test_application_bot_is_watched(self):
        self.assertIsInstance(self.svc.app.bot, PollWatchedBot)
        self.assertIs(self.svc.app.bot._poll_watch, self.svc.poll_watch)

    def test_status_reports_unproven_polling(self):
        text = self.svc._status(None)
        self.assertIn("polling: NO successful getUpdates yet", text)

    def test_status_reports_last_ok_age(self):
        self.svc.poll_watch.note_poll_ok(0)
        text = self.svc._status(None)
        self.assertIn("polling: last getUpdates ok", text)
        self.assertIn("1 ok, 0 error(s)", text)

    def test_heartbeat_logs_alive_line_then_cancels(self):
        self.svc.poll_watch.note_poll_ok(0)

        sleepy = mock.AsyncMock(side_effect=[None, asyncio.CancelledError()])
        with mock.patch("app.bot.service.asyncio.sleep", sleepy), \
                self.assertLogs("nova.bot.service", level="INFO") as cm, \
                self.assertRaises(asyncio.CancelledError):
            asyncio.run(self.svc._pollwatch_loop(interval=0))
        self.assertTrue(any("telegram polling alive" in r
                            and "last getUpdates ok" in r
                            for r in cm.output), cm.output)

    def test_heartbeat_warns_when_unproven(self):
        sleepy = mock.AsyncMock(side_effect=[None, asyncio.CancelledError()])
        with mock.patch("app.bot.service.asyncio.sleep", sleepy), \
                self.assertLogs("nova.bot.service", level="WARNING") as cm, \
                self.assertRaises(asyncio.CancelledError):
            asyncio.run(self.svc._pollwatch_loop(interval=0))
        self.assertTrue(any("NOT yet proven" in r for r in cm.output))

    def test_no_content_fields_in_snapshot(self):
        self.svc.poll_watch.note_poll_ok(2)
        self.svc.poll_watch.note_poll_error("TimedOut")
        snap = self.svc.poll_watch.snapshot()
        for key in snap:
            self.assertNotIn(key.lower(),
                             ("token", "url", "query", "payload", "chat",
                              "text", "message"))
        self.assertNotIn("123:abc", repr(snap))


if __name__ == "__main__":
    unittest.main()
