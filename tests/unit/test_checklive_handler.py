"""Handler-level /checklive through the real BotService._command path
(admin auth, pre-ack inbox, router dispatch, reply). Boundary: this does NOT
cross Telegram's network transport; the framework object is built but no
update is fetched from Telegram."""
import asyncio
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from tests.unit.test_pair_handler import _Msg  # also installs the PTB stub if needed
from app.bot.pairing import PairingStore
from app.bot.service import BotService
from app.bot.watermark import UpdateWatermark


def _update(uid, update_id, text, user_id):
    m = _Msg()
    m.text = text
    return types.SimpleNamespace(update_id=update_id, effective_user=types.SimpleNamespace(id=user_id), message=m)


class ChecklivePathTests(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.dir = Path(self.td.name)
        env = mock.patch.dict("os.environ", {"NOVA_STATE_DIR": str(self.dir)})
        env.start(); self.addCleanup(env.stop)
        self.store = PairingStore(str(self.dir / "admin.json"))
        self.store.begin_challenge("abcdefgh")
        self.svc = BotService("token", self.store,
                              watermark=UpdateWatermark(str(self.dir / "updates.json")))
        ctx = types.SimpleNamespace(args=["abcdefgh"])
        asyncio.run(self.svc._pair(_update(0, 1, "/pair abcdefgh", 12345), ctx))
        self.assertEqual(self.store.admin_id(), 12345)

    def tearDown(self):
        self.td.cleanup()

    def run_cmd(self, text, update_id, user_id=12345):
        u = _update(0, update_id, text, user_id)
        asyncio.run(self.svc._command(u, types.SimpleNamespace(args=[])))
        return u.message.replies

    def test_admin_checklive_typed_channel_returns_status_through_service(self):
        with mock.patch("app.ingestion.twitch_playback.live_status", return_value=("offline", "no live playlist")) as m:
            replies = self.run_cmd("/checklive ishowspeed", 10)
        m.assert_called_once_with("ishowspeed")
        self.assertEqual(len(replies), 1)
        self.assertIn("ishowspeed: OFFLINE or not found", replies[0])
        self.assertNotIn("developer application", replies[0])

    def test_non_admin_gets_no_reply_and_no_network(self):
        with mock.patch("app.ingestion.twitch_playback.live_status") as m:
            replies = self.run_cmd("/checklive ishowspeed", 11, user_id=999)
        self.assertEqual(replies, [])
        m.assert_not_called()


if __name__ == "__main__":
    unittest.main()
