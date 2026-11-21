"""Handler-level tests for BotService._pair with a stubbed telegram
framework (PTB is vendored on-device only, so the real framework cannot
be imported in local tests). Covers the three reply branches."""
import asyncio
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


def _stub_telegram():
    tg = types.ModuleType("telegram")
    tgext = types.ModuleType("telegram.ext")

    class Update:  # noqa: D401 - stub
        pass

    class _Builder:
        def token(self, _t):
            return self

        def post_init(self, _cb):
            return self

        def post_shutdown(self, _cb):
            return self

        def build(self):
            return types.SimpleNamespace(
                add_handler=lambda *a, **k: None,
                update_queue=types.SimpleNamespace(put=None))

    class Application:
        @staticmethod
        def builder():
            return _Builder()

    class CommandHandler:
        def __init__(self, *a, **k):
            pass

    class MessageHandler:
        def __init__(self, *a, **k):
            pass

    class _F:
        def __and__(self, other):
            return self

        def __or__(self, other):
            return self

        def __invert__(self):
            return self

    class _Filters:
        TEXT = _F()
        COMMAND = _F()
        VIDEO = _F()
        Document = types.SimpleNamespace(ALL=_F(), VIDEO=_F())

    tg.Update = Update
    tgext.Application = Application
    tgext.CommandHandler = CommandHandler
    tgext.MessageHandler = MessageHandler
    tgext.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
    tgext.filters = _Filters()
    sys.modules["telegram"] = tg
    sys.modules["telegram.ext"] = tgext


try:
    import telegram  # noqa: F401
    import telegram.ext  # noqa: F401
except ImportError:
    # PTB is vendored on-device only; where it is not installed, stub
    # it. Where it IS installed, never clobber sys.modules: other test
    # modules in the same process use the real framework.
    _stub_telegram()

from app.bot.pairing import PairingStore  # noqa: E402
from app.bot.service import BotService  # noqa: E402
from app.bot.watermark import UpdateWatermark  # noqa: E402


class _Msg:
    def __init__(self):
        self.replies = []

    async def reply_text(self, text):
        self.replies.append(text)


class _User:
    id = 12345


def _update(update_id=1):
    u = types.SimpleNamespace()
    u.update_id = update_id
    u.effective_user = _User()
    u.message = _Msg()
    return u


def _ctx(*args):
    return types.SimpleNamespace(args=list(args))


class PairHandlerTests(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.store = PairingStore(str(Path(self.td.name) / "admin.json"))
        self.wm = UpdateWatermark(str(Path(self.td.name) / "updates.json"))
        self.svc = BotService("token", self.store, watermark=self.wm)

    def tearDown(self):
        self.td.cleanup()

    def _run(self, coro):
        return asyncio.run(coro)

    def test_missing_arg_asks_for_full_command_not_invalid(self):
        self.store.begin_challenge("abcdefgh")
        u = _update()
        self._run(self.svc._pair(u, _ctx()))
        self.assertEqual(len(u.message.replies), 1)
        self.assertIn("/pair <code>", u.message.replies[0])
        self.assertNotIn("Invalid or expired", u.message.replies[0])
        # challenge must NOT be consumed by a no-arg attempt
        self.assertIsNotNone(self.store._read().get("pending"))

    def test_wrong_code_says_invalid(self):
        self.store.begin_challenge("abcdefgh")
        u = _update()
        self._run(self.svc._pair(u, _ctx("wrongcod")))
        self.assertIn("Invalid or expired", u.message.replies[0])

    def test_correct_code_binds_admin(self):
        self.store.begin_challenge("abcdefgh")
        u = _update()
        self._run(self.svc._pair(u, _ctx("abcdefgh")))
        self.assertEqual(self.store.admin_id(), 12345)
        self.assertIn("Pairing complete", u.message.replies[0])


if __name__ == "__main__":
    unittest.main()
