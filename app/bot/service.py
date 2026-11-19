"""Phase 007/013: NOVA Clip Engine Telegram service.

Transport is the maintained python-telegram-bot framework (vendored
pure-Python wheels, Phase 007 device note in docs/DEVELOPMENT_STATUS.md).

Security posture:
- The bot token is read from the file path in NOVA_TOKEN_FILE; it never
  appears in argv, environment values, logs, or the repository.
- The administrator is established only through the one-time pairing
  challenge (app.bot.pairing), bound out-of-band through the owner's
  authenticated Instinct chat. Before pairing, the bot answers nothing
  except /pair; wrong codes get a plain failure, everything else is
  ignored silently (fail closed, no oracle for strangers).
"""
from __future__ import annotations

import os

from telegram import Update
from telegram.ext import (Application, CommandHandler, ContextTypes,
                          MessageHandler, filters)

from ..core.logging import get_logger
from .commands import CommandRouter
from .logfilter import install as install_log_filter
from .pairing import PairingStore

log = get_logger("bot.service")
install_log_filter()


class BotService:
    def __init__(self, token: str, store: PairingStore):
        self.store = store
        self.router = CommandRouter(admin_ids=[])
        self.app = Application.builder().token(token).build()
        self.app.add_handler(CommandHandler("pair", self._pair))
        self.app.add_handler(
            MessageHandler(filters.TEXT & filters.COMMAND, self._command))

    def _admin(self, user_id: int | None) -> int | None:
        admin = self.store.admin_id()
        if admin is not None and admin == user_id:
            return admin
        return None

    async def _pair(self, update: Update,
                    context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        if user is None or update.message is None:
            return
        if self.store.admin_id() is not None:
            await update.message.reply_text("Pairing is already complete.")
            return
        code = context.args[0] if context.args else ""
        if code and self.store.try_pair(code, user.id):
            log.info("owner pairing completed for user_id=%s", user.id)
            await update.message.reply_text(
                "Pairing complete. This account is now the NOVA Clip "
                "Engine administrator. Send /help to begin.")
        else:
            log.warning("failed pairing attempt user_id=%s", user.id)
            await update.message.reply_text(
                "Invalid or expired pairing code.")

    async def _command(self, update: Update,
                       context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        message = update.message
        if user is None or message is None:
            return
        admin = self._admin(user.id)
        if admin is None:
            log.warning("unauthorized command attempt user_id=%s", user.id)
            return  # fail closed: no reply, no oracle
        self.router.admin_ids = {admin}
        reply = self.router.dispatch(message.text or "", user_id=user.id)
        if reply:
            await message.reply_text(reply)

    def run(self) -> None:
        log.info("telegram service starting (long polling)")
        self.app.run_polling(drop_pending_updates=True)


def main() -> None:
    token_path = os.environ["NOVA_TOKEN_FILE"]
    state_dir = os.environ.get(
        "NOVA_STATE_DIR", os.path.expanduser("~/.nova-clip"))
    with open(token_path) as fh:
        token = fh.read().strip()
    BotService(token, PairingStore(os.path.join(state_dir, "admin.json"))).run()


if __name__ == "__main__":
    main()
