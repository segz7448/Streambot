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
import sqlite3
import time
from pathlib import Path

from telegram import Update
from telegram.ext import (Application, CommandHandler, ContextTypes,
                          MessageHandler, filters)

from ..core.logging import get_logger
from .commands import CommandRouter
from .logfilter import install as install_log_filter
from .pairing import PairingStore
from .watermark import UpdateWatermark

log = get_logger("bot.service")
install_log_filter()
from .rtmpfilter import install as install_rtmp_filter
install_rtmp_filter()


class BotService:
    def __init__(self, token: str, store: PairingStore,
                 watermark: UpdateWatermark | None = None):
        self.store = store
        self.watermark = watermark or UpdateWatermark(
            str(store.path.parent / "updates.json"))
        self.router = CommandRouter(admin_ids=[])
        self.started_at = time.time()
        self.app = Application.builder().token(token).build()
        self.app.add_handler(CommandHandler("pair", self._pair))
        self.app.add_handler(
            MessageHandler(filters.TEXT & filters.COMMAND, self._command))
        self.router.register("status", self._status,
                             "application health")
        self.router.register("storage", self._storage,
                             "storage consumption")
        self.router.register("sources", self._sources,
                             "list registered channels")
        self.router.register("clipnow", self._clipnow,
                             "create a Twitch clip via the official API "
                             "(requires configured Twitch OAuth)")
        self.router.register("destinations", self._destinations,
                             "list/add/remove clip delivery destinations: "
                             "/destinations [add <chat_id> <label>|remove <chat_id>]")
        self.router.register("relay", self._relay,
                             "restream a feed you own to RTMP destinations: "
                             "/relay start <feed_url> <rtmp_dest>...")
        self.router.register("stoprelay", self._stoprelay,
                             "stop the running restream")

    def _relay(self, cmd) -> str:
        from . import ops
        return ops.relay_reply(self._state_dir(), cmd.args)

    def _stoprelay(self, cmd) -> str:
        from . import ops
        return ops.stoprelay_reply(self._state_dir())

    def _state_dir(self) -> Path:
        return Path(os.environ.get("NOVA_STATE_DIR",
                                   os.path.expanduser("~/.nova-clip")))

    def _clipnow(self, cmd) -> str:
        from . import ops
        return ops.clipnow_reply(self._state_dir(), cmd.args)

    def _destinations(self, cmd) -> str:
        from . import ops
        return ops.destinations_reply(self._state_dir(), cmd.args)

    # --- real handler implementations (registered => advertised) ---

    def _status(self, _cmd) -> str:
        uptime = int(time.time() - self.started_at)
        handled, failed = self.watermark.snapshot()
        admin = "paired" if self.store.admin_id() is not None else "unpaired"
        return (f"NOVA Clip Engine status\n"
                f"bot: running ({uptime}s uptime, {admin})\n"
                f"updates handled: {handled}, failed pending: {len(failed)}\n"
                f"pipeline: capture idle - no Twitch source authorized yet")

    def _storage(self, _cmd) -> str:
        state = Path(os.environ.get("NOVA_STATE_DIR",
                                    os.path.expanduser("~/.nova-clip")))
        total = sum(f.stat().st_size for f in state.rglob("*")
                    if f.is_file()) if state.exists() else 0
        return f"State directory usage: {total / 1024:.1f} KiB ({state})"

    def _sources(self, _cmd) -> str:
        db = Path(os.environ.get("NOVA_STATE_DIR",
                                 os.path.expanduser("~/.nova-clip"))) / "nova.db"
        try:
            conn = sqlite3.connect(str(db))
            rows = conn.execute(
                "SELECT username FROM twitch_sources ORDER BY username"
            ).fetchall()
            conn.close()
        except sqlite3.Error:
            rows = []
        if not rows:
            return "No channels registered yet."
        return "Registered channels:\n" + "\n".join(r[0] for r in rows)

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
        if self.watermark.already_handled(update.update_id):
            return
        try:
            if self.store.admin_id() is not None:
                await update.message.reply_text("Pairing is already complete.")
            else:
                code = context.args[0] if context.args else ""
                if not code:
                    # Missing argument is not an invalid code: say what
                    # is actually required so the owner can complete the
                    # one copy action.
                    await update.message.reply_text(
                        "Pairing needs the full command with its code: "
                        "/pair <code>. Get the current code from the "
                        "chat where you received it.")
                elif self.store.try_pair(code, user.id):
                    log.info("owner pairing completed for user_id=%s", user.id)
                    await update.message.reply_text(
                        "Pairing complete. This account is now the NOVA Clip "
                        "Engine administrator. Send /help to begin.")
                else:
                    log.warning("failed pairing attempt user_id=%s", user.id)
                    await update.message.reply_text(
                        "Invalid or expired pairing code.")
        except Exception:
            self.watermark.mark_failed(update.update_id)
            raise
        self.watermark.mark_handled(update.update_id)

    async def _command(self, update: Update,
                       context: ContextTypes.DEFAULT_TYPE) -> None:
        user = update.effective_user
        message = update.message
        if user is None or message is None:
            return
        if self.watermark.already_handled(update.update_id):
            return
        admin = self._admin(user.id)
        if admin is None:
            log.warning("unauthorized command attempt user_id=%s", user.id)
            self.watermark.mark_handled(update.update_id)
            return  # fail closed: no reply, no oracle
        try:
            self.router.admin_ids = {admin}
            reply = self.router.dispatch(message.text or "", user_id=user.id)
            if reply:
                await message.reply_text(reply)
        except Exception:
            self.watermark.mark_failed(update.update_id)
            raise
        self.watermark.mark_handled(update.update_id)

    def run(self) -> None:
        log.info("telegram service starting (long polling)")
        # keep the pending backlog: commands sent during a restart must be
        # processed, not discarded; the watermark makes replays idempotent
        self.app.run_polling(drop_pending_updates=False)


def main() -> None:
    token_path = os.environ["NOVA_TOKEN_FILE"]
    state_dir = os.environ.get(
        "NOVA_STATE_DIR", os.path.expanduser("~/.nova-clip"))
    with open(token_path) as fh:
        token = fh.read().strip()
    BotService(token, PairingStore(os.path.join(state_dir, "admin.json"))).run()


if __name__ == "__main__":
    main()
