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
import asyncio
import sqlite3
import time
from pathlib import Path

from telegram import Update
from telegram.ext import (Application, CommandHandler, ContextTypes,
                          MessageHandler, filters)

from ..core.logging import get_logger
from .commands import CommandRouter
from .inbox import PreAckInbox
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
        self.inbox = PreAckInbox(str(store.path.parent / "inbox.json"))
        self.router = CommandRouter(admin_ids=[])
        self.started_at = time.time()
        self.app = Application.builder().token(token).build()
        self.app.add_handler(CommandHandler("pair", self._pair))
        self.app.add_handler(
            MessageHandler(filters.TEXT & filters.COMMAND, self._command))
        self.app.add_handler(MessageHandler(
            (filters.VIDEO | filters.Document.ALL) & ~filters.COMMAND,
            self._media, block=False))
        # one video at a time: processing is heavy on this host. A plain
        # flag, not Semaphore.locked()+acquire: check-and-set with no
        # await in between is atomic on the single event loop, so an
        # overload is REJECTED with a busy reply instead of queueing an
        # unbounded waiter inside acquire().
        self._media_busy = False
        # true pre-ack: wrap the update queue's put so command updates
        # are persisted before the framework advances its offset
        from . import ops as _ops
        queue = self.app.update_queue
        queue.put = _ops.make_pre_ack_wrapper(self.inbox, self.watermark,
                                              queue.put,
                                              self.store.admin_id)
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
        from . import commands_full as cf
        self.router.register("addsource",
                             lambda c: cf.addsource_reply(self._state_dir(), c.args),
                             "add a Twitch channel: /addsource <username>")
        self.router.register("removesource",
                             lambda c: cf.removesource_reply(self._state_dir(), c.args),
                             "remove a channel: /removesource <username>")
        self.router.register("checklive",
                             lambda c: cf.checklive_reply(self._state_dir(), c.args),
                             "check if a channel is live: /checklive <username>")
        self.router.register("startclip",
                             lambda c: self._startclip(c.args),
                             "start an authorized clipping session over "
                             "the source's registered feed")
        self.router.register("stopclip",
                             lambda c: self._stopclip(),
                             "stop active clipping sessions")
        self.router.register("sessions",
                             lambda c: self._sessions(),
                             "list clipping sessions")
        self.router.register("clips",
                             lambda c: cf.clips_reply(self._state_dir(), c.args),
                             "browse generated clips")
        self.router.register("clipdetails",
                             lambda c: cf.clipdetails_reply(self._state_dir(), c.args),
                             "clip info: /clipdetails <id>")
        self.router.register("settings",
                             lambda c: cf.settings_reply(self._state_dir(), c.args),
                             "view/set configuration")
        self.router.register("quality",
                             lambda c: cf.quality_reply(self._state_dir(), c.args),
                             "output quality preset")
        self.router.register("subtitles",
                             lambda c: cf.SUBTITLES_REPLY,
                             "subtitles (out of scope per owner)")
        self.router.register("logs",
                             lambda c: cf.logs_reply(self._state_dir(), c.args),
                             "recent application errors")
        self.router.register("restart",
                             lambda c: cf.RESTART_REPLY,
                             "restart workers (host-controlled)")
        self.router.register("deliveries", self._deliveries,
                             "list held clips; /deliveries flush sends "
                             "them to you")
        self.router.register("youtubeauth",
                             lambda c: cf.youtubeauth_reply(self._state_dir(), c.args),
                             "YouTube consent run: /youtubeauth [status]")
        self.router.register("publish",
                             lambda c: cf.publish_reply(self._state_dir(), c.args, c.update_id),
                             "upload a clip to YouTube as PRIVATE: "
                             "/publish <id> [title]")

    def _relay(self, cmd) -> str:
        from . import ops
        return ops.relay_reply(self._state_dir(), cmd.args, cmd.update_id)

    def _stoprelay(self, cmd) -> str:
        from . import ops
        return ops.stoprelay_reply(self._state_dir())

    def _state_dir(self) -> Path:
        return Path(os.environ.get("NOVA_STATE_DIR",
                                   os.path.expanduser("~/.nova-clip")))

    def _clipnow(self, cmd) -> str:
        from . import ops
        return ops.clipnow_reply(self._state_dir(), cmd.args, cmd.update_id)

    def _destinations(self, cmd) -> str:
        from . import ops
        return ops.destinations_reply(self._state_dir(), cmd.args)

    def _deliveries(self, cmd) -> str:
        from . import ops
        return ops.deliveries_reply(self._state_dir(), cmd.args)

    def _startclip(self, args: str) -> str:
        from . import media as bot_media
        from . import sessions as sess
        ff, fp = bot_media.ffmpeg_paths()
        return sess.startclip_reply(self._state_dir(), args, ff, fp)

    def _stopclip(self) -> str:
        from . import sessions as sess
        return sess.stopclip_reply(self._state_dir())

    def _sessions(self) -> str:
        from . import sessions as sess
        return sess.sessions_reply(self._state_dir())

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
        self.inbox.remove(update.update_id)
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
            # defense in depth: the wrapper never persists non-admin
            # commands, but remove any stale entry from older builds so
            # a later drain can never touch it
            self.inbox.remove(update.update_id)
            self.watermark.mark_handled(update.update_id)
            return  # fail closed: no reply, no oracle
        try:
            self.router.admin_ids = {admin}
            if not self.inbox.add(update.update_id, user.id,
                                  message.text or ""):
                # inbox full: fail CLOSED - never execute an unpersisted
                # command. The refusal is recorded in the overflow file
                # for reconciliation; the admin is told it did not run.
                self.watermark.mark_handled(update.update_id)
                await message.reply_text(
                    "The command inbox is full, so your command was NOT "
                    "executed (it was recorded for reconciliation). "
                    "Please try again later.")
                return
            # dispatch off the event loop: feed validation resolves DNS
            # synchronously and must never stall command handling
            reply = await asyncio.to_thread(
                self.router.dispatch, message.text or "",
                user_id=user.id, update_id=update.update_id)
            if reply:
                await message.reply_text(reply)
            self.inbox.remove(update.update_id)
        except Exception as exc:
            # entry STAYS in the inbox: redelivery is not guaranteed
            # once the offset advanced, so the startup drain retries it
            self.watermark.mark_failed(update.update_id)
            from . import commands_full as cf
            cf.record_error(self._state_dir(),
                            f"command failed: {exc.__class__.__name__}")
            raise
        self.watermark.mark_handled(update.update_id)

    async def _media(self, update: Update,
                     context: ContextTypes.DEFAULT_TYPE) -> None:
        """Mode C: the admin sends a video; the retrospective pipeline
        clips it. Fail-closed for strangers like _command."""
        user = update.effective_user
        message = update.message
        if user is None or message is None:
            return
        if self.watermark.already_handled(update.update_id):
            return
        admin = self._admin(user.id)
        if admin is None:
            log.warning("unauthorized media attempt user_id=%s", user.id)
            self.inbox.remove(update.update_id)
            self.watermark.mark_handled(update.update_id)
            return
        try:
            if self._media_busy:
                self.inbox.remove(update.update_id)
                self.watermark.mark_handled(update.update_id)
                await message.reply_text(
                    "I'm still processing the previous video. Send this "
                    "one again when it's done.")
                return
            self._media_busy = True
            try:
                from . import media as bot_media
                reply = await bot_media.ingest_media(
                    update, context.bot, self._state_dir())
            finally:
                self._media_busy = False
            if reply:
                await message.reply_text(reply)
            self.inbox.remove(update.update_id)
        except Exception:
            self.watermark.mark_failed(update.update_id)
            from . import commands_full as cf
            cf.record_error(self._state_dir(), "media ingest failed")
            raise
        self.watermark.mark_handled(update.update_id)

    def drain_inbox(self, reply=None) -> dict:
        from . import ops
        if reply is None:
            reply = lambda _uid, _text: None
        return ops.drain_inbox(self._state_dir(), self.router,
                               self.watermark, self.inbox, reply,
                               admin_id=self.store.admin_id())

    def run(self) -> None:
        log.info("telegram service starting (long polling)")
        # single-instance enforcement: crash recovery assumes exactly one
        # writer owns this state dir. A second poller refuses to start.
        import fcntl
        import os as _os
        lock_fd = _os.open(str(self._state_dir() / "service.lock"),
                           _os.O_CREAT | _os.O_RDWR, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise SystemExit(
                "another NOVA service instance holds the state lock; "
                "refusing to start a second poller")
        self._lock_fd = lock_fd  # held for the life of the process
        from . import sessions as _sess_mod
        from ..pipeline.session import recover_interrupted_sessions
        recovered = recover_interrupted_sessions(self._state_dir())
        if recovered:
            log.warning("marked %d crashed session(s) INTERRUPTED",
                        recovered)
        self.drain_inbox()
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
