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


def _tok_src() -> str:
    from ..core.tokensrc import source
    try:
        return source()
    except Exception:
        return "INVALID (empty vault value)"


class BotService:
    def __init__(self, token: str, store: PairingStore,
                 watermark: UpdateWatermark | None = None):
        from .pollwatch import PollWatch, PollWatchedBot
        self.store = store
        self.watermark = watermark or UpdateWatermark(
            str(store.path.parent / "updates.json"))
        self.inbox = PreAckInbox(str(store.path.parent / "inbox.json"))
        self.router = CommandRouter(admin_ids=[])
        self.started_at = time.time()
        # polling-liveness record: positive proof past "Application started"
        # that getUpdates succeeds, without logging any content
        self.poll_watch = PollWatch()
        self.app = Application.builder() \
            .bot(PollWatchedBot(self.poll_watch, token=token)) \
            .post_init(self._start_handoff_consumer) \
            .post_shutdown(self._stop_handoff_consumer).build()
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
        self.router.register("clipmark", self._clipmark,
                             "mark the current moment in an active "
                             "recording: /clipmark <user> [seconds-ago]")
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
        self.router.register("startplayback",
                             lambda c: self._startplayback(c.args),
                             "capture a streamer's public live playback "
                             "(needs record authorization)")
        self.router.register("sourceauth",
                             lambda c: self._sourceauth(c.args),
                             "per-streamer record/repost authorization: "
                             "/sourceauth <user> [record|repost] [on|off]")
        self.router.register("stopclip",
                             lambda c: self._stopclip(),
                             "stop active clipping sessions")
        self.router.register("recoverclips",
                             lambda c: self._recoverclips(),
                             "finish clips left by interrupted sessions "
                             "to HELD for review (nothing delivered)")
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
        from . import pause as _pause_mod
        from . import schedule_cmds as sc
        for _n, _f, _d in (
            ("timezone", sc.timezone_reply, "schedule timezone: /timezone [IANA name]"),
            ("schedule", sc.schedule_reply, "schedule a clipping window: /schedule <user> <today|tomorrow|date> <HH:MM> <hours>"),
            ("schedules", sc.schedules_reply, "list scheduled windows"),
            ("unschedule", sc.unschedule_reply, "cancel a scheduled window: /unschedule <id>"),
        ):
            self.router.register(
                _n, (lambda f: lambda c: f(self._state_dir(), c.args))(_f), _d)
        from . import youtube_views as yv
        for _n, _f, _d in (
            ("youtube", yv.youtube_reply, "YouTube publishing overview (read-only)"),
            ("youtubestatus", yv.youtubestatus_reply, "YouTube credential/auto-upload status (read-only)"),
            ("publishqueue", yv.publishqueue_reply, "upload jobs and uncertain uploads (read-only)"),
            ("publishhistory", yv.publishhistory_reply, "confirmed uploads (read-only)"),
            ("autopublish", _pause_mod.autopublish_reply, "automatic upload status / pause / resume (private only)"),
        ):
            self.router.register(
                _n, (lambda f: lambda c: f(self._state_dir(), c.args))(_f), _d)
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

    def _content_qualified(self, user):
        # the shared GTA6 title gate; no title source ships, so it is closed
        from ..core import content_gate
        return content_gate.qualified(user)

    def _state_dir(self) -> Path:
        return Path(os.environ.get("NOVA_STATE_DIR",
                                   os.path.expanduser("~/.nova-clip")))

    def _clipnow(self, cmd) -> str:
        from . import ops
        return ops.clipnow_reply(self._state_dir(), cmd.args, cmd.update_id)

    def _clipmark(self, cmd) -> str:
        from . import manual_mark
        return manual_mark.clipmark_reply(
            self._state_dir(), cmd.args, cmd.update_id)

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

    def _startplayback(self, args: str) -> str:
        from . import media as bot_media
        from . import sessions as sess
        ff, fp = bot_media.ffmpeg_paths()
        return sess.startplayback_reply(self._state_dir(), args, ff, fp)

    def _sourceauth(self, args: str) -> str:
        from . import source_auth
        return source_auth.sourceauth_reply(self._state_dir(), args)

    def _recoverclips(self) -> str:
        # quick ack only: the tracked background run does the work, so a
        # long edit never holds the single-concurrency command loop
        # behind it (stopclip/sourceauth/status stay responsive)
        from . import media as bot_media
        from . import sessions as sess
        ff, fp = bot_media.ffmpeg_paths()
        return sess.recoverclips_reply_tracked(self._state_dir(), ff,
                                               fp, notify=self._notify)

    def _notify(self, text: str) -> None:
        # the durable notice is already queued atomically with the run
        # outcome (sessions.recovery_run_finish); this only wakes the
        # notice consumer for prompt delivery. Best-effort, safe from
        # the worker thread, never raises.
        loop = getattr(self, "_loop", None)
        wake = getattr(self, "_notice_wake", None)
        if loop is None or wake is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(wake.set)
        except Exception:
            log.warning("notice consumer wake failed")

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
        poll = self.poll_watch.snapshot()
        if poll["last_ok_at"] is None:
            polling = ("polling: NO successful getUpdates yet "
                       f"({poll['error_count']} error(s)"
                       + (f", last {poll['last_error_kind']}"
                          if poll["last_error_kind"] else "") + ")")
        else:
            polling = (f"polling: last getUpdates ok "
                       f"{int(poll['last_ok_age'])}s ago "
                       f"({poll['ok_count']} ok, {poll['error_count']} error(s), "
                       f"{poll['updates_received']} update(s) in, "
                       f"{poll['outbound_sent']} message(s) out)")
        return (f"NOVA Clip Engine status\n"
                f"bot: running ({uptime}s uptime, {admin})\n"
                f"updates handled: {handled}, failed pending: {len(failed)}\n"
                f"{polling}\n"
                f"token source: {_tok_src()}\n"
                f"{self._pipeline_status()}")

    def _pipeline_status(self) -> str:
        """Read-only snapshot; DB state is recorded state, not writer proof."""
        from . import sessions
        active = sessions.active_status()
        db = self._state_dir() / "nova.db"
        try:
            conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=1)
            try:
                authorized = conn.execute(
                    "SELECT COUNT(*) FROM twitch_sources s JOIN settings t"
                    " ON t.key='srcauth:' || s.username || ':record'"
                    " WHERE t.value='1'").fetchone()[0]
                recorded = conn.execute(
                    "SELECT COUNT(*) FROM stream_sessions WHERE state IN"
                    " ('CONNECTING','RECORDING','ANALYZING','CLIPPING',"
                    " 'EDITING','QUALITY_CHECK')").fetchone()[0]
            finally:
                conn.close()
        except sqlite3.Error:
            return (f"pipeline: {active}; source authorization and recorded "
                    "session state unavailable (database unreadable)")
        return (f"pipeline: {active}; {authorized} source(s) record-authorized; "
                f"{recorded} recorded active session row(s) "
                "(not process-liveness proof)")

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
                from .chunk import chunks as _chunks
                for _part in _chunks(reply):
                    await message.reply_text(_part)
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
                if reply:
                    await message.reply_text(reply)
                self.inbox.remove(update.update_id)
            finally:
                # covers ingest AND the reply send: an exception in
                # either must never strand the busy flag
                self._media_busy = False
        except Exception:
            self.watermark.mark_failed(update.update_id)
            from . import commands_full as cf
            cf.record_error(self._state_dir(), "media ingest failed")
            raise
        self.watermark.mark_handled(update.update_id)

    async def _start_handoff_consumer(self, _app) -> None:
        from . import handoff, media as _media
        # Real device media tools: the relocated termux prefix (or the
        # NOVA_FFPROBE override), never a bare PATH guess.
        _, ffprobe_path = _media.ffmpeg_paths()
        self._loop = asyncio.get_running_loop()
        self._notice_wake = asyncio.Event()
        from . import sessions as _sess
        # truthfully close out any recovery run cut off by the last
        # stop and queue its notice; this NEVER replays recovery work
        _sess.reconcile_recovery_runs(self._state_dir())
        self._handoff_task = asyncio.create_task(
            handoff.run_consumer(self._state_dir(),
                                 ffprobe=ffprobe_path))
        self._sched_task = asyncio.create_task(self._schedule_loop())
        self._pollwatch_task = asyncio.create_task(self._pollwatch_loop())
        if not self._retro_diag_disabled():
            self._retro_diag_task = asyncio.create_task(self._retro_diag_loop())

    def _retro_diag_disabled(self) -> bool:
        from ..diag import retro_export
        try:
            return retro_export.consumed(str(self._state_dir()))
        except Exception:
            return True  # unreadable fence state: never auto-run

    async def _retro_diag_loop(self, interval: float = 2.0) -> None:
        """ONE-OFF diagnostic export: at most one request, ever, and
        only while the global consumed fence is clear. The heavy work
        is one owned child (retro_export.run_request) with its own
        fixed deadline; after any outcome this task disables itself -
        an UNCERTAIN outcome keeps the fence and needs human review,
        never an automatic retry."""
        from ..diag import retro_export
        while True:
            await asyncio.sleep(interval)
            if getattr(self, "_retro_diag_stopping", False):
                return
            if self._retro_diag_disabled():
                return
            try:
                if (self._state_dir() / retro_export.REQUEST_DIR
                        / retro_export.REQUEST_FILE).exists():
                    ev = getattr(self, "_retro_diag_stop_event", None)
                    if ev is None:
                        import threading as _th
                        ev = self._retro_diag_stop_event = _th.Event()
                    result = await self._blocking(
                        retro_export.run_request, str(self._state_dir()),
                        ev)  # positional: _blocking is (fn, *args) only
                    log.info("retro diag outcome: %s", result.get("status"))
                    return  # one-off: never a second request
            except retro_export.RequestError as exc:
                log.warning("retro diag request refused: %s", exc)
            except Exception:
                log.exception("retro diag loop error")

    async def _pollwatch_loop(self, interval: float = 300.0) -> None:
        """Periodic content-free heartbeat: positive proof in the logs that
        getUpdates still succeeds (or that it has stopped). Counts and ages
        only; never tokens, URLs, payloads, chat ids or message text."""
        while True:
            await asyncio.sleep(interval)
            poll = self.poll_watch.snapshot()
            handled, _failed = self.watermark.snapshot()
            if poll["last_ok_at"] is None:
                log.warning("telegram polling NOT yet proven: no successful "
                            "getUpdates since start (errors=%d last=%s)",
                            poll["error_count"], poll["last_error_kind"])
            else:
                log.info("telegram polling alive: last getUpdates ok %.0fs "
                         "ago (ok=%d err=%d updates=%d outbound=%d handled=%d)",
                         poll["last_ok_age"], poll["ok_count"],
                         poll["error_count"], poll["updates_received"],
                         poll["outbound_sent"], handled)

    @staticmethod
    async def _blocking(fn, *args):
        """Run fn in a thread and ALWAYS wait for it to finish, even if this
        coroutine is cancelled: a schedule transition must never keep
        running after shutdown has returned."""
        fut = asyncio.ensure_future(asyncio.to_thread(fn, *args))
        cancelled = False
        while True:
            try:
                result = await asyncio.shield(fut)
                break
            except asyncio.CancelledError:
                cancelled = True
                if fut.done():
                    fut.exception()
                    break
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _schedule_loop(self) -> None:
        """Advance owner-scheduled plans and deliver durable notices.
        Sessions only start through allowed paths (registered feed, or
        public playback with record auth ON)."""
        from ..clipping.retrospective import validate_recording
        from ..core import schedule as _sched
        from . import sessions as _sess
        from . import source_auth as _auth
        from . import media as _m
        sd = self._state_dir()
        ff, fp = _m.ffmpeg_paths()

        def valid(path):
            try:
                validate_recording(fp, path)
                return True
            except Exception:
                return False
        ops = _sched.Ops(
            start=lambda u, a: _sess.start_scheduled(sd, u, ff, fp, a),
            stop=_sess.stop_session, running=_sess.session_running,
            alive=_sess.session_alive, stopping=_sess.session_stopping,
            confirm=lambda a: _sched.confirm_recording(sd, a, valid),
            error=_sess.session_error,
            writer_active=lambda u: _sess.writer_active(sd, u),
            # positive proof only: the session's own drained attestation.
            # Proof-based recovery via restartproof is DISABLED in
            # production: process-visibility completeness on Android is
            # unresolved, so no proof OR leg may terminate a window here.
            terminated=lambda u, a: _sess.session_terminated(sd, u, a),
            terminal=lambda u, a: _sess.session_terminal(u, a, sd),
            content_qualified=lambda u: self._content_qualified(u),
            record_allowed=lambda u: _auth.allowed(sd, u, "record"))
        while True:
            try:
                await self._blocking(_sched.tick, sd, ops)
                admin = self.store.admin_id()
                while admin is not None:
                    claimed = await self._blocking(_sched.claim_notice, sd)
                    if claimed is None:
                        break
                    nid, text = claimed
                    try:
                        await self.app.bot.send_message(admin, text)
                    except asyncio.CancelledError:
                        raise      # stays "sending": reconcile labels the repeat
                    except Exception:
                        log.exception("schedule notice send failed; stays queued")
                        await self._blocking(_sched.release_notice, sd, nid)
                        break
                    await self._blocking(_sched.mark_notice_sent, sd, nid)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("schedule tick failed")
            wake = getattr(self, "_notice_wake", None)
            if wake is not None:
                wake.clear()
            if wake is None:
                await asyncio.sleep(30)
            else:
                try:
                    await asyncio.wait_for(wake.wait(), 30)
                except asyncio.TimeoutError:
                    pass

    @staticmethod
    def _cancel_and_drain(tracker) -> None:
        # ownership is retained until the worker ACTUALLY exits: a
        # drain timeout only means we keep cancelling and keep waiting,
        # never that we return with live recovery work behind us
        tracker.request_cancel()
        warned = False
        while not tracker.wait_done():
            if not warned:
                log.error("recovery worker outlived the first shutdown "
                          "drain; still waiting - shutdown is not done "
                          "while recovery work is alive")
                warned = True
            tracker.request_cancel()

    async def _stop_handoff_consumer(self, _app) -> None:
        # shutdown-active child proof for the one-off diagnostic: cancel
        # its registered group, then drain the task so no diag worker
        # survives service shutdown unproven
        try:
            from ..diag import retro_export
            state_dir = str(self._state_dir())
            # stop flag BEFORE cancel: the poller must not claim or
            # spawn anything new once shutdown begins; the shared event
            # is checked inside run_request before claim and under the
            # launch lock before Popen, and CANCEL is durable
            self._retro_diag_stopping = True
            ev = getattr(self, "_retro_diag_stop_event", None)
            if ev is not None:
                ev.set()
            cancel_result = await self._blocking(
                retro_export.cancel_active, state_dir, "service shutdown")
            if isinstance(cancel_result, dict) and (
                    cancel_result.get("cancel_record_error")
                    or cancel_result.get("uncertain")):
                log.error("retro diag shutdown cancel incomplete: %s",
                          cancel_result)
            # drain = the controller task reaches a TERMINAL state,
            # not a fixed wait that ignores a pending thread
            task = getattr(self, "_retro_diag_task", None)
            if task is not None and not task.done():
                await asyncio.wait([task], timeout=30)
            if task is not None and not task.done():
                # unknown controller state: the claim fence and
                # in-progress record are retained; deployment stop
                # clearance must treat this as NOT cleanly stopped
                log.error("retro diag controller NOT drained during "
                          "shutdown; fence retained, state UNCERTAIN - "
                          "blocking clean-stop clearance")
            else:
                proof_note = await self._blocking(
                    retro_export.watchdog_check, state_dir)
                if proof_note:
                    log.warning("retro diag shutdown state: %s", proof_note)
        except Exception as exc:
            log.error("retro diag shutdown handling failed: %s", exc)
        from . import sessions as _sess
        tracker = _sess.recovery_tracker()
        # whole-sequence ownership under cancellation AND errors: every
        # owned phase (recovery drain, then consumer teardown) runs to
        # genuine completion behind a shield - a failing cancel request,
        # drain step or confirmation is NEVER treated as worker
        # completion, a parent cancellation is remembered and re-raised
        # only after ALL cleanup, and a retained phase error never
        # displaces that cancellation
        cancelled = False

        def _drain_owned() -> None:
            try:
                self._cancel_and_drain(tracker)
                return
            except Exception as exc:
                first_error = exc
                log.error("recovery drain failed during shutdown: %s; "
                          "still retaining worker ownership", exc)
            # the drain raised before confirming the worker exited:
            # ownership continues - keep cancelling and keep waiting,
            # and a failure to cancel or to CONFIRM is logged once and
            # retried, never treated as completion
            warned = False
            while True:
                try:
                    tracker.request_cancel()
                except Exception as exc:
                    if not warned:
                        warned = True
                        log.error("recovery cancel request failed during "
                                  "shutdown drain: %s; still retaining "
                                  "worker ownership", exc)
                try:
                    if tracker.wait_done():
                        break
                except Exception as exc:
                    if not warned:
                        warned = True
                        log.error("recovery drain confirmation failed "
                                  "during shutdown: %s; still retaining "
                                  "worker ownership", exc)
            raise first_error

        async def _teardown() -> None:
            # ONLY this inner coroutine cancels and awaits the owned
            # consumers, so a CancelledError at its own awaits is always
            # the child's own expected cancellation - and nothing ever
            # cancels the task wrapping it, which lets the shielded wait
            # below read ANY CancelledError as a real parent request
            error = None
            for name in ("_sched_task", "_pollwatch_task",
                         "_handoff_task"):
                task = getattr(self, name, None)
                if task is None:
                    continue
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass              # the child's expected cancellation
                except Exception as exc:
                    # retain cleanup ownership: record and continue with
                    # the remaining owned consumers; surfaced afterwards
                    if error is None:
                        error = exc
                    log.error("consumer %s cleanup failed during "
                              "shutdown: %s", name, exc)
            if error is not None:
                raise error

        async def _await_owned(phase):
            # shielded wait that NEVER cancels the inner phase task, so
            # a CancelledError here is by construction a REAL parent
            # request (possibly coincident with phase completion):
            # remember it and keep waiting until the phase genuinely
            # finishes; a phase error is RETURNED, never allowed to
            # escape past a remembered cancellation
            nonlocal cancelled
            while True:
                try:
                    await asyncio.shield(phase)
                    return None
                except asyncio.CancelledError:
                    cancelled = True
                    if phase.done():
                        return phase.exception()
                except Exception as exc:
                    return exc

        drain_error = await _await_owned(
            asyncio.ensure_future(self._blocking(_drain_owned)))
        teardown_error = await _await_owned(
            asyncio.ensure_future(_teardown()))
        # one consistent outcome: a remembered parent cancellation wins
        # over every retained phase error; the drain error (first owned
        # phase) surfaces before a teardown error
        if cancelled:
            raise asyncio.CancelledError
        if drain_error is not None:
            raise drain_error
        if teardown_error is not None:
            raise teardown_error

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
        from ..distribution import publish_records as _pubrec
        _pubrec.promote_legacy(self._state_dir())
        from . import sessions as _sess_mod
        from ..pipeline.session import recover_interrupted_sessions
        recovered = recover_interrupted_sessions(self._state_dir())
        if recovered:
            log.warning("marked %d crashed session(s) INTERRUPTED",
                        recovered)
        # Boot-time clip finalization is DISABLED in production: it is
        # proof-based recovery over interrupted sessions, and process-
        # visibility completeness on Android is unresolved, so no
        # recovery/overwrite path may run here. finalize_interrupted_
        # sessions is NOT run here: recovery only runs on the
        # owner's explicit /recoverclips command; interrupted
        # sessions keep their PENDING clips fenced until then.
        from ..core import schedule as _schedmod
        from . import sessions as _s2
        # restartproof proof OR leg removed here as well (see above):
        # interrupted windows end MISSED only on the session's own
        # positive attestation
        _n = _schedmod.reconcile(
            self._state_dir(),
            terminated=lambda u, a: _s2.session_terminated(
                self._state_dir(), u, a),
            terminal=lambda u, a: _s2.session_terminal(u, a, self._state_dir()))
        if _n:
            log.warning("ended %d interrupted schedule window(s) MISSED", _n)
        from . import handoff as _handoff
        requeued = _handoff.recover(self._state_dir())
        if requeued:
            log.warning("requeued %d interrupted handoff job(s)", requeued)
        stranded = _pubrec.recover_interrupted(self._state_dir())
        if stranded:
            log.warning("marked %d stranded publish reservation(s) "
                        "UNCERTAIN (reconcile, never blind-retry)",
                        stranded)
        self.drain_inbox()
        # keep the pending backlog: commands sent during a restart must be
        # processed, not discarded; the watermark makes replays idempotent
        self.app.run_polling(drop_pending_updates=False)


def main() -> None:
    from ..core.tokensrc import read_token
    state_dir = os.environ.get(
        "NOVA_STATE_DIR", os.path.expanduser("~/.nova-clip"))
    token = read_token()
    from ..core.logging import setup_service_logging
    setup_service_logging(os.path.join(state_dir, "logs"))
    log.info("service process starting pid=%d", os.getpid())
    # durable death diagnostics: heartbeat + breadcrumbs + faulthandler.
    # SIGKILL/host kills still leave no trace; see app.core.diagnostics.
    from ..core import diagnostics
    diagnostics.install(state_dir)
    if os.environ.get("NOVA_TITLE_SOURCE", "").strip().lower() != "off":
        # unofficial anonymous live-title read (owner-requested player route);
        # NOVA_TITLE_SOURCE=off keeps the gate closed. Read-only, no capture.
        from ..core import content_gate
        from ..ingestion import twitch_title
        content_gate.set_provider(twitch_title.provider)
        log.info("live-title source: anonymous player route (unofficial)")
    BotService(token, PairingStore(os.path.join(state_dir, "admin.json"))).run()


if __name__ == "__main__":
    main()
