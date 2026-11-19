"""Pure-stdlib command logic for the Telegram service.

Kept free of telegram-framework imports so it is unit-testable off the
phone; app.bot.service handlers delegate here.
"""
from __future__ import annotations

from pathlib import Path

from ..distribution.relay import (DestinationStore, Relayer,
                                  RestreamManager, secret_in_rtmp_url)
from .inbox import PreAckInbox
from .watermark import UpdateWatermark
from ..ingestion.twitch_clips import ClipFailed, TwitchClipsClient

UNCONFIGURED_CLIP = (
    "Manual clipping is not configured. It requires a Twitch developer "
    "application and your OAuth consent (clips:edit). File download "
    "additionally requires you to be the broadcaster or a channel "
    "editor. Until those are in place this command cannot create clips.")

USAGE_DESTINATIONS = ("Usage: /destinations "
                      "[add <chat_id> <label> | remove <chat_id>]")

USAGE_RELAY = ("Usage: /relay start <feed_url> <rtmp_dest> [rtmp_dest...] - "
               "feed must be one you own or are authorized to restream")


def relay_reply(state_dir: Path, args: str, update_id: int = 0) -> str:
    """Start an RTMP restream of an explicitly owner-provided feed.
    Secret-bearing RTMP destinations are REFUSED: stream keys must not
    appear in ffmpeg process arguments on this shared host (same-UID
    processes can read /proc cmdline), and no safe credential transport
    is implemented yet. Only keyless destinations (path is empty or
    "/") or local file outputs are accepted. Journaled against replay."""
    cached = journal_result(state_dir, update_id, "relay")
    if cached is not None:
        return "Already processed (replay guard): " + cached
    parts = args.split()
    if len(parts) < 3 or parts[0] != "start":
        return USAGE_RELAY
    feed, dests = parts[1], parts[2:]
    for dest in dests:
        if secret_in_rtmp_url(dest):
            return ("Refused: that RTMP destination embeds a stream key, "
                    "and keys in process arguments are exposed to other "
                    "workloads on this host. Secret-bearing restream is "
                    "disabled until a safe credential transport exists. "
                    "Your key was not used or stored.")
    relayer = Relayer(DestinationStore(str(state_dir / "destinations.json")),
                      lambda c, p, t: 0)
    cmds = relayer.restream(feed, dests,
                            ffmpeg_bin=str(
                                Path.home() / "instinct-clip/prefix/bin/ffmpeg"))
    manager = RestreamManager(str(state_dir / "restream.pids"))
    if manager.active():
        return "A restream is already running. Use /stoprelay first."
    pids = manager.start(cmds)
    result = (f"Restreaming your feed to {len(pids)} destination(s). "
              "/stoprelay stops it.")
    journal_record(state_dir, update_id, "relay", result)
    return result


def stoprelay_reply(state_dir: Path) -> str:
    stopped = RestreamManager(str(state_dir / "restream.pids")).stop()
    if stopped:
        return f"Restream stopped ({stopped} process(es))."
    return "No restream is running."


def make_pre_ack_wrapper(inbox, watermark, orig_put):
    """TRUE pre-ack durability: wraps the framework update queue's put,
    persisting each command update BEFORE it is queued. The framework
    advances its offset only after queueing, and Telegram's server-side
    ack happens on the NEXT getUpdates call - so a persisted update can
    never be acked past without an inbox entry, and the startup drain
    replays anything not marked handled. The framework queue window is
    closed by construction."""
    async def put(update):
        try:
            text = (update.message.text
                    if getattr(update, "message", None) else "") or ""
            if text.startswith("/"):
                uid = int(update.update_id)
                if not watermark.already_handled(uid):
                    user = getattr(update, "effective_user", None)
                    inbox.add(uid, int(user.id) if user else 0, text)
        except Exception:
            import logging
            logging.getLogger("bot.ops").exception(
                "pre-ack inbox write failed")
        return await orig_put(update)
    return put


JOURNAL = "op_journal.json"


def _journal_read(state_dir: Path) -> dict:
    import json
    try:
        return json.loads((state_dir / JOURNAL).read_text())
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return {}


SIDE_EFFECTING = ("clipnow", "relay")


def journal_result(state_dir: Path, update_id: int, command: str):
    """Stored result of an already-executed side-effecting command, or
    None. Gives replayed commands operation-level idempotency: a drain
    replay returns the recorded result instead of repeating the side
    effect (a second Twitch clip, a second restream)."""
    if not update_id:
        return None
    entry = _journal_read(state_dir).get(str(update_id))
    if entry and entry.get("command") == command:
        return entry.get("result")
    return None


def journal_mark_uncertain(state_dir: Path, update_id: int,
                           command: str, text: str) -> None:
    """A side-effecting command stranded WITHOUT a journal record is
    uncertain: its external effect may or may not have happened (the
    success-before-journal-commit ambiguity). Blind retry can duplicate
    the effect, so the operation is preserved for reconciliation and
    reported, not re-executed."""
    import json
    import os as _os
    if not update_id:
        return
    data = _journal_read(state_dir)
    data[str(update_id)] = {"command": command, "status": "uncertain",
                            "text": text}
    path = state_dir / JOURNAL
    fd = _os.open(path, _os.O_WRONLY | _os.O_CREAT | _os.O_TRUNC, 0o600)
    with _os.fdopen(fd, "w") as fh:
        json.dump(data, fh)


def journal_uncertain(state_dir: Path) -> list[dict]:
    return [{"update_id": int(k), **v} for k, v in
            _journal_read(state_dir).items()
            if v.get("status") == "uncertain"]


def journal_record(state_dir: Path, update_id: int, command: str,
                   result: str) -> None:
    import json
    import os as _os
    if not update_id:
        return
    data = _journal_read(state_dir)
    data[str(update_id)] = {"command": command, "result": result}
    while len(data) > 200:
        data.pop(min(data, key=lambda k: int(k)))
    path = state_dir / JOURNAL
    fd = _os.open(path, _os.O_WRONLY | _os.O_CREAT | _os.O_TRUNC, 0o600)
    with _os.fdopen(fd, "w") as fh:
        json.dump(data, fh)


def drain_inbox(state_dir: Path, router, watermark: UpdateWatermark,
                inbox: PreAckInbox, reply=lambda _uid, _text: None) -> dict:
    """Retry updates stranded by a crash between confirmation and
    handling. Idempotent via the watermark: entries already handled are
    just dropped. Entries that fail again STAY for the next drain."""
    import logging
    log = logging.getLogger("bot.ops")
    replayed = dropped = failed = uncertain = 0
    for entry in inbox.pending():
        uid = int(entry["update_id"])
        if watermark.already_handled(uid):
            inbox.remove(uid)
            dropped += 1
            continue
        command = (entry["text"].lstrip("/").split() or [""])[0].lower()
        if (command in SIDE_EFFECTING
                and journal_result(state_dir, uid, command) is None):
            # crash window: the side effect may already have executed
            # without a journal record - preserve for reconciliation,
            # never blind-retry
            journal_mark_uncertain(state_dir, uid, command,
                                   entry["text"])
            watermark.mark_handled(uid)
            inbox.remove(uid)
            reply(int(entry["user_id"]),
                  f"Command {entry['text']} was interrupted by a restart "
                  "and may already have run. It was NOT repeated. "
                  "Re-send it if you want it executed again.")
            uncertain += 1
            continue
        try:
            router.admin_ids = {int(entry["user_id"])}
            reply_text = router.dispatch(entry["text"],
                                         user_id=int(entry["user_id"]),
                                         update_id=uid)
            if reply_text:
                reply(int(entry["user_id"]), reply_text)
            watermark.mark_handled(uid)
            inbox.remove(uid)
            replayed += 1
        except Exception:
            log.exception("inbox drain failed for update %s", uid)
            failed += 1
    if replayed or dropped or failed or uncertain:
        log.info("inbox drain: %d replayed, %d already-handled, %d failed,"
                 " %d uncertain", replayed, dropped, failed, uncertain)
    return {"replayed": replayed, "dropped": dropped, "failed": failed,
            "uncertain": uncertain}


def clipnow_reply(state_dir: Path, channel: str, update_id: int = 0) -> str:
    """Manual official-route clip. Fails loudly when unconfigured -
    never pretends a clip exists. Journaled: a replayed update returns
    the recorded result instead of creating a second clip."""
    cached = journal_result(state_dir, update_id, "clipnow")
    if cached is not None:
        return "Already processed (replay guard): " + cached
    token_file = state_dir / "twitch_user_token"
    client_file = state_dir / "twitch_client_id"
    if not token_file.exists() or not client_file.exists():
        return UNCONFIGURED_CLIP
    channel = channel.strip()
    if not channel:
        return "Usage: /clipnow <channel-login>"
    client = TwitchClipsClient(client_file.read_text().strip(),
                               token_file.read_text().strip())
    try:
        bid = client.resolve_broadcaster_id(channel)
        clip_id = client.create_clip(bid)
        info = client.wait_for_clip(clip_id)
    except ClipFailed as exc:
        return f"Clip creation failed: {exc}"
    from ..intelligence.memory import Memory
    from ..intelligence.planner import plan_clip
    mem = Memory(str(state_dir / "memory.json"))
    plan = plan_clip("manual", channel, mem,
                     game=str(info.get("game_id", "")))
    mem.remember("clip", f"twitch-clip:{clip_id}",
                 f"manual clip of {channel} via /clipnow",
                 "twitch api", confidence="observed")
    plan_note = (f"\nPlan: {plan.target_seconds}s {plan.preset}, "
                 f"title \"{plan.title}\"")
    try:
        url = client.download_url(info)
    except ClipFailed:
        result = (f"Clip created ({info.get('url', 'pending')}) but file "
                  "download is restricted to the broadcaster or channel "
                  "editors - no file was pulled." + plan_note)
        journal_record(state_dir, update_id, "clipnow", result)
        return result
    result = (f"Clip created: {info.get('url', '')} "
              f"(download: {url})" + plan_note)
    journal_record(state_dir, update_id, "clipnow", result)
    return result


def destinations_reply(state_dir: Path, args: str) -> str:
    store = DestinationStore(str(state_dir / "destinations.json"))
    parts = args.split()
    if not parts:
        rows = store.list()
        if not rows:
            return "No delivery destinations registered."
        return "Destinations:\n" + "\n".join(
            f"{r['chat_id']} - {r['label']}" for r in rows)
    if parts[0] == "add" and len(parts) >= 3:
        try:
            chat_id = int(parts[1])
        except ValueError:
            return "chat_id must be a number"
        store.add(chat_id, " ".join(parts[2:]))
        return f"Destination {chat_id} registered."
    if parts[0] == "remove" and len(parts) == 2:
        try:
            chat_id = int(parts[1])
        except ValueError:
            return "chat_id must be a number"
        return (f"Destination {chat_id} removed." if store.remove(chat_id)
                else f"No destination {chat_id}.")
    return USAGE_DESTINATIONS
