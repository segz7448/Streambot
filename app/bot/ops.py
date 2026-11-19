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


def relay_reply(state_dir: Path, args: str) -> str:
    """Start an RTMP restream of an explicitly owner-provided feed.
    Secret-bearing RTMP destinations are REFUSED: stream keys must not
    appear in ffmpeg process arguments on this shared host (same-UID
    processes can read /proc cmdline), and no safe credential transport
    is implemented yet. Only keyless destinations (path is empty or
    "/") or local file outputs are accepted."""
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
    return f"Restreaming your feed to {len(pids)} destination(s). /stoprelay stops it."


def stoprelay_reply(state_dir: Path) -> str:
    stopped = RestreamManager(str(state_dir / "restream.pids")).stop()
    if stopped:
        return f"Restream stopped ({stopped} process(es))."
    return "No restream is running."


def drain_inbox(state_dir: Path, router, watermark: UpdateWatermark,
                inbox: PreAckInbox, reply=lambda _uid, _text: None) -> dict:
    """Retry updates stranded by a crash between confirmation and
    handling. Idempotent via the watermark: entries already handled are
    just dropped. Entries that fail again STAY for the next drain."""
    import logging
    log = logging.getLogger("bot.ops")
    replayed = dropped = failed = 0
    for entry in inbox.pending():
        uid = int(entry["update_id"])
        if watermark.already_handled(uid):
            inbox.remove(uid)
            dropped += 1
            continue
        try:
            router.admin_ids = {int(entry["user_id"])}
            reply_text = router.dispatch(entry["text"],
                                         user_id=int(entry["user_id"]))
            if reply_text:
                reply(int(entry["user_id"]), reply_text)
            watermark.mark_handled(uid)
            inbox.remove(uid)
            replayed += 1
        except Exception:
            log.exception("inbox drain failed for update %s", uid)
            failed += 1
    if replayed or dropped or failed:
        log.info("inbox drain: %d replayed, %d already-handled, %d failed",
                 replayed, dropped, failed)
    return {"replayed": replayed, "dropped": dropped, "failed": failed}


def clipnow_reply(state_dir: Path, channel: str) -> str:
    """Manual official-route clip. Fails loudly when unconfigured -
    never pretends a clip exists."""
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
    try:
        url = client.download_url(info)
    except ClipFailed:
        return (f"Clip created ({info.get('url', 'pending')}) but file "
                "download is restricted to the broadcaster or channel "
                "editors - no file was pulled.")
    return f"Clip created: {info.get('url', '')} (download: {url})"


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
