"""Pure-stdlib command logic for the Telegram service.

Kept free of telegram-framework imports so it is unit-testable off the
phone; app.bot.service handlers delegate here.
"""
from __future__ import annotations

from pathlib import Path

from ..distribution.relay import DestinationStore
from ..ingestion.twitch_clips import ClipFailed, TwitchClipsClient

UNCONFIGURED_CLIP = (
    "Manual clipping is not configured. It requires a Twitch developer "
    "application and your OAuth consent (clips:edit). File download "
    "additionally requires you to be the broadcaster or a channel "
    "editor. Until those are in place this command cannot create clips.")

USAGE_DESTINATIONS = ("Usage: /destinations "
                      "[add <chat_id> <label> | remove <chat_id>]")


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
