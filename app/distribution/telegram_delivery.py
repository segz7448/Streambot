"""Phase 013: deliver clips to the paired owner over Telegram.

Owner-scoped by construction: the only chat this module will send to is
the administrator established by the pairing challenge (app.bot.pairing).
Uses the maintained python-telegram-bot framework; the token is read
from a file, never from argv or the environment.
"""
from __future__ import annotations

import asyncio

from telegram import Bot
from telegram.error import NetworkError, TimedOut
from telegram.request import HTTPXRequest

from ..bot.pairing import PairingStore


class NoAdminError(Exception):
    pass


async def deliver_clip(token: str, store: PairingStore, video_path: str,
                       caption: str = "", attempts: int = 3) -> int:
    """Send one clip to the paired owner. Returns the Telegram message_id.
    Mobile uplinks cut long uploads: generous timeouts and a few retries
    on transient network errors."""
    admin = store.admin_id()
    if admin is None:
        raise NoAdminError("no paired admin; run /pair first")
    request = HTTPXRequest(connect_timeout=30, read_timeout=120,
                           write_timeout=180, pool_timeout=30)
    bot = Bot(token=token, request=request)
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            async with bot:
                with open(video_path, "rb") as fh:
                    message = await bot.send_video(
                        chat_id=admin, video=fh,
                        caption=caption or None,
                        supports_streaming=True)
            return message.message_id
        except (TimedOut, NetworkError) as exc:
            last = exc
            await asyncio.sleep(2 * (attempt + 1))
    raise last  # type: ignore[misc]


async def notify_admin_text(token: str, store: PairingStore, text: str,
                            attempts: int = 3) -> int:
    """Send one plain-text message to the paired owner. Same owner
    scoping and transient-retry shape as deliver_clip."""
    admin = store.admin_id()
    if admin is None:
        raise NoAdminError("no paired admin; run /pair first")
    request = HTTPXRequest(connect_timeout=30, read_timeout=60,
                           write_timeout=60, pool_timeout=30)
    bot = Bot(token=token, request=request)
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            async with bot:
                message = await bot.send_message(chat_id=admin,
                                                 text=text)
            return message.message_id
        except (TimedOut, NetworkError) as exc:
            last = exc
            await asyncio.sleep(2 * (attempt + 1))
    raise last  # type: ignore[misc]


def notify_admin_text_sync(token_path: str, state_dir: str,
                           text: str) -> int:
    from ..core.tokensrc import read_token
    token = read_token(token_path)
    import os
    store = PairingStore(os.path.join(state_dir, "admin.json"))
    return asyncio.run(notify_admin_text(token, store, text))


def deliver_clip_sync(token_path: str, state_dir: str, video_path: str,
                      caption: str = "") -> int:
    from ..core.tokensrc import read_token
    token = read_token(token_path)
    import os
    store = PairingStore(os.path.join(state_dir, "admin.json"))
    return asyncio.run(deliver_clip(token, store, video_path, caption))


if __name__ == "__main__":
    import os
    import sys
    message_id = deliver_clip_sync(
        os.environ.get("NOVA_TOKEN_FILE", ""),
        os.environ.get("NOVA_STATE_DIR", os.path.expanduser("~/.nova-clip")),
        sys.argv[1],
        sys.argv[2] if len(sys.argv) > 2 else "")
    print("DELIVERED", message_id)
