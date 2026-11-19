"""Phase 013: deliver clips to the paired owner over Telegram.

Owner-scoped by construction: the only chat this module will send to is
the administrator established by the pairing challenge (app.bot.pairing).
Uses the maintained python-telegram-bot framework; the token is read
from a file, never from argv or the environment.
"""
from __future__ import annotations

import asyncio

from telegram import Bot

from ..bot.pairing import PairingStore


class NoAdminError(Exception):
    pass


async def deliver_clip(token: str, store: PairingStore, video_path: str,
                       caption: str = "") -> int:
    """Send one clip to the paired owner. Returns the Telegram message_id."""
    admin = store.admin_id()
    if admin is None:
        raise NoAdminError("no paired admin; run /pair first")
    bot = Bot(token=token)
    async with bot:
        with open(video_path, "rb") as fh:
            message = await bot.send_video(
                chat_id=admin, video=fh,
                caption=caption or None,
                supports_streaming=True)
    return message.message_id


def deliver_clip_sync(token_path: str, state_dir: str, video_path: str,
                      caption: str = "") -> int:
    with open(token_path) as fh:
        token = fh.read().strip()
    import os
    store = PairingStore(os.path.join(state_dir, "admin.json"))
    return asyncio.run(deliver_clip(token, store, video_path, caption))


if __name__ == "__main__":
    import os
    import sys
    message_id = deliver_clip_sync(
        os.environ["NOVA_TOKEN_FILE"],
        os.environ.get("NOVA_STATE_DIR", os.path.expanduser("~/.nova-clip")),
        sys.argv[1],
        sys.argv[2] if len(sys.argv) > 2 else "")
    print("DELIVERED", message_id)
