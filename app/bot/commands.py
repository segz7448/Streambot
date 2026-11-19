"""Telegram command layer (transport-agnostic).

Authorization is administrator-ID based and enforced before any dispatch.
Handlers return plain text; the Telegram transport (long-poll or webhook,
Phase 007) delivers it and never receives secrets. No arbitrary shell
execution exists anywhere in this layer.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from ..core.logging import get_logger

log = get_logger("bot.commands")


class UnauthorizedError(Exception):
    pass


@dataclass
class Command:
    name: str
    args: str
    user_id: int


def parse_command(text: str) -> Optional[Command]:
    text = (text or "").strip()
    if not text.startswith("/"):
        return None
    head, _, args = text.partition(" ")
    name = head[1:].split("@")[0].lower()  # strip optional @BotName suffix
    user = 0
    return Command(name=name, args=args.strip(), user_id=user)


WELCOME = (
    "WELCOME TO NOVA CLIP ENGINE\n\n"
    "Choose an operation:\n"
    "/addsource <twitch username> - register a channel\n"
    "/sources - list registered channels\n"
    "/removesource <username> - remove a channel\n"
    "/checklive <username> - live status\n"
    "/startclip - start an authorized clipping session\n"
    "/stopclip - stop a session safely\n"
    "/sessions - active and previous sessions\n"
    "/clips - browse generated clips\n"
    "/clipdetails <id> - clip information\n"
    "/settings - clipping configuration\n"
    "/quality - output quality presets\n"
    "/subtitles - subtitles on/off\n"
    "/relay - configure authorized destinations\n"
    "/destinations - list destinations\n"
    "/stoprelay - stop live distribution\n"
    "/status - application health\n"
    "/storage - storage consumption\n"
    "/logs - recent application errors\n"
    "/restart - restart workers safely\n"
    "/youtube - YouTube publishing configuration\n"
    "/youtubeconnect - begin YouTube OAuth\n"
    "/youtubestatus - YouTube connection status\n"
    "/autopublish - automatic publishing on/off\n"
    "/publishqueue - pending uploads\n"
    "/publishnow <id> - publish an approved clip\n"
    "/publishhistory - upload history\n"
    "/youtubeconfig - titles, descriptions, hashtags, privacy")

HELP_TEXT = WELCOME


class CommandRouter:
    """Dispatches authorized commands to registered handlers."""

    def __init__(self, admin_ids: list[int]):
        self.admin_ids = set(admin_ids)
        self._handlers: dict[str, Callable[[Command], str]] = {}
        self.register("start", lambda c: WELCOME)
        self.register("help", lambda c: HELP_TEXT)

    def register(self, name: str, handler: Callable[[Command], str]) -> None:
        self._handlers[name] = handler

    def authorize(self, user_id: int) -> None:
        if user_id not in self.admin_ids:
            log.warning("unauthorized command attempt user_id=%s", user_id)
            raise UnauthorizedError("not authorized")

    def dispatch(self, text: str, user_id: int) -> str:
        cmd = parse_command(text)
        if cmd is None:
            return "Send /help to see available commands."
        cmd.user_id = user_id
        self.authorize(user_id)
        handler = self._handlers.get(cmd.name)
        if handler is None:
            return f"Unknown command /{cmd.name}. Send /help."
        return handler(cmd)
