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
    update_id: int = 0


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
    "Available commands are listed below - only commands that are\n"
    "actually implemented are advertised.")


class CommandRouter:
    """Dispatches authorized commands to registered handlers.

    The help text is GENERATED from the registered handlers, so the bot
    can never advertise a command it would reject.
    """

    def __init__(self, admin_ids: list[int]):
        self.admin_ids = set(admin_ids)
        self.handlers: dict[str, tuple[Callable[[Command], str], str]] = {}
        self.register("start", lambda c: WELCOME, "welcome message")
        self.register("help", self._help,
                      "list the commands this bot actually implements")

    def _help(self, _cmd: Command) -> str:
        lines = [WELCOME, ""]
        for name in sorted(self.handlers):
            lines.append(f"/{name} - {self.handlers[name][1]}")
        return "\n".join(lines)

    def register(self, name: str, handler: Callable[[Command], str],
                 description: str = "no description") -> None:
        self.handlers[name] = (handler, description)

    def authorize(self, user_id: int) -> None:
        if user_id not in self.admin_ids:
            log.warning("unauthorized command attempt user_id=%s", user_id)
            raise UnauthorizedError("not authorized")

    def dispatch(self, text: str, user_id: int, update_id: int = 0) -> str:
        cmd = parse_command(text)
        if cmd is None:
            return "Send /help to see available commands."
        cmd.user_id = user_id
        cmd.update_id = update_id
        self.authorize(user_id)
        entry = self.handlers.get(cmd.name)
        if entry is None:
            return f"Unknown command /{cmd.name}. Send /help."
        # positive evidence that an authorized inbound update reached the
        # service and was dispatched: startup logs alone cannot prove the
        # polling path is alive. The command NAME is logged, never args.
        log.info("authorized command dispatched update_id=%s name=%s",
                 update_id, cmd.name)
        return entry[0](cmd)
