"""Persistent pre-ack inbox: closes the confirm-to-handle gap.

Telegram confirms an update when the long-poll offset advances past it.
Each admin command is persisted BEFORE it enters the framework queue
(the update_queue.put wrapper), so a crash at any later point - queue,
dispatch, or the side effect itself - leaves a durable entry the
startup drain reconciles. Entries are removed only after successful
handling; the watermark makes replays idempotent, and the op journal
gives side-effecting commands operation-level idempotency.

Only the paired administrator's commands are ever written here; the
inbox is bounded with backpressure (refusal is recorded to an overflow
file, never silent eviction). All files are 0600 in the state dir.
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from ..core.state import locked_read, locked_update, locked_write

MAX_PENDING = 500


class PreAckInbox:
    def __init__(self, path: str):
        self.path = Path(path)
        self._lock = threading.Lock()

    def _read(self) -> list[dict]:
        data = locked_read(self.path, [])
        return [e for e in data if isinstance(e, dict)]

    def _write(self, entries: list[dict]) -> None:
        locked_write(self.path, entries)

    def add(self, update_id: int, user_id: int, text: str) -> bool:
        """Persist a pending command. Bounded with BACKPRESSURE, never
        silent eviction: when the inbox is full the new entry is
        REFUSED (returns False) and the refusal is recorded durably in
        an overflow file, so a rejected command is reconcilable instead
        of acknowledged-and-lost. Unprocessed entries are never dropped
        to make room."""
        refused = {"full": False}

        def apply(entries):
            kept = [e for e in entries
                    if e.get("update_id") != update_id]
            if len(kept) >= MAX_PENDING:
                refused["full"] = True
                return entries, False  # unchanged: nothing acknowledged
            kept.append({"update_id": int(update_id),
                         "user_id": int(user_id), "text": text})
            return kept, True

        ok = locked_update(self.path, [], apply)
        if refused["full"]:
            import logging
            logging.getLogger("bot.inbox").critical(
                "pre-ack inbox full (%d); refusing update %s - "
                "recorded to overflow for reconciliation",
                MAX_PENDING, update_id)
            self._record_overflow(update_id, user_id, text)
        return ok

    def _record_overflow(self, update_id: int, user_id: int,
                         text: str) -> None:
        """Durable refusal record for reconciliation. Bounded to the
        last 100 refusals; stores ONLY the command name, never raw
        arguments, so the overflow file cannot accumulate secrets."""
        path = self.path.with_suffix(".overflow.json")
        command = (text.lstrip("/").split() or [""])[0][:32]

        def apply(rows):
            rows.append({"update_id": int(update_id),
                         "user_id": int(user_id), "command": command})
            return rows[-100:], None

        locked_update(path, [], apply)

    def remove(self, update_id: int) -> None:
        def apply(entries):
            return ([e for e in entries
                     if e.get("update_id") != update_id], None)
        locked_update(self.path, [], apply)

    def set_attempts(self, update_id: int, attempts: int) -> None:
        def apply(entries):
            for e in entries:
                if e.get("update_id") == update_id:
                    e["attempts"] = int(attempts)
            return entries, None
        locked_update(self.path, [], apply)

    def pending(self) -> list[dict]:
        with self._lock:
            return self._read()
