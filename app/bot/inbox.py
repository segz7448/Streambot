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

MAX_PENDING = 500


class PreAckInbox:
    def __init__(self, path: str):
        self.path = Path(path)
        self._lock = threading.Lock()

    def _read(self) -> list[dict]:
        try:
            data = json.loads(self.path.read_text())
            return [e for e in data if isinstance(e, dict)]
        except (FileNotFoundError, json.JSONDecodeError, ValueError,
                TypeError):
            return []

    def _write(self, entries: list[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump(entries, fh)

    def add(self, update_id: int, user_id: int, text: str) -> bool:
        """Persist a pending command. Bounded with BACKPRESSURE, never
        silent eviction: when the inbox is full the new entry is
        REFUSED (returns False) and the refusal is recorded durably in
        an overflow file, so a rejected command is reconcilable instead
        of acknowledged-and-lost. Unprocessed entries are never dropped
        to make room."""
        with self._lock:
            entries = [e for e in self._read()
                       if e.get("update_id") != update_id]
            if len(entries) >= MAX_PENDING:
                import logging
                logging.getLogger("bot.inbox").critical(
                    "pre-ack inbox full (%d); refusing update %s - "
                    "recorded to overflow for reconciliation",
                    MAX_PENDING, update_id)
                self._record_overflow(update_id, user_id, text)
                return False
            entries.append({"update_id": int(update_id),
                            "user_id": int(user_id), "text": text})
            self._write(entries)
            return True

    def _record_overflow(self, update_id: int, user_id: int,
                         text: str) -> None:
        """Durable refusal record for reconciliation. Bounded to the
        last 100 refusals; stores ONLY the command name, never raw
        arguments, so the overflow file cannot accumulate secrets."""
        import json as _json
        path = self.path.with_suffix(".overflow.json")
        try:
            rows = _json.loads(path.read_text())
        except (FileNotFoundError, ValueError):
            rows = []
        command = (text.lstrip("/").split() or [""])[0][:32]
        rows.append({"update_id": int(update_id), "user_id": int(user_id),
                     "command": command})
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            _json.dump(rows[-100:], fh)

    def remove(self, update_id: int) -> None:
        with self._lock:
            self._write([e for e in self._read()
                         if e.get("update_id") != update_id])

    def pending(self) -> list[dict]:
        with self._lock:
            return self._read()
