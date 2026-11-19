"""Persistent pre-ack inbox: closes the confirm-to-handle gap.

Telegram confirms an update when the long-poll offset advances past it.
If the handler crashes after that confirmation, the watermark records
the failure but Telegram never redelivers - the update would sit in the
failed set forever. This inbox persists each command BEFORE dispatch
(0600 file in the state dir), removes it only after successful
BOUNDARY (honest): the entry is written when the handler is entered.
Telegram's server-side confirmation happens when the framework's NEXT
getUpdates call carries the advanced offset. A crash between that ack
and handler entry (framework queue window) is still uncovered; a crash
before the ack is covered by Telegram redelivery, and a crash after
handler entry is covered by this inbox + startup drain. Replay
idempotence beyond the watermark is per-command: a command that crashed
AFTER an external side effect (clip creation, restream start) can
repeat that side effect on drain - side-effecting commands need
operation-level idempotency, which is NOT yet implemented."""
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

    def add(self, update_id: int, user_id: int, text: str) -> None:
        with self._lock:
            entries = [e for e in self._read()
                       if e.get("update_id") != update_id]
            entries.append({"update_id": int(update_id),
                            "user_id": int(user_id), "text": text})
            if len(entries) > MAX_PENDING:
                import logging
                logging.getLogger("bot.inbox").critical(
                    "pre-ack inbox exceeds %d entries; operator attention "
                    "required", MAX_PENDING)
            self._write(entries)

    def remove(self, update_id: int) -> None:
        with self._lock:
            self._write([e for e in self._read()
                         if e.get("update_id") != update_id])

    def pending(self) -> list[dict]:
        with self._lock:
            return self._read()
