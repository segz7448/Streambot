"""Durable handled-update watermark for restart-safe command processing.

Telegram redelivers updates that were never confirmed by an offset
advance, so after a service restart the tail of the backlog can arrive
twice. Handlers record each processed update_id here (0600 file) and
skip anything at or below the watermark, making command handling
idempotent across restarts without dropping pending updates.
"""
from __future__ import annotations

import json
import os
from pathlib import Path


class UpdateWatermark:
    def __init__(self, path: str):
        self.path = Path(path)

    def _read(self) -> int:
        try:
            return int(json.loads(self.path.read_text()).get("handled", 0))
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            return 0

    def already_handled(self, update_id: int) -> bool:
        return update_id <= self._read()

    def mark_handled(self, update_id: int) -> None:
        current = self._read()
        if update_id <= current:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump({"handled": int(update_id)}, fh)
