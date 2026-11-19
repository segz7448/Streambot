"""Phase 014: relay - distribute delivered clips to authorized destinations.

A destination is a Telegram chat (channel/group) where the bot has been
added as an administrator by the owner. Authorization model: the paired
owner registers destinations explicitly; the relay never sends anywhere
the owner did not configure. Continuous live-feed restreaming (RTMP) is
out of scope: it requires a capture route that does not officially exist
for arbitrary channels (see app.ingestion.twitch_clips).
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

from ..core.logging import get_logger

log = get_logger("distribution.relay")


class DestinationStore:
    """0600 JSON file in the state dir; outside the repo."""

    def __init__(self, path: str):
        self.path = Path(path)

    def _read(self) -> list[dict]:
        try:
            return json.loads(self.path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _write(self, rows: list[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump(rows, fh)

    def add(self, chat_id: int, label: str) -> None:
        rows = [r for r in self._read() if r["chat_id"] != chat_id]
        rows.append({"chat_id": int(chat_id), "label": label,
                     "added_at": time.time()})
        self._write(rows)

    def remove(self, chat_id: int) -> bool:
        rows = self._read()
        kept = [r for r in rows if r["chat_id"] != chat_id]
        self._write(kept)
        return len(kept) != len(rows)

    def list(self) -> list[dict]:
        return self._read()


class Relayer:
    def __init__(self, store: DestinationStore, send_video):
        """send_video(chat_id, path, caption) -> message_id; injected so
        the transport stays the maintained framework and tests stay
        local."""
        self.store = store
        self.send_video = send_video

    def relay_clip(self, video_path: str, caption: str = "") -> dict:
        """Send one clip to every registered destination. Returns
        {chat_id: message_id-or-error} - partial failures are visible,
        never silently dropped."""
        results: dict[int, object] = {}
        for dest in self.store.list():
            chat_id = int(dest["chat_id"])
            try:
                results[chat_id] = self.send_video(
                    chat_id, video_path, caption)
            except Exception as exc:
                log.warning("relay to %s failed: %s", chat_id, exc)
                results[chat_id] = f"FAILED: {exc.__class__.__name__}"
        return results
