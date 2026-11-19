"""Durable handled-update watermark for restart-safe command processing.

Telegram redelivers updates that were never confirmed by an offset
advance, so after a service restart the tail of the backlog can arrive
twice. Handlers record each processed update_id here (0600 file) and
skip anything at or below the watermark, making command handling
idempotent across restarts without dropping pending updates.

An update whose handler FAILED is recorded in a failed set; the
monotonic watermark never suppresses it, so a later successful update
cannot skip an earlier failed one, and a redelivery of the failed update
is processed again. The failed set is never evicted: failed updates
persist until they succeed or an operator clears them (an operator
alert threshold is exposed via MAX_FAILED), so no failure is silently
forgotten. Thread-safe.
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from ..core.state import locked_read, locked_write

MAX_FAILED = 200


class UpdateWatermark:
    def __init__(self, path: str):
        self.path = Path(path)
        self._lock = threading.Lock()

    def _read(self) -> tuple[int, list[int]]:
        data = locked_read(self.path, {})
        try:
            return (int(data.get("handled", 0)),
                    [int(x) for x in data.get("failed", [])])
        except (ValueError, TypeError, AttributeError):
            return 0, []

    def _write(self, handled: int, failed: list[int]) -> None:
        locked_write(self.path, {"handled": handled, "failed": failed})

    def snapshot(self) -> tuple[int, list[int]]:
        """(handled high-watermark, failed ids) for status reporting."""
        with self._lock:
            return self._read()

    def already_handled(self, update_id: int) -> bool:
        with self._lock:
            handled, failed = self._read()
            return update_id <= handled and update_id not in failed

    def mark_handled(self, update_id: int) -> None:
        with self._lock:
            handled, failed = self._read()
            if update_id in failed:
                failed.remove(update_id)
            if update_id > handled:
                handled = update_id
            self._write(handled, failed)

    def mark_failed(self, update_id: int) -> None:
        with self._lock:
            handled, failed = self._read()
            if update_id not in failed:
                failed.append(update_id)
            # never evict: a failed update stays retryable until it
            # succeeds or an operator clears state; overflow is loud
            if len(failed) > MAX_FAILED:
                import logging
                logging.getLogger("bot.watermark").critical(
                    "failed-update set exceeds %d entries; operator "
                    "attention required", MAX_FAILED)
            self._write(handled, failed)
