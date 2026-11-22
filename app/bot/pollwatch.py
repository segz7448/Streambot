"""Positive evidence that Telegram long polling is alive beyond startup.

"Application started" proves the process booted, not that getUpdates is
succeeding. Android blocks the socket/proc inspection an operator would
otherwise use, so the service records its own typed, content-free facts:
counts and timestamps of successful getUpdates responses (including empty
ones), poll errors by class name only, updates handled and outbound sends.

The polling Bot cannot be patched after construction (PTB forbids setting
attributes on a Bot), so polling runs on a small subclass that only records
and delegates. Nothing here ever logs or stores tokens, URLs, query
parameters, payloads, chat ids or message text.
"""
from __future__ import annotations

import asyncio
import threading
import time

from telegram import Bot


class PollWatch:
    """Content-free polling liveness record. Thread-safe."""

    def __init__(self, clock=time.time):
        self._clock = clock
        self._lock = threading.Lock()
        self.started_at = clock()
        self.last_ok_at: float | None = None
        self.ok_count = 0
        self.error_count = 0
        self.last_error_kind: str | None = None
        self.last_error_at: float | None = None
        self.updates_received = 0
        self.outbound_sent = 0

    def note_poll_ok(self, updates: int) -> None:
        with self._lock:
            self.last_ok_at = self._clock()
            self.ok_count += 1
            self.updates_received += max(0, int(updates))

    def note_poll_error(self, kind: str) -> None:
        with self._lock:
            self.error_count += 1
            self.last_error_kind = kind
            self.last_error_at = self._clock()

    def note_outbound(self) -> None:
        with self._lock:
            self.outbound_sent += 1

    def snapshot(self) -> dict:
        with self._lock:
            now = self._clock()
            return {
                "started_at": self.started_at,
                "last_ok_at": self.last_ok_at,
                "last_ok_age": (None if self.last_ok_at is None
                                else round(now - self.last_ok_at, 1)),
                "ok_count": self.ok_count,
                "error_count": self.error_count,
                "last_error_kind": self.last_error_kind,
                "last_error_age": (None if self.last_error_at is None
                                   else round(now - self.last_error_at, 1)),
                "updates_received": self.updates_received,
                "outbound_sent": self.outbound_sent,
            }


class PollWatchedBot(Bot):
    """A Bot that only records poll/outbound liveness, then delegates."""

    def __init__(self, watch: PollWatch, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # a private attribute; TelegramObject only forbids overriding
        # existing public members
        self._poll_watch = watch

    async def get_updates(self, *args, **kwargs):
        try:
            result = await super().get_updates(*args, **kwargs)
        except asyncio.CancelledError:
            raise                      # normal shutdown: not an error
        except Exception as exc:
            self._poll_watch.note_poll_error(type(exc).__name__)
            raise
        self._poll_watch.note_poll_ok(len(result))
        return result

    async def send_message(self, *args, **kwargs):
        result = await super().send_message(*args, **kwargs)
        self._poll_watch.note_outbound()
        return result

    async def send_video(self, *args, **kwargs):
        result = await super().send_video(*args, **kwargs)
        self._poll_watch.note_outbound()
        return result
