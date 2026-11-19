"""One-time owner pairing challenge for establishing the admin identity.

The bot must never trust the first arbitrary visitor. Instead the owner
receives a short single-use code through their authenticated Instinct
channel, sends it to the bot as ``/pair <code>``, and that sender's
Telegram user ID becomes the sole administrator. Only the code's SHA-256
is stored on-device; the challenge expires (default 15 minutes) and is
consumed on success. State lives in a 0600 JSON file outside the repo.
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from pathlib import Path

from ..core.state import locked_read, locked_update, locked_write

DEFAULT_TTL_SECONDS = 900


class PairingStore:
    def __init__(self, path: str):
        self.path = Path(path)

    def _read(self) -> dict:
        return locked_read(self.path, {})

    def _write(self, data: dict) -> None:
        locked_write(self.path, data)

    def admin_id(self) -> int | None:
        value = self._read().get("admin_user_id")
        return value if isinstance(value, int) else None

    @staticmethod
    def generate_code() -> str:
        """Human-typeable 8-character code (unambiguous alphabet)."""
        alphabet = "abcdefghjkmnpqrstuvwxyz23456789"
        return "".join(secrets.choice(alphabet) for _ in range(8))

    def begin_challenge(self, code: str,
                        ttl_seconds: int = DEFAULT_TTL_SECONDS,
                        now: float | None = None) -> None:
        now = time.time() if now is None else now
        def apply(data):
            data["pending"] = {
                "code_sha256": hashlib.sha256(code.encode()).hexdigest(),
                "expires_at": now + ttl_seconds,
            }
            return data, None
        locked_update(self.path, {}, apply)

    def try_pair(self, code: str, user_id: int,
                 now: float | None = None) -> bool:
        """Consume the challenge. Succeeds at most once, only while the
        challenge is unexpired and no admin is established yet."""
        now = time.time() if now is None else now
        digest = hashlib.sha256((code or "").encode()).hexdigest()

        def apply(data):
            if data.get("admin_user_id") is not None:
                return data, False
            pending = data.get("pending") or {}
            if (pending.get("code_sha256") != digest
                    or now > pending.get("expires_at", 0)):
                return data, False
            return {"admin_user_id": int(user_id)}, True

        return locked_update(self.path, {}, apply)

    def clear_challenge(self) -> None:
        def apply(data):
            data.pop("pending", None)
            return data, None
        locked_update(self.path, {}, apply)
