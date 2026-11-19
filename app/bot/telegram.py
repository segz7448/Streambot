"""Phase 007/013: Telegram Bot API transport (stdlib only).

Long-poll loop plus message/video sending. The token reaches this module
only as a constructor argument from the runtime secret store; it is never
logged or persisted by this layer. All HTTP is injectable for tests.
"""
from __future__ import annotations

import json
import urllib.parse
import urllib.request

API = "https://api.telegram.org"


class TelegramClient:
    def __init__(self, token: str, timeout: int = 30,
                 opener: urllib.request.OpenerDirector | None = None):
        self._token = token
        self._timeout = timeout
        self._opener = opener or urllib.request.build_opener()

    def _url(self, method: str) -> str:
        return f"{API}/bot{self._token}/{method}"

    def _call(self, method: str, params: dict) -> dict:
        data = urllib.parse.urlencode(params).encode()
        req = urllib.request.Request(self._url(method), data=data)
        with self._opener.open(req, timeout=self._timeout) as resp:
            payload = json.loads(resp.read().decode())
        if not payload.get("ok"):
            raise RuntimeError(f"telegram {method} failed: "
                               f"{payload.get('description', 'unknown')}")
        return payload["result"]

    def get_updates(self, offset: int = 0, long_poll: int = 25) -> list[dict]:
        return self._call("getUpdates", {"offset": offset,
                                         "timeout": long_poll})

    def send_message(self, chat_id: int, text: str) -> dict:
        return self._call("sendMessage", {"chat_id": chat_id, "text": text})

    def send_video(self, chat_id: int, video_path: str,
                   caption: str = "") -> dict:
        """Multipart upload for local files (Bot API sendVideo)."""
        boundary = "nova-boundary-7f3a"
        with open(video_path, "rb") as fh:
            media = fh.read()
        parts = []
        for name, value in (("chat_id", str(chat_id)), ("caption", caption)):
            parts.append(f"--{boundary}\r\nContent-Disposition: form-data; "
                         f'name="{name}"\r\n\r\n{value}\r\n'.encode())
        parts.append(f"--{boundary}\r\nContent-Disposition: form-data; "
                     f'name="video"; filename="clip.mp4"\r\n'
                     f"Content-Type: video/mp4\r\n\r\n".encode() + media
                     + b"\r\n")
        parts.append(f"--{boundary}--\r\n".encode())
        body = b"".join(parts)
        req = urllib.request.Request(
            self._url("sendVideo"), data=body,
            headers={"Content-Type":
                     f"multipart/form-data; boundary={boundary}"})
        with self._opener.open(req, timeout=max(self._timeout, 120)) as resp:
            payload = json.loads(resp.read().decode())
        if not payload.get("ok"):
            raise RuntimeError(f"telegram sendVideo failed: "
                               f"{payload.get('description', 'unknown')}")
        return payload["result"]


class AdminGate:
    """Verified-owner handshake: the administrator's Telegram user ID is
    established only from an explicit setup exchange, never from the first
    arbitrary visitor. Until set, every command is refused."""
    def __init__(self, admin_user_id: int | None = None):
        self.admin_user_id = admin_user_id

    @property
    def configured(self) -> bool:
        return self.admin_user_id is not None

    def authorize(self, user_id: int) -> None:
        if not self.configured:
            raise PermissionError("admin not configured; run owner setup")
        if user_id != self.admin_user_id:
            raise PermissionError("unauthorized user")


class PollingLoop:
    """Minimal getUpdates loop wiring updates to the command dispatcher."""
    def __init__(self, client: TelegramClient, dispatcher,
                 gate: AdminGate):
        self.client = client
        self.dispatcher = dispatcher
        self.gate = gate
        self.offset = 0

    def poll_once(self, long_poll: int = 0) -> int:
        updates = self.client.get_updates(self.offset, long_poll)
        handled = 0
        for upd in updates:
            self.offset = max(self.offset, upd["update_id"] + 1)
            msg = upd.get("message") or {}
            text = msg.get("text") or ""
            chat = msg.get("chat") or {}
            user = (msg.get("from") or {}).get("id", 0)
            if not text.startswith("/"):
                continue
            try:
                self.gate.authorize(user)
            except PermissionError:
                continue  # silent refusal, no oracle for strangers
            reply = self.dispatcher(text, user_id=user)
            if reply:
                self.client.send_message(chat.get("id"), reply)
                handled += 1
        return handled
