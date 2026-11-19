"""Twitch Helix API client (stdlib-only).

Implements the official app-access (client credentials) flow for server-side
discovery: username resolution and live-status checks. Recording or
redistributing a broadcast is a separate authorization question; this client
only reads public stream metadata through documented endpoints.

HTTP is injected so tests run offline; the default transport uses urllib.
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Callable, Optional

from ...core.logging import get_logger

log = get_logger("integrations.twitch")

API_BASE = "https://api.twitch.tv/helix"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"

Transport = Callable[[str, str, dict, Optional[bytes]], tuple[int, dict]]


class TwitchError(Exception):
    pass


class TwitchAuthError(TwitchError):
    pass


def _urllib_transport(method: str, url: str, headers: dict,
                      body: Optional[bytes]) -> tuple[int, dict]:
    req = urllib.request.Request(url, data=body, method=method)
    for k, v in headers.items():
        req.add_header(k, v)
    import urllib.error
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw or "{}")
        except json.JSONDecodeError:
            return e.code, {"message": raw[:500]}


@dataclass
class TwitchUser:
    id: str
    login: str
    display_name: str


@dataclass
class StreamInfo:
    user_id: str
    user_login: str
    title: str
    game_name: str
    viewer_count: int
    started_at: str


class TwitchClient:
    def __init__(self, client_id: str, client_secret: str,
                 transport: Optional[Transport] = None):
        if not client_id or not client_secret:
            raise TwitchAuthError("Twitch client id/secret not configured")
        self.client_id = client_id
        self.client_secret = client_secret
        self._transport = transport or _urllib_transport
        self._token: Optional[str] = None
        self._token_expiry = 0.0

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expiry - 60:
            return self._token
        body = urllib.parse.urlencode({
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }).encode()
        status, data = self._transport(
            "POST", TOKEN_URL,
            {"Content-Type": "application/x-www-form-urlencoded"}, body)
        if status != 200 or "access_token" not in data:
            raise TwitchAuthError(f"token request failed: {status}")
        self._token = data["access_token"]
        self._token_expiry = time.time() + float(data.get("expires_in", 3600))
        return self._token

    def _get(self, path: str, params: dict, _retry: bool = True) -> dict:
        url = API_BASE + path + "?" + urllib.parse.urlencode(params)
        headers = {"Client-ID": self.client_id,
                   "Authorization": f"Bearer {self._get_token()}"}
        status, data = self._transport("GET", url, headers, None)
        if status == 401 and _retry:
            self._token = None  # expired credential: refresh once
            return self._get(path, params, _retry=False)
        if status == 429:
            raise TwitchError("rate limited by Twitch")
        if status != 200:
            raise TwitchError(f"helix {path} failed: {status}")
        return data

    def resolve_user(self, username: str) -> Optional[TwitchUser]:
        """Resolve a username to a channel identity, or None if unknown."""
        username = username.strip().lower()
        if not username or not username.replace("_", "").isalnum():
            raise TwitchError(f"invalid username: {username!r}")
        data = self._get("/users", {"login": username})
        users = data.get("data") or []
        if not users:
            return None
        u = users[0]
        return TwitchUser(id=u["id"], login=u["login"],
                          display_name=u.get("display_name", u["login"]))

    def get_stream(self, username: str) -> Optional[StreamInfo]:
        """Live stream info for a username, or None when offline."""
        data = self._get("/streams", {"user_login": username.strip().lower()})
        streams = data.get("data") or []
        if not streams:
            return None
        s = streams[0]
        return StreamInfo(user_id=s["user_id"], user_login=s["user_login"],
                          title=s.get("title", ""),
                          game_name=s.get("game_name", ""),
                          viewer_count=s.get("viewer_count", 0),
                          started_at=s.get("started_at", ""))
