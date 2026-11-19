"""Phase 006 (official route): trigger-based ingestion via Twitch Clips API.

VERIFIED ROUTE (https://dev.twitch.tv/docs/api/clips/): Create Clip works
on any broadcaster's live stream with a user access token carrying the
clips:edit scope; the authenticating user owns the clip. The API captures
up to ~90s around the call. Get Clips works with an app access token.

HONEST LIMITS (verified against official docs):
1. No official continuous-capture route exists for channels the owner
   does not own, so automatic signal-driven detection cannot run on them.
2. Clip FILE download (Get Clips Download, beta) is reserved to the
   broadcaster or channel editors (channel:manage:clips /
   editor:manage:clips scope). Create Clip itself works on any channel
   with clips:edit, but without download rights the file cannot be
   pulled through any supported endpoint.
=> The officially supported end-to-end shapes are: the owner's OWN
   channel, and channels where the owner holds a channel editor role.
   The calling token's USER must be the broadcaster or an editor; a
   third party merely granting OAuth to their account confers nothing
   unless that consented user is the broadcaster or an editor.
   Arbitrary-channel automation is not officially deliverable.

No credentials are stored here; tokens come from files placed by the
owner's OAuth flow (separate, gated step). Nothing in this module runs
without explicit configuration.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from pathlib import Path

HELIX = "https://api.twitch.tv/helix"


class TwitchNotConfigured(Exception):
    """No client id / tokens configured; the owner has not connected
    Twitch. Raised instead of pretending the feature works."""


class ClipFailed(Exception):
    pass


class TwitchClipsClient:
    def __init__(self, client_id: str, user_token: str,
                 app_token: str | None = None):
        if not client_id or not user_token:
            raise TwitchNotConfigured(
                "Twitch client id and user token (clips:edit) required")
        self.client_id = client_id
        self.user_token = user_token
        self.app_token = app_token or user_token

    def _request(self, method: str, path: str, token: str) -> dict:
        req = urllib.request.Request(
            HELIX + path, method=method,
            headers={"Authorization": "Bearer " + token,
                     "Client-Id": self.client_id})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode()
        except urllib.error.HTTPError as exc:
            raise ClipFailed(f"Helix {method} {path} -> HTTP {exc.code}") \
                from None
        return json.loads(body) if body else {}

    def resolve_broadcaster_id(self, login: str) -> str:
        data = self._request("GET", f"/users?login={login}", self.app_token)
        users = data.get("data") or []
        if not users:
            raise ClipFailed(f"no Twitch user named {login!r}")
        return str(users[0]["id"])

    def create_clip(self, broadcaster_id: str) -> str:
        """Start clip capture; returns the clip id. Async on Twitch's
        side - poll clip_ready() before downloading."""
        data = self._request(
            "POST", f"/clips?broadcaster_id={broadcaster_id}",
            self.user_token)
        items = data.get("data") or []
        if not items:
            raise ClipFailed("Create Clip returned no clip id")
        return str(items[0]["id"])

    def clip_ready(self, clip_id: str) -> dict | None:
        """Get Clips by id. Per the docs, if the clip has not appeared
        after ~15 seconds, assume creation failed."""
        data = self._request("GET", f"/clips?id={clip_id}", self.app_token)
        items = data.get("data") or []
        return items[0] if items else None

    def wait_for_clip(self, clip_id: str, timeout: float = 20.0,
                      interval: float = 3.0) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = self.clip_ready(clip_id)
            if info is not None:
                return info
            time.sleep(interval)
        raise ClipFailed(f"clip {clip_id} not ready within {timeout}s")

    def download_url(self, clip_info: dict) -> str:
        """OFFICIAL route only: Get Clips Download (open beta).

        Requirements (verified): the token's user must be the clip's
        BROADCASTER or an EDITOR of that channel, with scope
        channel:manage:clips or editor:manage:clips. For arbitrary
        channels the owner merely watches, Twitch offers no supported
        clip-file download; this client refuses instead of pulling files
        through undocumented asset-URL patterns."""
        broadcaster_id = clip_info.get("broadcaster_id")
        clip_id = clip_info.get("id")
        if not broadcaster_id or not clip_id:
            raise ClipFailed("clip info lacks broadcaster_id/id")
        data = self._request(
            "GET",
            f"/clips/downloads?broadcaster_id={broadcaster_id}"
            f"&clip_id={clip_id}",
            self.user_token)
        items = data.get("data") or []
        if not items:
            raise ClipFailed(
                "Get Clips Download returned nothing - token user is "
                "likely not the broadcaster or an editor of this channel "
                "(required by the official endpoint)")
        entry = items[0]
        url = entry.get("landscape_download_url") or \
            entry.get("portrait_download_url")
        if not url:
            raise ClipFailed(f"clip {clip_id} has no downloadable file")
        return str(url)

    def download_clip(self, clip_info: dict, out_path: str) -> Path:
        url = self.download_url(clip_info)
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=120) as resp, \
                open(out_path, "wb") as fh:
            while True:
                chunk = resp.read(1 << 20)
                if not chunk:
                    break
                fh.write(chunk)
        path = Path(out_path)
        if path.stat().st_size < 10000:
            raise ClipFailed(f"downloaded clip suspiciously small: {url}")
        return path


def client_from_state(state_dir: str) -> TwitchClipsClient:
    """Build a client from owner-provisioned files, or raise
    TwitchNotConfigured. Files: twitch_client_id, twitch_user_token,
    optional twitch_app_token (all 0600, written by the OAuth step)."""
    base = Path(state_dir)
    def read(name: str) -> str:
        p = base / name
        return p.read_text().strip() if p.exists() else ""
    return TwitchClipsClient(read("twitch_client_id"),
                             read("twitch_user_token"),
                             read("twitch_app_token") or None)
