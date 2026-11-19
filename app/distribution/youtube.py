"""Phase 014 (added scope): YouTube publishing via Data API v3.

Official route only: OAuth 2.0 device authorization flow (no browser
automation), resumable upload (videos.insert), privacyStatus PRIVATE by
default - the owner publishes explicitly. No publication claim is made
until YouTube's API confirms the upload. Gated on the owner's Google
OAuth consent; without credentials every entry point raises
YouTubeNotConfigured.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DEVICE_CODE_URL = "https://oauth2.googleapis.com/device/code"
TOKEN_URL = "https://oauth2.googleapis.com/token"
UPLOAD_URL = ("https://www.googleapis.com/upload/youtube/v3/videos"
              "?uploadType=resumable&part=snippet,status")
SCOPE = "https://www.googleapis.com/auth/youtube.upload"


class YouTubeNotConfigured(Exception):
    pass


class YouTubeUploadFailed(Exception):
    pass


def _post_form(url: str, fields: dict) -> dict:
    body = urllib.parse.urlencode(fields).encode()
    req = urllib.request.Request(url, data=body)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raise YouTubeUploadFailed(
            f"{url} -> HTTP {exc.code}: {exc.read()[:200]!r}") from None


class YouTubePublisher:
    def __init__(self, client_id: str, refresh_token: str,
                 client_secret: str = ""):
        if not client_id or not refresh_token:
            raise YouTubeNotConfigured(
                "YouTube client id and refresh token required; the owner "
                "has not completed OAuth consent")
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token

    @staticmethod
    def begin_device_flow(client_id: str) -> dict:
        """Start OAuth device flow. Returns the device-code payload
        including user_code + verification_url for the owner."""
        return _post_form(DEVICE_CODE_URL,
                          {"client_id": client_id, "scope": SCOPE})

    @staticmethod
    def poll_device_flow(client_id: str, device_code: str,
                         client_secret: str = "") -> dict | None:
        """One poll; None while the owner has not consented yet."""
        try:
            return _post_form(TOKEN_URL, {
                "client_id": client_id, "client_secret": client_secret,
                "device_code": device_code,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code"})
        except YouTubeUploadFailed:
            return None  # authorization_pending / slow_down

    def _access_token(self) -> str:
        data = _post_form(TOKEN_URL, {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token"})
        return str(data["access_token"])

    def publish(self, video_path: str, title: str, description: str = "",
                privacy: str = "private", tags: list[str] | None = None
                ) -> str:
        """Resumable upload; returns the YouTube video id only after the
        API confirms the upload. privacy defaults to private and is never
        widened silently by this module."""
        if privacy not in ("private", "unlisted", "public"):
            raise ValueError(f"invalid privacy {privacy!r}")
        token = self._access_token()
        size = os.path.getsize(video_path)
        meta = json.dumps({
            "snippet": {"title": title, "description": description,
                        "tags": tags or []},
            "status": {"privacyStatus": privacy},
        }).encode()
        req = urllib.request.Request(
            UPLOAD_URL, data=meta, method="POST",
            headers={"Authorization": "Bearer " + token,
                     "Content-Type": "application/json; charset=UTF-8",
                     "X-Upload-Content-Type": "video/mp4",
                     "X-Upload-Content-Length": str(size)})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                session = resp.headers["Location"]
        except urllib.error.HTTPError as exc:
            raise YouTubeUploadFailed(
                f"resumable session -> HTTP {exc.code}") from None
        with open(video_path, "rb") as fh:
            payload = fh.read()
        req = urllib.request.Request(
            session, data=payload, method="PUT",
            headers={"Content-Type": "video/mp4",
                     "Content-Length": str(size)})
        try:
            with urllib.request.urlopen(req, timeout=600) as resp:
                result = json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            raise YouTubeUploadFailed(
                f"upload -> HTTP {exc.code}: {exc.read()[:200]!r}") from None
        video_id = result.get("id")
        if not video_id:
            raise YouTubeUploadFailed(f"no video id in response: {result!r}")
        return str(video_id)


def publisher_from_state(state_dir: str) -> YouTubePublisher:
    base = Path(state_dir)
    def read(name: str) -> str:
        p = base / name
        return p.read_text().strip() if p.exists() else ""
    return YouTubePublisher(read("youtube_client_id"),
                            read("youtube_refresh_token"),
                            read("youtube_client_secret"))
