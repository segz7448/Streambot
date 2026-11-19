"""Phase 014 (added scope): YouTube publishing via Data API v3.

AUTH ROUTE (verified 2026-11-19, least privilege): Desktop app OAuth
client + loopback redirect + PKCE, scope youtube.upload ONLY. Google
classifies CLI tools, background utilities and scripts as desktop apps
(https://developers.google.com/identity/protocols/oauth2/native-app),
so this phone-hosted daemon fits that client type; the owner completes
consent in any browser ON THE SAME HOST (redirect lands on the daemon's
127.0.0.1 listener). The device flow is NOT used: its allowed scope
list excludes youtube.upload and would force the broad youtube scope,
and a daemon is not the intended limited-input device. Constraints the
owner must hear before consenting: an unverified project in Testing
mode requires the owner as a listed test user, shows an unverified-app
warning, and the refresh token EXPIRES 7 DAYS after consent (weekly
re-consent) until the project passes Google's sensitive-scope
verification; uploads from an unaudited project stay locked private.
Official route only: OAuth 2.0 device authorization flow (no browser
automation), resumable upload (videos.insert), privacyStatus PRIVATE by
default - the owner publishes explicitly. No publication claim is made
until YouTube's API confirms the upload. Gated on the owner's Google
OAuth consent; without credentials every entry point raises
YouTubeNotConfigured.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

DEVICE_CODE_URL = "https://oauth2.googleapis.com/device/code"
TOKEN_URL = "https://oauth2.googleapis.com/token"
UPLOAD_URL = ("https://www.googleapis.com/upload/youtube/v3/videos"
              "?uploadType=resumable&part=snippet,status")
SCOPE = "https://www.googleapis.com/auth/youtube.upload"
# Least privilege: youtube.upload via the Desktop-app loopback flow.
# The device flow's allowed scope list excludes youtube.upload, so it
# would require the broad youtube scope - not used here.


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


class LoopbackAuth:
    """Desktop-app OAuth: PKCE + loopback redirect listener. The owner
    opens authorization_url in a browser on the SAME host; the redirect
    delivers the code to the local listener. Scope is youtube.upload
    only - the least privilege that permits publishing."""

    def __init__(self, client_id: str, client_secret: str = "",
                 host: str = "127.0.0.1", port: int = 0):
        self.client_id = client_id
        self.client_secret = client_secret
        self.verifier = base64.urlsafe_b64encode(
            secrets.token_bytes(32)).rstrip(b"=").decode()
        digest = hashlib.sha256(self.verifier.encode()).digest()
        self.challenge = base64.urlsafe_b64encode(
            digest).rstrip(b"=").decode()
        self.state = secrets.token_urlsafe(16)
        self._server = HTTPServer((host, port), _CodeCatcher)
        self.port = self._server.server_address[1]
        self.redirect_uri = f"http://{host}:{self.port}"
        params = urllib.parse.urlencode({
            "client_id": client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": SCOPE,
            "access_type": "offline",
            "prompt": "consent",
            "state": self.state,
            "code_challenge": self.challenge,
            "code_challenge_method": "S256",
        })
        self.authorization_url = (
            "https://accounts.google.com/o/oauth2/v2/auth?" + params)

    def wait_for_code(self, timeout: float = 300.0) -> str:
        """Block until the owner's browser delivers the code."""
        self._server.timeout = timeout
        self._server.handle_request()
        code = getattr(self._server, "code", None)
        state = getattr(self._server, "state", None)
        if state != self.state:
            raise YouTubeUploadFailed("OAuth state mismatch - refused")
        if not code:
            raise YouTubeUploadFailed("no authorization code received")
        return str(code)

    def exchange(self, code: str) -> dict:
        """Code -> tokens (refresh_token on first consent). Caller must
        persist the refresh token in the state dir, 0600."""
        return _post_form(TOKEN_URL, {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": code,
            "code_verifier": self.verifier,
            "redirect_uri": self.redirect_uri,
            "grant_type": "authorization_code"})


class _CodeCatcher(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - stdlib hook name
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)
        self.server.code = (params.get("code") or [None])[0]
        self.server.state = (params.get("state") or [None])[0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Authorization received. You can close this tab.")

    def log_message(self, *args):  # keep tokens out of logs
        pass


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
