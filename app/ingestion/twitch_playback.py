"""Anonymous, public-only Twitch live playback resolver (INERT by default).

What it does: for a public live channel, ask Twitch's own player endpoints
for an anonymous playback token, fetch the HLS master playlist and pick one
variant. This is the same unauthenticated path the Twitch web player uses.

What it refuses (fail closed, never retried here):
  * no explicit per-call ``authorized=True`` (default is refuse, no network);
  * any sign-in, session cookie, OAuth token or stored credential is never
    read or sent;
  * any integrity/challenge/captcha response, 401/403/429, a token marked
    forbidden/restricted, or any redirect: the result is a typed refusal. This
    module does not try to get around a restriction;
  * non-HTTPS URLs and hosts outside the Twitch playback allowlist.

Secrets: the signed token and playlist URL are held in memory only, are
never logged, never put in exception text, and never appear in repr().
Capture/upload is NOT performed here and nothing calls this module yet:
turning it on needs the owner's source/rights decision (see the caller).
Residual, explicit: the signed URL is handed to ffmpeg as an argument, so a
same-UID process listing can see it for the life of that process. It
expires with the token. Undocumented endpoints can change without notice.
"""
from __future__ import annotations

import json
import random
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

GQL_URL = "https://gql.twitch.tv/gql"
USHER_HOST = "usher.ttvnw.net"
# Public web-player client id (not a secret, not an account credential).
PLAYER_CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"
_ALLOWED_SUFFIXES = (".ttvnw.net", ".live-video.net")
_LOGIN = re.compile(r"^[a-z0-9_]{3,25}$")
MIN_INTERVAL_SEC = 30.0
MAX_BYTES = 1_000_000

_QUERY = ("query PlaybackAccessToken($login: String!, $isLive: Boolean!, "
          "$playerType: String!) { streamPlaybackAccessToken(channelName: "
          "$login, params: {platform: \"web\", playerBackend: \"mediaplayer\", "
          "playerType: $playerType}) @include(if: $isLive) { value signature } }")


class PlaybackRefused(Exception):
    """kind: unauthorized | invalid | offline | restricted | challenge |
    blocked | error. Message never contains a URL or token."""

    def __init__(self, kind: str, detail: str = ""):
        super().__init__(f"{kind}: {detail}" if detail else kind)
        self.kind = kind


@dataclass(frozen=True)
class Variant:
    name: str
    height: int
    bandwidth: int
    _url: str = field(repr=False, compare=False, default="")


@dataclass(frozen=True)
class Resolved:
    channel: str
    variant: Variant
    variants: tuple
    resolved_at: float

    def ffmpeg_input(self) -> str:
        """The ephemeral signed URL. Pass it straight to ffmpeg; never log,
        store or send it anywhere else."""
        return self.variant._url

    def __repr__(self) -> str:  # redacted by construction
        return (f"Resolved(channel={self.channel!r}, variant="
                f"{self.variant.name!r}, url=<redacted>)")

    __str__ = __repr__


_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_SECRET_RE = re.compile(r"(sig|token|signature)=[^&\s\"']+", re.IGNORECASE)


def redact(text: str) -> str:
    return _SECRET_RE.sub(r"\1=<redacted>", _URL_RE.sub("<url>", str(text)))


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


def default_opener(req: urllib.request.Request, timeout: float):
    """(status, body) with redirects refused and the body size bounded."""
    op = urllib.request.build_opener(_NoRedirect)
    try:
        with op.open(req, timeout=timeout) as r:
            return r.status, r.read(MAX_BYTES)
    except urllib.error.HTTPError as e:
        try:
            body = e.read(4096)
        except Exception:
            body = b""
        return e.code, body


_last: dict = {}
_last_lock = threading.Lock()


def _rate_ok(channel: str, now: float, interval: float) -> bool:
    with _last_lock:
        prev = _last.get(channel)
        if prev is not None and now - prev < interval:
            return False
        _last[channel] = now
        return True


def _host_ok(url: str, host_ok=None) -> bool:
    p = urllib.parse.urlparse(url)
    if p.scheme != "https" or not p.hostname or p.username or p.password:
        return False
    if host_ok is not None:
        return bool(host_ok(p.hostname))
    h = p.hostname.lower()
    return h == USHER_HOST or h.endswith(_ALLOWED_SUFFIXES)


def _status_refusal(code: int, stage: str) -> PlaybackRefused:
    if code == 404:
        return PlaybackRefused("offline", f"{stage} 404 (channel offline or unknown)")
    if code in (401, 403):
        return PlaybackRefused("restricted", f"{stage} HTTP {code}")
    if code == 429:
        return PlaybackRefused("blocked", f"{stage} rate limited (no retry)")
    return PlaybackRefused("error", f"{stage} HTTP {code}")


def _get_token(channel, opener, timeout):
    body = json.dumps({"operationName": "PlaybackAccessToken",
                       "variables": {"isLive": True, "login": channel,
                                     "playerType": "site"},
                       "query": _QUERY}).encode()
    req = urllib.request.Request(GQL_URL, data=body, headers={
        "Client-ID": PLAYER_CLIENT_ID, "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"})
    code, raw = opener(req, timeout)
    if code != 200:
        raise _status_refusal(code, "token")
    try:
        doc = json.loads(raw.decode("utf-8"))
    except Exception:
        raise PlaybackRefused("error", "token response not JSON")
    errs = doc.get("errors") if isinstance(doc, dict) else None
    if errs:
        txt = json.dumps(errs).lower()
        if any(w in txt for w in ("integrity", "challenge", "captcha", "kasada")):
            raise PlaybackRefused("challenge", "client-integrity required")
        raise PlaybackRefused("error", "token query rejected")
    try:
        tok = doc["data"]["streamPlaybackAccessToken"]
        value, sig = tok["value"], tok["signature"]
    except (KeyError, TypeError):
        raise PlaybackRefused("offline", "no playback token returned")
    if not (isinstance(value, str) and isinstance(sig, str) and value and sig):
        raise PlaybackRefused("error", "token malformed")
    try:
        meta = json.loads(value)
        auth = meta.get("authorization") or {}
        if auth.get("forbidden"):
            raise PlaybackRefused("restricted", "playback forbidden for anonymous viewers")
    except PlaybackRefused:
        raise
    except Exception:
        pass
    return value, sig


def parse_master(text: str, host_ok=None) -> list:
    out, lines = [], text.splitlines()
    for i, ln in enumerate(lines):
        if not ln.startswith("#EXT-X-STREAM-INF:"):
            continue
        attrs = ln.split(":", 1)[1]
        m = re.search(r"RESOLUTION=(\d+)x(\d+)", attrs)
        b = re.search(r"BANDWIDTH=(\d+)", attrs)
        n = re.search(r'NAME="([^"]*)"', attrs)
        url = lines[i + 1].strip() if i + 1 < len(lines) else ""
        if not m or not url or not _host_ok(url, host_ok):
            continue  # audio-only, malformed or off-allowlist: skipped
        out.append(Variant(n.group(1) if n else f"{m.group(2)}p",
                           int(m.group(2)), int(b.group(1)) if b else 0, url))
    return out


def resolve(channel: str, *, authorized: bool = False, max_height: int = 720,
            opener=None, host_ok=None, clock=time.time, timeout: float = 15.0,
            min_interval: float = MIN_INTERVAL_SEC) -> Resolved:
    """Resolve one public live channel. Raises PlaybackRefused, never
    returns a partial result. ``authorized`` must be passed True by a
    caller that holds the owner's per-source decision."""
    if authorized is not True:
        raise PlaybackRefused("unauthorized", "no source authorization")
    login = (channel or "").strip().lstrip("@").lower()
    if not _LOGIN.match(login):
        raise PlaybackRefused("invalid", "channel name")
    now = clock()
    if not _rate_ok(login, now, min_interval):
        raise PlaybackRefused("blocked", "too soon since last resolve")
    op = opener or default_opener
    try:
        value, sig = _get_token(login, op, timeout)
        usher = (f"https://{USHER_HOST}/api/channel/hls/{login}.m3u8?"
                 + urllib.parse.urlencode({
                     "sig": sig, "token": value, "allow_source": "true",
                     "allow_audio_only": "true", "player": "twitchweb",
                     "platform": "web", "supported_codecs": "avc1",
                     "p": random.randint(0, 999999)}))
        if not _host_ok(usher, host_ok):
            raise PlaybackRefused("blocked", "playlist host not allowed")
        code, raw = op(urllib.request.Request(
            usher, headers={"User-Agent": "Mozilla/5.0"}), timeout)
        if code != 200:
            raise _status_refusal(code, "playlist")
        variants = parse_master(raw.decode("utf-8", "replace"), host_ok)
    except PlaybackRefused:
        raise
    except Exception as exc:  # network error text can carry a URL
        raise PlaybackRefused("error", redact(type(exc).__name__))
    if not variants:
        raise PlaybackRefused("error", "no usable video variant")
    fit = [v for v in variants if v.height <= max_height]
    best = (max(fit, key=lambda v: (v.height, v.bandwidth)) if fit
            else min(variants, key=lambda v: v.height))
    return Resolved(login, best, tuple(variants), now)


def ffmpeg_args(ffmpeg: str, resolved: Resolved, out_path: str,
                seconds: int, protocols: str = "https,tls,tcp,crypto") -> list:
    """Bounded capture args (stream copy, hard duration cap). The caller
    owns the process (track and reap it) and the authorization."""
    if not (0 < int(seconds) <= 3600):
        raise ValueError("seconds out of range")
    return [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin",
            "-protocol_whitelist", protocols,
            "-i", resolved.ffmpeg_input(), "-t", str(int(seconds)),
            "-c", "copy", "-f", "mpegts", out_path]
