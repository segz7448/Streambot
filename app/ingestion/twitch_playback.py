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
turning it on needs the owner's source/rights decision. ffmpeg is never
given this URL: app.ingestion.hls_pump downloads every playlist and segment
through the same validated opener and pipes bytes to a network-less ffmpeg.
Undocumented endpoints can change without notice.
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
    blocked | quality | error. Message never contains a URL or token."""

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


def default_opener(req: urllib.request.Request, timeout: float,
                   limit: int = MAX_BYTES, deadline=None):
    """(status, body). Redirects refused. Reads limit+1 bytes in chunks and
    raises PlaybackRefused('error') on overflow instead of truncating. An
    absolute monotonic ``deadline`` is checked between chunks and bounds the
    connect timeout; a single blocked socket read can still run up to one
    socket timeout past it (documented, bounded by min(timeout, remaining))."""
    def remaining():
        return (deadline - time.monotonic()) if deadline is not None else timeout
    if remaining() <= 0:
        raise PlaybackRefused("error", "deadline reached")
    op = urllib.request.build_opener(_NoRedirect)
    try:
        with op.open(req, timeout=max(0.2, min(timeout, remaining()))) as r:
            chunks, n = [], 0
            while True:
                if remaining() <= 0:
                    raise PlaybackRefused("error", "deadline reached during read")
                b = r.read1(min(65536, limit + 1 - n))
                if not b:
                    break
                chunks.append(b); n += len(b)
                if n > limit:
                    raise PlaybackRefused("error", "response too large")
            return r.status, b"".join(chunks)
    except urllib.error.HTTPError as e:
        try:
            body = e.read(4096)
        except Exception:
            body = b""
        return e.code, body


def call_opener(op, req, timeout, limit=MAX_BYTES, deadline=None):
    """Uniform call so injected test openers and the real one agree."""
    return op(req, timeout, limit, deadline)


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


def _get_token(channel, opener, timeout, deadline=None):
    body = json.dumps({"operationName": "PlaybackAccessToken",
                       "variables": {"isLive": True, "login": channel,
                                     "playerType": "site"},
                       "query": _QUERY}).encode()
    req = urllib.request.Request(GQL_URL, data=body, headers={
        "Client-ID": PLAYER_CLIENT_ID, "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"})
    code, raw = call_opener(opener, req, timeout, 100_000, deadline)
    if code != 200:
        raise _status_refusal(code, "token")
    try:
        doc = json.loads(raw.decode("utf-8"))
    except Exception:
        raise PlaybackRefused("error", "token response not JSON") from None
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
        raise PlaybackRefused("offline", "no playback token returned") from None
    if not (isinstance(value, str) and isinstance(sig, str) and value and sig):
        raise PlaybackRefused("error", "token malformed")
    try:
        meta = json.loads(value)
    except Exception:
        raise PlaybackRefused("error", "token metadata not JSON") from None
    if not isinstance(meta, dict):
        raise PlaybackRefused("error", "token metadata malformed")
    auth = meta.get("authorization")
    if not isinstance(auth, dict) or auth.get("forbidden") is not False:
        raise PlaybackRefused("restricted", "playback not authorized for anonymous viewers")
    if meta.get("blackout_enabled") is True:
        raise PlaybackRefused("restricted", "channel blackout in effect")
    if str(meta.get("geoblock_reason") or "").strip():
        raise PlaybackRefused("restricted", "geoblocked")
    return value, sig


def parse_master(text: str, host_ok=None) -> list:
    out, lines = [], text.splitlines()
    for i, ln in enumerate(lines):
        if not ln.startswith("#EXT-X-STREAM-INF:"):
            continue
        attrs = ln.split(":", 1)[1]
        m = re.search(r"RESOLUTION=(\d+)x(\d+)", attrs)
        b = re.search(r"BANDWIDTH=(\d+)", attrs)
        url = lines[i + 1].strip() if i + 1 < len(lines) else ""
        if not m or not url or not _host_ok(url, host_ok):
            continue  # audio-only, malformed or off-allowlist: skipped
        # display label derived ONLY from validated numbers; no external
        # string (NAME etc.) is ever copied into a repr/log-visible field
        h = int(m.group(2))
        out.append(Variant(f"{h}p", h, int(b.group(1)) if b else 0, url))
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
    dl = time.monotonic() + max(timeout * 3, 1.0)   # whole-resolve deadline
    try:
        value, sig = _get_token(login, op, timeout, dl)
        usher = (f"https://{USHER_HOST}/api/channel/hls/{login}.m3u8?"
                 + urllib.parse.urlencode({
                     "sig": sig, "token": value, "allow_source": "true",
                     "allow_audio_only": "true", "player": "twitchweb",
                     "platform": "web", "supported_codecs": "avc1",
                     "p": random.randint(0, 999999)}))
        if not _host_ok(usher, host_ok):
            raise PlaybackRefused("blocked", "playlist host not allowed")
        code, raw = call_opener(op, urllib.request.Request(
            usher, headers={"User-Agent": "Mozilla/5.0"}), timeout,
            MAX_BYTES, dl)
        if code != 200:
            raise _status_refusal(code, "playlist")
        variants = parse_master(raw.decode("utf-8", "replace"), host_ok)
    except PlaybackRefused:
        raise
    except Exception as exc:  # network error text can carry a URL
        raise PlaybackRefused("error", redact(type(exc).__name__)) from None
    if not variants:
        raise PlaybackRefused("error", "no usable video variant")
    fit = [v for v in variants if v.height <= max_height]
    if not fit:
        raise PlaybackRefused("quality", "no variant within the height cap")
    best = max(fit, key=lambda v: (v.height, v.bandwidth))
    return Resolved(login, best, tuple(variants), now)
