"""Anonymous, read-only live-title evidence for the content gate.

UNOFFICIAL. This asks the same public web-player GraphQL endpoint that
app.ingestion.twitch_playback already uses (same public player client id, no
account, no cookie, no OAuth, no stored credential) one small question about
a channel: is a stream running right now, and what is the broadcast title.
Twitch's terms restrict automated access and the endpoint can change or block
this at any time. It is not an official API and is not approved by Twitch.

The owner asked for this player-facing route (11/19 00:28) and for the
selected streamer's live title to decide GTA 6 capture (11/19 10:19).

Evidence rules (all fail closed, enforced again by content_gate):
  * one response must name the channel (``user.login`` equals the request)
    and carry a non-null ``stream`` with a string id. Only then is it live;
  * the title is ``lastBroadcast.title`` from that SAME response, and is used
    only when the stream is live. ``lastBroadcast`` alone never counts: an
    offline channel returns ``live=False``, so the gate says not_live;
  * a missing user, malformed or empty shape, or a non-string/blank title
    while live raises (the gate then reports "unavailable");
  * 401/403/429, a challenge or integrity response, a redirect, a non-JSON
    body, or a transport error raises. Nothing is retried or worked around:
    after a block/restriction/challenge this module makes NO request for
    BLOCK_BACKOFF_S, after another error for ERROR_BACKOFF_S;
  * no cookies, no impersonation, no proxy rotation, no integrity forging.

Rate: at most one request per channel per MIN_INTERVAL_S; inside that window
the previous evidence (with its original fetch time) is returned, so the
gate's age check still applies. Nothing is logged except a typed kind.
Audio, video and segments are never requested here.
"""
from __future__ import annotations

import json
import re
import threading
import time
import urllib.request

from ..core.content_gate import Evidence
from . import twitch_playback as tp

MIN_INTERVAL_S = 15.0
BLOCK_BACKOFF_S = 600.0
ERROR_BACKOFF_S = 30.0
TIMEOUT_S = 15.0
_LOGIN = re.compile(r"^[a-z0-9_]{3,25}$")

_QUERY = ("query StreamTitle($login: String!) { user(login: $login) "
          "{ login stream { id } lastBroadcast { title } } }")


class TitleUnavailable(Exception):
    """Raised for every inconclusive answer. ``kind`` is a short typed
    label; the message never carries a URL, token or response text."""

    def __init__(self, kind: str):
        super().__init__(kind)
        self.kind = kind


_lock = threading.Lock()
_cache: dict = {}        # login -> (Evidence, monotonic fetched)
_backoff: dict = {}      # login -> monotonic deadline
_locks: dict = {}        # login -> per-channel lock (serialises a burst)


def reset() -> None:
    """Forget cache and backoff (tests)."""
    with _lock:
        _cache.clear()
        _backoff.clear()
        _locks.clear()


def _chan_lock(login: str):
    with _lock:
        return _locks.setdefault(login, threading.Lock())


def _parse(doc, login: str, now: float) -> Evidence:
    if not isinstance(doc, dict):
        raise TitleUnavailable("shape")
    errs = doc.get("errors")
    if errs:
        txt = json.dumps(errs).lower()
        if any(w in txt for w in ("integrity", "challenge", "captcha", "kasada")):
            raise TitleUnavailable("challenge")
        raise TitleUnavailable("rejected")
    data = doc.get("data")
    user = data.get("user") if isinstance(data, dict) else None
    if not isinstance(user, dict):
        raise TitleUnavailable("no_user")        # unknown channel or bad shape
    got = user.get("login")
    if not isinstance(got, str) or not got.strip():
        raise TitleUnavailable("shape")
    got = got.strip().lower()
    stream = user.get("stream")
    if stream is None:                            # not live: typed, not an error
        return Evidence(got, "", now, False)
    if not isinstance(stream, dict) or not isinstance(stream.get("id"), str) \
            or not stream["id"].strip():
        raise TitleUnavailable("shape")
    lb = user.get("lastBroadcast")
    title = lb.get("title") if isinstance(lb, dict) else None
    if not isinstance(title, str) or not title.strip():
        raise TitleUnavailable("no_title")
    return Evidence(got, title.strip(), now, True)


def fetch(channel: str, *, opener=None, clock=time.time,
          mono=time.monotonic, timeout: float = TIMEOUT_S) -> Evidence:
    """One title/live read. Raises TitleUnavailable; never returns a guess."""
    login = (channel or "").strip().lstrip("@").lower()
    if not _LOGIN.match(login):
        raise TitleUnavailable("invalid")
    with _chan_lock(login):
        t = mono()
        with _lock:
            until = _backoff.get(login, 0.0)
            hit = _cache.get(login)
        if t < until:
            raise TitleUnavailable("backoff")
        if hit is not None and t - hit[1] < MIN_INTERVAL_S:
            return hit[0]
        body = json.dumps({
            "operationName": "StreamTitle", "query": _QUERY,
            "variables": {"login": login}}).encode()
        req = urllib.request.Request(tp.GQL_URL, data=body, headers={
            "Client-ID": tp.PLAYER_CLIENT_ID,
            "Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
        op = opener or tp.default_opener
        try:
            code, raw = tp.call_opener(op, req, timeout, 100_000,
                                       mono() + max(timeout * 2, 1.0), None)
            if code in (401, 403, 429):
                raise TitleUnavailable("blocked")
            if code != 200:
                raise TitleUnavailable("http")
            try:
                doc = json.loads(raw.decode("utf-8"))
            except Exception:
                raise TitleUnavailable("not_json") from None
            ev = _parse(doc, login, clock())
        except TitleUnavailable as exc:
            wait = BLOCK_BACKOFF_S if exc.kind in ("blocked", "challenge") \
                else ERROR_BACKOFF_S
            with _lock:
                _backoff[login] = mono() + wait
            raise
        except tp.PlaybackRefused as exc:
            with _lock:
                _backoff[login] = mono() + ERROR_BACKOFF_S
            raise TitleUnavailable("transport") from None
        except Exception:
            with _lock:
                _backoff[login] = mono() + ERROR_BACKOFF_S
            raise TitleUnavailable("error") from None
        with _lock:
            _cache[login] = (ev, mono())
            _backoff.pop(login, None)
        return ev


def provider(username: str) -> Evidence:
    """The content_gate provider: ``content_gate.set_provider(provider)``."""
    return fetch(username)
