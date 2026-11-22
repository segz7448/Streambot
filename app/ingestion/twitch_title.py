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
  * ACCESS REFUSAL IS TERMINAL: HTTP 401/403/429, a GraphQL error that
    says unauthorized/forbidden/rate-limited/challenge/integrity/captcha, or
    a shared PlaybackRefused of kind blocked/challenge/restricted latches this
    source closed. The latch is written to disk (0600, a typed kind and a
    time only), so an ordinary process restart does not clear it, and NO
    request is made again, ever, by any timer or scheduler. Only an explicit,
    authorized ``reset_latch(authorized=True)`` (or deleting the latch file)
    clears it; nothing in the service calls that. Nothing is worked around.
    The latch has priority over every result: a fetch that finished after a
    latch was set (for any channel) is discarded, and no request is made at
    all unless the latch file location is writable right now, so a refusal
    that could not be persisted can never be forgotten by a restart;
  * a redirect, a non-JSON body, a 5xx or a transport error is transient:
    bounded retry after ERROR_BACKOFF_S; a query the endpoint rejects for
    another reason after REJECT_BACKOFF_S;
  * no cookies, no impersonation, no proxy rotation, no integrity forging.

Rate: at most one request per channel per MIN_INTERVAL_S; inside that window
the previous evidence (with its original fetch time) is returned, so the
gate's age check still applies. Nothing is logged except a typed kind.
Audio, video and segments are never requested here.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.request

from ..core.content_gate import Evidence
from . import twitch_playback as tp

MIN_INTERVAL_S = 15.0
ERROR_BACKOFF_S = 30.0
REJECT_BACKOFF_S = 300.0
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


TERMINAL = ("blocked", "challenge", "restricted")
_CHALLENGE_WORDS = ("integrity", "challenge", "captcha", "kasada")
_BLOCK_WORDS = ("unauthorized", "unauthenticated", "forbidden", "toomanyrequests",
                "too many requests", "rate limit", "ratelimit", "rate_limit",
                "access denied", "not authorized")
LATCH_FILE = "title_source_latch.json"
_latch_path_override = None      # tests
_latched = None                  # in-memory copy of the kind, once set

_lock = threading.Lock()
_cache: dict = {}        # login -> (Evidence, monotonic fetched)
_backoff: dict = {}      # login -> monotonic deadline
_locks: dict = {}        # login -> per-channel lock (serialises a burst)


def latch_path() -> str:
    if _latch_path_override:
        return _latch_path_override
    state = os.environ.get("NOVA_STATE_DIR") or os.path.expanduser("~/.nova-clip")
    return os.path.join(state, LATCH_FILE)


def latched():
    """The latched refusal kind, or None. Checks the file on every call so a
    restart (or another process) sees the same latch; the in-memory copy
    keeps it closed even if the file cannot be written or read."""
    global _latched
    if _latched:
        return _latched
    try:
        with open(latch_path(), "r", encoding="utf-8") as fh:
            doc = json.load(fh)
        kind = doc.get("kind") if isinstance(doc, dict) else None
        _latched = kind if kind in TERMINAL else "blocked"
    except FileNotFoundError:
        return None
    except Exception:           # unreadable/corrupt latch file: stay closed
        _latched = "blocked"
    return _latched


def _write(path: str, doc: dict) -> None:
    os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(doc, fh)
    os.replace(tmp, path)


def _set_latch(kind: str, now: float) -> None:
    """Latch under the module lock so a concurrent publisher sees it. The
    memory copy always holds; the file write is best effort here because
    ``_durable_ready`` already proved the location writable before the
    request was made."""
    global _latched
    with _lock:
        _latched = kind if kind in TERMINAL else "blocked"
        kind = _latched
    try:
        _write(latch_path(), {"kind": kind, "at": int(now)})
    except OSError:
        pass


def _durable_ready() -> bool:
    """True only if the latch file location can be written right now (a
    probe file is written and replaced). Called before every request: if a
    refusal could not be recorded, no request is made."""
    try:
        path = os.path.join(os.path.dirname(latch_path()), "title_source_ready")
        _write(path, {"ok": 1})
        return True
    except OSError:
        return False


def reset_latch(*, authorized: bool = False) -> bool:
    """Explicit, authorized clear of the refusal latch. Never called by the
    service, a timer or the scheduler."""
    global _latched
    if authorized is not True:
        return False
    _latched = None
    try:
        os.remove(latch_path())
    except FileNotFoundError:
        pass
    except OSError:
        return False
    return True


def reset() -> None:
    """Forget cache and backoff (tests). Does NOT clear the latch."""
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
        if any(w in txt for w in _CHALLENGE_WORDS):
            raise TitleUnavailable("challenge")
        if any(w in txt for w in _BLOCK_WORDS):
            raise TitleUnavailable("blocked")
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
    if latched():
        raise TitleUnavailable("latched")           # no request, ever
    with _chan_lock(login):
        if latched():
            raise TitleUnavailable("latched")
        t = mono()
        with _lock:
            until = _backoff.get(login, 0.0)
            hit = _cache.get(login)
        if t < until:
            raise TitleUnavailable("backoff")
        if hit is not None and t - hit[1] < MIN_INTERVAL_S:
            with _lock:
                if latched():
                    raise TitleUnavailable("latched")
            return hit[0]
        if not _durable_ready():
            raise TitleUnavailable("no_state")      # cannot persist a refusal
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
            if exc.kind in TERMINAL:
                _set_latch(exc.kind, clock())
            else:
                wait = REJECT_BACKOFF_S if exc.kind == "rejected" \
                    else ERROR_BACKOFF_S
                with _lock:
                    _backoff[login] = mono() + wait
            raise
        except tp.PlaybackRefused as exc:
            if exc.kind in TERMINAL:
                _set_latch(exc.kind, clock())
                raise TitleUnavailable(exc.kind) from None
            with _lock:
                _backoff[login] = mono() + ERROR_BACKOFF_S
            raise TitleUnavailable("transport") from None
        except Exception:
            with _lock:
                _backoff[login] = mono() + ERROR_BACKOFF_S
            raise TitleUnavailable("error") from None
        with _lock:
            if latched():                 # a refusal (any channel) won the race
                raise TitleUnavailable("latched")
            _cache[login] = (ev, mono())
            _backoff.pop(login, None)
        return ev


def provider(username: str) -> Evidence:
    """The content_gate provider: ``content_gate.set_provider(provider)``."""
    return fetch(username)
