"""Shared GTA6 content gate for every live-capture path.

The owner's brief (phonemsg-01M7TGJFM2GS9MJM4PDKEKJBJ5, 11/19 10:19) replaces
unaffordable video understanding with metadata matching: the owner picks the
streamer and the selected streamer's CURRENT live stream title must match the
supplied GTA 6 terms. Nothing here looks at video or proves gameplay.

Fail closed: a capture may start or continue only when a title source returned
typed evidence that (a) names the selected streamer, (b) says the stream is
live, (c) has a finite, current fetch time and (d) has a title that matches.
No provider, a provider error, malformed evidence, a different channel, not
live, stale, or mismatch is a refusal. A manual start decides timing only.
Local recordings and injected fixtures are not live capture and skip this.

No title source ships. One must be official or otherwise authorized and is
supplied through ``set_provider``; until then every live path refuses.

Locking: ``check`` calls the provider with NO lock held and records the typed
outcome. ``check_cached`` only validates that recorded outcome (in memory,
non-blocking) and is the only form allowed under a registry lock.
"""
from __future__ import annotations

import math
import re
import threading
import time
from dataclasses import dataclass

GTA6_TERMS = ("gta 6", "gta6", "gta vi", "gtavi",
              "grand theft auto 6", "grand theft auto vi")
MAX_TITLE_AGE_S = 120.0
FUTURE_SKEW_S = 5.0

_lock = threading.Lock()      # guards only the two small dicts below
_provider = None              # fn(username) -> Evidence
_last: dict = {}              # username -> (Result, Evidence | None)


@dataclass(frozen=True)
class Evidence:
    channel: str       # the streamer this title belongs to
    title: str
    fetched_at: float  # epoch seconds
    live: bool         # the provider says this stream is live now


@dataclass(frozen=True)
class Result:
    ok: bool
    reason: str   # ok|no_source|unavailable|invalid|wrong_channel|not_live|stale|mismatch


_TEXT = {
    "ok": "the selected stream's current live title matches GTA 6",
    "no_source": "no authorized live-title source is configured",
    "unavailable": "the selected stream's current live title is unavailable",
    "invalid": "the live-title evidence is invalid",
    "wrong_channel": "the live-title evidence is for a different channel",
    "not_live": "the selected stream is not live",
    "stale": "the live-title evidence is out of date",
    "mismatch": "the selected stream's current live title does not match GTA 6",
}


def describe(reason: str) -> str:
    return _TEXT.get(reason, "the live-title evidence could not be validated")


def set_provider(fn) -> None:
    global _provider
    with _lock:
        _provider = fn
        _last.clear()


def _user(name) -> str:
    return str(name).strip().lstrip("@").lower()


def _norm(text: str) -> str:
    return " ".join(re.sub(r"[^0-9a-z]+", " ", str(text).lower()).split())


def title_matches(title, terms=GTA6_TERMS) -> bool:
    if not isinstance(title, str) or not title.strip():
        return False
    hay = f" {_norm(title)} "
    return any(f" {_norm(t)} " in hay for t in terms if _norm(t))


def _judge(username: str, ev, now: float) -> Result:
    if not isinstance(ev, Evidence):
        return Result(False, "invalid")
    t = ev.fetched_at
    if (isinstance(t, bool) or not isinstance(t, (int, float))
            or not math.isfinite(t) or not isinstance(ev.title, str)
            or not isinstance(ev.live, bool) or not isinstance(ev.channel, str)):
        return Result(False, "invalid")
    if _user(ev.channel) != _user(username):
        return Result(False, "wrong_channel")
    if not ev.live:
        return Result(False, "not_live")
    age = now - float(t)
    if age > MAX_TITLE_AGE_S or age < -FUTURE_SKEW_S:
        return Result(False, "stale")
    if not title_matches(ev.title):
        return Result(False, "mismatch")
    return Result(True, "ok")


def check(username: str, now=time.time) -> Result:
    """Ask the provider (blocking; never call under a registry lock)."""
    u = _user(username)
    with _lock:
        fn = _provider
    if fn is None:
        res, ev = Result(False, "no_source"), None
    else:
        try:
            ev = fn(u)
        except Exception:  # noqa: BLE001 - any failure is a closed gate
            res, ev = Result(False, "unavailable"), None
        else:
            try:
                res = _judge(u, ev, now())
            except Exception:  # noqa: BLE001
                res = Result(False, "invalid")
    with _lock:
        if _provider is fn:
            _last[u] = (res, ev)
    return res


def check_cached(username: str, now=time.time) -> Result:
    """Validate the last recorded evidence without calling the provider.
    Fails closed when there is none, when the last check failed, or when the
    evidence has since gone stale. Safe under a registry lock."""
    u = _user(username)
    with _lock:
        got = _last.get(u)
    if got is None:
        return Result(False, "no_source" if _provider is None else "unavailable")
    res, ev = got
    if not res.ok:
        return res
    try:
        return _judge(u, ev, now())
    except Exception:  # noqa: BLE001
        return Result(False, "invalid")


def qualified(username: str) -> bool:
    return check(username).ok
