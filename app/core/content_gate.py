"""Shared GTA6 content gate for every live-capture path.

The owner's brief (phonemsg-01M7TGJFM2GS9MJM4PDKEKJBJ5, 11/19 10:19) replaces
unaffordable video understanding with metadata matching: the streamer is
chosen by the owner and the stream title must match the supplied GTA 6
terms. This module is that gate and nothing more. It does NOT look at video,
does NOT prove what is on screen, and a matching title is only the owner's
own chosen criterion.

Contract (fail closed): a capture may start or continue only when a title
source supplied a CURRENT title for the selected streamer and that title
matches a GTA 6 term. No provider, a provider error, a missing or stale
title, or a mismatch is a refusal. Starting manually decides timing only; it
never waives this check. Local recordings and injected fixtures are not live
capture and do not call this gate.

No title source ships here. A source must be official or otherwise
authorized (for example a Twitch app token), supplied through
``set_provider``; until then every live path refuses.
"""
from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass

GTA6_TERMS = ("gta 6", "gta6", "gta vi", "gtavi",
              "grand theft auto 6", "grand theft auto vi")
MAX_TITLE_AGE_S = 120.0
FUTURE_SKEW_S = 5.0

_lock = threading.Lock()
_provider = None   # fn(username) -> (title: str, fetched_at_epoch: float) | None


@dataclass(frozen=True)
class Result:
    ok: bool
    reason: str   # ok | no_source | unavailable | stale | mismatch


_TEXT = {
    "ok": "the current stream title matches GTA 6",
    "no_source": "no authorized stream-title source is configured",
    "unavailable": "the current stream title could not be read",
    "stale": "the stream title is out of date",
    "mismatch": "the current stream title does not match GTA 6",
}


def describe(reason: str) -> str:
    return _TEXT.get(reason, "the stream title could not be verified")


def set_provider(fn) -> None:
    global _provider
    with _lock:
        _provider = fn


def _norm(text: str) -> str:
    return " ".join(re.sub(r"[^0-9a-z]+", " ", str(text).lower()).split())


def title_matches(title, terms=GTA6_TERMS) -> bool:
    if not isinstance(title, str) or not title.strip():
        return False
    hay = f" {_norm(title)} "
    return any(f" {_norm(t)} " in hay for t in terms if _norm(t))


def check(username: str, now=time.time) -> Result:
    with _lock:
        fn = _provider
    if fn is None:
        return Result(False, "no_source")
    try:
        got = fn(username)
        title, fetched_at = got
        age = now() - float(fetched_at)
    except Exception:  # noqa: BLE001 - any failure is a closed gate
        return Result(False, "unavailable")
    if age > MAX_TITLE_AGE_S or age < -FUTURE_SKEW_S:
        return Result(False, "stale")
    if not title_matches(title):
        return Result(False, "mismatch")
    return Result(True, "ok")


def qualified(username: str) -> bool:
    return check(username).ok
