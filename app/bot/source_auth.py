"""Per-source authorization for third-party playback capture.

Two SEPARATE scopes, both default OFF, never inferred from a channel being
public, registered, or live:
  record : the bot may capture this source's public live playback
  repost : clips from a playback session may be queued for YouTube upload
Stored in nova.db settings (srcauth:<user>:<scope>). Unreadable or any
unexpected value counts as NOT authorized. Only the paired owner can set
them (the /sourceauth command goes through the admin-only router).
"""
from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path

SCOPES = ("record", "repost")
_NAME = re.compile(r"^[a-z0-9_]{3,25}$")


def _key(user: str, scope: str) -> str:
    return f"srcauth:{user}:{scope}"


def _db(state_dir, ro=False):
    p = Path(state_dir) / "nova.db"
    if ro:
        return sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=5)
    return sqlite3.connect(str(p), timeout=10)


def allowed(state_dir, user: str, scope: str) -> bool:
    """True only for an explicit stored '1'. Fail closed."""
    if scope not in SCOPES or not _NAME.match(user or ""):
        return False
    try:
        c = _db(state_dir, ro=True)
        try:
            r = c.execute("SELECT value FROM settings WHERE key=?",
                          (_key(user, scope),)).fetchone()
        finally:
            c.close()
    except sqlite3.Error:
        return False
    return bool(r and r[0] == "1")


def set_scope(state_dir, user: str, scope: str, on: bool) -> int:
    """Save the scope and (for repost on) requeue this source's grant-denied
    jobs in ONE transaction: either both happen or neither. Returns the
    number of jobs requeued."""
    c = _db(state_dir)
    try:
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key)"
                      " DO UPDATE SET value=excluded.value",
                      (_key(user, scope), "1" if on else "0"))
            n = 0
            if scope == "repost" and on:
                from ..core.provenance import requeue_denied
                n = requeue_denied(c, user)
            c.commit()
            return n
        except BaseException:
            c.rollback()
            raise
    finally:
        c.close()


def sourceauth_reply(state_dir, args: str) -> str:
    """/sourceauth <user> [record|repost] [on|off]  (no scope: status)."""
    parts = (args or "").split()
    usage = ("Usage: /sourceauth <twitch-username> [record|repost] [on|off]\n"
             "record = the bot may capture that streamer's public live "
             "playback. repost = clips from it may be queued for YouTube. "
             "Both default OFF and are separate. Turn a scope on only if "
             "you have that streamer's permission for it.")
    if not parts:
        return usage
    user = parts[0].lstrip("@").lower()
    if not _NAME.match(user):
        return "That is not a valid Twitch username.\n" + usage
    if len(parts) == 1:
        return (f"{user}: record {'ON' if allowed(state_dir, user, 'record') else 'off'}, "
                f"repost {'ON' if allowed(state_dir, user, 'repost') else 'off'}.")
    if len(parts) != 3 or parts[1].lower() not in SCOPES \
            or parts[2].lower() not in ("on", "off"):
        return usage
    scope, on = parts[1].lower(), parts[2].lower() == "on"
    try:
        requeued = set_scope(state_dir, user, scope, on)
    except sqlite3.Error:
        return (f"Could NOT change {scope} authorization for {user} "
                "(database unavailable). Nothing changed.")
    note = {
        ("record", True): "Capture of this streamer's public live playback is now "
                          "permitted once playback capture is available. Clips "
                          "stay HELD for you and are not queued for YouTube "
                          "unless repost is also on.",
        ("record", False): "Capture is off. A running playback session is told to "
                           "stop at its next authorization check (not instant).",
        ("repost", True): "Playback clips from this streamer may now be queued for "
                          "YouTube (private upload, existing daily cap). Only clips whose "
                          "publish was refused earlier are re-queued; clips made while "
                          "repost was off were never queued and are NOT backfilled.",
        ("repost", False): "Repost is off: any later publish attempt for this "
                           "streamer's playback clips (automatic, queued or manual) "
                           "is refused. Work already reserved or in flight may still start or "
                           "finish; it cannot be recalled.",
    }[(scope, on)]
    extra = (f" {requeued} earlier-refused job(s) for this streamer re-queued "
             "(each is re-checked before anything is sent).") if requeued else ""
    return f"{user} {scope}: {'ON' if on else 'off'}. {note}{extra}"
