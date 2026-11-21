"""Telegram surfaces for owner-scheduled clipping windows."""
from __future__ import annotations

import os
import re
import time
from datetime import datetime
from pathlib import Path

from ..core import schedule as sched
from ..core.intake import DEFAULT_TIMEZONE, resolve_timezone

_USER = re.compile(r"^[a-z0-9_]{3,25}$")


TZ_KEY = "schedule_timezone"


def get_tz(state_dir) -> str | None:
    try:
        with sched.session(state_dir) as c:
            r = c.execute("SELECT value FROM settings WHERE key=?",
                          (TZ_KEY,)).fetchone()
        return r[0] if r else None
    except Exception:
        return None


def timezone_reply(state_dir, args: str) -> str:
    a = args.strip()
    cur = get_tz(state_dir)
    if not a:
        return (f"Schedule timezone: {cur}." if cur else
                "Schedule timezone is NOT set, so /schedule is disabled. "
                f"Set it with /timezone <IANA name>, for example /timezone "
                f"{DEFAULT_TIMEZONE}. (Daily clip counting also uses "
                f"{DEFAULT_TIMEZONE}.)")
    try:
        resolve_timezone(a)
    except Exception:
        return ("Unknown or unavailable timezone on this device. The "
                f"supported one here is {DEFAULT_TIMEZONE}.")
    with sched.session(state_dir) as c:
        c.execute("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key)"
                  " DO UPDATE SET value=excluded.value", (TZ_KEY, a))
    return f"Schedule timezone set to {a}. Existing windows keep their exact instants."


def _fmt(ts: float, tzname: str = DEFAULT_TIMEZONE) -> str:
    return datetime.fromtimestamp(ts, resolve_timezone(tzname)
                                  ).strftime("%a %Y-%m-%d %H:%M %Z")


def schedule_reply(state_dir, args: str) -> str:
    usage = ("Usage:\n/schedule <user> <today|tomorrow|YYYY-MM-DD>   (mark the "
             "streamer for that whole local day)\n"
             "Optional, only if you want a fixed window instead: "
             "/schedule <user> <day> <HH:MM> <hours>  (hours 0.17 to 12). "
             "No time or duration is ever assumed for you.")
    tz = get_tz(state_dir)
    if tz is None:
        return timezone_reply(state_dir, "")
    p = args.split()
    if len(p) not in (2, 4):
        return usage + f"\nTimes use {tz}."
    if p[1] in ("today", "tomorrow"):
        now_dt = datetime.fromtimestamp(time.time(), resolve_timezone(tz))
        if now_dt.hour < 4:
            return ("It is between 00:00 and 04:00 for you, so 'today'/'tomorrow' "
                    "is ambiguous. Use an explicit YYYY-MM-DD date.")
    try:
        if len(p) == 4:
            dur = int(float(p[3]) * 3600)
            when = sched.parse_when(f"{p[1]} {p[2]}", tzname=tz)
            sid = sched.add(state_dir, p[0], when, dur)
            what = f"{_fmt(when, tz)} for {dur / 3600:g}h"
        else:
            when, end = sched.parse_day(p[1], tzname=tz)
            sid = sched.add(state_dir, p[0], when, None, day_end=end)
            what = f"the whole day {_fmt(when, tz)[:14]} ({tz}, to {_fmt(end, tz)})"
    except (ValueError, OverflowError) as exc:
        return f"Not scheduled: {exc}\n{usage}"
    return (f"Scheduled #{sid}: {p[0].lstrip('@').lower()} {what}. This only "
            "marks the streamer as eligible; the mark itself records nothing. "
            "While it is in effect I try every few minutes to start a session, "
            "only through an allowed route: a registered broadcaster feed, or "
            "public playback if recording is authorized for this streamer "
            "(default OFF). If the streamer is offline or the route is not "
            "allowed, nothing starts. It counts as recording only when a valid "
            "segment is saved. If a session ends early I try again until the "
            "schedule ends. I message you when a session starts, ends, or the "
            "schedule ends with nothing recorded. /stopclip or /unschedule <id> "
            "cancels it and nothing restarts. If Twitch restricts, challenges or "
            "throttles access I stop and tell you; I do not keep retrying. I "
            "cannot tell what game is being played, so nothing here is "
            "filtered to a particular game. Clips stay private.")


_STATE_TEXT = {
    "AWAITING_QUALIFICATION": "waiting: cannot verify the game is GTA6, not recording",
    "AWAITING_AUTH": "waiting: recording is not authorized for this streamer",
    "BLOCKED": "stopped: Twitch access was refused (/unschedule dismisses it)",
    "UNCERTAIN": "held: cannot prove an earlier session finished",
}


def schedules_reply(state_dir, _args="") -> str:
    rows = sched.list_visible(state_dir)
    fenced = [q["username"] for q in sched.quarantined(state_dir)]
    if not rows and not fenced:
        return "No scheduled clipping windows."
    tz = get_tz(state_dir) or DEFAULT_TIMEZONE
    out = ["Schedules:"] if rows else []
    out += [
        f"#{r['id']} {r['username']} {_fmt(r['start_at'], tz)} "
        f"{'whole day' if r['mode'] == 'day' else format((r['end_at'] - r['start_at']) / 3600, 'g') + 'h'}"
        f" {_STATE_TEXT.get(r['state'], r['state'])}" for r in rows]
    if fenced:
        out.append("Fenced (no new session can start until the earlier one is "
                   "proven finished, even if its schedule was cancelled): "
                   + ", ".join(fenced))
    return "\n".join(out)


def unschedule_reply(state_dir, args: str) -> str:
    try:
        sid = int(args.strip())
    except ValueError:
        return "Usage: /unschedule <id>"
    r = sched.cancel(state_dir, sid)
    if r == "cancelled":
        return f"Schedule #{sid} cancelled. Nothing will start from it."
    if r == "cancelled_running":
        return (f"Schedule #{sid} cancelled. Its running session is being "
                "stopped (within about 30 seconds) and will not restart.")
    return f"No open schedule #{sid} (already finished or cancelled)."
