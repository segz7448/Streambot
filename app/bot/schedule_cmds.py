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
             "whole local day: clip when a feed is available)\n"
             "/schedule <user> <day> <HH:MM> <hours>   (a fixed window)\n"
             "Hours 0.17 to 12.")
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
    return (f"Scheduled #{sid}: {p[0].lstrip('@').lower()} {what}. It starts the "
            "normal authorized-feed session when a feed is registered (see "
            "/setfeed), and counts as recording only after the first segment "
            "is saved. Live detection needs Twitch credentials, which are not "
            "configured, so a day mark cannot see when the streamer goes live; "
            "it only retries launching your registered feed. Clips stay "
            "private. /schedules lists, /unschedule <id> cancels.")


def schedules_reply(state_dir, _args="") -> str:
    rows = sched.list_open(state_dir)
    if not rows:
        return "No scheduled clipping windows."
    tz = get_tz(state_dir) or DEFAULT_TIMEZONE
    return "Scheduled windows:\n" + "\n".join(
        f"#{r['id']} {r['username']} {_fmt(r['start_at'], tz)} "
        f"{'day' if r.get('mode') == 'day' else format(r['duration_s'] / 3600, 'g') + 'h'}"
        f" {r['state']}" for r in rows)


def unschedule_reply(state_dir, args: str) -> str:
    try:
        sid = int(args.strip())
    except ValueError:
        return "Usage: /unschedule <id>"
    return (f"Schedule #{sid} cancelled." if sched.cancel(state_dir, sid)
            else f"No cancellable schedule #{sid} (only SCHEDULED can be cancelled; "
                 "use /stopclip for a running one).")
