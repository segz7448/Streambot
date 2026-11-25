"""Owner clip-generation limit, decided at the extraction boundary.

Scope: SETTLED by the owner (phonemsg-01M88R554DGFGGKA201G3KXN9G, Nov 24
2026 11:01 PM PST: "Yeah total of eight clips per day"): eight clips in
total per day across all sources ("global_day").

Counting rule: only clips that are not failed use a slot. This was NOT
stated by the owner as a rule; when asked, the owner replied that the
judgment was the assistant's to make (phonemsg-01M88R6ZNAFCX286X84C2W1ZF3)
and the assistant chose "failed attempts do not use slots"
(count_failed=False). In-flight clips (PENDING / RECOVERING) hold a
reservation: their clips row is inserted in the same BEGIN IMMEDIATE
transaction that checks the cap, so simultaneous extractors cannot exceed
the cap; a FAILED / QC_FAILED transition releases the slot. Slots released
after a later event was already refused (QUOTA_EXCEEDED) do not revive
that event.

The day is evaluated in the policy timezone (America/Los_Angeles, the one
the intake authority already uses; core/intake.py comments that timezone
"provisional; disclose before live", so the reset at local midnight is to
be disclosed to the owner). This module does NOT change the separate
upload/admission cap; it limits creation of rows in clips. Retried events
reuse their existing clips row and do not consume another slot.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from ..core.intake import DEFAULT_TIMEZONE, resolve_timezone

GENERATION_DAILY_CAP = 8
GENERATION_CAP_TIMEZONE = DEFAULT_TIMEZONE


def local_day_bounds(now: float,
                     timezone: str = GENERATION_CAP_TIMEZONE):
    """Return (day, start_epoch, end_epoch) for the policy-local day."""
    tz = resolve_timezone(timezone)
    day = datetime.fromtimestamp(now, tz).date()
    start = datetime(day.year, day.month, day.day, tzinfo=tz)
    next_day = day + timedelta(days=1)
    end = datetime(next_day.year, next_day.month, next_day.day,
                   tzinfo=tz)
    return day.isoformat(), start.timestamp(), end.timestamp()


# Scope: SETTLED by the owner as "global_day" (see module docstring).
# The other scopes remain selectable for tests / future change only:
#   scope: "global_day"  - all sources per policy-local day (OWNER CHOICE)
#          "source_day"  - per Twitch source per policy-local day
#          "session"     - per stream session
# count_failed: whether FAILED / QC_FAILED clip rows use up a slot.
#   assistant's judgment (owner delegated it): False.
GENERATION_CAP_SCOPE = "global_day"
GENERATION_COUNT_FAILED = False
_FAILED_STATUSES = ("FAILED", "QC_FAILED")


def check_quota(conn, session_id: int, now: float,
                cap: int = GENERATION_DAILY_CAP,
                timezone: str = GENERATION_CAP_TIMEZONE,
                scope: str | None = None,
                count_failed: bool | None = None) -> dict:
    """Read the count inside the caller's transaction (write transaction
    for the atomic gate; a plain read is a cheap precheck only)."""
    scope = GENERATION_CAP_SCOPE if scope is None else scope
    count_failed = (GENERATION_COUNT_FAILED if count_failed is None
                    else count_failed)
    if scope not in ("source_day", "session", "global_day"):
        raise ValueError(f"unknown generation cap scope {scope!r}")
    row = conn.execute(
        "SELECT s.source_id, t.username FROM stream_sessions s"
        " JOIN twitch_sources t ON t.id=s.source_id"
        " WHERE s.id=?", (session_id,)).fetchone()
    if row is None:
        raise RuntimeError(f"clip session {session_id} has no source")
    day, start, end = local_day_bounds(now, timezone)
    # global_day counts every clip row of the day, with no join that could
    # drop a row; the other scopes need the session/source link
    sql = ("SELECT count(*) FROM clips c"
           + ("" if scope == "global_day"
              else " JOIN stream_sessions s ON s.id=c.session_id")
           + " WHERE 1=1")
    args: list = []
    if scope == "session":
        sql += " AND c.session_id=?"
        args.append(session_id)
    else:
        sql += " AND c.created_at >= ? AND c.created_at < ?"
        args += [start, end]
        if scope == "source_day":
            sql += " AND s.source_id=?"
            args.append(row["source_id"])
    if not count_failed:
        sql += " AND c.status NOT IN ('FAILED','QC_FAILED')"
    count = conn.execute(sql, args).fetchone()[0]
    return {"allowed": count < cap,
            "count": int(count),
            "cap": int(cap),
            "day": day,
            "scope": scope,
            "count_failed": bool(count_failed),
            "source_id": int(row["source_id"]),
            "source": row["username"],
            "timezone": timezone}
