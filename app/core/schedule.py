"""Owner-scheduled clipping windows.

The owner marks a streamer and a start time ahead of time. At that time
the service starts the normal authorized-feed session for that source.
A schedule never creates a feed, never bypasses authorization and never
changes publish privacy: a window with no registered feed ends MISSED
with the reason, it never pretends recording started.

States: SCHEDULED -> STARTING (claim committed BEFORE any launch) ->
LAUNCHED (thread started, token bound, NOT yet recording) -> RECORDING
(first recording segment persisted: the only confirmation) -> STOPPING ->
DONE; failures end MISSED(reason); CANCELLED by the owner. Launch and stop
run OUTSIDE any DB transaction. A restart ends every non-terminal active
state as MISSED("interrupted") via reconcile(): sessions die with the
process, and a window is never relaunched automatically after a claim.
Times are stored as UTC epoch and entered in the admission-day zone.
"""
from __future__ import annotations

import contextlib
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

from .intake import DEFAULT_TIMEZONE, resolve_timezone

DDL = """
CREATE TABLE IF NOT EXISTS clip_schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    start_at REAL NOT NULL,
    duration_s INTEGER NOT NULL,
    end_at REAL,
    mode TEXT NOT NULL DEFAULT 'window',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_try_at REAL,
    state TEXT NOT NULL DEFAULT 'SCHEDULED',
    note TEXT,
    token TEXT,
    claim_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
"""
MAX_DURATION_S = 12 * 3600
RETRY_S = 300
MAX_ATTEMPTS = 288
MAX_OPEN = 50
GRACE_S = 15 * 60  # a window older than this at pickup is MISSED
_USER = re.compile(r"^[a-z0-9_]{3,25}$")


def _conn(state_dir) -> sqlite3.Connection:
    c = sqlite3.connect(str(Path(state_dir) / "nova.db"), timeout=10)
    c.row_factory = sqlite3.Row
    c.executescript(DDL)
    cols = {r["name"] for r in c.execute("PRAGMA table_info(clip_schedules)")}
    for col, ddl in (("end_at", "REAL"), ("mode", "TEXT NOT NULL DEFAULT 'window'"),
                     ("attempts", "INTEGER NOT NULL DEFAULT 0"), ("next_try_at", "REAL")):
        if col not in cols:
            c.execute(f"ALTER TABLE clip_schedules ADD COLUMN {col} {ddl}")
    return c


@contextlib.contextmanager
def session(state_dir):
    """Transaction that commits/rolls back AND closes (sqlite3's own
    `with` never closes, which leaks a connection per call)."""
    c = _conn(state_dir)
    try:
        with c:
            yield c
    finally:
        c.close()


def parse_when(text: str, now: float | None = None,
               tzname: str = DEFAULT_TIMEZONE) -> float:
    """'today|tomorrow|YYYY-MM-DD' + 'HH:MM' (24h, local zone) -> epoch."""
    tz = resolve_timezone(tzname)
    now_dt = datetime.fromtimestamp(now if now is not None else time.time(), tz)
    parts = text.split()
    if len(parts) != 2:
        raise ValueError("when must be '<today|tomorrow|YYYY-MM-DD> <HH:MM>'")
    day, hhmm = parts
    if day == "today":
        d = now_dt.date()
    elif day == "tomorrow":
        d = now_dt.date() + timedelta(days=1)
    else:
        d = datetime.strptime(day, "%Y-%m-%d").date()
    h, m = datetime.strptime(hhmm, "%H:%M").hour, datetime.strptime(hhmm, "%H:%M").minute
    candidates = []
    for fold in (0, 1):
        local = datetime(d.year, d.month, d.day, h, m, tzinfo=tz, fold=fold)
        rt = datetime.fromtimestamp(local.timestamp(), tz)
        if (rt.year, rt.month, rt.day, rt.hour, rt.minute) == (d.year, d.month, d.day, h, m):
            candidates.append(local.timestamp())
    uniq = sorted(set(candidates))
    if not uniq:
        raise ValueError("that local time does not exist (clock change); pick another time")
    if len(uniq) > 1:
        raise ValueError("that local time happens twice (clock change); pick another time")
    return uniq[0]


def parse_day(day: str, now: float | None = None,
              tzname: str = DEFAULT_TIMEZONE) -> tuple[float, float]:
    """'today|tomorrow|YYYY-MM-DD' -> (local midnight start, next local
    midnight end) as epochs in the zone (DST-safe: 23/25h days are exact)."""
    tz = resolve_timezone(tzname)
    now_dt = datetime.fromtimestamp(now if now is not None else time.time(), tz)
    if day == "today":
        d = now_dt.date()
    elif day == "tomorrow":
        d = now_dt.date() + timedelta(days=1)
    else:
        d = datetime.strptime(day, "%Y-%m-%d").date()
    nd = d + timedelta(days=1)
    return (datetime(d.year, d.month, d.day, tzinfo=tz).timestamp(),
            datetime(nd.year, nd.month, nd.day, tzinfo=tz).timestamp())


def add(state_dir, username: str, start_at: float, duration_s: int | None = None,
        now: float | None = None, day_end: float | None = None) -> int:
    username = username.strip().lstrip("@").lower()
    if not _USER.match(username):
        raise ValueError("invalid username")
    now = now if now is not None else time.time()
    mode = "window" if duration_s is not None else "day"
    if mode == "window":
        dur = duration_s
        if not 600 <= dur <= MAX_DURATION_S:
            raise ValueError("duration must be between 10 minutes and 12 hours")
        end_at = start_at + dur
    else:
        end_at = day_end
        dur = int(end_at - start_at)
    if end_at <= now:
        raise ValueError("that time is in the past")
    with session(state_dir) as c:
        if not c.execute("SELECT 1 FROM twitch_sources WHERE username=?",
                         (username,)).fetchone():
            raise ValueError(f"{username} is not registered (use /addsource)")
        if c.execute("SELECT COUNT(*) FROM clip_schedules WHERE state='SCHEDULED'"
                     ).fetchone()[0] >= MAX_OPEN:
            raise ValueError("too many open schedules")
        if c.execute("SELECT 1 FROM clip_schedules WHERE username=? AND state IN "
                     "('SCHEDULED','STARTING','LAUNCHED','RECORDING','STOPPING') "
                     "AND start_at < ? AND COALESCE(end_at,start_at+duration_s) > ?",
                     (username, end_at, start_at)).fetchone():
            raise ValueError("overlaps another window for that streamer")
        cur = c.execute("INSERT INTO clip_schedules (username,start_at,duration_s,"
                        "end_at,mode,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
                        (username, start_at, dur, end_at, mode, now, now))
        return cur.lastrowid


def list_open(state_dir) -> list[dict]:
    with session(state_dir) as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM clip_schedules WHERE state IN "
            "('SCHEDULED','STARTING','LAUNCHED','RECORDING','STOPPING')"
            " ORDER BY start_at")]


def cancel(state_dir, sid: int) -> bool:
    with session(state_dir) as c:
        cur = c.execute("UPDATE clip_schedules SET state='CANCELLED',updated_at=?"
                        " WHERE id=? AND state='SCHEDULED'", (time.time(), sid))
        return cur.rowcount == 1


ACTIVE = ("STARTING", "LAUNCHED", "RECORDING", "STOPPING")
CONFIRM_TIMEOUT_S = 180


def _auto(state_dir) -> sqlite3.Connection:
    c = sqlite3.connect(str(Path(state_dir) / "nova.db"), timeout=10,
                        isolation_level=None)
    c.row_factory = sqlite3.Row
    return c


def _cas(c, sid, frm, to, note=None, token=None, claim_at=None) -> bool:
    """One committed single-statement compare-and-set (autocommit)."""
    sets, args = "state=?,note=COALESCE(?,note),updated_at=?", [to, note, time.time()]
    if token is not None:
        sets += ",token=?"
        args.append(token)
    if claim_at is not None:
        sets += ",claim_at=?"
        args.append(claim_at)
    args += [sid, frm]
    return c.execute(f"UPDATE clip_schedules SET {sets} WHERE id=? AND state=?",
                     args).rowcount == 1


def reconcile(state_dir) -> int:
    """Startup: sessions did not survive the restart. Active windows end
    MISSED; nothing is relaunched or reported as recording."""
    _conn(state_dir).close()
    c = _auto(state_dir)
    try:
        return c.execute(
            "UPDATE clip_schedules SET state='MISSED',updated_at=?,note="
            "'interrupted by a service restart; verify with /sessions' "
            "WHERE state IN ('STARTING','LAUNCHED','RECORDING','STOPPING')",
            (time.time(),)).rowcount
    finally:
        c.close()


def tick(state_dir, start_fn, stop_fn, *, alive_fn, confirm_fn, error_fn,
         now: float | None = None) -> list[tuple[int, str, str]]:
    """Advance schedules once; returns owner notices [(id, state, text)].

    start_fn(username) -> (reply, token|None)
    alive_fn(username, token) -> bool        exact session still running
    confirm_fn(username, since_epoch) -> bool  a recording segment exists
    stop_fn(username, token) -> bool         stopped exactly that session
    error_fn(username, token) -> str         session's own error, if any
    No callback runs inside a DB transaction; every transition is a
    committed compare-and-set, claimed before the launch it guards."""
    now = now if now is not None else time.time()
    _conn(state_dir).close()  # ensure table
    c = _auto(state_dir)
    ev = []
    try:
        # 1. finish windows that ended
        for r in c.execute("SELECT * FROM clip_schedules WHERE state IN "
                           "('LAUNCHED','RECORDING') AND COALESCE(end_at,start_at+duration_s)<=?",
                           (now,)).fetchall():
            if not _cas(c, r["id"], r["state"], "STOPPING"):
                continue
            ok = stop_fn(r["username"], r["token"])
            _cas(c, r["id"], "STOPPING", "DONE",
                 "stopped" if ok else "session was no longer running")
            ev.append((r["id"], "DONE", f"Scheduled window #{r['id']} for "
                       f"{r['username']} ended; " + ("session stopped."
                       if ok else "its session was no longer running.")))
        # 2. verify launched/recording sessions
        for r in c.execute("SELECT * FROM clip_schedules WHERE state IN "
                           "('LAUNCHED','RECORDING')").fetchall():
            alive = alive_fn(r["username"], r["token"])
            if r["state"] == "LAUNCHED":
                if confirm_fn(r["username"], r["claim_at"]) and alive:
                    if _cas(c, r["id"], "LAUNCHED", "RECORDING"):
                        ev.append((r["id"], "RECORDING", f"Scheduled window "
                                   f"#{r['id']}: recording confirmed for "
                                   f"{r['username']} (first segment saved)."))
                    continue
                if not alive or now - r["claim_at"] > CONFIRM_TIMEOUT_S:
                    why = error_fn(r["username"], r["token"]) or \
                        ("no recording segment appeared" if alive else
                         "session ended before recording")
                    stop_fn(r["username"], r["token"])
                    if _cas(c, r["id"], "LAUNCHED", "MISSED", why[:200]):
                        ev.append((r["id"], "MISSED", f"Scheduled window "
                                   f"#{r['id']} for {r['username']} never "
                                   f"started recording: {why[:200]}"))
            elif not alive:
                why = error_fn(r["username"], r["token"]) or "session ended early"
                if _cas(c, r["id"], "RECORDING", "DONE", why[:200]):
                    ev.append((r["id"], "DONE", f"Scheduled window #{r['id']} "
                               f"for {r['username']}: session ended early "
                               f"({why[:200]})."))
        # 3. claim and launch due windows (day marks retry until day end)
        for r in c.execute("SELECT * FROM clip_schedules WHERE state="
                           "'SCHEDULED' AND start_at<=? ORDER BY start_at",
                           (now,)).fetchall():
            end_at = r["end_at"] or (r["start_at"] + r["duration_s"])
            day = r["mode"] == "day"
            if day and now >= end_at:
                if _cas(c, r["id"], "SCHEDULED", "MISSED",
                        (r["note"] or "no recording started that day")[:200]):
                    ev.append((r["id"], "MISSED", f"Marked day #{r['id']} for "
                               f"{r['username']} ended with no recording started"
                               + (f" (last reason: {r['note'][:150]})" if r["note"] else "")
                               + "."))
                continue
            if not day and now - r["start_at"] > GRACE_S:
                if _cas(c, r["id"], "SCHEDULED", "MISSED",
                        "service was not running at its start time"):
                    ev.append((r["id"], "MISSED", f"Scheduled window #{r['id']}"
                               f" for {r['username']} was missed (service was "
                               "not running at its start time). Nothing recorded."))
                continue
            if day and r["next_try_at"] and now < r["next_try_at"]:
                continue
            if not _cas(c, r["id"], "SCHEDULED", "STARTING", claim_at=now):
                continue
            try:
                reply, token = start_fn(r["username"])
            except Exception as exc:
                reply, token = f"launch error {exc.__class__.__name__}", None
            if token:
                _cas(c, r["id"], "STARTING", "LAUNCHED", token=token)
                ev.append((r["id"], "LAUNCHED", f"Scheduled #{r['id']}: "
                           f"launched {r['username']}; waiting for the first "
                           "recorded segment before calling it recording."))
            elif day and r["attempts"] + 1 < MAX_ATTEMPTS:
                c.execute("UPDATE clip_schedules SET state='SCHEDULED',attempts="
                          "attempts+1,next_try_at=?,note=?,updated_at=? WHERE id=?"
                          " AND state='STARTING'", (now + RETRY_S, reply[:200], now, r["id"]))
                if r["attempts"] == 0:
                    ev.append((r["id"], "WAITING", f"Marked day #{r['id']} for "
                               f"{r['username']}: not recording yet ({reply[:150]}). "
                               "I'll retry every 5 minutes until the day ends."))
            else:
                _cas(c, r["id"], "STARTING", "MISSED", reply[:200])
                ev.append((r["id"], "MISSED", f"Scheduled #{r['id']} for "
                           f"{r['username']} could not start: {reply[:200]}"))
    finally:
        c.close()
    return ev


OUTBOX_DDL = """
CREATE TABLE IF NOT EXISTS schedule_notices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_id INTEGER, state TEXT, text TEXT NOT NULL,
    created_at REAL NOT NULL, sent_at REAL
);
"""


def enqueue_notices(state_dir, events) -> None:
    """Durable outbox: committed before any send attempt."""
    if not events:
        return
    c = sqlite3.connect(str(Path(state_dir) / "nova.db"), timeout=10)
    try:
        c.executescript(OUTBOX_DDL)
        with c:
            c.executemany("INSERT INTO schedule_notices (schedule_id,state,text,"
                          "created_at) VALUES (?,?,?,?)",
                          [(i, st, tx, time.time()) for i, st, tx in events])
    finally:
        c.close()


def pending_notices(state_dir, limit=20) -> list[tuple[int, str]]:
    c = sqlite3.connect(str(Path(state_dir) / "nova.db"), timeout=10)
    try:
        c.executescript(OUTBOX_DDL)
        return [(r[0], r[1]) for r in c.execute(
            "SELECT id,text FROM schedule_notices WHERE sent_at IS NULL "
            "ORDER BY id LIMIT ?", (limit,))]
    finally:
        c.close()


def mark_notice_sent(state_dir, nid: int) -> None:
    c = sqlite3.connect(str(Path(state_dir) / "nova.db"), timeout=10)
    try:
        with c:
            c.execute("UPDATE schedule_notices SET sent_at=? WHERE id=?",
                      (time.time(), nid))
    finally:
        c.close()


def confirm_recording(state_dir, username: str, since: float) -> bool:
    """True once the source has a persisted recording segment in a session
    started at/after `since`: the only recording confirmation. Read-only;
    any DB error is "not confirmed"."""
    try:
        c = sqlite3.connect(f"file:{Path(state_dir) / 'nova.db'}?mode=ro",
                            uri=True, timeout=5)
    except sqlite3.Error:
        return False
    try:
        return c.execute(
            "SELECT 1 FROM recording_segments rs JOIN stream_sessions ss ON "
            "ss.id=rs.session_id JOIN twitch_sources ts ON ts.id=ss.source_id "
            "WHERE ts.username=? AND ss.started_at>=? LIMIT 1",
            (username, since - 1)).fetchone() is not None
    except sqlite3.Error:
        return False
    finally:
        c.close()
