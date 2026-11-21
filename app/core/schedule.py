"""Owner-scheduled clipping eligibility (windows and whole-day marks).

A PLAN (clip_schedules) says "this streamer is eligible to be clipped in
[start_at, end_at)". It is not a recording. Every try to start a session is
an ATTEMPT (schedule_attempts) whose id is committed BEFORE anything is
launched (state INTENT) and is stamped durably on the session row
(stream_sessions.notes.sched_attempt) and as the in-memory token on the
session object. So "which session is mine" is never inferred from a
username or a time.

Plan states:    SCHEDULED (eligible, no attempt running) -> ACTIVE (an attempt
                owns the streamer) -> back to SCHEDULED when the attempt ended
                early, until end_at; DONE (recorded at least once) or MISSED
                at end_at; UNCERTAIN (a launch raised: no automatic retry
                until the process restarts or the owner cancels); CANCELLED.
Attempt states: INTENT -> LAUNCHED -> RECORDING -> STOPPING -> ENDED;
                NOT_STARTED (definitive: nothing launched); UNCERTAIN;
                INTERRUPTED (service restarted).
RECORDING is only set when a recording segment that belongs to THIS
attempt's session exists on disk and probes as valid finalized media.
Every state change and the owner notice it implies commit in ONE
transaction (BEGIN IMMEDIATE). Launch and stop callbacks run outside any
transaction. The retry budget is the plan's end_at (exact local midnight,
23/25-hour days included), never an attempt count. A user /stopclip or
/unschedule cancels the plan's eligibility; nothing relaunches after it.
A restart ends every open attempt INTERRUPTED (sessions die with the
process) and a plan still inside its window is eligible again.
Times are UTC epochs entered in an explicit, owner-set timezone.
"""
from __future__ import annotations

import contextlib
import json
import os
import re
import sqlite3
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from .intake import DEFAULT_TIMEZONE, resolve_timezone

DDL = """
CREATE TABLE IF NOT EXISTS clip_schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    mode TEXT NOT NULL,
    start_at REAL NOT NULL,
    end_at REAL NOT NULL,
    state TEXT NOT NULL DEFAULT 'SCHEDULED',
    note TEXT,
    next_try_at REAL,
    fails INTEGER NOT NULL DEFAULT 0,
    tries INTEGER NOT NULL DEFAULT 0,
    ever_recorded INTEGER NOT NULL DEFAULT 0,
    notified_wait INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS schedule_attempts (
    id TEXT PRIMARY KEY,
    schedule_id INTEGER NOT NULL REFERENCES clip_schedules(id),
    username TEXT NOT NULL,
    state TEXT NOT NULL,
    stop_by TEXT,
    stop_calls INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    created_at REAL NOT NULL,
    launched_at REAL,
    stopping_at REAL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS schedule_notices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_id INTEGER,
    text TEXT NOT NULL,
    created_at REAL NOT NULL,
    sending_at REAL,
    sent_at REAL,
    repeat INTEGER NOT NULL DEFAULT 0
);
"""
MAX_DURATION_S = 12 * 3600
MIN_DURATION_S = 600
MAX_OPEN = 50
GRACE_S = 15 * 60          # a fixed window never started this long after its start is MISSED
CONFIRM_TIMEOUT_S = 180    # launched but no valid segment yet
STOP_WARN_S = 300
PLAN_OPEN = ("SCHEDULED", "ACTIVE", "UNCERTAIN")
ATTEMPT_OPEN = ("INTENT", "LAUNCHED", "RECORDING", "STOPPING", "UNCERTAIN")
_USER = re.compile(r"^[a-z0-9_]{3,25}$")


@contextlib.contextmanager
def txn(state_dir):
    """One atomic BEGIN IMMEDIATE transaction; always closes the connection."""
    c = sqlite3.connect(str(Path(state_dir) / "nova.db"), timeout=10,
                        isolation_level=None)
    try:
        c.row_factory = sqlite3.Row
        c.executescript(DDL)
        c.execute("BEGIN IMMEDIATE")
        try:
            yield c
        except BaseException:
            c.execute("ROLLBACK")
            raise
        c.execute("COMMIT")
    finally:
        c.close()


@contextlib.contextmanager
def session(state_dir):
    """Alias kept for command code: an atomic transaction."""
    with txn(state_dir) as c:
        yield c


def _read(state_dir, sql, args=()):
    c = sqlite3.connect(str(Path(state_dir) / "nova.db"), timeout=10)
    try:
        c.row_factory = sqlite3.Row
        c.executescript(DDL)
        return [dict(r) for r in c.execute(sql, args).fetchall()]
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
    """Create a plan. duration_s None means a whole-day mark ending at
    day_end; a duration is only ever the owner's explicit choice."""
    username = username.strip().lstrip("@").lower()
    if not _USER.match(username):
        raise ValueError("invalid username")
    now = now if now is not None else time.time()
    if duration_s is not None:
        if not MIN_DURATION_S <= duration_s <= MAX_DURATION_S:
            raise ValueError("duration must be between 10 minutes and 12 hours")
        mode, end_at = "window", start_at + duration_s
    else:
        if day_end is None:
            raise ValueError("a day mark needs its day end")
        mode, end_at = "day", day_end
    if end_at <= now:
        raise ValueError("that time is in the past")
    with txn(state_dir) as c:
        if not c.execute("SELECT 1 FROM twitch_sources WHERE username=?",
                         (username,)).fetchone():
            raise ValueError(f"{username} is not registered (use /addsource)")
        if c.execute("SELECT COUNT(*) FROM clip_schedules WHERE state IN "
                     "('SCHEDULED','ACTIVE','UNCERTAIN')").fetchone()[0] >= MAX_OPEN:
            raise ValueError("too many open schedules")
        if c.execute("SELECT 1 FROM clip_schedules WHERE username=? AND state IN "
                     "('SCHEDULED','ACTIVE','UNCERTAIN') AND start_at<? AND end_at>?",
                     (username, end_at, start_at)).fetchone():
            raise ValueError("overlaps another schedule for that streamer")
        cur = c.execute(
            "INSERT INTO clip_schedules (username,mode,start_at,end_at,created_at,"
            "updated_at) VALUES (?,?,?,?,?,?)",
            (username, mode, start_at, end_at, now, now))
        return cur.lastrowid


def list_open(state_dir) -> list[dict]:
    return _read(state_dir,
                 "SELECT * FROM clip_schedules WHERE state IN "
                 "('SCHEDULED','ACTIVE','UNCERTAIN') ORDER BY start_at")


def cancel(state_dir, sid: int, now: float | None = None) -> str:
    """End a plan's eligibility. Returns 'none' (nothing cancellable),
    'cancelled' or 'cancelled_running' (a session is running: the next
    tick stops exactly that session). Nothing relaunches afterwards."""
    now = now if now is not None else time.time()
    with txn(state_dir) as c:
        p = c.execute("SELECT * FROM clip_schedules WHERE id=?", (sid,)).fetchone()
        if p is None or p["state"] not in PLAN_OPEN:
            return "none"
        running = c.execute(
            "SELECT 1 FROM schedule_attempts WHERE schedule_id=? AND state IN "
            "('INTENT','LAUNCHED','RECORDING','STOPPING')", (sid,)).fetchone()
        c.execute("UPDATE clip_schedules SET state='CANCELLED',note=?,updated_at=? "
                  "WHERE id=?", ("cancelled by the owner", now, sid))
        c.execute("UPDATE schedule_attempts SET state='ABANDONED',note=?,updated_at=? "
                  "WHERE schedule_id=? AND state='UNCERTAIN'",
                  ("owner cancelled; verify /sessions", now, sid))
        return "cancelled_running" if running else "cancelled"


def user_stopped(state_dir, attempt_id: str, now: float | None = None) -> bool:
    """/stopclip ended a scheduled session: cancel the plan so it can never
    silently relaunch. Atomic with its notice."""
    now = now if now is not None else time.time()
    with txn(state_dir) as c:
        a = c.execute("SELECT * FROM schedule_attempts WHERE id=? AND state IN "
                      "('INTENT','LAUNCHED','RECORDING','STOPPING')",
                      (attempt_id,)).fetchone()
        if a is None:
            return False
        c.execute("UPDATE schedule_attempts SET stop_by=COALESCE(stop_by,'user'),"
                  "updated_at=? WHERE id=?", (now, attempt_id))
        n = c.execute("UPDATE clip_schedules SET state='CANCELLED',note=?,"
                      "updated_at=? WHERE id=? AND state IN ('SCHEDULED','ACTIVE',"
                      "'UNCERTAIN')", ("stopped with /stopclip: schedule cancelled",
                                       now, a["schedule_id"])).rowcount
        if n:
            _notice(c, a["schedule_id"], f"Schedule #{a['schedule_id']} for "
                    f"{a['username']} cancelled because you stopped its session. "
                    "It will not start again.", now)
        return True


def _notice(c, sid, text, now) -> None:
    c.execute("INSERT INTO schedule_notices (schedule_id,text,created_at) "
              "VALUES (?,?,?)", (sid, text[:900], now))


def _err_class(err: str) -> str:
    """Session errors may embed external text: keep only the class name."""
    head = (err or "").split(":", 1)[0].strip()
    return head if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]{0,60}", head) else "error"


def _backoff(fails: int) -> float:
    return 300.0 if fails < 3 else 900.0


def _after_attempt(c, plan_id, now, why: str) -> list[str]:
    """Attempt just ended: decide the plan's next state. Returns text for the
    owner notice (callers insert it in the same transaction)."""
    p = c.execute("SELECT * FROM clip_schedules WHERE id=?", (plan_id,)).fetchone()
    if p is None or p["state"] not in ("ACTIVE", "UNCERTAIN"):
        return []     # cancelled or already terminal: stays that way
    if now >= p["end_at"]:
        return _finish(c, p, now)
    c.execute("UPDATE clip_schedules SET state='SCHEDULED',next_try_at=?,note=?,"
              "updated_at=? WHERE id=?", (now + 60, why[:200], now, plan_id))
    return [f"Schedule #{plan_id} for {p['username']}: session ended ({why[:150]}). "
            "Still eligible until the end of the schedule; I will try again."]


def _finish(c, p, now) -> list[str]:
    if p["ever_recorded"]:
        c.execute("UPDATE clip_schedules SET state='DONE',updated_at=? WHERE id=?",
                  (now, p["id"]))
        return [f"Schedule #{p['id']} for {p['username']} finished; it recorded."]
    why = p["note"] or "no session started"
    c.execute("UPDATE clip_schedules SET state='MISSED',updated_at=? WHERE id=?",
              (now, p["id"]))
    return [f"Schedule #{p['id']} for {p['username']} ended with nothing "
            f"recorded (last reason: {why[:150]})."]


def find_session_id(state_dir, attempt_id: str):
    rows = _read(state_dir, "SELECT id FROM stream_sessions WHERE notes LIKE ?",
                 ('%"sched_attempt": ' + json.dumps(attempt_id) + '%',))
    return rows[0]["id"] if len(rows) == 1 else None


def confirm_recording(state_dir, attempt_id: str, validate_fn) -> bool:
    """True only when THIS attempt's own session row has a recorded segment
    whose file exists and validates (probe-closed finalized media). Another
    session of the same streamer, a missing file or an unreadable file never
    confirms. Read-only; any error means not confirmed."""
    try:
        sid = find_session_id(state_dir, attempt_id)
        if sid is None:
            return False
        for r in _read(state_dir, "SELECT file_path FROM recording_segments WHERE "
                       "session_id=? ORDER BY sequence DESC LIMIT 3", (sid,)):
            p = r["file_path"]
            if p and os.path.isfile(p) and os.path.getsize(p) > 0 and validate_fn(p):
                return True
    except Exception:
        return False
    return False


class Ops:
    """Callbacks into the live session registry (all keyed by attempt id).
    start(username, attempt_id) -> ('started'|'not_started', reply); raising
    means the outcome is unknown. stop(user, aid) -> bool; running/alive/
    stopping(user, aid) -> bool; confirm(aid) -> bool; error(user, aid) -> str."""

    def __init__(self, start, stop, running, alive, stopping, confirm, error):
        self.start, self.stop, self.running = start, stop, running
        self.alive, self.stopping, self.confirm, self.error = alive, stopping, confirm, error


def reconcile(state_dir, now: float | None = None) -> int:
    """Startup, one transaction: sessions died with the process, so every
    open attempt is INTERRUPTED and plans still inside their window are
    eligible again; notices caught mid-send are flagged as possible repeats."""
    now = now if now is not None else time.time()
    n = 0
    with txn(state_dir) as c:
        c.execute("UPDATE schedule_notices SET sending_at=NULL,repeat=1 WHERE "
                  "sending_at IS NOT NULL AND sent_at IS NULL")
        for a in c.execute("SELECT * FROM schedule_attempts WHERE state IN "
                           "('INTENT','LAUNCHED','RECORDING','STOPPING','UNCERTAIN')"
                           ).fetchall():
            c.execute("UPDATE schedule_attempts SET state='INTERRUPTED',note=?,"
                      "updated_at=? WHERE id=?",
                      ("service restarted", now, a["id"]))
            n += 1
        for p in c.execute("SELECT * FROM clip_schedules WHERE state IN "
                           "('ACTIVE','UNCERTAIN')").fetchall():
            if now >= p["end_at"]:
                for t in _finish(c, p, now):
                    _notice(c, p["id"], t, now)
            else:
                c.execute("UPDATE clip_schedules SET state='SCHEDULED',"
                          "next_try_at=NULL,note=?,updated_at=? WHERE id=?",
                          ("service restarted", now, p["id"]))
                _notice(c, p["id"], f"Service restarted. Schedule #{p['id']} for "
                        f"{p['username']} is eligible again until its end; any "
                        "earlier session was stopped by the restart.", now)
    return n


def _set_attempt(c, aid, frm, to, now, note=None, **cols) -> bool:
    sets = ["state=?", "updated_at=?"]
    args = [to, now]
    if note is not None:
        sets.append("note=?"); args.append(note[:200])
    for k, v in cols.items():
        sets.append(f"{k}=?"); args.append(v)
    q = ",".join("?" * len(frm))
    return c.execute(f"UPDATE schedule_attempts SET {','.join(sets)} WHERE id=? "
                     f"AND state IN ({q})", [*args, aid, *frm]).rowcount == 1


def tick(state_dir, ops: Ops, now: float | None = None) -> None:
    """Advance every plan once. Notices are written inside the same
    transaction as the state change they describe."""
    now = now if now is not None else time.time()
    # 1. open attempts that own a live session
    for a in _read(state_dir,
                   "SELECT a.*, p.state AS pstate, p.end_at AS pend FROM "
                   "schedule_attempts a JOIN clip_schedules p ON p.id=a.schedule_id "
                   "WHERE a.state IN ('LAUNCHED','RECORDING','STOPPING','INTENT','UNCERTAIN')"):
        _step_attempt(state_dir, ops, a, now)
    # 2. expired plans
    for p in _read(state_dir, "SELECT * FROM clip_schedules WHERE state IN "
                   "('SCHEDULED','UNCERTAIN') AND end_at<=?", (now,)):
        with txn(state_dir) as c:
            cur = c.execute("SELECT * FROM clip_schedules WHERE id=? AND state IN "
                            "('SCHEDULED','UNCERTAIN')", (p["id"],)).fetchone()
            if cur is not None:
                c.execute("UPDATE schedule_attempts SET state='ABANDONED',note=?,"
                          "updated_at=? WHERE schedule_id=? AND state='UNCERTAIN'",
                          ("schedule ended; verify /sessions", now, p["id"]))
                for t in _finish(c, cur, now):
                    _notice(c, p["id"], t, now)
    # 3. due plans
    for p in _read(state_dir, "SELECT * FROM clip_schedules WHERE state='SCHEDULED'"
                   " AND start_at<=? AND end_at>? ORDER BY start_at", (now, now)):
        if p["next_try_at"] and now < p["next_try_at"]:
            continue
        _launch(state_dir, ops, p, now)


def _launch(state_dir, ops: Ops, p, now) -> None:
    aid = uuid.uuid4().hex
    with txn(state_dir) as c:
        cur = c.execute("SELECT * FROM clip_schedules WHERE id=? AND state='SCHEDULED'",
                        (p["id"],)).fetchone()
        if cur is None:
            return
        if cur["mode"] == "window" and cur["tries"] == 0 and now - cur["start_at"] > GRACE_S:
            c.execute("UPDATE clip_schedules SET state='MISSED',note=?,updated_at=? "
                      "WHERE id=?", ("service was not running at its start time", now, p["id"]))
            _notice(c, p["id"], f"Schedule #{p['id']} for {p['username']} was missed "
                    "(service was not running at its start time). Nothing recorded.", now)
            return
        if c.execute("SELECT 1 FROM schedule_attempts WHERE username=? AND state IN "
                     "('INTENT','LAUNCHED','RECORDING','STOPPING','UNCERTAIN')",
                     (p["username"],)).fetchone():
            c.execute("UPDATE clip_schedules SET next_try_at=?,updated_at=? WHERE id=?",
                      (now + 60, now, p["id"]))
            return
        c.execute("UPDATE clip_schedules SET state='ACTIVE',tries=tries+1,"
                  "updated_at=? WHERE id=?", (now, p["id"]))
        c.execute("INSERT INTO schedule_attempts (id,schedule_id,username,state,"
                  "created_at,updated_at) VALUES (?,?,?,?,?,?)",
                  (aid, p["id"], p["username"], "INTENT", now, now))
    # the attempt id is durable; only now may anything launch
    try:
        kind, reply = ops.start(p["username"], aid)
        raised = False
    except Exception as exc:
        kind, reply, raised = None, exc.__class__.__name__, True
    with txn(state_dir) as c:
        if raised:
            if ops.running(p["username"], aid):
                _set_attempt(c, aid, ("INTENT",), "LAUNCHED", now,
                             "launch raised but the session is running",
                             launched_at=now)
                _notice(c, p["id"], f"Schedule #{p['id']}: {p['username']} launched "
                        "(with a launch error); waiting for the first recorded segment.", now)
            else:
                _set_attempt(c, aid, ("INTENT",), "UNCERTAIN", now,
                             f"launch raised {reply}")
                c.execute("UPDATE clip_schedules SET state='UNCERTAIN',note=?,"
                          "updated_at=? WHERE id=?", (f"launch raised {reply}", now, p["id"]))
                _notice(c, p["id"], f"Schedule #{p['id']} for {p['username']}: the "
                        f"launch failed with {reply} and I cannot tell whether "
                        "anything started. I will not retry automatically; check "
                        "/sessions. /unschedule cancels it.", now)
        elif kind == "started":
            _set_attempt(c, aid, ("INTENT",), "LAUNCHED", now, launched_at=now)
            _notice(c, p["id"], f"Schedule #{p['id']}: started {p['username']}; "
                    "waiting for the first recorded segment before calling it recording.", now)
        else:
            _set_attempt(c, aid, ("INTENT",), "NOT_STARTED", now, reply)
            cur = c.execute("SELECT * FROM clip_schedules WHERE id=?", (p["id"],)).fetchone()
            fails = cur["fails"] + 1
            c.execute("UPDATE clip_schedules SET state='SCHEDULED',fails=?,next_try_at=?,"
                      "note=?,notified_wait=1,updated_at=? WHERE id=?",
                      (fails, now + _backoff(fails), reply[:200], now, p["id"]))
            if not cur["notified_wait"]:
                _notice(c, p["id"], f"Schedule #{p['id']} for {p['username']}: not "
                        f"recording yet ({reply[:150]}). I will keep trying every few "
                        "minutes until the schedule ends and only message you when "
                        "something starts, ends or is missed.", now)


def _step_attempt(state_dir, ops: Ops, a, now) -> None:
    user, aid, st = a["username"], a["id"], a["state"]
    if st == "INTENT":
        # a launch whose outcome could not be committed: adopt the session
        # if it is really running under this attempt id, else hold it as
        # UNCERTAIN. Never relaunch, never assume.
        with txn(state_dir) as c:
            if ops.running(user, aid):
                if _set_attempt(c, aid, ("INTENT",), "LAUNCHED", now,
                                "adopted after an unrecorded launch", launched_at=now):
                    _notice(c, a["schedule_id"], f"Schedule #{a['schedule_id']}: {user} "
                            "launched; waiting for the first recorded segment.", now)
            elif _set_attempt(c, aid, ("INTENT",), "UNCERTAIN", now, "launch outcome lost"):
                c.execute("UPDATE clip_schedules SET state='UNCERTAIN',note=?,updated_at=? "
                          "WHERE id=? AND state='ACTIVE'", ("launch outcome lost", now,
                                                          a["schedule_id"]))
                _notice(c, a["schedule_id"], f"Schedule #{a['schedule_id']} for {user}: "
                        "I could not record whether a launch happened; not retrying "
                        "automatically. Check /sessions; /unschedule cancels.", now)
        return
    if st == "UNCERTAIN":
        return      # held until the owner cancels or the process restarts
    running = ops.running(user, aid)
    reason = None
    if a["pstate"] == "CANCELLED":
        reason = "cancel"
    elif now >= a["pend"]:
        reason = "window_end"
    elif st in ("LAUNCHED", "RECORDING") and ops.stopping(user, aid) and not a["stop_by"]:
        reason = "user"
    if st in ("LAUNCHED", "RECORDING") and reason:
        with txn(state_dir) as c:
            if reason == "user":
                c.execute("UPDATE clip_schedules SET state='CANCELLED',note=?,"
                          "updated_at=? WHERE id=? AND state IN ('ACTIVE','SCHEDULED')",
                          ("stopped with /stopclip: schedule cancelled", now, a["schedule_id"]))
                _notice(c, a["schedule_id"], f"Schedule #{a['schedule_id']} for {user} "
                        "cancelled because its session was stopped. It will not start again.", now)
            _set_attempt(c, aid, ("LAUNCHED", "RECORDING"), "STOPPING", now,
                         stop_by=reason, stopping_at=now)
        st = "STOPPING"
        a = {**a, "state": "STOPPING", "stop_by": reason, "stopping_at": now}
    if st == "STOPPING":
        if not running:
            with txn(state_dir) as c:
                _set_attempt(c, aid, ("STOPPING",), "ENDED", now)
                for t in _after_attempt(c, a["schedule_id"], now, a["stop_by"] or "stopped"):
                    _notice(c, a["schedule_id"], t, now)
            return
        failed = False
        try:
            ops.stop(user, aid)
        except Exception:
            failed = True
        with txn(state_dir) as c:
            row = c.execute("SELECT stop_calls FROM schedule_attempts WHERE id=?",
                            (aid,)).fetchone()
            calls = (row["stop_calls"] if row else 0) + 1
            c.execute("UPDATE schedule_attempts SET stop_calls=?,updated_at=? WHERE id=?",
                      (calls, now, aid))
            if (failed and calls == 1) or (a.get("stopping_at") and
                                           now - a["stopping_at"] > STOP_WARN_S and calls == 2):
                _notice(c, a["schedule_id"], f"Schedule #{a['schedule_id']} for {user}: "
                        "its session has not stopped yet; I keep asking it to stop. "
                        "Check /sessions.", now)
        return
    if not running:
        with txn(state_dir) as c:
            err = _err_class(ops.error(user, aid))
            _set_attempt(c, aid, ("LAUNCHED", "RECORDING"), "ENDED", now,
                         f"session ended ({err})" if err != "error" else "session ended")
            for t in _after_attempt(c, a["schedule_id"], now,
                                    "ended before recording" if st == "LAUNCHED"
                                    else "ended early"):
                _notice(c, a["schedule_id"], t, now)
        return
    if st == "LAUNCHED":
        if ops.confirm(aid) and ops.alive(user, aid):
            with txn(state_dir) as c:
                if _set_attempt(c, aid, ("LAUNCHED",), "RECORDING", now):
                    c.execute("UPDATE clip_schedules SET ever_recorded=1,fails=0,"
                              "updated_at=? WHERE id=?", (now, a["schedule_id"]))
                    _notice(c, a["schedule_id"], f"Schedule #{a['schedule_id']}: "
                            f"recording confirmed for {user} (a valid segment is saved).", now)
        elif a["launched_at"] and now - a["launched_at"] > CONFIRM_TIMEOUT_S:
            with txn(state_dir) as c:
                _set_attempt(c, aid, ("LAUNCHED",), "STOPPING", now,
                             "no valid segment appeared", stop_by="unconfirmed",
                             stopping_at=now)
                _notice(c, a["schedule_id"], f"Schedule #{a['schedule_id']} for {user}: "
                        "no valid recorded segment appeared; stopping that attempt.", now)


# ---- durable notice outbox (at-most-one in flight, repeats are labelled) --

def claim_notice(state_dir, now: float | None = None):
    now = now if now is not None else time.time()
    with txn(state_dir) as c:
        r = c.execute("SELECT * FROM schedule_notices WHERE sent_at IS NULL AND "
                      "sending_at IS NULL ORDER BY id LIMIT 1").fetchone()
        if r is None:
            return None
        c.execute("UPDATE schedule_notices SET sending_at=? WHERE id=?", (now, r["id"]))
        text = ("(possible repeat) " if r["repeat"] else "") + r["text"]
        return r["id"], text


def mark_notice_sent(state_dir, nid: int) -> None:
    with txn(state_dir) as c:
        c.execute("UPDATE schedule_notices SET sent_at=? WHERE id=?", (time.time(), nid))


def release_notice(state_dir, nid: int) -> None:
    """A send failed: it may or may not have been delivered, so the next
    delivery is labelled a possible repeat."""
    with txn(state_dir) as c:
        c.execute("UPDATE schedule_notices SET sending_at=NULL,repeat=1 WHERE id=? "
                  "AND sent_at IS NULL", (nid,))


def pending_notices(state_dir) -> list[tuple[int, str]]:
    return [(r["id"], r["text"]) for r in _read(
        state_dir, "SELECT id,text FROM schedule_notices WHERE sent_at IS NULL ORDER BY id")]
