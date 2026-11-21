"""Read-only Telegram views of YouTube publishing state.

Only reads. nova.db is opened read-only (no DDL, no writes). Credentials
are reported as present/absent only, never validated or echoed. Every
number is an uncapped COUNT; an unreadable source is reported
"unavailable", never shown as zero or empty. Times use the vendored IANA
zone (host zoneinfo is absent on AGT).
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path

from ..core.intake import DEFAULT_TIMEZONE, resolve_timezone
from . import pause as _pause

_KEYS = ("NOVA_YOUTUBE_CLIENT_ID", "NOVA_YOUTUBE_CLIENT_SECRET",
         "NOVA_YOUTUBE_REFRESH_TOKEN")
_HANDOFF = "clip_intake_handoff"
_ADMIT = "clip_intake_admit"


class Unavailable(Exception):
    pass


def _ro(state_dir):
    db = Path(state_dir) / "nova.db"
    if not db.is_file():
        raise Unavailable("database missing")
    try:
        c = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
        c.row_factory = sqlite3.Row
        return c
    except sqlite3.Error as exc:
        raise Unavailable(str(exc))


def _q(state_dir, sql, args=()):
    c = _ro(state_dir)
    try:
        return [dict(r) for r in c.execute(sql, args)]
    except sqlite3.Error as exc:
        raise Unavailable(str(exc))
    finally:
        c.close()


def _when(ts) -> str:
    return datetime.fromtimestamp(ts or 0, resolve_timezone(DEFAULT_TIMEZONE)
                                  ).strftime("%Y-%m-%d %H:%M %Z")


def _mode() -> str:
    present = [k for k in _KEYS if k in os.environ]
    if not present:
        return "not configured (no vault-injected credentials)"
    if all(os.environ.get(k, "").strip() for k in _KEYS):
        return "vault-injected credentials present (validity not checked)"
    return "INCOMPLETE vault-injected credentials (uploads fail closed)"


def _auto_with_pause(state_dir) -> str:
    from .native_upload import enabled
    st = _pause.state(state_dir)
    return (("ENABLED" if enabled() else "DISABLED (deploy gate)")
            + f", pause: {st.upper()}"
            + (" (unreadable, treated as paused)" if st == "unreadable" else ""))


def _num(state_dir, sql, args=()) -> str:
    try:
        return str(_q(state_dir, sql, args)[0]["n"])
    except Unavailable:
        return "unavailable"


def youtube_reply(state_dir, _args="") -> str:
    conf = _num(state_dir, "SELECT COUNT(*) n FROM publish_records WHERE state='CONFIRMED'")
    unc = _num(state_dir, "SELECT COUNT(*) n FROM publish_records WHERE state IN ('RESERVED','UNCERTAIN')")
    pend = _num(state_dir, "SELECT COUNT(*) n FROM job_queue WHERE kind=? AND state IN ('PENDING','ANALYZING')", (_HANDOFF,))
    fail = _num(state_dir, "SELECT COUNT(*) n FROM job_queue WHERE kind=? AND state='FAILED'", (_HANDOFF,))
    apend = _num(state_dir, "SELECT COUNT(*) n FROM job_queue WHERE kind=? AND state IN ('PENDING','ANALYZING')", (_ADMIT,))
    afail = _num(state_dir, "SELECT COUNT(*) n FROM job_queue WHERE kind=? AND state='FAILED'", (_ADMIT,))
    return ("YouTube publishing\n"
            f"credentials: {_mode()}\n"
            f"automatic upload: {_auto_with_pause(state_dir)}\n"
            "privacy: PRIVATE always (public publishing is not enabled)\n"
            f"confirmed uploads: {conf}\n"
            f"queued or running: {pend}\n"
            f"failed jobs: {fail}\n"
            f"intake admission (before upload): queued or running {apend}, failed {afail}\n"
            f"reserved/uncertain (need attention): {unc}\n"
            "Commands: /youtubestatus /publishqueue /publishhistory "
            "/autopublish /publish <id>")


def youtubestatus_reply(state_dir, _args="") -> str:
    return (f"credentials: {_mode()}\n"
            f"automatic upload: {_auto_with_pause(state_dir)}\n"
            "Token validity is not checked here; only a YouTube-confirmed "
            "upload proves access.")


def publishqueue_reply(state_dir, _args="") -> str:
    out = []
    try:
        jobs = _q(state_dir, "SELECT id,state,attempts,max_attempts,run_after,"
                  "last_error,payload_json,kind FROM job_queue WHERE kind IN (?,?) AND state "
                  "IN ('PENDING','ANALYZING','FAILED') ORDER BY id DESC LIMIT 10",
                  (_HANDOFF, _ADMIT))
        jtotal = _q(state_dir, "SELECT COUNT(*) n FROM job_queue WHERE kind IN (?,?) "
                    "AND state IN ('PENDING','ANALYZING','FAILED')",
                    (_HANDOFF, _ADMIT))[0]["n"]
    except Unavailable:
        return "Publish queue unavailable (database unreadable)."
    try:
        recs = _q(state_dir, "SELECT clip_id,state,attempts,last_error,updated_at "
                  "FROM publish_records WHERE state IN ('RESERVED','UNCERTAIN','FAILED')"
                  " ORDER BY updated_at DESC LIMIT 10")
        rtotal = _q(state_dir, "SELECT COUNT(*) n FROM publish_records WHERE state "
                    "IN ('RESERVED','UNCERTAIN','FAILED')")[0]["n"]
    except Unavailable:
        return "Publish records unavailable (database unreadable)."
    if not jobs and not recs:
        return ("Nothing pending in intake admission or upload, and no uploads "
                "need attention. (Automatic upload: "
                f"{_auto_with_pause(state_dir)}.)")
    import json
    import time
    out.append(f"Automatic upload: {_auto_with_pause(state_dir)}")
    if jobs:
        out.append(f"Pipeline jobs ({jtotal} total, newest {len(jobs)}):")
        for j in jobs:
            try:
                cid = json.loads(j["payload_json"]).get("clip_id", "?")
            except Exception:
                cid = "?"
            wait = ""
            if j["state"] == "PENDING" and (j["run_after"] or 0) > time.time():
                wait = f", retry after {_when(j['run_after'])}"
            err = f" ({j['last_error'][:60]})" if j["last_error"] else ""
            stage = "admit" if j["kind"] == _ADMIT else "upload"
            out.append(f"{stage} job {j['id']} {cid} {j['state']} attempt "
                       f"{j['attempts']}/{j['max_attempts']}{wait}{err}")
    if recs:
        out.append(f"Publish records needing attention ({rtotal} total, newest {len(recs)}):")
        for r in recs:
            note = {"UNCERTAIN": " - reconcile in YouTube Studio, no auto-retry",
                    "RESERVED": " - upload in flight or crashed mid-upload",
                    "FAILED": ""}[r["state"]]
            err = f" ({r['last_error'][:60]})" if r["last_error"] else ""
            out.append(f"#{r['clip_id']} {r['state']} attempts={r['attempts']} "
                       f"{_when(r['updated_at'])}{note}{err}")
    return "\n".join(out)


def publishhistory_reply(state_dir, _args="") -> str:
    try:
        rows = _q(state_dir, "SELECT clip_id,video_id,updated_at FROM publish_records"
                  " WHERE state='CONFIRMED' ORDER BY updated_at DESC LIMIT 15")
        total = _q(state_dir, "SELECT COUNT(*) n FROM publish_records WHERE "
                   "state='CONFIRMED'")[0]["n"]
    except Unavailable:
        return "Upload history unavailable (database unreadable)."
    if not rows:
        return "No confirmed YouTube uploads yet."
    return (f"Confirmed uploads, private ({total} total, newest {len(rows)}):\n"
            + "\n".join(f"#{r['clip_id']} {r['video_id']} {_when(r['updated_at'])}"
                        for r in rows))
