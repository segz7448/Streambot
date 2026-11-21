"""Owner pause/resume of automatic upload, with a committed boundary.

The pause flag lives in nova.db settings. It is read in three places:
  1. before a cycle claims a job (fail-closed),
  2. inside publish_records.reserve's BEGIN IMMEDIATE (the gate) - the
     single serialization point before any provider call. A pause that
     commits before that transaction is seen by it; one that commits
     after it finds the RESERVED row and is told the upload is in flight.
  3. views (/autopublish, /youtube) via state(), which separates
     unreadable from running so a read error is never shown as "running".
Pause never enables uploads (deploy gate) and never changes privacy.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

PAUSE_KEY = "autopublish_paused"


def _db(state_dir, ro=False):
    p = Path(state_dir) / "nova.db"
    if ro:
        return sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=5)
    return sqlite3.connect(str(p), timeout=10)


def state(state_dir) -> str:
    """'paused' | 'running' | 'unreadable'."""
    try:
        c = _db(state_dir, ro=True)
        try:
            r = c.execute("SELECT value FROM settings WHERE key=?",
                          (PAUSE_KEY,)).fetchone()
        finally:
            c.close()
    except sqlite3.Error:
        return "unreadable"
    return "paused" if r and r[0] == "1" else "running"


def is_paused(state_dir) -> bool:
    """Fail closed: unreadable counts as paused."""
    return state(state_dir) != "running"


def gate_in_tx(conn) -> bool:
    """True if uploading may proceed. Call INSIDE the reserve transaction
    on its own connection. Unreadable -> False."""
    try:
        r = conn.execute("SELECT value FROM settings WHERE key=?",
                         (PAUSE_KEY,)).fetchone()
    except sqlite3.Error:
        return False
    return not (r and r[0] == "1")


def set_paused(state_dir, paused: bool) -> None:
    c = _db(state_dir)
    try:
        c.execute("BEGIN IMMEDIATE")
        c.execute("INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key)"
                  " DO UPDATE SET value=excluded.value",
                  (PAUSE_KEY, "1" if paused else "0"))
        c.commit()
    finally:
        c.close()


def counts(state_dir) -> dict:
    """Truthful, uncapped: in-flight reservations and job-queue states."""
    out = {"reserved": None, "queued": None, "running": None, "failed": None,
           "admit_queued": None, "admit_running": None, "admit_failed": None}
    try:
        c = _db(state_dir, ro=True)
        try:
            out["reserved"] = c.execute(
                "SELECT COUNT(*) FROM publish_records WHERE state='RESERVED'"
            ).fetchone()[0]
            for key, st in (("queued", "PENDING"), ("running", "ANALYZING"),
                            ("failed", "FAILED")):
                out[key] = c.execute(
                    "SELECT COUNT(*) FROM job_queue WHERE kind="
                    "'clip_intake_handoff' AND state=?", (st,)).fetchone()[0]
                out["admit_" + key] = c.execute(
                    "SELECT COUNT(*) FROM job_queue WHERE kind="
                    "'clip_intake_admit' AND state=?", (st,)).fetchone()[0]
        finally:
            c.close()
    except sqlite3.Error:
        pass
    return out


def _fmt(n):
    return "unavailable" if n is None else str(n)


def autopublish_reply(state_dir, args: str) -> str:
    from .native_upload import enabled
    a = args.strip().lower()
    if a in ("pause", "resume"):
        try:
            set_paused(state_dir, a == "pause")
        except sqlite3.Error:
            return ("Could NOT change the pause setting (database unavailable). "
                    "Uploads are not paused by this command. Try again.")
        if a == "resume":
            return ("Automatic upload resumed (still private only)."
                    + ("" if enabled() else " NOTE: automatic upload is "
                       "DISABLED by the deploy gate, so nothing will upload "
                       "until that is enabled."))
        n = counts(state_dir)["reserved"]
        if n is None:
            return ("Pause saved, but I could not read in-flight uploads. "
                    "Uploads that already started cannot be stopped by a pause.")
        return ("Automatic upload PAUSED: queued clips wait and no new upload "
                "starts. "
                + (f"{n} upload(s) are already reserved: they may still "
                   "start or finish (a reservation can also be a crashed "
                   "upload); pause cannot cancel them. " if n else
                   "No upload is in flight. ")
                + "/autopublish resume undoes it.")
    st = state(state_dir)
    c = counts(state_dir)
    return ("Automatic upload: "
            + ("ENABLED" if enabled() else "DISABLED (deploy gate)")
            + f", pause: {st.upper()}"
            + (" (setting unreadable, treated as paused)" if st == "unreadable" else "")
            + ", private only.\n"
            f"upload handoff: queued {_fmt(c['queued'])}, running {_fmt(c['running'])}, "
            f"failed {_fmt(c['failed'])}. reserved/in-flight {_fmt(c['reserved'])}.\n"
            f"intake admission: queued {_fmt(c['admit_queued'])}, running "
            f"{_fmt(c['admit_running'])}, failed {_fmt(c['admit_failed'])}.\n"
            "Usage: /autopublish [pause|resume]")
