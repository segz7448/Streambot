"""Positive writer-death proof for restart-fenced attempts.

schedule.reconcile() fences an open attempt as UNRESOLVED when positive
termination proof is missing, and the tick releases only on
sessions.session_terminated(): the drained attestation the session's own
finalizer writes after its children were drained. A hard process kill
never runs that finalizer, so after a real restart the fence can be
permanent even when every writer died with the process.

This module supplies the missing POSITIVE proof leg. It releases nothing
by itself; wiring it into the terminated chain is a maintainer decision.

Proof standard - ALL required; anything missing, unreadable or ambiguous
is NOT proof, and quiet/old mtimes alone are never proof:

1. the attempt maps to exactly one durable stream_sessions row whose
   notes carry leader_pid/leader_start (record_leader at session start:
   the process that OWNED the session);
2. the recorded leader is verifiably dead: /proc/<pid> absent, or its
   start time differs from the recorded one (PID reuse is not death);
3. the session's writer registry exists with NO unresolved spawn intent
   (record_spawn_intent is written durably before a child may be spawned
   and record_spawned binds its identity right after; an unresolvable
   intent means a possibly-unrecorded writer and blocks proof forever)
   and EVERY recorded writer is verifiably dead by the same test;
4. for every entry that recorded a process-group id, a full /proc sweep
   finds no surviving member of that group (a group member that outlived
   its recorded leader is an orphan and blocks proof);
5. process visibility is complete: the /proc listing succeeded and every
   stat read succeeded. Permission-denied or hidden processes are NOT
   "absent": they block proof (Android can hide other UIDs; same-UID
   orphans are visible, but any denied read means we cannot know);
6. the advisory writer scan (sessions.writer_active) finds nothing. It
   is a fail-closed backstop that can only BLOCK proof, never a proof leg.

Sessions started before this module existed have no leader record and no
registry: for them the answer is always "not proof" and they stay fenced.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path

_LOCK = threading.Lock()


def _read_stat(pid: int, proc: str):
    """(pgrp, start_time) from /proc/<pid>/stat, or None. comm may contain
    spaces or parens, so parse after the last ')'. None conflates absent
    and unreadable; callers that must distinguish use stat_detail."""
    d = stat_detail(pid, proc)
    return (d[1], d[2]) if d else None


def stat_detail(pid: int, proc: str = "/proc"):
    """(True, pgrp, start) when fully readable, (False, why, None) else.
    why is 'absent' only when the entry definitively does not exist;
    'denied'/'unreadable' mean visibility is incomplete, never death."""
    try:
        with open(os.path.join(proc, str(pid), "stat"), "rb") as fh:
            data = fh.read()
    except FileNotFoundError:
        return (False, "absent", None)
    except ProcessLookupError:
        return (False, "absent", None)
    except PermissionError:
        return (False, "denied", None)
    except OSError:
        return (False, "unreadable", None)
    try:
        rest = data[data.rindex(b")") + 2:].split()
        return (True, int(rest[2]), int(rest[19]))
    except (ValueError, IndexError):
        return (False, "unreadable", None)


def read_start_time(pid: int, proc: str = "/proc") -> int | None:
    d = stat_detail(pid, proc)
    return d[2] if d[0] else None


def record_leader(notes: dict, pid: int | None = None,
                  proc: str = "/proc") -> dict:
    """Return notes with the session owner's pid and /proc start-time.
    Unreadable /proc records nothing: no recorded start, no later proof."""
    pid = os.getpid() if pid is None else pid
    start = read_start_time(pid, proc)
    if start is None:
        return dict(notes)
    out = dict(notes)
    out["leader_pid"] = pid
    out["leader_start"] = start
    return out


def _persist(registry_path: Path, items: list) -> bool:
    """Atomic durable write: tmp file, fsync, replace, fsync the directory
    so the record survives a crash, not just a process exit."""
    registry_path = Path(registry_path)
    try:
        tmp = registry_path.with_suffix(".tmp")
        with open(tmp, "w") as fh:
            fh.write(json.dumps(items))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, registry_path)
        dfd = os.open(str(registry_path.parent), os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
        return True
    except Exception:
        return False


_MANIFEST_VERSION = 1


def _valid_pos_int(v) -> bool:
    """A valid positive process identity integer (bools are not ints
    here; zero/negative pids, starts and pgids are not identities)."""
    return isinstance(v, int) and not isinstance(v, bool) and v > 0


def _read_manifest(registry_path: Path) -> tuple[str, list | None]:
    """Strict manifest read. Returns ("missing"|"ok"|"invalid", writers).
    Only a dict carrying the current version and a writers list is ok:
    corrupt JSON, legacy (pre-version) layouts and any other shape are
    "invalid" and must fail every consumer closed - never wiped, never
    treated as empty."""
    registry_path = Path(registry_path)
    if not registry_path.exists():
        return "missing", None
    try:
        doc = json.loads(registry_path.read_text())
    except Exception:
        return "invalid", None
    if (isinstance(doc, dict) and doc.get("v") == _MANIFEST_VERSION
            and isinstance(doc.get("writers"), list)):
        return "ok", doc["writers"]
    return "invalid", None


def init_registry(registry_path: Path) -> bool:
    """Create the session's durable writer manifest (empty, versioned)
    BEFORE any session work. After this call, a MISSING manifest can
    only mean it was lost, and a lost manifest must block every
    attestation that depends on it - so callers treat False as fatal
    for the session. Never wipes an existing manifest: an unreadable
    or legacy one is evidence, not an initialization target."""
    registry_path = Path(registry_path)
    with _LOCK:
        status, _ = _read_manifest(registry_path)
        if status == "ok":
            return True
        if status == "invalid":
            return False            # corrupt/legacy: fail closed
        return _persist(registry_path,
                        {"v": _MANIFEST_VERSION, "writers": []})


def record_spawn_intent(registry_path: Path) -> str | None:
    """Durably persist an UNRESOLVED spawn intent BEFORE the child exists.
    Returns the intent token; None on any persistence failure, in which
    case the caller MUST NOT spawn: a writer that cannot be recorded may
    never run. Requires an initialized manifest (init_registry at
    session setup); a missing manifest blocks rather than recreating
    lost history. An intent left unresolved blocks proof forever (a crash
    between intent and completion is indistinguishable from a live
    unrecorded writer, and fails closed)."""
    import time
    token = f"{time.time_ns()}-{os.getpid()}"
    registry_path = Path(registry_path)
    with _LOCK:
        status, items = _read_manifest(registry_path)
        if status != "ok":
            # a MISSING manifest is lost history, not a fresh start:
            # recreating it here would let a new spawn bind while an
            # earlier registered writer still runs unrecorded. Only
            # explicit before-work initialization may create it.
            return None
        items.append({"intent": token, "ts": time.time()})
        return token if _persist(
            registry_path, {"v": _MANIFEST_VERSION, "writers": items})             else None


def record_spawned(registry_path: Path, token: str, pid: int,
                   pgid: int | None = None, proc: str = "/proc") -> bool:
    """Complete a spawn intent with the child's identity. False on ANY
    failure - the caller MUST terminate the spawned child and fail the
    spawn: an unidentifiable writer must never keep running."""
    if not _valid_pos_int(pid):
        return False                # no valid identity: cannot record
    # pgid stays optional at bind time (a writer without a recorded
    # group is legal); every death ATTESTATION requires a complete
    # valid identity and fails closed without one
    start = read_start_time(pid, proc)
    if start is None:
        return False
    registry_path = Path(registry_path)
    with _LOCK:
        status, items = _read_manifest(registry_path)
        if status != "ok":
            return False            # missing/corrupt/legacy: cannot bind
        for it in items:
            if it.get("intent") == token and "pid" not in it:
                it["pid"] = pid
                it["start"] = start
                if pgid:
                    it["pgid"] = pgid
                return _persist(
                    registry_path,
                    {"v": _MANIFEST_VERSION, "writers": items})
        return False                # intent lost: cannot bind the child


def _load_registry(registry_path: Path):
    """Writers list for proof consumers; None unless the manifest is a
    valid current-version document (missing, corrupt and legacy all
    fail closed)."""
    status, items = _read_manifest(registry_path)
    return items if status == "ok" else None


def _load_notes(state_dir: Path, session_id) -> dict | None:
    try:
        c = sqlite3.connect(
            f"file:{Path(state_dir) / 'nova.db'}?mode=ro", uri=True, timeout=5)
        try:
            r = c.execute("SELECT notes FROM stream_sessions WHERE id=?",
                          (session_id,)).fetchone()
        finally:
            c.close()
        return json.loads(r[0]) if r and r[0] else None
    except Exception:
        return None


def _pid_dead(pid: int, start: int, proc: str):
    """(True, why) only when the recorded process is verifiably gone."""
    ok, why, cur = stat_detail(pid, proc)
    if ok:
        if cur == start:
            return False, f"pid {pid} still alive"
        return True, f"pid {pid} reused (start {cur} != recorded {start})"
    if why == "absent":
        return True, f"pid {pid} absent"
    return False, f"pid {pid} visibility {why}"


def claim_owner_dead(pid: int, start: int, pgid: int,
                     proc: str = "/proc") -> tuple[bool, str]:
    """(True, why) only when the EXACT recorded claim-owner process is
    verifiably gone (pid absent or reused with a different start tick)
    AND no live process remains in its recorded process group, with
    complete process visibility. (False, why) for anything unprovable -
    a denied or unreadable sweep is never evidence of absence."""
    if not (_valid_pos_int(pid) and _valid_pos_int(start)
            and _valid_pos_int(pgid)):
        return False, "claim owner identity malformed"
    dead, why = _pid_dead(pid, start, proc)
    if not dead:
        return False, f"claim owner {why}"
    try:
        pids = os.listdir(proc)
    except OSError:
        return False, "no process visibility: /proc not listable"
    for p in pids:
        if not p.isdigit() or int(p) == pid:
            continue
        d = stat_detail(int(p), proc)     # ONE authoritative read per pid
        if not d[0]:
            if d[1] == "absent":
                continue            # exited while scanning: definitively gone
            return False, f"process visibility {d[1]} for pid {p}"
        if d[1] == pgid:
            return False, f"survivor pid {p} in claim group {pgid}"
    return True, "claim owner dead, group empty"


def writer_death_proof(state_dir: Path, username: str, attempt_id,
                       proc: str = "/proc", writer_active=None,
                       registry_path: Path | None = None) -> tuple[bool, str]:
    """(True, reason) only on positive proof that the exact attempt's
    owner and every recorded writer are dead, with complete process
    visibility. (False, reason) for everything else, including errors."""
    if writer_active is None:
        from . import sessions as _sess
        writer_active = lambda u: _sess.writer_active(state_dir, u)
    from ..core import schedule as _sc
    try:
        sid = _sc.find_session_id(state_dir, attempt_id)
    except Exception:
        return False, "session lookup failed"
    if sid is None:
        return False, "no unique session row for this attempt"
    return proof_for_session(state_dir, username, sid, proc=proc,
                             writer_active=writer_active,
                             registry_path=registry_path)


def proof_for_session(state_dir: Path, username: str, session_id,
                      proc: str = "/proc", writer_active=None,
                      registry_path: Path | None = None) -> tuple[bool, str]:
    """Same proof keyed by the stream_sessions row id (boot recovery)."""
    if writer_active is None:
        from . import sessions as _sess
        writer_active = lambda u: _sess.writer_active(state_dir, u)
    sid = session_id
    notes = _load_notes(state_dir, sid)
    if notes is None:
        return False, "session notes unreadable"
    pid, start = notes.get("leader_pid"), notes.get("leader_start")
    if not isinstance(pid, int) or not isinstance(start, int):
        return False, "no recorded leader: nothing to prove the death of"
    dead, why = _pid_dead(pid, start, proc)
    if not dead:
        return False, f"recorded leader {why}"
    registry_path = Path(registry_path) if registry_path else (
        Path(state_dir) / "sessions" / username / f"writers-{sid}.json")
    items = _load_registry(registry_path)
    if items is None:
        return False, "no writer registry: writers never recorded, cannot prove"
    for it in items:
        if "pid" not in it:
            # a spawn was intended but never bound to an identity: crash
            # mid-spawn or lost completion - indistinguishable from a
            # live unrecorded writer, so it blocks proof forever
            return False, "unresolved spawn intent: a writer may be unrecorded"
        if not (_valid_pos_int(it.get("pid"))
                and _valid_pos_int(it.get("start"))):
            return False, "writer registry entry malformed"
        dead, why = _pid_dead(it["pid"], it["start"], proc)
        if not dead:
            return False, f"recorded writer {why}"
    pgids = {it["pgid"] for it in items if isinstance(it.get("pgid"), int)}
    # Complete process visibility is a proof leg even with no recorded
    # groups: if we cannot list processes at all we know nothing, and a
    # denied/hidden read is never evidence of absence.
    try:
        pids = os.listdir(proc)
    except OSError:
        return False, "no process visibility: /proc not listable"
    recorded = {it["pid"] for it in items}
    for p in pids:
        if not p.isdigit() or int(p) in recorded:
            continue
        d = stat_detail(int(p), proc)     # ONE authoritative read per pid
        if not d[0]:
            if d[1] == "absent":
                continue            # exited while scanning: definitively gone
            return False, f"process visibility {d[1]} for pid {p}"
        if d[1] in pgids:
            return False, f"orphan survives: pid {p} in recorded group {d[1]}"
    try:
        active = bool(writer_active(username))
    except Exception:
        return False, "writer probe failed: not verifiably quiet"
    if active:
        return False, "writer evidence remains (recent write or matching process)"
    return True, (f"leader pid {pid} and {len(items)} recorded writer(s) "
                  "verifiably dead under complete process visibility")
