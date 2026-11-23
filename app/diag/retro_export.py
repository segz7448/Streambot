"""One-off, fail-closed, service-owned diagnostic export of ONE exact
approved on-device recording through the real retrospective pipeline.

The service spawns the whole pipeline as ONE owned child process
(app.diag.retro_worker) in its own process group, registered through
the SAME durable writer registry as the live path (restartproof):
intent-before-spawn, identity bound right after, unresolved intents
block proof forever, visibility errors fail closed. A fixed overall
deadline, an explicit cancel file, and service shutdown all kill that
one registered group (birth-verified), which covers every ffmpeg
grandchild. NOT a general ingestion API: exactly one request, claimed
atomically from its bytes before parsing, validated against the
compiled-in approved tuple, one global consumed fence, isolated
diagnostic DB, no delivery/publish, no automatic retry.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import stat as S
import subprocess
import threading
import sqlite3
import sys
import time
from pathlib import Path

from ..bot import restartproof as rp

REQUEST_DIR = "retro-diag"
REQUEST_FILE = "request.json"
CLAIMED_DIR = "claimed"
CLAIM_FENCE = "CLAIM-FENCE"
EDITOR_REGISTRY = "writers-edit.json"
DONE_DIR = "done"
CONSUMED_FILE = "CONSUMED"
CANCEL_FILE = "CANCEL"
IN_PROGRESS_FILE = "in-progress.json"
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{0,63}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
HARD_CEILING_SECONDS = 3600
_LAUNCH_LOCK = threading.Lock()  # absolute bound; the APPROVED tuple carries
# the practical overall budget (max_total_seconds), justified per source
# from measured work - never a pretend-tiny default

# The ONLY request this diagnostic may serve: (request_id, source path,
# sha256, size). The approval artifact is this reviewed constant itself.
# Empty by default => every request is refused.
APPROVED: tuple = ()


class RequestError(Exception):
    """Fail-closed request validation failure."""


class _BindFailure(Exception):
    """Internal controlled branch: worker identity could not be
    recorded. The result dict is already populated; raising this (not
    returning) routes through the common finalization so the terminal
    durable record is written AFTER the finally teardown, never an
    early pre-cleanup return."""


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fsync_json(path: Path, obj: dict, mode: int = 0o600) -> None:
    """Explicit modes only: no reliance on process umask."""
    tmp = path.with_suffix(".tmp")
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, "w") as fh:
        json.dump(obj, fh, indent=1, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.chmod(str(tmp), mode)
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _mkdir_fresh700(path: Path) -> None:
    """Fresh LEAF directory at exactly 0700: must not already exist
    (exist_ok False); chmod only the leaf we just created, never a
    shared or pre-existing parent."""
    os.mkdir(str(path))  # FileExistsError if present: fresh required
    os.chmod(str(path), 0o700)


def _ensure_private700(path: Path) -> None:
    """An existing control dir must already be a private 0700 real
    non-symlink dir owned by us (validated, NEVER chmod'ed); otherwise
    create it fresh. Parents are validated via _check_ancestors only."""
    if os.path.lexists(str(path)):
        if not _private700(path):
            raise RequestError(f"{path} is not a private 0700 real dir")
    else:
        _mkdir_fresh700(path)


def _check_ancestors(path: Path) -> None:
    """Every existing component from root to path must be a real
    directory, never a symlink (dangling or not). Runs BEFORE any
    write under path."""
    chain = list(reversed(Path(path).parents)) + [Path(path)]
    for cur in chain:
        try:
            st = os.lstat(str(cur))
        except FileNotFoundError:
            break  # not yet created; existing ancestors all checked
        if S.S_ISLNK(st.st_mode):
            raise RequestError(f"ancestor is a symlink: {cur}")
        if not S.S_ISDIR(st.st_mode):
            raise RequestError(f"ancestor is not a directory: {cur}")


def _private700(path: Path) -> bool:
    """Real non-symlink directory, owned by this euid, mode exactly
    private (no group/other bits)."""
    try:
        st = os.lstat(str(path))
    except OSError:
        return False
    return (S.S_ISDIR(st.st_mode) and not S.S_ISLNK(st.st_mode)
            and st.st_uid == os.geteuid()
            and (st.st_mode & 0o777) == 0o700)


def _fsync_dir(path: Path) -> None:
    dfd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def _private_dir(path: Path) -> bool:
    """Real directory, not a symlink, owned by this euid."""
    try:
        st = os.lstat(str(path))
    except OSError:
        return False
    return (S.S_ISDIR(st.st_mode) and not S.S_ISLNK(st.st_mode)
            and st.st_uid == os.geteuid())


def consumed(state_dir: str) -> bool:
    return (Path(state_dir) / REQUEST_DIR / CONSUMED_FILE).exists()


def claim_request(state_dir: str) -> dict | None:
    """Claim protocol (fail closed, no clobber, restart-safe):
    1. request must EXIST (existence only; bytes are never pre-read)
    2. global O_EXCL fence BEFORE any read: one claim ever; a leftover
       fence blocks ALL new claims, including any restart after an
       UNCERTAIN run - a human reviews first
    3. rename request -> a FRESH unique claimed path (rename to a
       nonexistent target never overwrites; a missing source means the
       request vanished and is reported, never re-read)
    4. open the CLAIMED path O_NOFOLLOW, fstat regular+owned, then read
    Parent dirs must be real, private, non-symlink directories."""
    req_dir = Path(state_dir) / REQUEST_DIR
    _check_ancestors(Path(state_dir) / "diag")
    for d in (req_dir, req_dir / CLAIMED_DIR, req_dir / DONE_DIR):
        if d.exists() and not _private700(d):
            raise RequestError(f"{d} is not a private 0700 real directory")
    req_path = req_dir / REQUEST_FILE
    if not req_path.exists():
        return None
    fence = req_dir / CLAIM_FENCE
    try:
        fd = os.open(str(fence), os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                     0o600)
    except FileExistsError:
        raise RequestError("claim fence exists: all new claims blocked; "
                           "a restart after any prior claim needs review")
    with os.fdopen(fd, "w") as fh:
        json.dump({"pid": os.getpid(), "claimed_at": time.time()}, fh)
        fh.flush()
        os.fsync(fh.fileno())
    _fsync_dir(req_dir)
    claimed_dir = req_dir / CLAIMED_DIR
    _ensure_private700(claimed_dir)
    # fresh UUID 0700 subdir (exist_ok False equivalent: fresh uuid),
    # then rename the request INSIDE it; fsync both parents
    import uuid
    claim_box = claimed_dir / uuid.uuid4().hex
    try:
        _mkdir_fresh700(claim_box)
    except FileExistsError:
        raise RequestError("claim subdir collision; refusing")
    claimed = claim_box / "request.json"
    try:
        os.rename(str(req_path), str(claimed))  # fresh target: no clobber
    except FileNotFoundError:
        raise RequestError("request vanished during claim (race lost)")
    except IsADirectoryError:
        raise RequestError("request path is a directory")
    _fsync_dir(req_dir)
    _fsync_dir(claim_box)
    try:
        fd = os.open(str(claimed), os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise RequestError(f"claimed request not openable: {exc}")
    with os.fdopen(fd, "rb") as fh:
        st = os.fstat(fh.fileno())
        if not S.S_ISREG(st.st_mode) or st.st_uid != os.geteuid():
            raise RequestError("claimed request is not an owned regular file")
        data = fh.read(65536)
    try:
        doc = json.loads(data.decode("utf-8", "strict"))
    except Exception as exc:
        _refuse(state_dir, claimed, f"request is not valid JSON: {exc}")
    if not isinstance(doc, dict):
        _refuse(state_dir, claimed, "request must be a JSON object")
    return {"doc": doc, "claimed_path": str(claimed)}


def _refuse(state_dir: str, claimed_path: Path, why: str):
    """A claimed-but-invalid request is consumed (fail closed, no
    re-pick loop) with the reason recorded durably."""
    done = Path(state_dir) / REQUEST_DIR / DONE_DIR
    _ensure_private700(done)
    _fsync_json(done / f"refused-{int(time.time())}.json",
                {"claimed": str(claimed_path), "reason": why})
    raise RequestError(why)


def _validate(doc: dict) -> dict:
    """Field match against APPROVED BEFORE any source read: an arbitrary
    request path is never stat'ed or hashed. Only after the full tuple
    matches is the APPROVED path itself verified (lstat, realpath, size,
    sha256)."""
    rid = doc.get("request_id")
    if not isinstance(rid, str) or not ID_RE.match(rid):
        raise RequestError("request_id missing or invalid")
    src = doc.get("source_path")
    if not isinstance(src, str) or not src.startswith("/"):
        raise RequestError("source_path must be absolute")
    sha = doc.get("source_sha256")
    if not isinstance(sha, str) or not HEX64.match(sha):
        raise RequestError("source_sha256 must be 64 lowercase hex")
    size = doc.get("source_bytes")
    if not isinstance(size, int) or size <= 0:
        raise RequestError("source_bytes must be a positive integer")
    match = None
    for a in APPROVED:
        if (a.get("request_id") == rid and a.get("path") == src
                and a.get("sha256") == sha and a.get("bytes") == size):
            match = a
            break
    if match is None:
        raise RequestError("request does not match the approved tuple")
    budget = match.get("max_total_seconds")
    if (not isinstance(budget, (int, float)) or isinstance(budget, bool)
            or not 0 < budget <= HARD_CEILING_SECONDS):
        raise RequestError("approved tuple lacks a valid max_total_seconds "
                           f"(1..{HARD_CEILING_SECONDS})")
    try:
        st = os.lstat(src)
    except OSError as exc:
        raise RequestError(f"source not readable: {exc}")
    if not S.S_ISREG(st.st_mode):
        raise RequestError("source is not a regular file")
    if os.path.realpath(src) != src:
        raise RequestError("source path has symlinked components")
    if st.st_size != size:
        raise RequestError(f"source size mismatch: expected {size}, got {st.st_size}")
    if _sha256(src) != sha:
        raise RequestError("source sha256 mismatch")
    return {"request_id": rid, "source_path": src,
            "source_sha256": sha, "source_bytes": size,
            "max_total_seconds": float(budget)}


def _group_proof(registry_paths) -> dict:
    """Tiny-v4 semantics over the restartproof registry: every intent
    resolved, every recorded writer verifiably dead by birth identity,
    no live member in any recorded group, complete process visibility;
    ANY gap is UNCERTAIN, never empty-by-default."""
    if isinstance(registry_paths, Path):
        registry_paths = [registry_paths]
    worker_reg = registry_paths[0]
    # missing or empty registries are NOT clean: the worker registry
    # must exist and hold at least one BOUND writer
    if not worker_reg.exists():
        return {"clean": False, "why": "worker registry missing"}
    items = []
    for rp_path in registry_paths:
        if not rp_path.exists():
            continue
        got = rp._load_registry(rp_path)
        if got is None:
            return {"clean": False,
                    "why": f"registry unreadable: {rp_path.name}"}
        items.extend(got)
    if not items:
        return {"clean": False, "why": "no bound writer in registry"}
    for it in items:
        if "pid" not in it:
            return {"clean": False, "why": "unresolved spawn intent"}
        dead, why = rp._pid_dead(it["pid"], it["start"], "/proc")
        if not dead:
            return {"clean": False, "why": f"writer {why}"}
    pgids = {it["pgid"] for it in items if isinstance(it.get("pgid"), int)}
    try:
        pids = os.listdir("/proc")
    except OSError:
        return {"clean": False, "why": "no process visibility"}
    for p in pids:
        if not p.isdigit():
            continue
        ok, why, cur = rp.stat_detail(int(p), "/proc")
        if not ok:
            if why == "absent":
                continue
            return {"clean": False, "why": f"visibility {why} pid {p}"}
        if why in pgids:
            return {"clean": False,
                    "why": f"orphan survives: pid {p} in group {why}"}
    return {"clean": True,
            "writers": [{"pid": it["pid"], "start": it["start"],
                         "pgid": it.get("pgid")} for it in items]}


def cancel_active(state_dir: str, reason: str) -> dict:
    """Kill every registered group (worker + editor), each verified
    FIRST: /proc birth identity AND actual pgid must match the record,
    and the group must be the private one we spawned (pgid == a
    registered leader pid). Unverifiable entries are reported, never
    killed blind. Bounded post-kill reap check: survivors or
    unverifiable state keep the outcome UNCERTAIN."""
    req_dir = Path(state_dir) / REQUEST_DIR
    cancel_record_error = None
    try:
        _fsync_json(req_dir / CANCEL_FILE,
                    {"reason": reason, "at": time.time()})
    except OSError as exc:
        # NOT swallowed: the kill proceeds (safety), but the missing
        # durable record is reported and blocks any clean-stop claim
        cancel_record_error = str(exc)
    try:
        with open(req_dir / IN_PROGRESS_FILE) as fh:
            claim = json.load(fh)
    except Exception:
        return {"killed": [], "note": "no in-progress diagnostic"}
    diag = Path(state_dir) / "diag" / f"retro-diag-{claim['request_id']}"
    killed, uncertain = [], []

    def _kill_registered(reg_path) -> None:
        items = rp._load_registry(reg_path) if reg_path.exists() else None
        if items is None and reg_path.exists():
            uncertain.append(f"registry unreadable: {reg_path.name}")
            return
        leaders = {it["pid"] for it in (items or []) if "pid" in it}
        for it in items or []:
            if "pid" not in it or not isinstance(it.get("pgid"), int):
                uncertain.append("unresolved spawn intent")
                continue
            ok, why, cur = rp.stat_detail(it["pid"], "/proc")
            if not (ok and cur == it["start"]):
                continue  # already dead or reused: never kill blind
            try:
                if os.getpgid(it["pid"]) != it["pgid"] \
                        or it["pgid"] not in leaders:
                    uncertain.append(f"pgid mismatch pid {it['pid']}")
                    continue
                os.killpg(it["pgid"], signal.SIGKILL)
                killed.append(it["pid"])
            except (ProcessLookupError, PermissionError) as exc:
                uncertain.append(f"kill failed pid {it['pid']}: {exc}")

    def _await_dead(reg_path, seconds: float) -> None:
        items = rp._load_registry(reg_path) if reg_path.exists() else None
        deadline = time.monotonic() + seconds
        for it in items or []:
            if "pid" not in it:
                continue
            while time.monotonic() < deadline:
                dead, _why = rp._pid_dead(it["pid"], it["start"], "/proc")
                if dead:
                    break
                time.sleep(0.05)
            else:
                uncertain.append(f"pid {it['pid']} not confirmed dead")

    # ORDER MATTERS: the editor ffmpeg is the worker's child in its OWN
    # session; kill the editor group FIRST and let the still-alive
    # worker reap its child (a worker killed first leaves an unreaped
    # zombie that a minimal PID 1 may never collect, falsely reading
    # as a surviving orphan). Then kill the worker group. Bounded
    # waits throughout, never unbounded.
    _kill_registered(diag / EDITOR_REGISTRY)
    _await_dead(diag / EDITOR_REGISTRY, 5.0)
    _kill_registered(diag / "writers.json")
    _await_dead(diag / "writers.json", 5.0)
    return {"killed": killed, "reason": reason,
            **({"cancel_record_error": cancel_record_error}
               if cancel_record_error else {}),
            **({"uncertain": uncertain} if uncertain else {})}


def watchdog_check(state_dir: str) -> str | None:
    req_dir = Path(state_dir) / REQUEST_DIR
    if (req_dir / CANCEL_FILE).exists():
        return "cancel file"
    try:
        with open(req_dir / IN_PROGRESS_FILE) as fh:
            claim = json.load(fh)
        budget = float(claim.get("max_total_seconds", HARD_CEILING_SECONDS))
        if time.time() - float(claim.get("started_at", 0)) > budget:
            return "overall deadline exceeded"
    except Exception:
        pass
    return None


def run_request(state_dir: str, stop_event=None) -> dict:
    """Claim, validate, run ONE owned child, prove, record durably.
    stop_event (threading.Event, optional): checked BEFORE claiming and
    again under the launch lock immediately before Popen, so a service
    shutdown can never race a new spawn."""
    req_dir = Path(state_dir) / REQUEST_DIR
    _check_ancestors(req_dir)  # BEFORE the first mkdir/write
    _ensure_private700(req_dir)
    if stop_event is not None and stop_event.is_set():
        raise RequestError("service stopping; no new claim")
    if consumed(state_dir):
        raise RequestError("diagnostic already consumed (one-off fence)")
    claimed = claim_request(state_dir)
    if claimed is None:
        raise RequestError("no request file")
    doc = claimed["doc"]
    rid = doc.get("request_id") if isinstance(doc, dict) else None
    if not isinstance(rid, str) or not ID_RE.match(rid or ""):
        _refuse(state_dir, Path(claimed["claimed_path"]),
                "request_id missing or invalid")
    result: dict = {"request_id": rid, "started_at": time.time(),
                    "status": "uncertain", "acceptance": False}
    diag_root = Path(state_dir) / "diag"
    _check_ancestors(diag_root)  # BEFORE any mkdir/write on the chain
    _ensure_private700(diag_root)  # parent: validate or create fresh
    diag = diag_root / f"retro-diag-{rid}"
    try:
        _mkdir_fresh700(diag)  # must be fresh: exist_ok False
    except FileExistsError:
        raise RequestError(f"diag dir for {rid} already exists; "
                           "fresh work dir required, needs review")
    _fsync_json(req_dir / IN_PROGRESS_FILE,
                {"request_id": rid, "pid": os.getpid(),
                 "started_at": result["started_at"],
                 "max_total_seconds": _approved_budget(doc)})
    registry = diag / "writers.json"
    registries = [registry, diag / EDITOR_REGISTRY]
    if not rp.init_registry(registry):
        result["error"] = "writer registry initialization failed"
        _fsync_json(diag / "result.json", result)
        return result
    proc = None
    token = None
    try:
        req = _validate(doc)
        result["source"] = req
        with _LAUNCH_LOCK:
            if stop_event is not None and stop_event.is_set():
                raise RequestError("service stopping before spawn")
            token = rp.record_spawn_intent(registry)
            if token is None:
                raise RuntimeError("spawn intent not recordable; not spawning")
            proc = subprocess.Popen(
            [sys.executable, "-B", "-m", "app.diag.retro_worker",
             "--state", state_dir, "--source", req["source_path"],
             "--diag", str(diag), "--label", f"diag:{rid}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            start_new_session=True)
        if not rp.record_spawned(registry, token, proc.pid, proc.pid):
            # bind failed: the child's group identity is NOT recorded,
            # so killpg is FORBIDDEN (blind). Direct Popen kill with a
            # bounded reap only; "dead" means the direct child was
            # actually REAPED (wait returned) - an unreadable identity
            # is never dead. The unresolved intent stays in the registry
            # forever, keeping every later proof UNCERTAIN.
            try:
                proc.kill()
            except (ProcessLookupError, PermissionError):
                pass
            for _wait in (5.0, 5.0):
                try:
                    proc.wait(timeout=_wait)
                    break
                except subprocess.TimeoutExpired:
                    try:
                        proc.kill()
                    except (ProcessLookupError, PermissionError):
                        pass
            reaped = proc.returncode is not None
            for _fh in (proc.stdout, proc.stderr):
                try:
                    if _fh is not None:
                        _fh.close()
                except OSError:
                    pass
            result["status"] = "uncertain"
            result["error"] = ("worker identity not recorded; child "
                               + ("reaped and confirmed dead" if reaped
                                  else "kill UNVERIFIED - not reaped"))
            _fsync_json(diag / "result.json",
                        {"interim": True, "request_id": rid,
                         "note": "bind failure before launch; terminal "
                                 "record follows cleanup"})
            raise _BindFailure()
        cancel_reason = None
        budget = req["max_total_seconds"]
        deadline = time.monotonic() + budget
        while True:
            try:
                out_s, err_s = proc.communicate(
                    timeout=min(5.0, max(0.1, deadline - time.monotonic())))
                break
            except subprocess.TimeoutExpired:
                cancel_reason = watchdog_check(state_dir)
                if (cancel_reason is None and stop_event is not None
                        and stop_event.is_set()):
                    cancel_reason = "service stop requested"
                if cancel_reason:
                    cancel_active(state_dir, cancel_reason)
                    try:
                        out_s, err_s = proc.communicate(timeout=10)
                    except subprocess.TimeoutExpired:
                        out_s, err_s = "", ""
                    break
                if time.monotonic() > deadline:
                    cancel_reason = "overall deadline exceeded"
                    cancel_active(state_dir, cancel_reason)
                    try:
                        out_s, err_s = proc.communicate(timeout=10)
                    except subprocess.TimeoutExpired:
                        out_s, err_s = "", ""
                    break
        result["worker_rc"] = proc.returncode
        if cancel_reason:
            result["cancel_reason"] = cancel_reason
        stderr_tail = (err_s or "")[-300:]
        if stderr_tail:
            result["worker_stderr_tail"] = stderr_tail
        # proofs ALWAYS, on every outcome, across BOTH registries
        # (worker group AND editor ffmpeg's own session group)
        proof = _group_proof(registries)
        result["proof"] = proof
        result["registries"] = _registries_state(registries)
        try:
            with open(diag / "pipeline-result.json") as fh:
                pipe = json.load(fh)
            result["pipeline"] = pipe
        except Exception as exc:
            pipe = None
            result["pipeline_read_error"] = str(exc)
        result["source_unchanged"] = (
            _sha256(req["source_path"]) == req["source_sha256"])
        # outputs come ONLY from the worker's durable evidence (exact
        # HELD DB paths recorded in-pipeline), never a glob of partials;
        # the controller reads, re-hashes and cross-checks the DB row,
        # and never spawns a subprocess itself
        outputs_ok = True
        recorded = (pipe or {}).get("outputs") or []
        try:
            _db = sqlite3.connect(
                f"file:{diag / 'nova.db'}?mode=ro", uri=True)
            try:
                held_ids = {r[0] for r in _db.execute(
                    "SELECT id FROM clips WHERE status = 'HELD'")}
            finally:
                _db.close()
        except Exception:
            held_ids = None
        result["held_db_count"] = (len(held_ids)
                                   if held_ids is not None else -1)
        recorded_ids = {o.get("db_clip_id") for o in recorded}
        if held_ids is None or recorded_ids != held_ids:
            outputs_ok = False  # distinct recorded IDs == HELD row IDs
        for o in recorded:
            # per-output worker facts, not only the summary flag
            if not (o.get("ok") and o.get("qc_ok")):
                outputs_ok = False
            try:
                import math as _m
                if not (_m.isfinite(float(o["duration"]))
                        and float(o["duration"]) > 0
                        and _m.isfinite(float(o["expected_duration"]))
                        and float(o["expected_duration"]) > 0):
                    outputs_ok = False
            except (KeyError, TypeError, ValueError):
                outputs_ok = False
            entry = dict(o)
            entry["controller_verified"] = _verify_output(diag, entry)
            outputs_ok = outputs_ok and entry["controller_verified"]
            result.setdefault("outputs", []).append(entry)
        clean = (proc.returncode == 0 and proof["clean"]
                 and result["source_unchanged"] and pipe is not None
                 and pipe.get("worker_completed") is True
                 and not cancel_reason)
        if clean:
            result["status"] = pipe.get("status", "unknown")
            # acceptance ONLY for a real held export: actual output
            # file(s), each probed and hashed, durable evidence, all
            # groups gone, source unchanged. A valid no-export outcome
            # (no_candidates / below_score_gate) is NOT acceptance.
            reg_state = result["registries"]
            if result["status"] == "held":
                if not reg_state["editor_bound"]:
                    # an export without a bound editor child means the
                    # output came from an unowned writer: never accept
                    result["status"] = "uncertain"
                    result["why"] = "held without a bound editor"
                elif not result.get("outputs"):
                    result["status"] = "uncertain"
                    result["why"] = "held claimed without output files"
                elif not (outputs_ok
                          and (pipe or {}).get("outputs_verified")):
                    result["status"] = "uncertain"
                    result["why"] = "held output failed verification"
                else:
                    result["acceptance"] = True
            else:
                result["note"] = ("valid no-export outcome; not an "
                                  "export acceptance")
        elif cancel_reason and proof["clean"]:
            result["status"] = "cancelled"
        else:
            result["status"] = "uncertain"
            result["why"] = (proof.get("why") or "worker failed or "
                             "proof/source incomplete")
    except _BindFailure:
        pass  # result already set; common finalization below finally
    except RequestError:
        raise
    except Exception as exc:
        result["status"] = "uncertain"
        result["error"] = f"{exc.__class__.__name__}: {exc}"
    finally:
        # owned teardown on EVERY post-spawn path: the WORKER may be
        # exited while the EDITOR (own session) is still alive or
        # uncertain, so snapshot and prove BOTH registries ALWAYS. A
        # proof computed while any owned writer continues is invalid.
        try:
            if proc is not None:
                reg_snap = _registries_state(registries)
                worker_live = proc.poll() is None
                if worker_live or not reg_snap["editor_registry_exists"]:
                    needs_cancel = worker_live
                else:
                    # worker exited: any live/uncertain editor member?
                    items = (rp._load_registry(registries[1]) or [])
                    editor_live = False
                    for it in items:
                        if "pid" not in it:
                            editor_live = True  # unresolved = uncertain
                            break
                        dead, _w = rp._pid_dead(it["pid"], it["start"],
                                                "/proc")
                        if not dead:
                            editor_live = True
                            break
                    needs_cancel = editor_live
                if needs_cancel:
                    cancel_active(state_dir, "controller teardown")
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        try:
                            proc.kill()
                        except (ProcessLookupError, PermissionError):
                            pass
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            result["worker_reap"] = "unconfirmed"
                    for _fh in (proc.stdout, proc.stderr):
                        try:
                            if _fh is not None:
                                _fh.close()
                        except OSError:
                            pass
                    if result.get("status") != "uncertain":
                        result["status"] = "uncertain"
                        result["why"] = ("owned writer required "
                                         "controller teardown")
                    result["acceptance"] = False
                # final snapshot ALWAYS, on every post-spawn path,
                # whether or not a cancel was needed: GROUP proof over
                # BOTH registries (never a leader-only test - a dead
                # leader with a live group orphan is NOT clean), plus
                # the registry facts. Missing or unreadable registries
                # are never clean. An incomplete final proof downgrades
                # to UNCERTAIN and clears acceptance; the fence and
                # artifacts stay (no blind kill of unverifiable
                # orphans, never an automatic retry).
                result["proof"] = _group_proof(registries)
                result["registries"] = _registries_state(registries)
                if not result["proof"].get("clean"):
                    if result.get("status") != "uncertain":
                        result["status"] = "uncertain"
                        result["why"] = (
                            "final group proof incomplete: "
                            + str(result["proof"].get("why")))
                    result["acceptance"] = False
        except Exception as _teardown_exc:
            # guarded: teardown failure keeps UNCERTAIN and the fence
            result["status"] = "uncertain"
            result["acceptance"] = False
            result["teardown_error"] = (
                f"{_teardown_exc.__class__.__name__}: {_teardown_exc}")
    result["finished_at"] = time.time()
    _fsync_json(diag / "result.json", result)
    if result["status"] == "uncertain":
        # retain the in-progress fence and artifacts; never consumed,
        # never auto-retried: a human reviews the evidence first
        return result
    done = req_dir / DONE_DIR
    _ensure_private700(done)
    _fsync_json(done / f"{rid}.json",
                {"request_id": rid, "status": result["status"],
                 "finished_at": result["finished_at"]})
    try:
        os.replace(claimed["claimed_path"],
                   str(done / f"{rid}.request.json"))
    except OSError:
        pass
    _fsync_json(req_dir / CONSUMED_FILE,
                {"request_id": rid, "status": result["status"],
                 "consumed_at": time.time()})
    try:
        (req_dir / IN_PROGRESS_FILE).unlink()
    except OSError:
        pass
    return result


def _approved_budget(doc: dict) -> float:
    for a in APPROVED:
        if a.get("request_id") == doc.get("request_id"):
            try:
                return float(a["max_total_seconds"])
            except (KeyError, TypeError, ValueError):
                break
    return float(HARD_CEILING_SECONDS)


def _registries_state(registries) -> dict:
    """Existence + binding facts, snapshotted AFTER the worker is
    reaped. Missing or unbound is a fact, never assumed clean."""
    out = {}
    for name, rp_path in (("worker", registries[0]),
                          ("editor", registries[1])):
        items = (rp._load_registry(rp_path)
                 if rp_path.exists() else None)
        out[f"{name}_registry_exists"] = items is not None
        out[f"{name}_bound"] = bool(items) and any(
            "pid" in it for it in items)
    return out


def _verify_output(diag: Path, o: dict) -> bool:
    """Controller-side output verification, reads only: the path ITSELF
    (lstat, never realpath-followed) must be a regular non-symlink file
    inside the fresh diag dir with no symlinked components, matching
    the worker's recorded size and sha256; the isolated DB row must be
    HELD for the same id+path; and the worker's probed dimensions must
    equal the PLANNED preset dims with the probed duration within
    tolerance of the planned DB duration. No subprocess here."""
    try:
        path = o.get("path", "")
        norm = os.path.normpath(path)
        # no symlink may hide anywhere: the literal path must BE real
        if os.path.realpath(norm) != norm:
            return False
        if not norm.startswith(os.path.realpath(str(diag)) + os.sep):
            return False
        st = os.lstat(norm)
        if not S.S_ISREG(st.st_mode) or S.S_ISLNK(st.st_mode):
            return False
        if st.st_size != o.get("bytes"):
            return False
        if _sha256(norm) != o.get("sha256"):
            return False
        # planned-vs-actual: dims and duration, not just "present"
        if (o.get("width") != o.get("expected_width")
                or o.get("height") != o.get("expected_height")):
            return False
        try:
            if abs(float(o["duration"])
                   - float(o["expected_duration"])) > 1.0:
                return False
        except (KeyError, TypeError, ValueError):
            return False
        db = sqlite3.connect(
            f"file:{diag / 'nova.db'}?mode=ro", uri=True)
        try:
            row = db.execute(
                "SELECT status, file_path FROM clips WHERE id = ?",
                (o.get("db_clip_id"),)).fetchone()
        finally:
            db.close()
        return bool(row) and row[0] == "HELD" and row[1] == path
    except Exception:
        return False
