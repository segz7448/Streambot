"""One-off diagnostic export: atomic claim, approved-tuple fence,
single owned child, restartproof registry reuse, cancel/deadline,
UNCERTAIN fencing. Worker children are real (python -c sims)."""
import hashlib
import json
import os
import subprocess
import sys
import time

import pytest

from app.diag import retro_export as re_mod
from app.bot import restartproof as rp


def _mkstate(tmp_path):
    state = tmp_path / "state"
    (state / "sessions" / "agent00").mkdir(parents=True)
    (state / re_mod.REQUEST_DIR).mkdir(parents=True)
    for d in (state, state / "sessions", state / "sessions" / "agent00",
              state / re_mod.REQUEST_DIR):
        os.chmod(d, 0o700)  # production creates these via _mkdir700
    return state


def _source(tmp_path, state, content=b"fake-video-bytes"):
    src = state / "sessions" / "agent00" / "clip-9-11-test.mp4"
    src.write_bytes(content)
    return os.path.realpath(str(src))


def _doc(src, rid="req-1", sha=None, size=None):
    with open(src, "rb") as fh:
        data = fh.read()
    return {"request_id": rid, "source_path": src,
            "source_sha256": sha or hashlib.sha256(data).hexdigest(),
            "source_bytes": size if size is not None else len(data)}


def _write_req(state, doc):
    (state / re_mod.REQUEST_DIR / "request.json").write_text(json.dumps(doc))


def _approve(monkeypatch, doc):
    monkeypatch.setattr(re_mod, "APPROVED",
                        ({"request_id": doc["request_id"],
                          "path": doc["source_path"],
                          "sha256": doc["source_sha256"],
                          "bytes": doc["source_bytes"],
                          "max_total_seconds": 300},))


WORKER_OK = (
    "import sys,json,os,shutil\n"
    "d=sys.argv[sys.argv.index('--diag')+1]\n"
    "s=sys.argv[sys.argv.index('--source')+1]\n"
    "os.makedirs(os.path.join(d,'work'),exist_ok=True)\n"
    "shutil.copyfile(s,os.path.join(d,'work','clip-edited-sim.mp4'))\n"
    "p=os.path.join(d,'work','clip-edited-sim.mp4')\n"
    "import sqlite3\n"
    "c=sqlite3.connect(os.path.join(d,'nova.db'))\n"
    "c.execute('CREATE TABLE clips (id INTEGER PRIMARY KEY, file_path TEXT, status TEXT)')\n"
    "c.execute('INSERT INTO clips (id,file_path,status) "
    "VALUES (1,:p,:s)', {'p': p, 's': 'HELD'})\n"
    "c.commit(); c.close()\n"
    "json.dump({'status':'held','worker_completed':True,"
    "'outputs_verified':True,'outputs':[{'db_clip_id':1,'path':p,"
    "'bytes':os.path.getsize(p),'sha256':'x','ok':True,'qc_ok':True,"
    "'duration':30.0,'expected_duration':30.0,"
    "'width':720,'height':1280,'expected_width':720,"
    "'expected_height':1280}]},"
    " open(os.path.join(d,'pipeline-result.json'),'w'))\n")
WORKER_FAIL = "import sys; sys.exit(2)\n"
WORKER_SLEEP = "import time; time.sleep(60)\n"


def _true_probe(monkeypatch):
    # controller-side output/DB verification and registry facts are
    # stubbed in fake-worker tests; the real paths run in the
    # real-ffmpeg e2e (held fixture, editor child, probe/QC)
    monkeypatch.setattr(re_mod, "_verify_output", lambda d, o: True)
    monkeypatch.setattr(re_mod, "_registries_state", lambda regs: {
        "worker_registry_exists": True, "worker_bound": True,
        "editor_registry_exists": True, "editor_bound": True})


def _fake_popen(code):
    real = subprocess.Popen

    class FakePopen(real):
        def __init__(self, args, **kw):
            # replace ["-m", "app.diag.retro_worker"] with ["-c", code]
            if "app.diag.retro_worker" in args:
                i = args.index("app.diag.retro_worker")
                args = args[:i - 1] + ["-c", code] + args[i + 1:]
            super().__init__(args, **kw)
    return FakePopen


def test_no_request(tmp_path):
    state = _mkstate(tmp_path)
    assert re_mod.claim_request(str(state)) is None


def test_consumed_fence_blocks(tmp_path):
    state = _mkstate(tmp_path)
    (state / re_mod.REQUEST_DIR / "CONSUMED").write_text("{}")
    with pytest.raises(re_mod.RequestError, match="consumed"):
        re_mod.run_request(str(state))


def test_invalid_json_claimed_then_refused(tmp_path):
    state = _mkstate(tmp_path)
    (state / re_mod.REQUEST_DIR / "request.json").write_text("{nope")
    with pytest.raises(re_mod.RequestError):
        re_mod.run_request(str(state))
    # claimed away (no re-pick loop), refusal recorded
    assert not (state / re_mod.REQUEST_DIR / "request.json").exists()
    refusals = list((state / re_mod.REQUEST_DIR / "done").glob("refused-*.json"))
    assert refusals


def test_unapproved_tuple_refused(tmp_path, monkeypatch):
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    monkeypatch.setattr(re_mod, "APPROVED", ())  # nothing approved
    with pytest.raises(re_mod.RequestError, match="approved tuple"):
        re_mod.run_request(str(state))


def test_symlink_request_refused(tmp_path, monkeypatch):
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _approve(monkeypatch, doc)
    real_req = state / re_mod.REQUEST_DIR / "real.json"
    real_req.write_text(json.dumps(doc))
    os.symlink(str(real_req), str(state / re_mod.REQUEST_DIR / "request.json"))
    with pytest.raises(re_mod.RequestError):
        re_mod.run_request(str(state))


def test_success_one_owned_child(tmp_path, monkeypatch):
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    monkeypatch.setattr(re_mod.subprocess, "Popen", _fake_popen(WORKER_OK))
    _true_probe(monkeypatch)
    result = re_mod.run_request(str(state))
    assert result["status"] == "held"
    assert result["worker_rc"] == 0
    assert result["proof"]["clean"] is True
    assert len(result["proof"]["writers"]) == 1  # the ONE owned child
    w = result["proof"]["writers"][0]
    assert w["pgid"] == w["pid"] and w["start"] > 0
    assert result["source_unchanged"] is True
    assert re_mod.consumed(str(state))
    assert not (state / re_mod.REQUEST_DIR / "in-progress.json").exists()
    assert (state / re_mod.REQUEST_DIR / "done" / "req-1.json").exists()
    rj = json.loads((state / "diag" / "retro-diag-req-1" / "result.json").read_text())
    assert rj["status"] == "held"
    # second request ever: refused by the consumed fence
    doc2 = _doc(src, rid="req-2")
    _approve(monkeypatch, doc2)
    _write_req(state, doc2)
    with pytest.raises(re_mod.RequestError, match="consumed"):
        re_mod.run_request(str(state))


def test_worker_crash_is_uncertain_and_fenced(tmp_path, monkeypatch):
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    monkeypatch.setattr(re_mod.subprocess, "Popen", _fake_popen(WORKER_FAIL))
    result = re_mod.run_request(str(state))
    assert result["status"] == "uncertain"
    assert result["worker_rc"] == 2
    # fence retained: NOT consumed, in-progress kept, no auto-retry
    assert not re_mod.consumed(str(state))
    assert (state / re_mod.REQUEST_DIR / "in-progress.json").exists()
    assert (state / "diag" / "retro-diag-req-1" / "result.json").exists()


def test_cancel_kills_registered_group(tmp_path, monkeypatch):
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    monkeypatch.setattr(re_mod.subprocess, "Popen", _fake_popen(WORKER_SLEEP))
    started = time.time()

    def arm_cancel():
        time.sleep(2)
        (state / re_mod.REQUEST_DIR / "CANCEL").write_text("stop")

    import threading
    t = threading.Thread(target=arm_cancel)
    t.start()
    result = re_mod.run_request(str(state))
    t.join()
    assert time.time() - started < 30
    assert result["status"] == "cancelled"
    assert result["cancel_reason"] == "cancel file"
    assert result["proof"]["clean"] is True  # group verifiably dead
    # sleeping child must be gone: pid absent or reused
    pid = result["proof"]["writers"][0]["pid"]
    dead, why = rp._pid_dead(pid, result["proof"]["writers"][0]["start"], "/proc")
    assert dead, why


def test_unresolved_intent_blocks_proof(tmp_path):
    reg = tmp_path / "writers.json"
    assert rp.init_registry(reg)
    token = rp.record_spawn_intent(reg)
    assert token
    proof = re_mod._group_proof(reg)
    assert proof["clean"] is False
    assert "unresolved" in proof["why"]


def test_deadline_enforced(tmp_path, monkeypatch):
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    monkeypatch.setattr(re_mod.subprocess, "Popen", _fake_popen(WORKER_SLEEP))
    monkeypatch.setattr(re_mod, "APPROVED",
                        ({"request_id": doc["request_id"],
                          "path": doc["source_path"],
                          "sha256": doc["source_sha256"],
                          "bytes": doc["source_bytes"],
                          "max_total_seconds": 6},))
    started = time.time()
    result = re_mod.run_request(str(state))
    assert time.time() - started < 30
    assert result["status"] == "cancelled"
    assert "deadline" in result["cancel_reason"]
    assert result["proof"]["clean"] is True


def test_source_changed_is_uncertain(tmp_path, monkeypatch):
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    monkeypatch.setattr(re_mod.subprocess, "Popen", _fake_popen(WORKER_OK))
    _true_probe(monkeypatch)
    real_sha = re_mod._sha256
    calls = {"n": 0}

    def flip(path):
        calls["n"] += 1
        # first call: validation; second: post-run re-hash
        return "0" * 64 if calls["n"] > 1 else real_sha(path)
    monkeypatch.setattr(re_mod, "_sha256", flip)
    result = re_mod.run_request(str(state))
    assert result["status"] == "uncertain"
    assert result["source_unchanged"] is False
    assert not re_mod.consumed(str(state))


@pytest.mark.skipif(not (os.path.exists("/usr/bin/ffmpeg")
                         or os.environ.get("NOVA_FFMPEG")),
                    reason="real ffmpeg unavailable")
def test_real_ffmpeg_end_to_end_and_failure(tmp_path, monkeypatch):
    """Real worker module, real ffmpeg: a clean tiny input runs the
    actual retrospective pipeline in the owned child; an unreadable
    input fails with a durable worker result and clean group proof."""
    import shutil
    ff = shutil.which("ffmpeg")
    monkeypatch.setenv("NOVA_FFMPEG", ff)
    monkeypatch.setenv("NOVA_FFPROBE", shutil.which("ffprobe"))
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    subprocess.run([ff, "-hide_banner", "-loglevel", "error", "-y",
                    "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=15",
                    "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100",
                    "-t", "3", "-pix_fmt", "yuv420p", src], check=True)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    monkeypatch.setattr(re_mod, "APPROVED",
                        ({"request_id": doc["request_id"],
                          "path": doc["source_path"],
                          "sha256": doc["source_sha256"],
                          "bytes": doc["source_bytes"],
                          "max_total_seconds": 600},))
    result = re_mod.run_request(str(state))
    assert result["worker_rc"] == 0, result.get("worker_stderr_tail")
    assert result["proof"]["clean"] is True
    assert result["source_unchanged"] is True
    assert result["status"] in ("held", "no_candidates",
                                "below_score_gate", "qc_failed")
    assert result["pipeline"]["worker_completed"] is True
    # no_candidates/below_score_gate are truthful no-clip outcomes for
    # synthetic silence; held means a real clip was produced and hashed
    if result["status"] == "held":
        assert result["outputs"] and all(
            o["sha256"] for o in result["outputs"])

    # failure path: a second approved id whose source is not media
    state2 = tmp_path / "state2"
    (state2 / "sessions" / "agent00").mkdir(parents=True)
    (state2 / re_mod.REQUEST_DIR).mkdir(parents=True)
    for d in (state2, state2 / "sessions",
              state2 / "sessions" / "agent00", state2 / re_mod.REQUEST_DIR):
        os.chmod(d, 0o700)
    bad = os.path.realpath(str(state2 / "sessions" / "agent00" / "bad.mp4"))
    with open(bad, "wb") as fh:
        fh.write(b"this is not a video")
    doc2 = _doc(bad, rid="req-bad")
    (state2 / re_mod.REQUEST_DIR / "request.json").write_text(json.dumps(doc2))
    _approve(monkeypatch, doc2)
    result2 = re_mod.run_request(str(state2))
    assert result2["worker_rc"] == 2  # worker caught the validation error
    assert result2["status"] == "uncertain"
    assert result2["proof"]["clean"] is True
    pipe = json.loads((state2 / "diag" / "retro-diag-req-bad"
                       / "pipeline-result.json").read_text())
    assert pipe["worker_completed"] is False
    assert "error" in pipe


FFMPEG_REAL = os.path.exists("/usr/bin/ffmpeg") or os.environ.get("NOVA_FFMPEG")


def _action_fixture(path, size="320x240", dur=24, burst_at=10):
    """Deterministic synthetic ACTION fixture (test-only; production
    gate unchanged): quiet baseline, then a loud 3s burst with a hard
    visual cut at burst_at -> a known audio-spike + scene-change
    candidate pair that the real gate accepts."""
    pre, burst, post = burst_at, 3, dur - burst_at - 3
    subprocess.run([
        "/usr/bin/ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", f"color=c=darkred:s={size}:r=15:d={pre}",
        "-f", "lavfi", "-i", f"testsrc2=size={size}:rate=15:duration={burst}",
        "-f", "lavfi", "-i", f"color=c=black:s={size}:r=15:d={post}",
        "-f", "lavfi", "-i",
        f"sine=frequency=220:sample_rate=44100:d={dur},"
        f"volume='if(between(t,{burst_at},{burst_at + burst}),0.9,0.02)':eval=frame",
        "-filter_complex", "[0:v][1:v][2:v]concat=n=3:v=1:a=0[v]",
        "-map", "[v]", "-map", "3:a", "-pix_fmt", "yuv420p", path],
        check=True)


@pytest.mark.skipif(not FFMPEG_REAL, reason="real ffmpeg unavailable")
def test_real_held_probe_acceptance(tmp_path, monkeypatch):
    """REAL export acceptance: fixture -> actual worker -> actual
    editor (own session) -> held clip -> in-worker probe/QC/hash ->
    controller read-only verification -> acceptance True."""
    import shutil
    monkeypatch.setenv("NOVA_FFMPEG", shutil.which("ffmpeg"))
    monkeypatch.setenv("NOVA_FFPROBE", shutil.which("ffprobe"))
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    _action_fixture(src)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    result = re_mod.run_request(str(state))
    assert result["status"] == "held", result.get("why") or result
    assert result["acceptance"] is True
    assert result["proof"]["clean"] is True
    assert result["source_unchanged"] is True
    assert result["registries"]["editor_bound"] is True
    out = result["outputs"][0]
    assert out["controller_verified"] is True
    assert out["qc_ok"] is True
    assert out["has_video"] and out["has_audio"]
    assert 0 < out["duration"] <= 180
    assert out["width"] and out["height"]
    assert out["sha256"] and out["bytes"] > 0


@pytest.mark.skipif(not FFMPEG_REAL, reason="real ffmpeg unavailable")
def test_cancel_while_real_editor_active(tmp_path, monkeypatch):
    """Cancel lands while the ACTUAL editor ffmpeg runs in its OWN
    session: both groups die, proof stays clean, nothing is orphaned,
    no acceptance."""
    import shutil
    monkeypatch.setenv("NOVA_FFMPEG", shutil.which("ffmpeg"))
    monkeypatch.setenv("NOVA_FFPROBE", shutil.which("ffprobe"))
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    _action_fixture(src, size="640x480", dur=30, burst_at=15)
    doc = _doc(src)
    _write_req(state, doc)
    monkeypatch.setattr(re_mod, "APPROVED",
                        ({"request_id": doc["request_id"],
                          "path": doc["source_path"],
                          "sha256": doc["source_sha256"],
                          "bytes": doc["source_bytes"],
                          "max_total_seconds": 600},))
    edit_reg = (state / "diag" / f"retro-diag-{doc['request_id']}"
                / "writers-edit.json")

    def _cancel_once_editor_active():
        # deterministic: wait until the editor child is bound, then
        # cancel while it is actively encoding
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                if edit_reg.exists() and b'"pid"' in edit_reg.read_bytes():
                    break
            except OSError:
                pass
            time.sleep(0.1)
        re_mod.cancel_active(str(state), "test cancel mid-edit")

    import threading as _th
    t = _th.Thread(target=_cancel_once_editor_active, daemon=True)
    t.start()
    result = re_mod.run_request(str(state))
    t.join(timeout=5)
    assert result["status"] in ("cancelled", "uncertain")
    assert result["acceptance"] is False
    assert result["proof"]["clean"] is True, result["proof"].get("why")
    assert result["registries"]["editor_bound"] is True


def test_regression_proof_death_check_reachable(tmp_path):
    """A: a bound but LIVE writer must fail proof with the writer
    reason (the death check was orphaned unreachable once)."""
    state = _mkstate(tmp_path)
    diag = state / "diag" / "retro-diag-x"
    diag.mkdir(parents=True)
    reg = diag / "writers.json"
    re_mod.rp.init_registry(reg)
    token = re_mod.rp.record_spawn_intent(reg)
    assert re_mod.rp.record_spawned(reg, token, os.getpid(), os.getpid())
    proof = re_mod._group_proof([reg, diag / "writers-edit.json"])
    assert proof["clean"] is False
    assert "writer" in proof["why"] or "orphan" in proof["why"]


def test_regression_mkdir_fresh_and_private(tmp_path):
    """B: fresh-leaf mkdir refuses existing; existing control dir with
    wrong mode refused; dangling symlink ancestor refused."""
    state = _mkstate(tmp_path)
    (state / "diag").mkdir()
    leaf = state / "diag" / "fresh"
    re_mod._mkdir_fresh700(leaf)
    with pytest.raises(FileExistsError):
        re_mod._mkdir_fresh700(leaf)
    bad = tmp_path / "badmode"
    bad.mkdir()
    os.chmod(bad, 0o755)
    with pytest.raises(re_mod.RequestError):
        re_mod._ensure_private700(bad)
    # dangling symlink inside the control chain
    req = state / re_mod.REQUEST_DIR
    (req / "claimed").mkdir()
    os.symlink("/nonexistent-target", str(req / "claimed" / "link"))
    assert not re_mod._private700(req / "claimed" / "link")


def test_regression_post_spawn_exception_teardown(tmp_path, monkeypatch):
    """C: an exception AFTER spawn still cancels+reaps the worker
    before proof; nothing keeps running."""
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    monkeypatch.setattr(re_mod.subprocess, "Popen",
                        _fake_popen(WORKER_SLEEP))
    real_sha = re_mod._sha256
    calls = {"n": 0}

    def boom(path):
        calls["n"] += 1
        if calls["n"] > 1:
            raise RuntimeError("injected post-spawn failure")
        return real_sha(path)
    monkeypatch.setattr(re_mod, "_sha256", boom)
    result = re_mod.run_request(str(state))
    assert result["status"] == "uncertain"
    assert "injected" in result.get("error", "")
    assert result["proof"]["clean"] is True  # worker cancelled+reaped


def test_regression_stop_event_before_claim_and_mid_run(tmp_path, monkeypatch):
    """D: a set stop event blocks claiming; set mid-run it cancels the
    worker via the controller's own wait loop."""
    import threading
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    ev = threading.Event()
    ev.set()
    with pytest.raises(re_mod.RequestError, match="stopping"):
        re_mod.run_request(str(state), stop_event=ev)
    # mid-run: set the event as soon as the worker is bound
    ev2 = threading.Event()
    monkeypatch.setattr(re_mod.subprocess, "Popen",
                        _fake_popen(WORKER_SLEEP))
    reg = state / "diag" / f"retro-diag-{doc['request_id']}" / "writers.json"

    def _set_when_bound():
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if reg.exists() and b'"pid"' in reg.read_bytes():
                    ev2.set()
                    return
            except OSError:
                pass
            time.sleep(0.05)
    import threading as _th
    t = _th.Thread(target=_set_when_bound, daemon=True)
    t.start()
    result = re_mod.run_request(str(state), stop_event=ev2)
    t.join(timeout=5)
    assert result["status"] == "cancelled"
    assert result["cancel_reason"] == "service stop requested"
    assert result["proof"]["clean"] is True


def test_regression_verify_output_dims_duration_symlink(tmp_path):
    """E: planned dims mismatch, duration out of tolerance and a
    symlinked output path all fail controller verification."""
    diag = tmp_path / "diag"
    work = diag / "work"
    work.mkdir(parents=True)
    out = work / "clip.mp4"
    out.write_bytes(b"x" * 64)
    import hashlib
    sha = hashlib.sha256(b"x" * 64).hexdigest()
    base = {"db_clip_id": 1, "path": str(out), "bytes": 64,
            "sha256": sha, "width": 720, "height": 1280,
            "expected_width": 720, "expected_height": 1280,
            "duration": 30.0, "expected_duration": 30.2}
    import sqlite3
    db = sqlite3.connect(str(diag / "nova.db"))
    db.execute("CREATE TABLE clips (id INTEGER PRIMARY KEY,"
               " file_path TEXT, status TEXT)")
    db.execute("INSERT INTO clips VALUES (1, ?, 'HELD')", (str(out),))
    db.commit(); db.close()
    assert re_mod._verify_output(diag, dict(base)) is True
    assert re_mod._verify_output(
        diag, dict(base, expected_width=1080)) is False
    assert re_mod._verify_output(
        diag, dict(base, expected_duration=45.0)) is False
    link = work / "link.mp4"
    os.symlink(str(out), str(link))
    assert re_mod._verify_output(
        diag, dict(base, path=str(link))) is False


def test_regression_worker_exited_editor_still_live(tmp_path, monkeypatch):
    """V6 blocker 1: worker exits normally but the EDITOR (own
    session) is still alive: the finally must cancel it, re-prove,
    downgrade to uncertain and clear acceptance."""
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    _true_probe(monkeypatch)
    monkeypatch.setattr(re_mod.subprocess, "Popen",
                        _fake_popen(WORKER_OK))
    # a real live "editor" in its own session, bound in the editor
    # registry BEFORE the controller's final snapshot
    sleeper = subprocess.Popen(["sleep", "300"], start_new_session=True)
    import threading as _th0
    _th0.Thread(target=sleeper.wait, daemon=True).start()  # parent reaps
    diag = state / "diag" / f"retro-diag-{doc['request_id']}"
    edit_reg = diag / "writers-edit.json"

    real_run = re_mod.run_request

    def _plant_and_run(sd, stop_event=None):
        # plant right after the fresh diag dir exists: wrap claim to
        # trigger planting on first registry touch instead
        import threading as _th
        def _plant():
            deadline = time.time() + 30
            while time.time() < deadline:
                if diag.exists():
                    re_mod.rp.init_registry(edit_reg)
                    token = re_mod.rp.record_spawn_intent(edit_reg)
                    assert re_mod.rp.record_spawned(
                        edit_reg, token, sleeper.pid, sleeper.pid)
                    return
                time.sleep(0.02)
        t = _th.Thread(target=_plant, daemon=True)
        t.start()
        r = real_run(sd, stop_event=stop_event)
        t.join(timeout=5)
        return r
    result = _plant_and_run(str(state))
    assert result["status"] == "uncertain"
    assert result["acceptance"] is False
    assert result["proof"]["clean"] is True  # post-teardown re-proof
    # the orphaned editor is really dead and reaped
    sleeper.wait(timeout=5)
    assert sleeper.returncode is not None
    assert sleeper.returncode != 0


def test_bind_failure_terminal_record(tmp_path, monkeypatch):
    """V7 blocker 1: bind failure must NOT early-return an interim
    record. The terminal result.json is written AFTER the finally
    teardown and carries status uncertain + proof + finished_at."""
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    monkeypatch.setattr(re_mod.subprocess, "Popen",
                        _fake_popen(WORKER_SLEEP))
    monkeypatch.setattr(re_mod.rp, "record_spawned",
                        lambda *a, **k: False)  # bind always fails
    result = re_mod.run_request(str(state))
    assert result["status"] == "uncertain"
    assert result["acceptance"] is False
    assert "identity not recorded" in result["error"]
    diag = state / "diag" / f"retro-diag-{doc['request_id']}"
    data = json.loads((diag / "result.json").read_text())
    assert "interim" not in data  # terminal record, not the note
    assert data["status"] == "uncertain"
    assert data["acceptance"] is False
    assert "finished_at" in data
    # the unresolved spawn intent keeps the final proof not clean
    assert data["proof"]["clean"] is False
    assert "unresolved spawn intent" in data["proof"]["why"]
    # UNCERTAIN retains the in-progress fence; never consumed
    assert (state / re_mod.REQUEST_DIR / re_mod.IN_PROGRESS_FILE).exists()
    assert not (state / re_mod.REQUEST_DIR / re_mod.CONSUMED_FILE).exists()


def test_regression_exception_before_normal_proof(tmp_path, monkeypatch):
    """V7 blocker 2a: an exception raised BEFORE the normal-path proof
    still gets a final group proof from the finally: the live worker is
    cancelled + reaped, the proof is recorded, status uncertain."""
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    monkeypatch.setattr(re_mod.subprocess, "Popen",
                        _fake_popen(WORKER_SLEEP))

    def boom(_sd):
        raise RuntimeError("injected watchdog failure")
    monkeypatch.setattr(re_mod, "watchdog_check", boom)
    result = re_mod.run_request(str(state))
    assert result["status"] == "uncertain"
    assert result["acceptance"] is False
    assert "injected watchdog failure" in result.get("error", "")
    # proof exists ONLY because the finally supplied it (the normal
    # proof line never executed); worker was cancelled and reaped
    assert result["proof"]["clean"] is True
    diag = state / "diag" / f"retro-diag-{doc['request_id']}"
    data = json.loads((diag / "result.json").read_text())
    assert data["status"] == "uncertain"
    assert "finished_at" in data
    assert data["proof"]["clean"] is True


def test_regression_dead_leader_live_group_orphan(tmp_path, monkeypatch):
    """V7 blocker 2b: editor LEADER verifiably dead but a live orphan
    remains in its recorded GROUP: needs_cancel stays False (no blind
    kill of the unverifiable orphan) yet the final GROUP proof is not
    clean, so the outcome is UNCERTAIN, acceptance False, fence kept."""
    state = _mkstate(tmp_path)
    src = _source(tmp_path, state)
    doc = _doc(src)
    _write_req(state, doc)
    _approve(monkeypatch, doc)
    _true_probe(monkeypatch)
    monkeypatch.setattr(re_mod.subprocess, "Popen",
                        _fake_popen(WORKER_OK))
    orphan = subprocess.Popen(["sleep", "300"], start_new_session=True)
    import threading as _th
    _th.Thread(target=orphan.wait, daemon=True).start()  # parent reaps
    diag = state / "diag" / f"retro-diag-{doc['request_id']}"
    edit_reg = diag / "writers-edit.json"
    dead_pid = 99999999  # absent from /proc: verifiably not running
    real_load = re_mod.rp._load_registry
    real_dead = re_mod.rp._pid_dead

    def fake_load(path):
        if str(path) == str(edit_reg):
            return [{"pid": dead_pid, "start": "dead-birth",
                     "pgid": orphan.pid}]
        return real_load(path)

    def fake_dead(pid, start, root):
        if pid == dead_pid:
            return True, "reaped"
        return real_dead(pid, start, root)

    monkeypatch.setattr(re_mod.rp, "_load_registry", fake_load)
    monkeypatch.setattr(re_mod.rp, "_pid_dead", fake_dead)

    def _plant():
        deadline = time.time() + 30
        while time.time() < deadline:
            if diag.exists():
                re_mod.rp.init_registry(edit_reg)
                return
            time.sleep(0.01)
    t = _th.Thread(target=_plant, daemon=True)
    t.start()
    try:
        result = re_mod.run_request(str(state))
        t.join(timeout=5)
        assert result["status"] == "uncertain"
        assert result["acceptance"] is False
        assert result["proof"]["clean"] is False
        assert "orphan" in result["proof"]["why"]
        # no blind kill of the unverifiable orphan: still alive here
        assert orphan.poll() is None
        # fence and artifacts retained, never consumed
        assert (state / re_mod.REQUEST_DIR
                / re_mod.IN_PROGRESS_FILE).exists()
        assert not (state / re_mod.REQUEST_DIR
                    / re_mod.CONSUMED_FILE).exists()
    finally:
        try:
            orphan.kill()
        except (ProcessLookupError, PermissionError):
            pass
        orphan.wait(timeout=5)


def _stage_config(tmp_path, entries, mode=0o600):
    """A private 0700 leaf dir holding the config at the given mode."""
    d = tmp_path / "diag-activation"
    d.mkdir(exist_ok=True)
    os.chmod(d, 0o700)
    f = d / "config.json"
    f.write_text(json.dumps(entries))
    os.chmod(f, mode)
    return f


def _entry(**kw):
    base = {"request_id": "req-agent00-raw-044db707",
            "path": os.path.realpath("/p"), "sha256": "0" * 64,
            "bytes": 6554465, "max_total_seconds": 900}
    base.update(kw)
    return base


def test_activation_config_loader(tmp_path, monkeypatch):
    """Staged private config: happy path parses EXACTLY ONE tuple."""
    monkeypatch.delenv("NOVA_RETRO_APPROVED_FILE", raising=False)
    assert re_mod._load_approved() == ()  # unset env
    monkeypatch.setenv("NOVA_RETRO_APPROVED_FILE", "relative/path.json")
    assert re_mod._load_approved() == ()  # not absolute
    monkeypatch.setenv("NOVA_RETRO_APPROVED_FILE",
                       str(tmp_path) + "/./diag-activation/config.json")
    assert re_mod._load_approved() == ()  # non-canonical refused
    f = _stage_config(tmp_path, [_entry()])
    monkeypatch.setenv("NOVA_RETRO_APPROVED_FILE", str(f))
    ap = re_mod._load_approved()
    assert len(ap) == 1
    assert ap[0]["request_id"] == "req-agent00-raw-044db707"
    assert ap[0]["max_total_seconds"] == 900


def test_activation_config_loader_refusals(tmp_path, monkeypatch):
    """Every deviation fails closed to empty."""
    f = _stage_config(tmp_path, [_entry()])
    monkeypatch.setenv("NOVA_RETRO_APPROVED_FILE", str(f))
    # symlink config file
    link = f.parent / "link.json"
    os.symlink(str(f), str(link))
    monkeypatch.setenv("NOVA_RETRO_APPROVED_FILE", str(link))
    assert re_mod._load_approved() == ()
    monkeypatch.setenv("NOVA_RETRO_APPROVED_FILE", str(f))
    # symlinked ancestor dir
    dlink = tmp_path / "dlink"
    os.symlink(str(f.parent), str(dlink))
    monkeypatch.setenv("NOVA_RETRO_APPROVED_FILE", str(dlink / "config.json"))
    assert re_mod._load_approved() == ()
    monkeypatch.setenv("NOVA_RETRO_APPROVED_FILE", str(f))
    # world/group-readable config
    os.chmod(f, 0o644)
    assert re_mod._load_approved() == ()
    os.chmod(f, 0o640)
    assert re_mod._load_approved() == ()
    os.chmod(f, 0o600)
    # shared-permission config dir
    os.chmod(f.parent, 0o755)
    assert re_mod._load_approved() == ()
    os.chmod(f.parent, 0o700)
    # wrong owner (simulated euid mismatch)
    monkeypatch.setattr(os, "geteuid", lambda: os.getuid() + 1)
    assert re_mod._load_approved() == ()
    monkeypatch.undo()
    monkeypatch.setenv("NOVA_RETRO_APPROVED_FILE", str(f))
    # invalid JSON
    f.write_text("{not json")
    assert re_mod._load_approved() == ()
    # oversized config (fstat refused before parse)
    f.write_text(json.dumps([_entry()]) + " " * 20000)
    assert re_mod._load_approved() == ()
    # not exactly one tuple
    f.write_text(json.dumps([]))
    assert re_mod._load_approved() == ()
    f.write_text(json.dumps([_entry(), _entry(request_id="req-other")]))
    assert re_mod._load_approved() == ()
    # tuple field violations
    f.write_text(json.dumps([dict(_entry(), request_id="bad id!!")]))
    assert re_mod._load_approved() == ()
    f.write_text(json.dumps([dict(_entry(), path="relative.mp4")]))
    assert re_mod._load_approved() == ()
    f.write_text(json.dumps([dict(_entry(), path="/x/./y.mp4")]))
    assert re_mod._load_approved() == ()
    f.write_text(json.dumps([dict(_entry(), sha256="zz")]))
    assert re_mod._load_approved() == ()
    f.write_text(json.dumps([dict(_entry(), bytes=True)]))
    assert re_mod._load_approved() == ()
    f.write_text(json.dumps([dict(_entry(), bytes=0)]))
    assert re_mod._load_approved() == ()
    # budget: NaN / bool / zero / negative / over the hard ceiling
    f.write_text(json.dumps([dict(_entry(), max_total_seconds=float("nan"))]))
    assert re_mod._load_approved() == ()
    f.write_text(json.dumps([dict(_entry(), max_total_seconds=0)]))
    assert re_mod._load_approved() == ()
    f.write_text(json.dumps([dict(_entry(), max_total_seconds=-5)]))
    assert re_mod._load_approved() == ()
    f.write_text(json.dumps([dict(_entry(), max_total_seconds=True)]))
    assert re_mod._load_approved() == ()
    f.write_text(json.dumps(
        [dict(_entry(), max_total_seconds=re_mod.HARD_CEILING_SECONDS + 1)]))
    assert re_mod._load_approved() == ()
    f.write_text(json.dumps(
        [dict(_entry(), max_total_seconds=re_mod.HARD_CEILING_SECONDS)]))
    assert len(re_mod._load_approved()) == 1  # exactly at ceiling OK


def test_activation_fresh_import_integration(tmp_path):
    """REAL activation path: a FRESH interpreter imports the module with
    the env var already set (the staged-before-startup contract). Valid
    config loads exactly one tuple; a policy-violating config loads
    empty; env unset loads empty. Import-time evaluation order is what
    this proves - post-import direct calls cannot."""
    import subprocess as sp
    repo_root = os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(re_mod.__file__))))
    code = ("from app.diag import retro_export as m; "
            "print('LOADED', len(m.APPROVED))")

    def fresh(env_file):
        env = dict(os.environ)
        env.pop("NOVA_RETRO_APPROVED_FILE", None)
        if env_file is not None:
            env["NOVA_RETRO_APPROVED_FILE"] = str(env_file)
        env["PYTHONPATH"] = repo_root + os.pathsep + env.get(
            "PYTHONPATH", "")
        r = sp.run([sys.executable, "-B", "-c", code],
                   capture_output=True, text=True, env=env, timeout=60)
        assert r.returncode == 0, r.stderr[-500:]
        return r.stdout.strip()

    good = _stage_config(tmp_path, [_entry()])
    assert fresh(good) == "LOADED 1"          # valid staged config
    os.chmod(good, 0o644)
    assert fresh(good) == "LOADED 0"          # world-readable refused
    os.chmod(good, 0o600)
    good.write_text(json.dumps([_entry(), _entry(request_id="req-b")]))
    assert fresh(good) == "LOADED 0"          # multi tuple refused
    assert fresh(None) == "LOADED 0"          # unset env refused


def test_regression_service_loop_real_blocking_call(tmp_path, monkeypatch):
    """Regression for the deployed TypeError: the service loop must pass
    the stop event POSITIONALLY through the real positional-only
    _blocking wrapper. Drives the REAL BotService._retro_diag_loop with
    the REAL _blocking and the REAL run_request (unapproved tuple: real
    claim happens, then durable refusal - no ffmpeg, fast). With the
    kwarg bug the TypeError is swallowed by the loop's broad except and
    the claim fence never appears, so this test hangs then fails."""
    import asyncio
    import threading
    from app.bot.service import BotService

    state = _mkstate(tmp_path)
    req = state / re_mod.REQUEST_DIR / re_mod.REQUEST_FILE
    req.write_text(json.dumps(
        {"request_id": "req-loop-unapproved", "source_path": "/nope",
         "source_sha256": "0" * 64, "source_bytes": 1}),
        encoding="utf-8")
    monkeypatch.setattr(re_mod, "APPROVED", ())  # refuse after real claim

    svc = BotService.__new__(BotService)
    svc._state_dir = lambda: state

    async def drive():
        task = asyncio.ensure_future(svc._retro_diag_loop(interval=0.01))
        for _ in range(1000):
            if (state / re_mod.REQUEST_DIR / re_mod.CLAIM_FENCE).exists():
                break
            if task.done():
                break
            await asyncio.sleep(0.01)
        svc._retro_diag_stopping = True
        await asyncio.wait_for(task, 10)

    asyncio.run(drive())
    fence = state / re_mod.REQUEST_DIR / re_mod.CLAIM_FENCE
    assert fence.exists(), "loop never reached run_request through _blocking"


def test_regression_service_loop_shutdown_event_path(tmp_path, monkeypatch):
    """The stop Event must travel through the wrapper and be honored
    BEFORE any claim: a pre-set event means no fence, no claim, request
    preserved, and the loop exits cleanly once stopping is set."""
    import asyncio
    import threading
    from app.bot.service import BotService

    state = _mkstate(tmp_path)
    req = state / re_mod.REQUEST_DIR / re_mod.REQUEST_FILE
    req.write_text(json.dumps(
        {"request_id": "req-loop-stop", "source_path": "/nope",
         "source_sha256": "0" * 64, "source_bytes": 1}),
        encoding="utf-8")
    monkeypatch.setattr(re_mod, "APPROVED", ())

    svc = BotService.__new__(BotService)
    svc._state_dir = lambda: state
    ev = threading.Event()
    ev.set()  # shutdown signalled before the loop ever claims
    svc._retro_diag_stop_event = ev

    async def drive():
        task = asyncio.ensure_future(svc._retro_diag_loop(interval=0.01))
        await asyncio.sleep(0.2)  # several cycles with the event set
        svc._retro_diag_stopping = True
        await asyncio.wait_for(task, 10)

    asyncio.run(drive())
    assert req.exists(), "request must be preserved when stopping"
    assert not (state / re_mod.REQUEST_DIR / re_mod.CLAIM_FENCE).exists(), \
        "no claim may happen once the stop event is set"


def test_regression_generation2_control_root():
    """Generation boundary: the g2 control root must differ from the
    cancelled g1 root so a recovery run never touches the preserved
    fences, and the diag workdir name follows the request id."""
    assert re_mod.REQUEST_DIR == "retro-diag-g2"
    assert re_mod.REQUEST_DIR != "retro-diag"
