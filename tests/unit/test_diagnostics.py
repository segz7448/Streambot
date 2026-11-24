"""Tests for app.core.diagnostics death-instrumentation."""
from __future__ import annotations

import json
import os
import signal
import stat
import subprocess
import sys
import time

from app.core import diagnostics


def _read_jsonl(path):
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def test_install_writes_start_breadcrumb_and_files(tmp_path):
    diagnostics.install(str(tmp_path))
    try:
        log_dir = tmp_path / "logs"
        crumbs = _read_jsonl(log_dir / "exit-breadcrumbs.jsonl")
        assert any(c["event"] == "start" and c["pid"] == os.getpid() for c in crumbs)
        assert (log_dir / "faulthandler.log").exists()
    finally:
        diagnostics._shutdown_for_tests()


def test_private_file_permissions(tmp_path):
    diagnostics.install(str(tmp_path))
    try:
        log_dir = tmp_path / "logs"
        assert _mode(log_dir) == 0o700
        assert _mode(log_dir / "exit-breadcrumbs.jsonl") == 0o600
        assert _mode(log_dir / "faulthandler.log") == 0o600
        assert _mode(log_dir / "heartbeat.json") == 0o600
    finally:
        diagnostics._shutdown_for_tests()


def test_first_heartbeat_is_immediate_and_updates(tmp_path):
    old = diagnostics.HEARTBEAT_SECONDS
    diagnostics.HEARTBEAT_SECONDS = 0.1
    try:
        diagnostics.install(str(tmp_path))
        hb = tmp_path / "logs" / "heartbeat.json"
        # written synchronously during install - no waiting for the loop
        first = json.loads(hb.read_text())
        assert first["pid"] == os.getpid()
        time.sleep(0.3)
        second = json.loads(hb.read_text())
        assert second["monotonic"] > first["monotonic"]
        assert not (tmp_path / "logs" / "heartbeat.json.tmp").exists()
    finally:
        diagnostics._shutdown_for_tests()
        diagnostics.HEARTBEAT_SECONDS = old


def test_heartbeat_thread_survives_bad_write(tmp_path, monkeypatch):
    old = diagnostics.HEARTBEAT_SECONDS
    diagnostics.HEARTBEAT_SECONDS = 0.1
    calls = {"n": 0}
    real = diagnostics._atomic_write_json

    def flaky(path, payload):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("injected non-OSError failure")
        return real(path, payload)

    monkeypatch.setattr(diagnostics, "_atomic_write_json", flaky)
    try:
        diagnostics.install(str(tmp_path))
        hb = tmp_path / "logs" / "heartbeat.json"
        time.sleep(0.6)
        # thread kept looping past the injected failure
        assert diagnostics._thread.is_alive()
        after = json.loads(hb.read_text())
        assert calls["n"] >= 3
        time.sleep(0.25)
        assert json.loads(hb.read_text())["monotonic"] > after["monotonic"]
    finally:
        diagnostics._shutdown_for_tests()
        diagnostics.HEARTBEAT_SECONDS = old


def test_breadcrumbs_capped_with_single_backup(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    path = str(log_dir / "exit-breadcrumbs.jsonl")
    old = diagnostics._BREADCRUMB_CAP_BYTES
    diagnostics._BREADCRUMB_CAP_BYTES = 600
    try:
        marker = "keep-me-" + "x" * 200
        diagnostics._breadcrumb(str(log_dir), "start", note=marker)
        for i in range(6):
            diagnostics._breadcrumb(str(log_dir), "signal", signum=1, n=i)
        assert os.path.getsize(path) <= 600 + 400  # cap + one record
        backup = path + ".1"
        assert os.path.exists(backup)
        assert _mode(backup) == 0o600
        # newest records always present in the active file
        events = [c["event"] for c in _read_jsonl(path)]
        assert events and events[-1] == "signal"
    finally:
        diagnostics._BREADCRUMB_CAP_BYTES = old


def test_faultlog_rotated_at_install_when_oversize(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    faultlog = log_dir / "faulthandler.log"
    faultlog.write_bytes(b"z" * 2048)
    old = diagnostics._FAULTLOG_CAP_BYTES
    diagnostics._FAULTLOG_CAP_BYTES = 1024
    try:
        diagnostics.install(str(tmp_path))
        assert (log_dir / "faulthandler.log.1").exists()
        assert os.path.getsize(log_dir / "faulthandler.log.1") == 2048
        assert _mode(log_dir / "faulthandler.log.1") == 0o600
    finally:
        diagnostics._shutdown_for_tests()
        diagnostics._FAULTLOG_CAP_BYTES = old


def test_breadcrumb_is_append_only(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    diagnostics._breadcrumb(str(log_dir), "start")
    diagnostics._breadcrumb(str(log_dir), "signal", signum=1, signame="SIGHUP")
    crumbs = _read_jsonl(log_dir / "exit-breadcrumbs.jsonl")
    assert [c["event"] for c in crumbs] == ["start", "signal"]
    assert crumbs[1]["signame"] == "SIGHUP"
    assert set(crumbs[0]) >= {"event", "pid", "ts", "iso", "monotonic"}


def test_unhandled_exception_breadcrumb_drops_message(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    diagnostics._install_excepthook(str(log_dir))
    try:
        raise ValueError("secret-ish message that must not be recorded")
    except ValueError as exc:
        sys.excepthook(type(exc), exc, exc.__traceback__)
    crumbs = _read_jsonl(log_dir / "exit-breadcrumbs.jsonl")
    exc_crumbs = [c for c in crumbs if c["event"] == "unhandled_exception"]
    assert exc_crumbs and exc_crumbs[0]["exc_type"] == "builtins.ValueError"
    assert "secret-ish" not in json.dumps(exc_crumbs)


def test_install_never_raises_on_unwritable_dir(tmp_path):
    ro = tmp_path / "ro"
    ro.mkdir()
    os.chmod(ro, 0o500)
    try:
        diagnostics.install(str(ro))  # should log warnings, not raise
    finally:
        os.chmod(ro, 0o700)
        diagnostics._shutdown_for_tests()


_CHILD = r"""
import os, signal, sys
signal.signal(signal.SIGHUP, signal.SIG_DFL)  # job/supervisor shells may ignore it
sys.path.insert(0, {repo!r})
from app.core import diagnostics
diagnostics.install({state!r})
os.kill(os.getpid(), signal.SIGHUP)
"""


def test_sighup_breadcrumb_in_child(tmp_path):
    state = tmp_path / "childstate"
    repo = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))
    env = dict(os.environ)
    result = subprocess.run(
        [sys.executable, "-c", _CHILD.format(repo=repo, state=str(state))],
        capture_output=True, text=True, timeout=30, env=env)
    assert result.returncode == 128 + signal.SIGHUP, result.stderr
    crumbs = _read_jsonl(state / "logs" / "exit-breadcrumbs.jsonl")
    events = [c["event"] for c in crumbs]
    assert "start" in events
    assert "signal" in events
    sig = [c for c in crumbs if c["event"] == "signal"][0]
    assert sig["signame"] == "SIGHUP"
    # os._exit: atexit handlers never run
    assert "atexit_interpreter_exit" not in events


_CHILD_EXIT = r"""
import sys
sys.path.insert(0, {repo!r})
from app.core import diagnostics
diagnostics.install({state!r})
sys.exit(3)
"""


def test_sys_exit_records_atexit_breadcrumb_not_success(tmp_path):
    state = tmp_path / "childstate2"
    repo = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))
    result = subprocess.run(
        [sys.executable, "-c", _CHILD_EXIT.format(repo=repo, state=str(state))],
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 3
    crumbs = _read_jsonl(state / "logs" / "exit-breadcrumbs.jsonl")
    events = [c["event"] for c in crumbs]
    # nonzero exit still reaches atexit: the event means interpreter
    # teardown, not a clean or successful shutdown
    assert "atexit_interpreter_exit" in events
