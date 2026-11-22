"""Opt-in, one-attempt acceptance runner. No source contact at import.
Acquisition deadline is <=85s including title/resolve. This is a stop and
request-budget bound, not a hard total-process-exit guarantee. In-flight
network/process cleanup has latency. Editing alone has the existing 600s
cap; cleanup/finalize/private upload can exceed a ten-minute reporting
checkpoint. Never force-kill or report termination before exact owner drain.
There is no dry-run CLI: tests use disposable state, fake capture/sender,
and a network-denied test scope. No production state is a dry-run default.
Run only under a separately approved live window. Not a scheduler/retry loop.
"""
from __future__ import annotations
import argparse
import json
import threading
import time
from pathlib import Path
from . import sessions, source_auth
from ..pipeline.session import ClipSession


def run_once(state_dir, ffmpeg, ffprobe, *, target, seconds=85,
             monitor=None):
    """Live entry. Requires explicit state path, exact allowlisted target,
    current record ON/repost OFF, registered source, no competing session.
    monitor is diagnostics only: its exception stops acquisition and is
    retained, never allowed to bypass ownership cleanup.
    """
    sd = Path(state_dir)
    if target != "lacy" or not (0 < seconds <= 85):
        raise ValueError("acceptance requires lacy and 0 < seconds <= 85")
    if not (sd / "nova.db").is_file():
        raise ValueError("explicit existing state required")
    if not source_auth.allowed(sd, target, "record") or source_auth.allowed(sd, target, "repost"):
        raise ValueError("record ON and repost OFF required")
    return _run(sd, ffmpeg, ffprobe, target, seconds, monitor=monitor)


def _run(sd, ffmpeg, ffprobe, target, seconds, *, monitor=None,
         start_fn=None, factory_base=ClipSession):
    # Internal injection seam for disposable, network-denied local tests only.
    deadline = time.monotonic() + seconds
    owned = []
    fault = None
    expired = threading.Event()
    def stop_owned():
        expired.set()
        for session in tuple(owned):
            session.stop()
            worker = getattr(session, "_worker", None)
            if worker is not None:
                worker._stop_req = True  # independent of session editor/monitor
    timer = threading.Timer(max(0, deadline-time.monotonic()), stop_owned)
    timer.daemon = True
    def factory(*args, **kwargs):
        if expired.is_set() or time.monotonic() >= deadline:
            raise TimeoutError("capture window expired during launch")
        kwargs.update(acceptance_deadline=deadline, max_session_clips=1)
        session = factory_base(*args, **kwargs)
        owned.append(session)
        return session
    timer.start()
    try:
        # Existing launcher owns reservation, current title gate, source scope
        # checks and a single resolve. No schedule, retry or alternate route.
        if start_fn is None:
            from ..ingestion import twitch_playback as tp
            def resolve_once(*args, **kwargs):
                prior = kwargs.get("should_stop", lambda: False)
                kwargs["should_stop"] = lambda: expired.is_set() or time.monotonic() >= deadline or prior()
                if kwargs["should_stop"]():
                    raise TimeoutError("capture window expired before resolve")
                return tp.resolve(*args, **kwargs)
            sessions.startplayback_reply(sd, target, ffmpeg, ffprobe,
                session_factory=factory, resolve_fn=resolve_once)
        else:
            start_fn(sd, target, ffmpeg, ffprobe, session_factory=factory)
        if not owned:
            return {"launched": False, "terminated": True, "reason": "refused"}
        session = owned[0]
        while session.alive():
            if time.monotonic() >= deadline:
                stop_owned()
            if monitor is not None:
                try:
                    monitor(session)  # exact object, never global latest DB row
                except Exception as exc:
                    fault = type(exc).__name__
                    stop_owned()
                    monitor = None
            session.join(.1)
    finally:
        stop_owned()
        # No timeout-to-exit conversion. If the actual owner cannot attest
        # termination the runner stays alive and owns cleanup, not success.
        for session in owned:
            while session.alive():
                session.stop()
                session.join(.1)
        timer.cancel()
        timer.join()
    session = owned[0]
    proven = bool(getattr(session, "_finalized", False))
    return {"launched": True, "session_id": getattr(session, "_session_id", None),
            "terminated": proven, "monitor_fault": fault,
            "clips_finished": session.clips_finished,
            "reason": "finished" if proven else "termination_unproven"}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--target", required=True, choices=["lacy"])
    parser.add_argument("--seconds", type=float, default=85)
    args=parser.parse_args()
    from .media import ffmpeg_paths
    from ..core import content_gate
    from ..ingestion import twitch_title
    content_gate.set_provider(twitch_title.provider)
    ff,fp=ffmpeg_paths()
    print(json.dumps(run_once(args.state_dir,ff,fp,target=args.target,seconds=args.seconds)))

if __name__ == "__main__":
    main()
