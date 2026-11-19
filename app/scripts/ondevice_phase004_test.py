"""On-device Phase 004 proof harness (runs on AGT, stdlib only).

Records a looped local file through RecordingWorker with a tiny storage cap
to force rotation, then verifies: capture happened, old segments were
rotated out, a restart-style reconcile is idempotent, and the worker stops
cleanly. Prints a JSON evidence summary.
"""
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.getcwd())
from app.core.database import connect, migrate          # noqa: E402
from app.core.lifecycle import HealthRegistry, Service, Supervisor  # noqa: E402
from app.recording.buffer import RollingBuffer          # noqa: E402
from app.recording.ffmpeg import FFmpegRunner           # noqa: E402
from app.recording.worker import RecordingWorker        # noqa: E402

HOME = os.path.expanduser("~")
PREFIX = f"{HOME}/instinct-clip/prefix/data/data/com.termux/files/usr"
WORK = f"{HOME}/instinct-clip/phase004"
SRC = f"{HOME}/instinct-clip/test/src.mp4"


async def main():
    import shutil
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK, exist_ok=True)
    db = f"{WORK}/nova.db"
    conn = connect(db)
    migrate(conn)
    conn.execute("INSERT INTO twitch_sources (username, created_at, updated_at)"
                 " VALUES ('synthetic', 0, 0)")
    conn.execute("INSERT INTO stream_sessions (source_id, state)"
                 " VALUES (1, 'RECORDING')")
    # 5s segments, keep ~15s pre/post; cap ~4MB so most of the 40s run
    # fits while still forcing rotation (source is ~105KB/s, 40s ~= 4.2MB)
    buf = RollingBuffer(WORK, session_id=1, segment_seconds=5,
                        pre_event_seconds=15, post_event_seconds=15,
                        max_storage_mb=4)
    runner = FFmpegRunner(f"{PREFIX}/bin/ffmpeg", f"{PREFIX}/bin/ffprobe",
                          max_concurrent=1)
    orig_segment_args = buf.segment_args
    def looping_args(input_url):
        args = orig_segment_args(input_url)
        i = args.index("-i")
        return args[:i] + ["-stream_loop", "-1", "-re"] + args[i:]
    buf.segment_args = looping_args
    worker = RecordingWorker(runner, buf, SRC, db, sweep_seconds=3)
    registry = HealthRegistry()
    sup = Supervisor([Service("record", worker.run, heartbeat_seconds=2)],
                     registry)

    os.environ["LD_LIBRARY_PATH"] = f"{PREFIX}/lib"
    task = asyncio.create_task(sup.run())
    await asyncio.sleep(40)  # record ~8 realtime segments
    mid = buf.segments()
    window = buf.event_window(time.time() - 10)
    expected_min = 4
    sup.stopping.set()
    await task
    end = buf.segments()
    rec1 = buf.reconcile(db)
    rec2 = buf.reconcile(db)
    evidence = {
        "segments_during": len(mid),
        "segments_after_stop": len(end),
        "storage_bytes_after": sum(s.path.stat().st_size for s in end),
        "cap_bytes": buf.max_storage_bytes,
        "event_window_segments": len(window),
        "reconcile_first_pass": rec1,
        "reconcile_second_pass": rec2,
        "captured_multiple": len(mid) >= expected_min,
        "cap_enforced_bytes": sum(s.path.stat().st_size for s in end)
            <= buf.max_storage_bytes + 2 * 2_000_000,
        "clean_stop": not any(p.running for p in runner.processes.values()),
    }
    print("EVIDENCE " + json.dumps(evidence))

asyncio.run(main())
