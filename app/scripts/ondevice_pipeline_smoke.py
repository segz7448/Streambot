"""On-device integrated smoke: record -> detect candidates -> extract clip.

Runs entirely on AGT with stdlib only. Proves Phases 004+008+010 compose
on real ffmpeg 9.0.2 output. Prints a JSON EVIDENCE line.
"""
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.getcwd())
from app.clipping.extractor import extract_clip                 # noqa: E402
from app.core.database import connect, migrate                  # noqa: E402
from app.core.lifecycle import HealthRegistry, Service, Supervisor  # noqa: E402
from app.detection.detector import CandidateDetector            # noqa: E402
from app.recording.buffer import RollingBuffer                  # noqa: E402
from app.recording.ffmpeg import FFmpegRunner                   # noqa: E402
from app.recording.worker import RecordingWorker                # noqa: E402

HOME = os.path.expanduser("~")
PREFIX = f"{HOME}/instinct-clip/prefix/data/data/com.termux/files/usr"
WORK = f"{HOME}/instinct-clip/pipeline-smoke"
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
    conn.commit()
    conn.close()
    buf = RollingBuffer(WORK, session_id=1, segment_seconds=5,
                        pre_event_seconds=30, post_event_seconds=30,
                        max_storage_mb=64)
    runner = FFmpegRunner(f"{PREFIX}/bin/ffmpeg", f"{PREFIX}/bin/ffprobe",
                          max_concurrent=1)
    orig_args = buf.segment_args
    def looping_args(input_url):
        args = orig_args(input_url)
        i = args.index("-i")
        return args[:i] + ["-stream_loop", "-1", "-re"] + args[i:]
    buf.segment_args = looping_args
    worker = RecordingWorker(runner, buf, SRC, db, sweep_seconds=3)
    registry = HealthRegistry()
    sup = Supervisor([Service("record", worker.run, heartbeat_seconds=2)],
                     registry)
    os.environ["LD_LIBRARY_PATH"] = f"{PREFIX}/lib"
    task = asyncio.create_task(sup.run())
    await asyncio.sleep(25)  # ~5 realtime segments
    event_time = time.time() - 8
    detector = CandidateDetector(f"{PREFIX}/bin/ffmpeg", buf, db)
    candidates = await asyncio.to_thread(detector.process_new_segments)
    clip_path = f"{WORK}/clip.mp4"
    clip = await asyncio.to_thread(
        extract_clip, f"{PREFIX}/bin/ffmpeg", buf, event_time, clip_path,
        10.0, 5.0)
    sup.stopping.set()
    await asyncio.wait_for(task, timeout=20)
    clip_bytes = os.path.getsize(clip_path) if clip and os.path.exists(clip_path) else 0

    # chained stages: edit (vertical preset) -> QC -> optional owner delivery
    from app.editing.editor import edit_clip          # noqa: E402
    from app.editing.presets import PRESETS           # noqa: E402
    from app.qc.verify import check_media             # noqa: E402
    edited_path = f"{WORK}/clip-edited.mp4"
    edit_ok = qc_ok = False
    delivered_id = None
    if clip_bytes > 10000:
        edited = await asyncio.to_thread(
            edit_clip, f"{PREFIX}/bin/ffmpeg", clip_path, edited_path,
            PRESETS["vertical"], None, 0.5, f"{HOME}/instinct-clip/runtime")
        edit_ok = os.path.exists(edited_path) and os.path.getsize(edited_path) > 10000
        if edit_ok:
            qc = await asyncio.to_thread(
                check_media, f"{PREFIX}/bin/ffprobe", f"{PREFIX}/bin/ffmpeg",
                edited_path, 1.0, 100, 100, True)
            qc_ok = qc.ok
            if qc_ok and os.environ.get("NOVA_SMOKE_DELIVER") == "1":
                from app.distribution.telegram_delivery import deliver_clip_sync  # noqa: E402
                delivered_id = await asyncio.to_thread(
                    deliver_clip_sync, os.environ["NOVA_TOKEN_FILE"],
                    os.environ["NOVA_STATE_DIR"], edited_path,
                    "NOVA chained synthetic end-to-end test - not a real stream clip")
    conn = connect(db)
    kinds = [r[0] for r in conn.execute(
        "SELECT kind FROM candidate_events").fetchall()]
    conn.close()
    print("EVIDENCE " + json.dumps({
        "segments": len(buf.segments()),
        "candidates_recorded": candidates,
        "candidate_kinds": kinds,
        "clip_extracted": bool(clip and clip_bytes > 10000),
        "clip_bytes": clip_bytes,
        "edited_vertical": edit_ok,
        "qc_passed": qc_ok,
        "delivered_message_id": delivered_id,
        "clean_stop": True,
    }))


if __name__ == "__main__":
    asyncio.run(main())
