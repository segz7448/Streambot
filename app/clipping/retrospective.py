"""Mode C: retrospective clipping of an owner-supplied recording.

The admin sends a video file to the bot; this module runs the real
pipeline over it: candidate detection (audio spikes + scene changes,
heuristics - no AI claims), content-sized windowing, extraction, edit,
QC, planning and memory. The same building blocks the live chain uses,
driven from a file instead of a rolling buffer.

Duration discipline: the clip is sized from the actual detected content
window (spike start to the return toward baseline), never padded to a
target; 180s is a hard maximum only. Owner preferences in Memory
override the derived duration.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Callable

from ..core.database import connect, migrate
from ..core.logging import get_logger
from ..detection import audio, visual
from ..editing.editor import edit_clip
from ..editing.presets import PRESETS
from ..intelligence.planner import plan_clip
from ..qc.verify import check_media, probe

log = get_logger("clipping.retrospective")

PRE_ROLL_SECONDS = 10.0
MIN_CLIP_SECONDS = 5.0
BASELINE_RETURN_LUFS = 2.0


def content_window(samples: list, spike_t: float,
                   pre_roll: float = PRE_ROLL_SECONDS) -> tuple[float, float]:
    """(start, duration) sized from the actual content: pre-roll before
    the spike, ending when momentary loudness returns within
    BASELINE_RETURN_LUFS of the pre-spike baseline (+2s tail). Never
    padded beyond the moment."""
    pre = [s.momentary for s in samples if s.t < spike_t][-50:]
    baseline = sum(pre) / len(pre) if pre else -60.0
    end = spike_t
    for s in samples:
        if s.t > spike_t and s.momentary <= baseline + BASELINE_RETURN_LUFS:
            end = s.t
            break
    else:
        end = samples[-1].t if samples else spike_t
    start = max(0.0, spike_t - pre_roll)
    duration = max(MIN_CLIP_SECONDS, end - start + 2.0)
    return start, duration


def extract_window(ffmpeg: str, src: str, out: str, start: float,
                   duration: float) -> Path:
    """Extract a window from a local recording. Re-encoded to h264/aac:
    the Twitch-representative codec pair on this ffmpeg build (mpeg4
    breaks downstream concat/edit on the device build)."""
    import subprocess
    proc = subprocess.run(
        [ffmpeg, "-y", "-ss", f"{start:.3f}", "-i", src, "-t",
         f"{duration:.3f}", "-c:v", "libx264", "-preset", "veryfast",
         "-crf", "23", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a",
         "128k", "-movflags", "+faststart", out],
        capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(f"extract failed rc={proc.returncode}: "
                           f"{proc.stderr[-300:]!r}")
    return Path(out)


def process_recording(ffmpeg: str, ffprobe: str, src_path: str,
                      work_dir: str, db_path: str, memory,
                      source_label: str = "owner-upload",
                      audio_fn: Callable = audio.loudness_samples,
                      scene_fn: Callable = visual.scene_changes,
                      extract_fn: Callable = extract_window,
                      edit_fn: Callable = edit_clip,
                      qc_fn: Callable = check_media,
                      runtime_dir: str = ".nova-runtime") -> dict:
    """Run detect -> size -> plan -> extract -> edit -> QC over a local
    recording. Returns an evidence dict; never fabricates a clip: when
    no candidate is detected the result says so and nothing is
    extracted."""
    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    result: dict = {"source": source_label, "src": src_path}

    samples = audio_fn(ffmpeg, src_path)
    spikes = audio.loudness_spikes(samples, baseline=None)
    try:
        scenes = scene_fn(ffmpeg, src_path)
    except Exception as exc:
        log.warning("scene analysis failed for %s: %s", src_path, exc)
        scenes = []
    candidates = ([{"kind": "audio_spike", "t": t,
                    "conf": min(1.0, (lufs + 60) / 60)}
                   for t, lufs in spikes]
                  + [{"kind": "scene_change", "t": t,
                      "conf": min(1.0, s)} for t, s in scenes])
    result["candidates"] = len(candidates)
    if not candidates:
        result["status"] = "no_candidates"
        result["detail"] = ("no audio spike or scene change detected - "
                            "no clip extracted (nothing fabricated)")
        return result

    best = max(candidates, key=lambda c: c["conf"])
    result["event_kind"] = best["kind"]
    result["event_confidence"] = round(best["conf"], 3)
    if best["kind"] == "audio_spike":
        start, window = content_window(samples, best["t"])
    else:
        start, window = max(0.0, best["t"] - PRE_ROLL_SECONDS), 0.0
    result["content_window_seconds"] = round(window, 1)

    plan = plan_clip(best["kind"], source_label, memory,
                     content_seconds=window)
    result["plan"] = {"target_seconds": plan.target_seconds,
                      "preset": plan.preset, "title": plan.title,
                      "tags": plan.tags, "watermark": plan.watermark,
                      "reasons": plan.reasons}

    try:
        media = probe(ffprobe, src_path)
        available = float(media.get("duration", 0.0))
    except Exception:
        available = 0.0
    duration = float(plan.target_seconds)
    if available > 0:
        duration = min(duration, max(MIN_CLIP_SECONDS, available - start))
    result["clip_duration_seconds"] = round(duration, 1)

    raw = str(work / "clip-raw.mp4")
    extract_fn(ffmpeg, src_path, raw, start, duration)
    result["extracted"] = Path(raw).exists()

    preset = PRESETS.get(plan.preset, PRESETS["vertical"])
    edited = str(work / "clip-edited.mp4")
    edit_fn(ffmpeg, raw, edited, preset, None, 0.5, runtime_dir,
            watermark=plan.watermark or None)
    result["edited"] = Path(edited).exists()

    qc = qc_fn(ffprobe, ffmpeg, edited, 1.0, 100, 100, True)
    result["qc_passed"] = qc.ok
    if not qc.ok:
        result["status"] = "qc_failed"
        result["detail"] = "; ".join(getattr(qc, "failures", []) or [])
        return result

    conn = connect(db_path)
    migrate(conn)
    now = time.time()
    conn.execute(
        "INSERT OR IGNORE INTO twitch_sources (username, created_at,"
        " updated_at) VALUES (?,?,?)", (source_label, now, now))
    source_id = conn.execute(
        "SELECT id FROM twitch_sources WHERE username=?",
        (source_label,)).fetchone()[0]
    cur = conn.execute(
        "INSERT INTO stream_sessions (source_id, state, started_at,"
        " ended_at, ingest_mode, notes) VALUES (?,?,?,?,?,?)",
        (source_id, "RETROSPECTIVE", now, now, "upload",
         "mode C: owner-supplied recording"))
    session_id = cur.lastrowid
    cur = conn.execute(
        "INSERT INTO candidate_events (session_id, detected_at, kind,"
        " confidence, status, metadata_json) VALUES (?,?,?,?,?,?)",
        (session_id, now - duration, best["kind"], best["conf"],
         "CLIPPED", json.dumps({"retrospective": True,
                                "offset_seconds": best["t"]})))
    event_id = cur.lastrowid
    cur = conn.execute(
        "INSERT INTO clips (event_id, session_id, file_path, preset,"
        " duration_seconds, status, qc_report_json, created_at)"
        " VALUES (?,?,?,?,?,?,?,?)",
        (event_id, session_id, edited, preset.name, duration,
         "HELD", json.dumps({"qc_passed": True}), now))
    conn.commit()
    conn.close()
    result["clip_id"] = cur.lastrowid

    memory.remember(
        "clip", f"retrospective:{cur.lastrowid}",
        f"{duration:.0f}s {preset.name} clip from {source_label} "
        f"({best['kind']})", "mode C pipeline", confidence="observed")
    result["status"] = "held"
    result["detail"] = ("clip passed QC and is recorded as HELD - no "
                        "overnight delivery; /deliveries flush sends it")
    return result
