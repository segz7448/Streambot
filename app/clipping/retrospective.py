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
import math
from dataclasses import replace
import time
from pathlib import Path
from typing import Callable

from ..core.database import connect, migrate
from ..core.logging import get_logger
from ..detection import audio, visual
from ..editing.editor import edit_clip
from ..editing.presets import PRESETS
from ..intelligence.planner import plan_clip
from ..intelligence.providers import HeuristicProvider
from ..qc.verify import check_media, probe


def _default_score(duration_seconds, spikes, scenes):
    """Heuristic scoring parity with the live path (clipping/auto.py):
    the same HeuristicProvider fusion - a scene change alone scores
    0.5*score and can only boost an audio spike at the SAME timestamp,
    never trigger alone. This is heuristic parity ONLY: a configured
    remote/paid provider is deliberately never wired in here."""
    return HeuristicProvider().score_moments(duration_seconds, spikes,
                                             scenes)


def _validated_moments(moments, duration_seconds):
    """score_fn output contract: keep only moments with a finite score
    in [0, 1] and a finite timestamp inside [0, duration]; re-sorted by
    score descending. A non-finite or non-positive duration rejects the
    whole batch rather than silently widening the accepted range."""
    if not (isinstance(duration_seconds, (int, float))
            and not isinstance(duration_seconds, bool)
            and math.isfinite(duration_seconds) and duration_seconds > 0):
        return []
    out = []
    for m in moments:
        score = getattr(m, "score", None)
        ts = getattr(m, "timestamp", None)
        if (not isinstance(score, (int, float))
                or isinstance(score, bool)
                or not math.isfinite(score)
                or not (0.0 <= score <= 1.0)):
            continue
        if (not isinstance(ts, (int, float))
                or isinstance(ts, bool)
                or not math.isfinite(ts)
                or not (0.0 <= ts <= duration_seconds)):
            continue
        out.append(m)
    return sorted(out, key=lambda m: m.score, reverse=True)

log = get_logger("clipping.retrospective")

PRE_ROLL_SECONDS = 10.0
MIN_CLIP_SECONDS = 5.0
BASELINE_RETURN_LUFS = 2.0
MAX_INPUT_SECONDS = 3600.0
MAX_INPUT_WIDTH = 3840
MAX_INPUT_HEIGHT = 2160


class ValidationError(Exception):
    """Input recording failed fail-closed validation."""


def validate_recording(ffprobe: str, src: str,
                       max_seconds: float = MAX_INPUT_SECONDS) -> dict:
    """Fail-closed input validation BEFORE any analysis: the file must
    probe cleanly (network protocols disabled in the probe), contain a
    video stream within resolution bounds, have a known bounded
    duration, and not be a playlist format (hls/dash/concat containers
    could reference network or local resources). Raises
    ValidationError on any failure."""
    try:
        meta = probe(ffprobe, src)
    except Exception as exc:
        raise ValidationError(f"not a readable media file: {exc}")
    fmt = str(meta.get("format", {}).get("format_name", "")).lower()
    if any(bad in fmt for bad in ("hls", "dash", "m3u", "concat")):
        raise ValidationError(f"playlist container {fmt!r} not accepted")
    streams = meta.get("streams", [])
    video = next((s for s in streams
                  if s.get("codec_type") == "video"), None)
    if video is None:
        raise ValidationError("no video stream")
    width = int(video.get("width", 0) or 0)
    height = int(video.get("height", 0) or 0)
    if width <= 0 or height <= 0:
        raise ValidationError("video stream has no dimensions")
    if width > MAX_INPUT_WIDTH or height > MAX_INPUT_HEIGHT:
        raise ValidationError(
            f"resolution {width}x{height} above "
            f"{MAX_INPUT_WIDTH}x{MAX_INPUT_HEIGHT}")
    try:
        duration = float(meta.get("format", {}).get("duration", 0) or 0)
    except (TypeError, ValueError):
        duration = 0.0
    if not math.isfinite(duration) or duration <= 0:
        raise ValidationError("duration unknown or not finite")
    if duration > max_seconds:
        raise ValidationError(
            f"duration {duration:.0f}s above {max_seconds:.0f}s limit")
    return {"duration": duration, "width": width, "height": height}


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
        [ffmpeg, "-y", "-protocol_whitelist", "file",
         "-ss", f"{start:.3f}", "-i", src, "-t",
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
                      validate_fn: Callable = validate_recording,
                      runtime_dir: str = ".nova-runtime",
                      final_probe_fn: Callable = probe,
                      min_score: float = 0.6,
                      score_fn: Callable = _default_score) -> dict:
    """Run detect -> size -> plan -> extract -> edit -> QC over a local
    recording. Returns an evidence dict; never fabricates a clip: when
    no candidate is detected the result says so and nothing is
    extracted."""
    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    # fail closed: validation runs even when the caller claims to have
    # validated (defense in depth; the probe disables network protocols)
    media_info = validate_fn(ffprobe, src_path)
    result: dict = {"source": source_label, "src": src_path}
    try:
        validated_seconds = float(
            media_info.get("duration", 0.0) or 0.0)
    except (TypeError, ValueError):
        raise ValidationError(
            "validate_fn returned a non-numeric duration: %r"
            % (media_info.get("duration"),))
    if not math.isfinite(validated_seconds) or validated_seconds <= 0:
        # reject: a NaN/inf/non-positive duration must not be normalized
        # to 0 (which used to disable timestamp bounds below)
        raise ValidationError(
            "validate_fn returned a non-finite or non-positive "
            "duration: %r" % (media_info.get("duration"),))

    try:
        samples = audio_fn(ffmpeg, src_path)
    except Exception as exc:
        # same best-effort contract as scene analysis below: a failed
        # decode (now rc-checked in the real analyzer) means no audio
        # candidates for this pass, never a crashed ingest
        log.warning("audio analysis failed for %s: %s", src_path, exc)
        samples = []
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

    # Shared score/gate contract with the live path: the SAME provider
    # fusion and the SAME min_score gate decide here. Pre-fix, retro
    # picked the raw max-conf candidate with no gate, so a standalone
    # scene_change (raw 0.825) was extracted while the live path
    # rejected the identical event at its fused 0.41.
    try:
        scored = _validated_moments(
            score_fn(validated_seconds, spikes, scenes),
            validated_seconds)
    except Exception as exc:
        # explicit no-score outcome; NEVER a fallback to raw-confidence
        # picking (that is the pre-fix behavior this gate replaces)
        result["status"] = "scoring_failed"
        result["detail"] = ("candidate scoring failed (%s) - no clip "
                            "extracted; no fallback scoring"
                            % type(exc).__name__)
        return result
    if not scored or scored[0].score < min_score:
        result["status"] = "below_score_gate"
        result["detail"] = ("best fused score %s below min_score %s - "
                            "no clip extracted (same gate as live path)"
                            % (round(scored[0].score, 3) if scored else
                               "none", min_score))
        result["best_score"] = (round(scored[0].score, 3)
                                if scored else None)
        return result
    best_moment = scored[0]
    best = {"kind": ("audio_spike" if "audio_spike" in best_moment.reasons
                     else "scene_change"),
            "t": best_moment.timestamp, "conf": best_moment.score}
    result["event_kind"] = best["kind"]
    result["event_confidence"] = round(best["conf"], 3)
    result["event_reasons"] = list(best_moment.reasons)
    if best["kind"] == "audio_spike":
        start, window = content_window(samples, best["t"])
    else:
        start, window = max(0.0, best["t"] - PRE_ROLL_SECONDS), 0.0
    result["content_window_seconds"] = round(window, 1)

    plan = plan_clip(best["kind"], source_label, memory,
                     content_seconds=window)
    result["plan"] = {"target_seconds": plan.target_seconds,
                      "preset": plan.preset, "framing": plan.framing,
                      "title": plan.title,
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

    # unique, never-reused paths: fixed names let two concurrent manual
    # runs clobber each other's raw/edited files. Retrospective rows are
    # inserted HELD (never PENDING), so they are not recovery-eligible
    # and need no seal evidence; the uniqueness fix is for the producers
    # themselves.
    from .seal import reserve_output
    # per-attempt exclusive reservation (O_CREAT|O_EXCL) before any
    # writer is spawned: a unique name alone is not ownership
    raw = str(reserve_output(work, "clip-raw", ".mp4"))
    extract_fn(ffmpeg, src_path, raw, start, duration)
    result["extracted"] = Path(raw).exists()

    preset = replace(PRESETS.get(plan.preset, PRESETS["vertical"]),
                     max_duration_seconds=max(1, min(180, plan.target_seconds)))
    edited = str(reserve_output(work, "clip-edited", ".mp4"))
    edit_fn(ffmpeg, raw, edited, preset, None, 0.5, runtime_dir,
            watermark=plan.watermark or None, framing=plan.framing)
    result["edited"] = Path(edited).exists()

    qc = qc_fn(ffprobe, ffmpeg, edited, 1.0, 100, 100, True)
    result["qc_passed"] = qc.ok
    if not qc.ok:
        result["status"] = "qc_failed"
        result["detail"] = "; ".join(getattr(qc, "failures", []) or [])
        return result

    # durability before the durable HELD row: fsync the final media
    # and its directory so HELD never names a file a crash could lose
    from .seal import fsync_tree
    fsync_tree(edited)

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
    # Stored pipeline metadata the admission flow reads: the admitted
    # title names the detected kind, the registered source label and
    # the duration (a synthetic fixture's label appears verbatim),
    # never a job-payload claim.
    # The duration in the title is the FINAL measured duration of the
    # edited file (ffprobe), never the requested/planned window: the
    # edit step can change length. If the measurement fails the
    # duration is omitted from the title rather than guessed.
    measured = None
    try:
        measured = float(final_probe_fn(ffprobe, edited).get(
            "format", {}).get("duration", 0) or 0)
    except Exception:
        measured = None
    if measured is not None and measured > 0:
        duration = measured
        admitted_title = (f"{best['kind']} clip from {source_label} "
                          f"({duration:.0f}s)")
    else:
        admitted_title = f"{best['kind']} clip from {source_label}"
    # Atomic HELD-to-admission outbox, the same durable path the
    # session pipeline uses: the candidate event (with its stored
    # title), the HELD clip row and the admission job (kind
    # 'clip_intake_admit') commit in ONE explicit transaction (this
    # connection is autocommit otherwise). A failed outbox write rolls
    # the finish back instead of silently stranding the clip. The
    # handoff consumer performs the real ClipIntakeAuthority policy
    # verdict and only an ACCEPTED admission writes the durable
    # handoff job, so a retrospective clip reaches upload eligibility
    # exclusively through a genuine admission - never through a fake
    # registry or job insertion.
    from ..bot.handoff import ADMIT_JOB_KIND
    from ..core.jobs import JobQueue
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "INSERT INTO candidate_events (session_id, detected_at,"
            " kind, confidence, status, metadata_json) VALUES"
            " (?,?,?,?,?,?)",
            (session_id, now - duration, best["kind"], best["conf"],
             "CLIPPED", json.dumps({"retrospective": True,
                                    "offset_seconds": best["t"],
                                    "title": admitted_title})))
        event_id = cur.lastrowid
        cur = conn.execute(
            "INSERT INTO clips (event_id, session_id, file_path,"
            " preset, duration_seconds, status, qc_report_json,"
            " created_at) VALUES (?,?,?,?,?,?,?,?)",
            (event_id, session_id, edited, preset.name, duration,
             "HELD", json.dumps({"qc_passed": True}), now))
        clip_row_id = int(cur.lastrowid)
        JobQueue(conn).enqueue(ADMIT_JOB_KIND,
                               {"clip_row_id": clip_row_id})
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    result["clip_id"] = clip_row_id

    memory.remember(
        "clip", f"retrospective:{clip_row_id}",
        f"{preset.name} clip from {source_label} "
        f"({best['kind']})", "mode C pipeline", confidence="observed")
    result["status"] = "held"
    result["detail"] = ("clip passed QC and is recorded as HELD; "
                        "delivery follows the owner's auto_deliver "
                        "preference (/deliveries)")
    return result
