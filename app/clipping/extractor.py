"""Phase 010: clip boundary selection and extraction from the buffer.

Segments are keyframe-cut, so exact boundaries are achieved by concat +
re-encode around the event window. Pure argument construction is separated
from process execution for testability.
"""
from __future__ import annotations

import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from ..recording.buffer import RollingBuffer, Segment


@dataclass
class ClipPlan:
    segments: list[Segment]
    start_offset: float   # seconds into the concatenated window
    duration: float       # seconds of output


def plan_clip(buffer: RollingBuffer, event_time: float,
              pre_seconds: float = 20.0,
              post_seconds: float = 10.0) -> ClipPlan | None:
    """Select buffered segments covering [event-pre, event+post].

    Segment mtimes mark each segment's END. Returns None when the buffer no
    longer holds the full pre-event window (rotated away)."""
    segs = buffer.event_window(event_time)
    if not segs:
        return None
    all_segs = buffer.segments()
    by_path = {s.path: s for s in all_segs}
    window = [by_path[p] for p in segs if p in by_path]
    if not window:
        return None
    seg_dur = buffer.segment_seconds
    first = window[0]
    window_start = first.created_at - seg_dur
    wanted_start = event_time - pre_seconds
    if wanted_start < window_start - 0.5:
        return None  # pre-event footage already rotated out
    start_offset = max(0.0, wanted_start - window_start)
    window_end = window[-1].created_at
    duration = min(event_time + post_seconds, window_end) - (window_start + start_offset)
    if duration <= 0:
        return None
    return ClipPlan(window, start_offset, duration)


def extract_args(ffmpeg: str, concat_list: str, plan: ClipPlan,
                 out_path: str, preset: str = "veryfast") -> list[str]:
    return [ffmpeg, "-y", "-f", "concat", "-safe", "0", "-i", concat_list,
            "-ss", f"{plan.start_offset:.3f}", "-t", f"{plan.duration:.3f}",
            "-c:v", "libx264", "-preset", preset, "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-movflags", "+faststart", out_path]


def extract_clip(ffmpeg: str, buffer: RollingBuffer, event_time: float,
                 out_path: str, pre_seconds: float = 20.0,
                 post_seconds: float = 10.0) -> Path | None:
    plan = plan_clip(buffer, event_time, pre_seconds, post_seconds)
    if plan is None:
        return None
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
        for seg in plan.segments:
            fh.write(f"file '{seg.path}'\n")
        list_path = fh.name
    try:
        proc = subprocess.run(
            extract_args(ffmpeg, list_path, plan, out_path),
            capture_output=True, text=True, timeout=300)
    finally:
        Path(list_path).unlink(missing_ok=True)  # clean only our own temp
    if proc.returncode != 0:
        raise RuntimeError(f"clip extraction failed rc={proc.returncode}: "
                           f"{proc.stderr[-300:]!r}")
    return Path(out_path)
