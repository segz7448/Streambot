"""Visual scene-change candidates via ffmpeg scene scores (heuristic).

Uses the metadata filter to print lavfi.scene_score per frame - the format
verified against ffmpeg 9.0.2 on the AGT device (showinfo does not print
scene scores there).
"""
from __future__ import annotations

import re
import subprocess

_FRAME = re.compile(r"pts_time:([0-9.]+)")
_SCORE = re.compile(r"lavfi\.scene_score=([0-9.]+)")


def scene_changes(ffmpeg: str, path: str,
                  threshold: float = 0.4) -> list[tuple[float, float]]:
    """Return (t, score) for frames whose scene score exceeds threshold."""
    proc = subprocess.run(
        [ffmpeg, "-nostats", "-protocol_whitelist", "file",
         "-i", path,
         "-vf", "select='gte(scene,0)',metadata=print:key=lavfi.scene_score",
         "-f", "null", "-"],
        capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        # same contract as audio.loudness_samples: a failed decode must
        # not parse as "no scene changes in valid media"
        raise RuntimeError(
            "ffmpeg visual analysis exit %d" % proc.returncode)
    out: list[tuple[float, float]] = []
    last_t: float | None = None
    for line in proc.stderr.splitlines():
        f = _FRAME.search(line)
        if f:
            last_t = float(f.group(1))
            continue
        s = _SCORE.search(line)
        if s and last_t is not None:
            score = float(s.group(1))
            if score >= threshold:
                out.append((last_t, score))
    return out
