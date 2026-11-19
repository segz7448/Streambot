"""Visual scene-change candidates via ffmpeg scene scores (heuristic)."""
from __future__ import annotations

import re
import subprocess

_SCENE = re.compile(r"pts_time:([0-9.]+).*?scene_score=([0-9.]+)")


def scene_changes(ffmpeg: str, path: str,
                  threshold: float = 0.4) -> list[tuple[float, float]]:
    """Return (t, score) for frames whose scene score exceeds threshold."""
    proc = subprocess.run(
        [ffmpeg, "-nostats", "-i", path,
         "-vf", f"select='gt(scene,{threshold})',showinfo",
         "-f", "null", "-"],
        capture_output=True, text=True, timeout=120)
    out: list[tuple[float, float]] = []
    for line in proc.stderr.splitlines():
        m = _SCENE.search(line)
        if m:
            out.append((float(m.group(1)), float(m.group(2))))
    return out
