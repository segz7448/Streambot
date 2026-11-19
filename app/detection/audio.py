"""Audio energy analysis via ffmpeg ebur128 (heuristic, not AI).

Parses momentary loudness samples from ffmpeg's ebur128 output. A loudness
spike relative to a rolling baseline is a cheap *candidate* signal only;
Phase 009 does the actual classification.
"""
from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass

_MOMENTARY = re.compile(r"t:\s*([0-9.]+)\s+.*?M:\s*(-?[0-9.]+)")


@dataclass
class LoudnessSample:
    t: float
    momentary: float


def loudness_samples(ffmpeg: str, path: str) -> list[LoudnessSample]:
    """Return momentary loudness (LUFS) samples for a media file."""
    proc = subprocess.run(
        [ffmpeg, "-nostats", "-i", path, "-filter_complex", "ebur128",
         "-f", "null", "-"],
        capture_output=True, text=True, timeout=120)
    samples: list[LoudnessSample] = []
    for line in proc.stderr.splitlines():
        m = _MOMENTARY.search(line)
        if m:
            samples.append(LoudnessSample(float(m.group(1)), float(m.group(2))))
    return samples


def loudness_spikes(samples: list[LoudnessSample], baseline: float | None,
                    min_rise_lufs: float = 6.0,
                    floor_lufs: float = -60.0) -> list[tuple[float, float]]:
    """Timestamps where momentary loudness jumps >= min_rise_lufs over the
    recent baseline and is above the silence floor. Returns (t, lufs)."""
    spikes: list[tuple[float, float]] = []
    window: list[float] = []
    for s in samples:
        if s.momentary <= floor_lufs:
            continue
        base = baseline if baseline is not None else (
            sum(window) / len(window) if window else None)
        if base is not None and s.momentary - base >= min_rise_lufs:
            if not spikes or s.t - spikes[-1][0] >= 2.0:
                spikes.append((s.t, s.momentary))
        window.append(s.momentary)
        del window[:-50]
    return spikes
