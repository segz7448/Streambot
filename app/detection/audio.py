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
        [ffmpeg, "-nostats", "-protocol_whitelist", "file",
         "-i", path, "-filter_complex", "ebur128", "-f", "null", "-"],
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
    recent baseline and is above the silence floor. Returns (t, lufs).

    Episode merging: one sustained loud episode produces ONE spike, not
    one per rising edge. After a spike, no new spike can start until the
    signal falls back below base + min_rise_lufs/2 (or hits the silence
    floor) - so a 4s burst yields a single candidate, while two bursts
    separated by a return toward baseline stay distinct moments. The
    returned lufs tracks the episode's peak; the timestamp stays at the
    episode's first rising edge (pre-roll covers context)."""
    episode_end_lufs = min_rise_lufs / 2
    spikes: list[tuple[float, float]] = []
    window: list[float] = []
    in_episode = False
    ep_time = 0.0
    ep_peak = -float("inf")
    for s in samples:
        if s.momentary <= floor_lufs:
            in_episode = False  # silence ends the episode
            continue
        base = baseline if baseline is not None else (
            sum(window) / len(window) if window else None)
        if in_episode:
            if s.momentary > ep_peak:
                ep_peak = s.momentary
                spikes[-1] = (ep_time, ep_peak)
            if base is not None and s.momentary - base < episode_end_lufs:
                in_episode = False  # fell back toward baseline
        elif base is not None and s.momentary - base >= min_rise_lufs:
            if not spikes or s.t - spikes[-1][0] >= 2.0:
                in_episode = True
                ep_time = s.t
                ep_peak = s.momentary
                spikes.append((s.t, s.momentary))
        window.append(s.momentary)
        del window[:-50]
    return spikes
