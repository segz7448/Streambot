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
    if proc.returncode != 0:
        # a failed decode yields empty/partial output that parses as
        # "silence": indistinguishable from valid quiet media without the
        # return code. Fixed nonsecret error, never stderr text.
        raise RuntimeError(
            "ffmpeg audio analysis exit %d" % proc.returncode)
    samples: list[LoudnessSample] = []
    for line in proc.stderr.splitlines():
        m = _MOMENTARY.search(line)
        if m:
            samples.append(LoudnessSample(float(m.group(1)), float(m.group(2))))
    return samples


def spikes_episode(samples: list[LoudnessSample], baseline: float | None,
                    min_rise_lufs: float, floor_lufs: float,
                    episode_active: bool,
                    silence_end_seconds: float = 1.5) -> tuple[list[tuple[float, float]], bool]:
    """Core spike scan with episode carry-in. Returns (spikes, active_at_end)
    so a caller analyzing consecutive chunks can suppress a duplicate spike
    for one moment that spans a chunk boundary.

    Brief below-floor gaps (real dropouts, or the ~0.2-0.3s cold-start
    transient ebur128 reports at the start of each new segment file) do
    not end an episode; only silence sustained >= silence_end_seconds
    does. Without this, a boundary-spanning burst re-fires right after
    each chunk's cold-start (device-verified failure mode)."""
    episode_end_lufs = min_rise_lufs / 2
    spikes: list[tuple[float, float]] = []
    window: list[float] = []
    in_episode = episode_active
    ep_time = 0.0
    ep_peak = -float("inf")
    silence_start: float | None = None
    for s in samples:
        if s.momentary <= floor_lufs:
            if in_episode:
                if silence_start is None:
                    silence_start = s.t
                elif s.t - silence_start >= silence_end_seconds:
                    in_episode = False  # sustained silence ends the episode
            continue
        silence_start = None
        base = baseline if baseline is not None else (
            sum(window) / len(window) if window else None)
        if in_episode:
            if spikes and s.momentary > ep_peak:
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
    return spikes, in_episode


def loudness_spikes(samples: list[LoudnessSample], baseline: float | None,
                    min_rise_lufs: float = 6.0,
                    floor_lufs: float = -60.0) -> list[tuple[float, float]]:
    """Timestamps where momentary loudness jumps >= min_rise_lufs over the
    recent baseline and is above the silence floor. Returns (t, lufs).

    Episode merging: one sustained loud episode produces ONE spike, not
    one per rising edge. After a spike, no new spike can start until the
    signal falls back below base + min_rise_lufs/2 (or silence sustained
    past a short tolerance) - so a 4s burst yields a single candidate,
    while two bursts separated by a return toward baseline stay distinct
    moments. The returned lufs tracks the episode's peak; the timestamp
    stays at the episode's first rising edge (pre-roll covers context)."""
    spikes, _ = spikes_episode(samples, baseline, min_rise_lufs,
                                floor_lufs, episode_active=False)
    return spikes
