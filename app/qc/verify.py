"""Automated media verification via ffprobe/ffmpeg.

Checks: container parses, expected streams present, minimum duration,
resolution sanity, and a full decode pass for corruption.
"""
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field


@dataclass
class MediaInfo:
    width: int
    height: int
    duration: float
    has_video: bool
    has_audio: str | bool


@dataclass
class QCResult:
    ok: bool
    issues: list[str] = field(default_factory=list)


def probe(ffprobe: str, path: str) -> dict:
    proc = subprocess.run(
        [ffprobe, "-v", "error", "-protocol_whitelist", "file",
         "-show_format", "-show_streams", "-of", "json", path],
        capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"ffprobe rc={proc.returncode}: {proc.stderr[:200]!r}")
    return json.loads(proc.stdout)


def check_media(ffprobe: str, ffmpeg: str, path: str,
                min_duration: float = 1.0,
                min_width: int = 100, min_height: int = 100,
                require_audio: bool = True) -> QCResult:
    issues: list[str] = []
    try:
        meta = probe(ffprobe, path)
    except Exception as exc:
        return QCResult(False, [f"probe failed: {exc}"])
    streams = meta.get("streams", [])
    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    audio = next((s for s in streams if s.get("codec_type") == "audio"), None)
    if video is None:
        issues.append("no video stream")
    else:
        if int(video.get("width", 0)) < min_width:
            issues.append(f"width {video.get('width')} below {min_width}")
        if int(video.get("height", 0)) < min_height:
            issues.append(f"height {video.get('height')} below {min_height}")
    if require_audio and audio is None:
        issues.append("no audio stream")
    try:
        duration = float(meta.get("format", {}).get("duration", 0))
    except (TypeError, ValueError):
        duration = 0.0
    if duration < min_duration:
        issues.append(f"duration {duration:.2f}s below {min_duration}s")
    decode = subprocess.run(
        [ffmpeg, "-v", "error", "-i", path, "-f", "null", "-"],
        capture_output=True, text=True, timeout=300)
    if decode.returncode != 0 or decode.stderr.strip():
        issues.append(f"decode errors: {decode.stderr.strip()[:150]!r}")
    return QCResult(not issues, issues)
