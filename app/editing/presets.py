"""Phase 011: output presets for aspect ratio and quality."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OutputPreset:
    name: str
    width: int
    height: int
    video_crf: int
    audio_bitrate: str
    max_duration_seconds: int = 90


VERTICAL = OutputPreset("vertical", 1080, 1920, 23, "128k")
SQUARE = OutputPreset("square", 1080, 1080, 23, "128k")
HORIZONTAL = OutputPreset("horizontal", 1920, 1080, 21, "160k")

PRESETS = {p.name: p for p in (VERTICAL, SQUARE, HORIZONTAL)}
