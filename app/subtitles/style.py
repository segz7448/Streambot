"""Phase 011/013: subtitle document generation with safe-area-aware defaults.

libass scales fonts against the script's PlayRes; leaving it at the legacy
384x288 default makes text enormous on 1080x1920 output. PlayRes is always
set to the actual output frame and sizes derive from it.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..editing.presets import OutputPreset


@dataclass(frozen=True)
class Cue:
    start: float
    end: float
    text: str


def _ts(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int(seconds % 3600 // 60)
    s = seconds % 60
    return f"{h}:{m:02d}:{s:05.2f}"


def default_ass(preset: OutputPreset, cues: list[Cue],
                font: str = "Sans") -> str:
    """Render an ASS document sized for the actual output frame.

    Font ~3.3% of frame height, bottom margin ~6% (clear of platform UI
    safe areas), white text with a 2px outline for legibility on any
    background, bottom-centered, narrow side margins to force early wrap.
    """
    w, h = preset.width, preset.height
    fontsize = max(28, round(h * 0.033))
    margin_v = round(h * 0.06)
    margin_lr = round(w * 0.08)
    lines = [
        "[Script Info]",
        "ScriptType: v4.00+",
        f"PlayResX: {w}",
        f"PlayResY: {h}",
        "WrapStyle: 0",
        "",
        "[V4+ Styles]",
        "Format: Name, Fontname, Fontsize, PrimaryColour, OutlineColour, "
        "BackColour, Bold, BorderStyle, Outline, Shadow, Alignment, "
        "MarginL, MarginR, MarginV",
        f"Style: Default,{font},{fontsize},&H00FFFFFF,&H00000000,"
        f"&H80000000,0,1,2,0,2,{margin_lr},{margin_lr},{margin_v}",
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Text",
    ]
    for cue in cues:
        text = cue.text.replace("\n", "\\N")
        lines.append(f"Dialogue: 0,{_ts(cue.start)},{_ts(cue.end)},"
                     f"Default,{text}")
    return "\n".join(lines) + "\n"
