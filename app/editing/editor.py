"""Phase 011: edit a source clip into platform-ready outputs.

Pure filter-graph construction separated from execution. Vertical/square
outputs center-crop then scale; subtitles burn via libass when a subtitle
file is supplied; audio is loudness-normalized. All filters use codecs and
filters verified present in the on-device ffmpeg 9.0.2 build.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

from .presets import OutputPreset


def crop_filter(preset: OutputPreset) -> str:
    """Center-crop to the preset aspect ratio, then scale to target size."""
    target_ar = preset.width / preset.height
    # crop keeps full height for portrait/square targets, full width otherwise
    return (
        f"crop=w='if(gt(a,{target_ar:.6f}),ih*{target_ar:.6f},iw)'"
        f":h='if(gt(a,{target_ar:.6f}),ih,iw/{target_ar:.6f})',"
        f"scale={preset.width}:{preset.height}"
    )


def build_filter_graph(preset: OutputPreset,
                       subtitle_path: str | None = None,
                       focus_x: float = 0.5) -> str:
    """Video filter chain. focus_x shifts the crop window (0=left, 1=right);
    centered by default. Subtitles are burned last so they render at output
    resolution."""
    target_ar = preset.width / preset.height
    x_expr = (f"'if(gt(a,{target_ar:.6f}),(iw-ih*{target_ar:.6f})*{focus_x:.3f},0)'")
    chain = (
        f"crop=w='if(gt(a,{target_ar:.6f}),ih*{target_ar:.6f},iw)'"
        f":h='if(gt(a,{target_ar:.6f}),ih,iw/{target_ar:.6f})':x={x_expr}:y=0,"
        f"scale={preset.width}:{preset.height}"
    )
    if subtitle_path:
        escaped = subtitle_path.replace(":", "\\:").replace("'", "\\'")
        chain += f",subtitles='{escaped}'"
    return chain


def edit_args(ffmpeg: str, src: str, out: str, preset: OutputPreset,
              subtitle_path: str | None = None, focus_x: float = 0.5,
              loudnorm: bool = True) -> list[str]:
    args = [ffmpeg, "-y", "-i", src,
            "-vf", build_filter_graph(preset, subtitle_path, focus_x),
            "-c:v", "libx264", "-preset", "veryfast",
            "-crf", str(preset.video_crf), "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", preset.audio_bitrate,
            "-t", str(preset.max_duration_seconds)]
    if loudnorm:
        args += ["-af", "loudnorm=I=-16:TP=-1.5:LRA=11"]
    args += ["-movflags", "+faststart", out]
    return args


def edit_clip(ffmpeg: str, src: str, out: str, preset: OutputPreset,
              subtitle_path: str | None = None, focus_x: float = 0.5) -> Path:
    proc = subprocess.run(
        edit_args(ffmpeg, src, out, preset, subtitle_path, focus_x),
        capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(f"edit failed rc={proc.returncode}: "
                           f"{proc.stderr[-300:]!r}")
    return Path(out)
