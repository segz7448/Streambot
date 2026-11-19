"""Phase 011: edit a source clip into platform-ready outputs.

Pure filter-graph construction separated from execution. Vertical/square
outputs center-crop then scale; subtitles burn via libass when a subtitle
file is supplied; audio is loudness-normalized. All filters use codecs and
filters verified present in the on-device ffmpeg 9.0.2 build.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from ..subtitles.fonts import ensure_fontconfig_env
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


def _escape_drawtext(text: str) -> str:
    """Owner-supplied watermark text for ffmpeg drawtext. Collapsed to a
    single bounded line with every filter-graph metacharacter escaped;
    the value travels as one argv element (never a shell), so this only
    needs to keep the filter graph itself intact."""
    t = " ".join(text.split())[:100]
    for ch, rep in (("\\", "\\\\"), (":", "\\:"), ("'", "\\'"),
                    ("%", "\\%"), (",", "\\,"), (";", "\\;"),
                    ("[", "\\["), ("]", "\\]")):
        t = t.replace(ch, rep)
    return t


def build_filter_graph(preset: OutputPreset,
                       subtitle_path: str | None = None,
                       focus_x: float = 0.5,
                       watermark: str | None = None) -> str:
    """Video filter chain. focus_x shifts the crop window (0=left, 1=right);
    centered by default. Subtitles are burned last so they render at output
    resolution. An owner watermark is drawn bottom-right via drawtext."""
    target_ar = preset.width / preset.height
    x_expr = (f"'if(gt(a,{target_ar:.6f}),(iw-ih*{target_ar:.6f})*{focus_x:.3f},0)'")
    chain = (
        f"crop=w='if(gt(a,{target_ar:.6f}),ih*{target_ar:.6f},iw)'"
        f":h='if(gt(a,{target_ar:.6f}),ih,iw/{target_ar:.6f})':x={x_expr}:y=0,"
        f"scale={preset.width}:{preset.height}"
    )
    if watermark:
        chain += (",drawtext=text='" + _escape_drawtext(watermark) +
                  "':x=w-text_w-24:y=h-text_h-24:fontsize=36"
                  ":fontcolor=white:borderw=2:bordercolor=black")
    if subtitle_path:
        escaped = subtitle_path.replace(":", "\\:").replace("'", "\\'")
        chain += f",subtitles='{escaped}'"
    return chain


def edit_args(ffmpeg: str, src: str, out: str, preset: OutputPreset,
              subtitle_path: str | None = None, focus_x: float = 0.5,
              loudnorm: bool = True, watermark: str | None = None) -> list[str]:
    args = [ffmpeg, "-y", "-i", src,
            "-vf", build_filter_graph(preset, subtitle_path, focus_x,
                                      watermark),
            "-c:v", "libx264", "-preset", "veryfast",
            "-crf", str(preset.video_crf), "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", preset.audio_bitrate,
            "-t", str(preset.max_duration_seconds)]
    if loudnorm:
        args += ["-af", "loudnorm=I=-16:TP=-1.5:LRA=11"]
    args += ["-movflags", "+faststart", out]
    return args


def edit_clip(ffmpeg: str, src: str, out: str, preset: OutputPreset,
              subtitle_path: str | None = None, focus_x: float = 0.5,
              runtime_dir: str = ".nova-runtime",
              watermark: str | None = None) -> Path:
    env = dict(os.environ)
    if subtitle_path or watermark:
        # drawtext and libass both resolve fonts through fontconfig;
        # relocated prefixes ship a fontconfig that points at paths
        # which no longer exist (device-verified on the owner's AGT phone)
        env.update(ensure_fontconfig_env(ffmpeg, runtime_dir))
    proc = subprocess.run(
        edit_args(ffmpeg, src, out, preset, subtitle_path, focus_x,
                  watermark=watermark),
        capture_output=True, text=True, timeout=600, env=env)
    if proc.returncode != 0:
        raise RuntimeError(f"edit failed rc={proc.returncode}: "
                           f"{proc.stderr[-300:]!r}")
    return Path(out)
