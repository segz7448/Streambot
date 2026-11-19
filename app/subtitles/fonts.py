"""Runtime fontconfig repair for relocated ffmpeg prefixes.

The on-device ffmpeg comes from a Termux package extracted into an
isolated prefix, but its compiled-in fontconfig paths still point at the
original Termux location. fontconfig then finds no config and no fonts,
libass falls back to broken metrics (missing spaces, wrong scale), and
burned subtitles render unreadably. Device-verified failure and fix on
agt-3f177cf6 (ffmpeg 9.0.2): supplying a minimal fonts.conf that points
at the prefix's real font directory restores correct rendering.

ensure_fontconfig_env() writes that minimal config next to a caller-
supplied cache directory and returns the environment overrides to pass
to the ffmpeg subprocess. It never mutates the extracted prefix.
"""
from __future__ import annotations

import os
from pathlib import Path

_CONF_TEMPLATE = """<?xml version="1.0"?>
<fontconfig>
{dirs}
  <cachedir>{cache}</cachedir>
</fontconfig>
"""


def _font_dirs(ffmpeg_path: str) -> list[str]:
    dirs: list[str] = []
    # prefix layout: <prefix>/bin/ffmpeg -> <prefix>/share/fonts
    prefix_fonts = Path(ffmpeg_path).resolve().parent.parent / "share" / "fonts"
    if prefix_fonts.is_dir():
        dirs.append(str(prefix_fonts))
    if Path("/system/fonts").is_dir():  # Android system fonts
        dirs.append("/system/fonts")
    for extra in ("/usr/share/fonts", os.path.expanduser("~/.fonts")):
        if Path(extra).is_dir():
            dirs.append(extra)
    return dirs


def ensure_fontconfig_env(ffmpeg_path: str, cache_dir: str) -> dict[str, str]:
    """Return env overrides (FONTCONFIG_FILE) for subtitle burns.

    Returns {} when no usable font directory exists; the caller then
    runs ffmpeg unchanged and libass quality is whatever the platform
    provides. Never raises for missing directories.
    """
    dirs = _font_dirs(ffmpeg_path)
    if not dirs:
        return {}
    cache = Path(cache_dir)
    cache.mkdir(parents=True, exist_ok=True)
    conf = cache / "fonts.conf"
    body = _CONF_TEMPLATE.format(
        dirs="\n".join(f'  <dir>{d}</dir>' for d in dirs),
        cache=str(cache / "fc-cache"),
    )
    if not conf.exists() or conf.read_text() != body:
        conf.write_text(body)
    return {"FONTCONFIG_FILE": str(conf)}
