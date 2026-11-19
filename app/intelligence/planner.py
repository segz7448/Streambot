"""Publishing planner: turns a candidate event + memory into a clip
plan with verified platform rules separated from creative preferences.

VERIFIED YOUTUBE POLICY (support.google.com/youtube/answer/15424877 and
answer/10059070, fetched 2026-11-19):
- Square-or-vertical videos up to 3 minutes are categorized as Shorts
  (uploads after 2024-10-15; artist/music channels after 2025-12-08).
- Title maximum: 100 characters (answer/10059070, Shorts upload flow).
- A Short over 1 minute with ANY active copyright claim is blocked
  globally; Shorts Audio Library songs mostly usable up to 90s in a
  3-minute Short (answer/15424877).
SCOPE NOTE on resolution: "maximum resolution of 1080p" appears only in
the in-app Shorts creation flow (answer/10059070); the desktop upload
page (answer/12779649) states no resolution cap, and no API-upload
resolution cap is verified - none is enforced here.
RIGHTS NOTE: clips keep the original game audio (owner scope). A shorter
duration does NOT clear copyright or confer rights; the >1-minute
claim-block rule is a platform consequence to surface, and the audio
rights question stays with the owner. Default duration is the full
Shorts cap (3 minutes, the owner's stated target); editorial shortening
is an owner preference, not a default.

Everything the planner emits is a PROPOSAL the pipeline or owner can
override; creative choices (title template, tags, watermark text and
position) are preferences stored in Memory, clearly separate from the
policy constraints above. The planner reasons over stored rules and
event metadata; it is not an AI model and claims no understanding.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# verified policy constants (sources in module docstring)
SHORTS_MAX_SECONDS = 180
TITLE_MAX_CHARS = 100
EDITORIAL_DEFAULT_SECONDS = 30  # ours, not policy: used only when no
                                # content window or owner preference

DEFAULT_TAGS = ["gaming", "gameplay", "twitch", "clip"]


@dataclass
class ClipPlan:
    target_seconds: int
    preset: str
    title: str
    tags: list[str] = field(default_factory=list)
    watermark: str = ""
    reasons: list[str] = field(default_factory=list)


def plan_clip(event_kind: str, source_login: str, memory,
              game: str = "", content_seconds: float = 0.0) -> ClipPlan:
    """Build a clip plan from the event and stored memory."""
    reasons: list[str] = []
    # duration: policy cap, derived claim-safe default, owner override wins
    # 180s is a MAXIMUM, never a target: size from the content window,
    # owner preference overrides, never pad a short moment
    pref = memory.get("preference", "shorts_target_seconds")
    if pref and pref["confidence"] == "stated":
        target = int(pref["value"])
        reasons.append(f"duration {target}s from owner preference")
    elif content_seconds > 0:
        target = int(content_seconds)
        reasons.append(f"duration {target}s sized to the detected "
                       "content window; not padded")
    else:
        target = EDITORIAL_DEFAULT_SECONDS
        reasons.append(f"duration {EDITORIAL_DEFAULT_SECONDS}s editorial "
                       "default (no content window known); owner "
                       "preference overrides")
    target = min(target, SHORTS_MAX_SECONDS)
    if target == SHORTS_MAX_SECONDS:
        reasons.append("capped at 180s Shorts maximum (policy: "
                       "support.google.com/youtube/answer/15424877)")
    if target > 60:
        reasons.append("risk: a Short over 60s with ANY active copyright "
                       "claim is blocked globally (same source); original "
                       "game audio rights stay with the owner - shorter "
                       "does not clear rights")

    preset_row = memory.get("preference", "quality")
    preset = preset_row["value"] if preset_row else "vertical"
    if preset not in ("vertical", "square", "horizontal"):
        preset = "vertical"
    if preset == "horizontal":
        reasons.append("horizontal preset: NOT a Short (Shorts require "
                       "square or vertical) - regular upload")
    else:
        reasons.append(f"{preset} aspect: Shorts-eligible (policy)")

    # title: 100-char policy cap; template is a creative preference
    template_row = memory.get("preference", "title_template")
    template = (template_row["value"] if template_row
                else "{game_or_source} moment #{n}")
    title = template.replace("{game_or_source}", game or source_login)
    title = title.replace("{n}", "1")[:TITLE_MAX_CHARS]
    reasons.append("title capped at 100 chars (policy)")

    tags_row = memory.get("preference", "tags")
    tags = (tags_row["value"].split(",") if tags_row
            else list(DEFAULT_TAGS))
    if game:
        tags.insert(0, game.lower().replace(" ", ""))
    reasons.append("tags are creative preference, not platform policy")

    wm_row = memory.get("preference", "watermark")
    watermark = wm_row["value"] if wm_row else ""
    if watermark:
        reasons.append("watermark from owner preference")

    return ClipPlan(target_seconds=target, preset=preset, title=title,
                    tags=[t.strip() for t in tags if t.strip()],
                    watermark=watermark, reasons=reasons)
