"""Publishing planner: turns a candidate event + memory into a clip
plan with verified platform rules separated from creative preferences.

VERIFIED YOUTUBE POLICY (support.google.com/youtube/answer/15424877 and
answer/10059070, fetched 2026-11-19):
- Square-or-vertical videos up to 3 minutes are categorized as Shorts
  (uploads after 2024-10-15; artist/music channels after 2025-12-08).
- Title maximum: 100 characters.
- Shorts upload maximum resolution: 1080p.
- A Short over 1 minute with ANY active copyright claim is blocked
  globally; Shorts Audio Library songs mostly usable up to 90s in a
  3-minute Short.
DERIVED PLANNING RULE (ours, not YouTube's): gameplay clips keep the
original game audio (owner scope), and game audio can draw Content ID
claims; targeting <=60s keeps a claim from blocking the Short, so the
default target is 60s unless the owner overrides.

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
CLAIM_SAFE_MAX_SECONDS = 60  # derived rule, not YouTube policy

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
              game: str = "") -> ClipPlan:
    """Build a clip plan from the event and stored memory."""
    reasons: list[str] = []
    # duration: policy cap, derived claim-safe default, owner override wins
    pref = memory.get("preference", "shorts_target_seconds")
    if pref and pref["confidence"] == "stated":
        target = int(pref["value"])
        reasons.append(f"duration {target}s from owner preference")
    else:
        target = CLAIM_SAFE_MAX_SECONDS
        reasons.append("duration 60s: >60s Shorts are globally blocked "
                       "by any copyright claim (derived rule, YouTube "
                       "policy 15424877); owner can override")
    target = min(target, SHORTS_MAX_SECONDS)

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
