"""Mode C media ingest: the admin sends a video file to the bot and the
retrospective pipeline (app.clipping.retrospective) processes it.

Durability: the pre-ack wrapper persists admin media updates as /ingest
entries before the framework offset advances. Results are journaled by
update id, so a replayed update returns the recorded result instead of
re-running the pipeline; a stranded update with no journal record is
marked uncertain and the admin is asked to re-send (the Telegram file
download cannot be blindly re-driven from the drain).

Delivery discipline: clips are recorded HELD - no media is sent to the
owner without an explicit /deliveries flush. The text summary replies
in the admin's own chat.
"""
from __future__ import annotations

import asyncio
import os
import shutil
from pathlib import Path


def ffmpeg_paths() -> tuple[str, str]:
    """Device ffmpeg lives in the relocated termux prefix; allow env
    override and fall back to PATH for off-device tests."""
    home = Path.home()
    prefix = home / "instinct-clip/prefix/data/data/com.termux/files/usr/bin"
    ff = os.environ.get("NOVA_FFMPEG") or (
        str(prefix / "ffmpeg") if (prefix / "ffmpeg").exists()
        else shutil.which("ffmpeg") or "ffmpeg")
    fp = os.environ.get("NOVA_FFPROBE") or (
        str(prefix / "ffprobe") if (prefix / "ffprobe").exists()
        else shutil.which("ffprobe") or "ffprobe")
    return ff, fp


def _summarize(result: dict) -> str:
    status = result.get("status")
    if status == "no_candidates":
        return ("Processed your video: no gameplay moment detected "
                "(no audio spike or scene change). No clip was created - "
                "I won't fabricate one. Try a segment with action.")
    if status == "qc_failed":
        return (f"Processed your video: a moment was found "
                f"({result.get('event_kind')}) but the edited clip failed "
                f"quality checks ({result.get('detail', 'unknown')}). "
                "Nothing was delivered.")
    plan = result.get("plan", {})
    reasons = plan.get("reasons", [])
    lines = [
        f"Clip ready (HELD - not delivered).",
        f"moment: {result.get('event_kind')} "
        f"(confidence {result.get('event_confidence')})",
        f"duration: {result.get('clip_duration_seconds')}s "
        f"{plan.get('preset', 'vertical')}",
        f"title proposal: \"{plan.get('title', '')}\"",
        f"tags: {', '.join(plan.get('tags', []))}",
    ]
    if plan.get("watermark"):
        lines.append(f"watermark: {plan['watermark']}")
    if reasons:
        lines.append("why: " + reasons[0])
    lines.append("/deliveries flush sends it to you.")
    return "\n".join(lines)


async def ingest_media(update, bot, state_dir: Path) -> str:
    """Download the admin's video and run the retrospective pipeline.
    Journaled against replay; never delivers media by itself."""
    from . import ops
    uid = int(update.update_id)
    cached = ops.journal_result(state_dir, uid, "ingest")
    if cached is not None:
        return "Already processed (replay guard): " + cached
    message = update.message
    attachment = getattr(message, "video", None) or getattr(
        message, "document", None)
    if attachment is None:
        return "Send a video file and I'll find the moment in it."
    mime = getattr(attachment, "mime_type", "") or ""
    if mime and not mime.startswith("video/"):
        return "That file is not a video; send a recording to clip."
    tg_file = await bot.get_file(attachment.file_id)
    uploads = state_dir / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)
    src = uploads / f"{uid}.mp4"
    await tg_file.download_to_drive(src)
    ff, fp = ffmpeg_paths()
    from ..clipping.retrospective import process_recording
    from ..intelligence.memory import Memory
    result = await asyncio.to_thread(
        process_recording, ff, fp, str(src),
        str(state_dir / "retrospective" / str(uid)),
        str(state_dir / "nova.db"),
        Memory(str(state_dir / "memory.json")),
        f"owner-upload:{uid}", audio_fn=_audio, scene_fn=_scene,
        runtime_dir=str(state_dir / "runtime"))
    text = _summarize(result)
    ops.journal_record(state_dir, uid, "ingest", text)
    return text


# late bindings so tests can inject fakes via module attributes
from ..detection import audio as _audio_mod  # noqa: E402
from ..detection import visual as _visual_mod  # noqa: E402
_audio = _audio_mod.loudness_samples
_scene = _visual_mod.scene_changes
