"""Mode C media ingest: the admin sends a video file to the bot and the
retrospective pipeline (app.clipping.retrospective) processes it.

Bounds (fail closed, checked BEFORE any download or analysis):
- declared size must be present and <= MAX_VIDEO_BYTES
- free disk must cover the download plus working copies with margin
- documents without a video/* MIME type are refused
- downloaded payload must pass ffprobe validation (real container,
  video stream, bounded duration/resolution, no playlist formats) in
  app.clipping.retrospective.validate_recording; network protocols are
  disabled for every analysis subprocess (protocol_whitelist=file)
- uploads are retained at most RETENTION_SECONDS and bounded in total

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
import time
from pathlib import Path

MAX_VIDEO_BYTES = 500 * 1024 * 1024
FREE_MARGIN_BYTES = 200 * 1024 * 1024
WORKING_COPY_FACTOR = 3
RETENTION_SECONDS = 24 * 3600
MAX_UPLOADS_BYTES = 2 * 1024 * 1024 * 1024


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


def cleanup_uploads(uploads: Path, now: float | None = None) -> int:
    """Retention bound: drop uploads older than RETENTION_SECONDS, then
    oldest-first until the directory is under MAX_UPLOADS_BYTES."""
    now = now if now is not None else time.time()
    removed = 0
    files = [f for f in uploads.glob("*") if f.is_file()]
    for f in files:
        try:
            if now - f.stat().st_mtime > RETENTION_SECONDS:
                f.unlink()
                removed += 1
        except OSError:
            pass
    files = sorted((f for f in uploads.glob("*") if f.is_file()),
                   key=lambda f: f.stat().st_mtime)
    total = sum(f.stat().st_size for f in files)
    for f in files:
        if total <= MAX_UPLOADS_BYTES:
            break
        try:
            size = f.stat().st_size
            f.unlink()
            total -= size
            removed += 1
        except OSError:
            pass
    return removed


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
        "Clip ready (HELD - not delivered).",
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
    """Validate, download and process the admin's video. Every bound is
    checked BEFORE the download; the payload is validated by ffprobe
    BEFORE any analysis. Journaled against replay; never delivers media
    by itself."""
    from . import ops
    state_dir = Path(state_dir)
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
    if not mime.startswith("video/"):
        return "That file is not a video I can clip (no video type). " \
               "Nothing was downloaded."
    size = getattr(attachment, "file_size", None)
    if not size:
        return ("Refused: the file's size is unknown, so I cannot "
                "guarantee download bounds. Nothing was downloaded.")
    if size > MAX_VIDEO_BYTES:
        return (f"Refused: {size / 1024 / 1024:.0f} MB is too large "
                f"(limit {MAX_VIDEO_BYTES // 1024 // 1024} MB). Nothing "
                "was downloaded.")
    free = shutil.disk_usage(state_dir).free
    needed = size * WORKING_COPY_FACTOR + FREE_MARGIN_BYTES
    if free < needed:
        return ("Refused: not enough free storage on the device for "
                "this video plus its working copies. Nothing was "
                "downloaded.")
    tg_file = await bot.get_file(attachment.file_id)
    uploads = state_dir / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)
    src = uploads / f"{uid}.mp4"
    await tg_file.download_to_drive(src)
    cleanup_uploads(uploads)
    ff, fp = ffmpeg_paths()
    from ..clipping.retrospective import (ValidationError,
                                          process_recording,
                                          validate_recording)
    try:
        await asyncio.to_thread(validate_recording, fp, str(src))
    except ValidationError as exc:
        try:
            src.unlink()
        except OSError:
            pass
        text = f"That file is not a video I can clip ({exc}). Removed."
        ops.journal_record(state_dir, uid, "ingest", text)
        return text
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
