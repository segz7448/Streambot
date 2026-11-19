"""Brief-specified command handlers (Phase: Telegram control surface).

All functions are pure-stdlib and operate on the state dir; the service
registers one thin delegate per command. Handlers FAIL LOUDLY when the
underlying capability is not configured, and never pretend work started.
"""
from __future__ import annotations

import os
import re
import sqlite3
import time
from pathlib import Path

from ..editing.presets import PRESETS

_USERNAME = re.compile(r"^[A-Za-z0-9_]{3,25}$")


def _db(state_dir: Path) -> sqlite3.Connection:
    return sqlite3.connect(str(state_dir / "nova.db"))


def addsource_reply(state_dir: Path, username: str) -> str:
    username = username.strip().lstrip("@").lower()
    if not _USERNAME.match(username):
        return "Usage: /addsource <twitch-username>"
    now = time.time()
    with _db(state_dir) as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO twitch_sources"
            " (username, created_at, updated_at) VALUES (?,?,?)",
            (username, now, now))
    if cur.rowcount:
        from ..intelligence.memory import Memory
        mem = Memory(str(state_dir / "memory.json"))
        mem.remember("channel", f"twitch:{username}",
                     "registered via /addsource",
                     "owner telegram command")
        return (f"Source {username} registered. A username is a channel "
                "identifier, not video authorization: clipping starts "
                "only when an authorized input is available "
                "(broadcaster-provided feed, official clip route, or a "
                "local recording).")
    return f"Source {username} is already registered."


def removesource_reply(state_dir: Path, username: str) -> str:
    username = username.strip().lstrip("@").lower()
    with _db(state_dir) as conn:
        cur = conn.execute("DELETE FROM twitch_sources WHERE username=?",
                           (username,))
    return (f"Source {username} removed." if cur.rowcount
            else f"No source named {username}.")


def _twitch_configured(state_dir: Path) -> bool:
    return ((state_dir / "twitch_user_token").exists()
            and (state_dir / "twitch_client_id").exists())


def checklive_reply(state_dir: Path, username: str) -> str:
    username = username.strip().lstrip("@").lower()
    if not username:
        return "Usage: /checklive <twitch-username>"
    if not _twitch_configured(state_dir):
        return ("Live checks are not configured: they need a Twitch "
                "developer application and OAuth consent, which are not "
                "in place.")
    from ..ingestion.twitch_clips import ClipFailed, TwitchClipsClient
    client = TwitchClipsClient(
        (state_dir / "twitch_client_id").read_text().strip(),
        (state_dir / "twitch_user_token").read_text().strip())
    try:
        data = client._request("GET", f"/streams?user_login={username}",
                               client.app_token)
    except ClipFailed as exc:
        return f"Live check failed: {exc}"
    live = data.get("data") or []
    if live:
        s = live[0]
        return (f"{username} is LIVE: {s.get('title', '(no title)')} "
                f"[{s.get('game_name', '?')}], "
                f"{s.get('viewer_count', '?')} viewers.")
    return f"{username} is offline."


def startclip_reply(state_dir: Path, username: str) -> str:
    """Per the brief: if no permitted live-video input is available,
    explain the missing requirement and keep the source registered
    without pretending recording or clipping has started."""
    username = username.strip().lstrip("@").lower()
    if not username:
        return "Usage: /startclip <twitch-username>"
    with _db(state_dir) as conn:
        row = conn.execute(
            "SELECT id FROM twitch_sources WHERE username=?",
            (username,)).fetchone()
    if row is None:
        return f"{username} is not registered. Use /addsource first."
    feed = state_dir / "feeds" / f"{username}.url"
    if feed.exists():
        return ("A broadcaster-provided feed is registered for "
                f"{username}; continuous capture from it is not yet "
                "implemented in this build.")
    if _twitch_configured(state_dir):
        return ("No authorized continuous input for arbitrary channels: "
                "the official route is trigger-based clipping (own or "
                "editor channels). Use /clipnow while the channel is "
                "live. Continuous capture has NOT started.")
    return ("Clipping cannot start: no authorized video input is "
            "available. Missing requirements: a Twitch developer app + "
            "your OAuth consent (official clip route), or a "
            "broadcaster-provided feed, or a local recording to process. "
            f"The source {username} stays registered; nothing is "
            "recording.")


def stopclip_reply(state_dir: Path, _args: str) -> str:
    with _db(state_dir) as conn:
        rows = conn.execute(
            "SELECT id FROM stream_sessions WHERE state='ACTIVE'").fetchall()
        if rows:
            conn.execute(
                "UPDATE stream_sessions SET state='ENDED', ended_at=?"
                " WHERE state='ACTIVE'", (time.time(),))
    if rows:
        return f"Stopped {len(rows)} session(s)."
    return "No clipping session is active."


def sessions_reply(state_dir: Path, _args: str) -> str:
    with _db(state_dir) as conn:
        rows = conn.execute(
            "SELECT s.id, t.username, s.state, s.started_at"
            " FROM stream_sessions s JOIN twitch_sources t"
            " ON t.id = s.source_id ORDER BY s.id DESC LIMIT 10").fetchall()
    if not rows:
        return "No sessions recorded."
    lines = []
    for sid, user, state, started in rows:
        when = (time.strftime("%m-%d %H:%M", time.localtime(started))
                if started else "-")
        lines.append(f"#{sid} {user} {state} started {when}")
    return "Sessions:\n" + "\n".join(lines)


def clips_reply(state_dir: Path, _args: str) -> str:
    with _db(state_dir) as conn:
        rows = conn.execute(
            "SELECT id, status, preset, duration_seconds FROM clips"
            " ORDER BY id DESC LIMIT 10").fetchall()
    if not rows:
        return "No clips generated yet."
    return "Clips:\n" + "\n".join(
        f"#{i} {s} {p or '-'} {d or 0:.0f}s" for i, s, p, d in rows)


def clipdetails_reply(state_dir: Path, args: str) -> str:
    try:
        clip_id = int(args.strip())
    except ValueError:
        return "Usage: /clipdetails <clip-id>"
    with _db(state_dir) as conn:
        row = conn.execute(
            "SELECT id, status, preset, duration_seconds, file_path,"
            " created_at, delivered_at FROM clips WHERE id=?",
            (clip_id,)).fetchone()
    if row is None:
        return f"No clip #{clip_id}."
    i, status, preset, dur, path, created, delivered = row
    return (f"Clip #{i}: {status}\npreset: {preset or '-'}\n"
            f"duration: {dur or 0:.0f}s\nfile: {path or '-'}\n"
            f"created: {time.strftime('%m-%d %H:%M', time.localtime(created))}\n"
            f"delivered: {'yes' if delivered else 'no'}")


def settings_reply(state_dir: Path, args: str) -> str:
    parts = args.split(None, 1)
    with _db(state_dir) as conn:
        if len(parts) == 2:
            key = parts[0].strip()
            if not re.match(r"^[a-z_]{2,32}$", key):
                return "Setting keys: lowercase letters/underscores."
            conn.execute(
                "INSERT INTO settings(key,value) VALUES(?,?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, parts[1].strip()))
            return f"Setting {key} saved."
        rows = conn.execute("SELECT key, value FROM settings ORDER BY key"
                            ).fetchall()
    if not rows:
        return "No settings stored. Usage: /settings <key> <value>"
    return "Settings:\n" + "\n".join(f"{k} = {v}" for k, v in rows)


def quality_reply(state_dir: Path, args: str) -> str:
    arg = args.strip().lower()
    with _db(state_dir) as conn:
        if arg:
            if arg not in PRESETS:
                return ("Unknown preset. Available: "
                        + ", ".join(sorted(PRESETS)))
            conn.execute(
                "INSERT INTO settings(key,value) VALUES('quality',?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (arg,))
            p = PRESETS[arg]
            return (f"Quality preset set to {arg} "
                    f"({p.width}x{p.height}, crf {p.video_crf}, "
                    f"audio {p.audio_bitrate}).")
        row = conn.execute(
            "SELECT value FROM settings WHERE key='quality'").fetchone()
    current = row[0] if row else "vertical (default)"
    if arg:
        from ..intelligence.memory import Memory
        Memory(str(state_dir / "memory.json")).remember(
            "preference", "quality", arg, "owner via /quality",
            confidence="stated")
    return (f"Current quality: {current}. Available: "
            + ", ".join(sorted(PRESETS)) + ". Usage: /quality <preset>")


SUBTITLES_REPLY = ("Speech subtitles are out of scope per your decision: "
                   "clips are gameplay action with the original game "
                   "audio. There is nothing to enable or disable.")


def logs_reply(state_dir: Path, _args: str) -> str:
    log = state_dir / "errors.log"
    if not log.exists():
        return "No errors recorded."
    lines = log.read_text().strip().splitlines()[-15:]
    if not lines:
        return "No errors recorded."
    return "Recent errors:\n" + "\n".join(lines)


RESTART_REPLY = ("Restart is host-controlled on AGT: workers cannot "
                 "safely restart themselves from inside the deployment. "
                 "Request logged; the operator redeploys to restart.")


def record_error(state_dir: Path, text: str) -> None:
    """Append one line to the 0600 error ring that /logs reads.
    Atomic + locked like every other state file: the append and the
    ring trim happen inside one locked replace."""
    from ..core.state import locked_update_text
    line = f"{time.strftime('%m-%d %H:%M:%S')} {text[:300]}"

    def apply(content):
        lines = (content.splitlines() + [line])[-200:]
        return "\n".join(lines) + "\n", None

    try:
        locked_update_text(state_dir / "errors.log", "", apply)
    except OSError:
        pass



USAGE_PUBLISH = "Usage: /publish <clip-id> [title]"

PUBLISHABLE_STATUSES = ("HELD", "DELIVERED")


def publish_reply(state_dir: Path, args: str, update_id: int = 0) -> str:
    """Upload one QC-passed clip to YouTube as PRIVATE via the Data API.

    Gates, in order:
    - replay guard: a replayed Telegram update returns the recorded
      result instead of uploading twice (tg:<update_id> journal)
    - QC gate: only HELD or DELIVERED clips publish; PENDING, raw,
      QC_FAILED or FAILED material is refused
    - path gate: the clip file must resolve under the session clip
      root; anything outside is refused before it is opened
    - per-clip dedup: a clip already confirmed uploaded (publish:
      <clip_id> journal) is never re-uploaded by a new command
    - consent gate: without OAuth credentials nothing is attempted
    Failure honesty: pre-upload failures (auth token, resumable-session
    init) are definitive and journaled failed; once the upload PUT is
    in flight the outcome is ambiguous and is preserved for
    reconciliation, never blind-retried. privacy is hardcoded private.
    """
    from . import ops
    cached = ops.journal_result(state_dir, update_id, "publish")
    if cached is not None:
        return "Already processed (replay guard): " + cached
    parts = args.split(None, 1)
    try:
        clip_id = int(parts[0])
    except (ValueError, IndexError):
        return USAGE_PUBLISH
    title_arg = parts[1].strip() if len(parts) == 2 else ""
    with _db(state_dir) as conn:
        row = conn.execute(
            "SELECT c.file_path, c.status, c.session_id, t.username"
            " FROM clips c"
            " LEFT JOIN stream_sessions s ON c.session_id = s.id"
            " LEFT JOIN twitch_sources t ON s.source_id = t.id"
            " WHERE c.id=?", (clip_id,)).fetchone()
    if row is None:
        return f"No clip #{clip_id}."
    path, status, session_id, source = row
    if status not in PUBLISHABLE_STATUSES:
        return (f"Clip #{clip_id} is {status or 'unknown'} - only "
                "QC-passed clips (HELD or DELIVERED) can be published. "
                "Nothing was uploaded.")
    if not path:
        return f"Clip #{clip_id} has no file on disk."
    try:
        resolved = Path(path).resolve(strict=True)
    except OSError:
        return f"Clip #{clip_id} has no file on disk."
    clip_root = (state_dir / "sessions").resolve()
    if clip_root not in resolved.parents:
        record_error(state_dir,
                     f"publish refused path escape clip #{clip_id}")
        return (f"Refused: clip #{clip_id}'s file is outside the clip "
                "storage root. Nothing was uploaded.")
    prior = ops.journal_result_ns(state_dir, "publish", clip_id,
                                  "publish")
    if prior is not None:
        return (f"Clip #{clip_id} was already uploaded ({prior}). "
                "Not re-uploading.")
    from ..distribution.youtube import (YouTubeNotConfigured,
                                        YouTubeUploadFailed,
                                        publisher_from_state)
    try:
        pub = publisher_from_state(str(state_dir))
    except YouTubeNotConfigured:
        return ("YouTube upload is not configured: OAuth consent is "
                "still pending. Nothing was uploaded.")
    from ..intelligence.memory import Memory
    from ..intelligence.planner import TITLE_MAX_CHARS, plan_clip
    plan = plan_clip("manual", source or "clip",
                     Memory(str(state_dir / "memory.json")))
    title = (title_arg or plan.title)[:TITLE_MAX_CHARS]
    description = (f"Clipped from {source or 'a registered source'} by "
                   f"NOVA Clip Engine (clip #{clip_id}, session "
                   f"{session_id or '?'}).")
    try:
        vid = pub.publish(str(resolved), title,
                          description=description, privacy="private",
                          tags=plan.tags)
    except YouTubeUploadFailed as exc:
        if exc.uncertain:
            ops.journal_mark_uncertain(state_dir, update_id, "publish",
                                       f"/publish {args}")
            record_error(
                state_dir,
                f"publish clip #{clip_id} uncertain ({exc.stage}): {exc}")
            return ("Upload outcome uncertain: the request was already "
                    "in flight. Preserved for reconciliation and NOT "
                    "retried - check YouTube Studio before re-sending "
                    "/publish.")
        result = (f"YouTube upload of clip #{clip_id} failed before "
                  f"any video data was sent ({exc.stage}): {exc}")
        ops.journal_record(state_dir, update_id, "publish", result)
        record_error(state_dir, result)
        return result
    except Exception as exc:
        ops.journal_mark_uncertain(state_dir, update_id, "publish",
                                   f"/publish {args}")
        record_error(state_dir,
                     f"publish clip #{clip_id} outcome uncertain: {exc}")
        return ("Upload outcome uncertain (unexpected error). Preserved "
                "for reconciliation and NOT retried - check YouTube "
                "Studio before re-sending /publish.")
    # dedup record FIRST: a crash after the upload but before the replay
    # guard is written still blocks a second upload of this clip.
    ops.journal_record_ns(state_dir, "publish", clip_id, "publish",
                          f"yt:{vid}")
    result = (f"Clip #{clip_id} uploaded PRIVATE to YouTube "
              f"(video id {vid}). It stays private until you change it "
              "in YouTube Studio.")
    ops.journal_record(state_dir, update_id, "publish", result)
    return result
