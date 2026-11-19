"""Pure-stdlib command logic for the Telegram service.

Kept free of telegram-framework imports so it is unit-testable off the
phone; app.bot.service handlers delegate here.
"""
from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger("bot.ops")

from ..core.state import locked_read, locked_update
from ..distribution.relay import (DestinationStore, Relayer,
                                  RestreamManager, secret_in_rtmp_url)
from .inbox import PreAckInbox
from .watermark import UpdateWatermark
from ..ingestion.twitch_clips import ClipFailed, TwitchClipsClient

UNCONFIGURED_CLIP = (
    "Manual clipping is not configured. It requires a Twitch developer "
    "application and your OAuth consent (clips:edit). File download "
    "additionally requires you to be the broadcaster or a channel "
    "editor. Until those are in place this command cannot create clips.")

USAGE_DESTINATIONS = ("Usage: /destinations "
                      "[add <chat_id> <label> | remove <chat_id>]")

USAGE_RELAY = ("Usage: /relay start <feed_url> <rtmp_dest> [rtmp_dest...] - "
               "feed must be one you own or are authorized to restream")


def relay_reply(state_dir: Path, args: str, update_id: int = 0) -> str:
    """Start an RTMP restream of an explicitly owner-provided feed.
    Secret-bearing RTMP destinations are REFUSED: stream keys must not
    appear in ffmpeg process arguments on this shared host (same-UID
    processes can read /proc cmdline), and no safe credential transport
    is implemented yet. Only keyless destinations (path is empty or
    "/") or local file outputs are accepted. Journaled against replay."""
    cached = journal_result(state_dir, update_id, "relay")
    if cached is not None:
        return "Already processed (replay guard): " + cached
    if not (state_dir / "relay_enabled").exists():
        return ("Restream is disabled pending security review of the "
                "destination filter. No processes were started.")
    parts = args.split()
    if len(parts) < 3 or parts[0] != "start":
        return USAGE_RELAY
    feed, dests = parts[1], parts[2:]
    from ..distribution.relay import validate_feed, validate_output
    feed_root = state_dir / "feeds"
    out_root = state_dir / "streams"
    out_root.mkdir(parents=True, exist_ok=True)
    try:
        feed = validate_feed(feed, feed_root)
        dests = [validate_output(d, out_root) for d in dests]
    except ValueError as exc:
        return (f"Refused: {exc}. Feeds must be credential-free http(s) "
                "or a file you placed in the feeds directory; "
                "destinations must be keyless rtmp(s) endpoints or "
                "names under the streams directory.")
    for dest in dests:
        if secret_in_rtmp_url(dest):
            return ("Refused: that RTMP destination embeds a stream key, "
                    "and keys in process arguments are exposed to other "
                    "workloads on this host. Secret-bearing restream is "
                    "disabled until a safe credential transport exists. "
                    "Your key was not used or stored.")
    relayer = Relayer(DestinationStore(str(state_dir / "destinations.json")),
                      lambda c, p, t: 0)
    cmds = relayer.restream(feed, dests,
                            ffmpeg_bin=str(
                                Path.home() / "instinct-clip/prefix/bin/ffmpeg"))
    manager = RestreamManager(str(state_dir / "restream.pids"))
    if manager.active():
        return "A restream is already running. Use /stoprelay first."
    pids = manager.start(cmds)
    result = (f"Restreaming your feed to {len(pids)} destination(s). "
              "/stoprelay stops it.")
    journal_record(state_dir, update_id, "relay", result)
    return result


def stoprelay_reply(state_dir: Path) -> str:
    stopped = RestreamManager(str(state_dir / "restream.pids")).stop()
    if stopped:
        return f"Restream stopped ({stopped} process(es))."
    return "No restream is running."


def make_pre_ack_wrapper(inbox, watermark, orig_put, admin_id_fn):
    """TRUE pre-ack durability: wraps the framework update queue's put,
    persisting each command update BEFORE it is queued. The framework
    advances its offset only after queueing, and Telegram's server-side
    ack happens on the NEXT getUpdates call - so a persisted update can
    never be acked past without an inbox entry, and the startup drain
    replays anything not marked handled.

    AUTHORIZATION AT INGEST: only the paired administrator's commands
    are persisted. A stranger's command is never written to the inbox,
    so the drain can never replay it. /pair carries a secret code and
    is handled by its own handler with the watermark; its text is never
    persisted either."""
    async def put(update):
        try:
            message = getattr(update, "message", None)
            text = (message.text if message else "") or ""
            is_media = bool(message is not None and (
                getattr(message, "video", None)
                or getattr(message, "document", None)))
            if text.startswith("/") or is_media:
                command = (text[1:].split()[0].lower().lstrip("/")
                           if text.startswith("/") else "ingest")
                user = getattr(update, "effective_user", None)
                sender = int(user.id) if user else 0
                admin = admin_id_fn()
                if (command != "pair" and admin is not None
                        and sender == admin):
                    uid = int(update.update_id)
                    if not watermark.already_handled(uid):
                        inbox.add(uid, sender,
                                  text if text.startswith("/")
                                  else "/ingest")
        except Exception:
            import logging
            logging.getLogger("bot.ops").exception(
                "pre-ack inbox write failed")
        return await orig_put(update)
    return put


JOURNAL = "op_journal.json"


def _journal_read(state_dir: Path) -> dict:
    return locked_read(state_dir / JOURNAL, {})


SIDE_EFFECTING = ("clipnow", "relay", "ingest", "deliveries", "publish")


JOURNAL_MAX_AGE_S = 7 * 86400
JOURNAL_CAP = 500


def _jkey(command: str, ident: int) -> str:
    """Typed journal key per OPERATION NAMESPACE: Telegram commands are
    keyed by update id (tg:), per-clip deliveries by clip id
    (deliver:). Without the split, clip 7 and Telegram update 7
    collide on one key and overwrite each other's replay evidence."""
    if command == "deliver":
        return f"deliver:{int(ident)}"
    return f"tg:{int(ident)}"


def _journal_migrate(data: dict, now: float) -> dict:
    """Re-key legacy records by their ACTUAL command, never by
    guessing from the key: bare-numeric keys and the brief tg:<id>
    format (which wrongly namespaced per-clip deliveries under tg:)
    both become deliver:<id> when the entry's command is deliver,
    tg:<id> otherwise."""
    for k in list(data):
        if k.startswith("deliver:"):
            continue
        entry = data[k]
        if k.startswith("tg:"):
            if entry.get("command") == "deliver":
                entry.setdefault("ts", now)
                data[f"deliver:{k[3:]}"] = entry
                del data[k]
            continue
        if k.isdigit():
            entry = data.pop(k)
            entry.setdefault("ts", now)
            prefix = ("deliver" if entry.get("command") == "deliver"
                      else "tg")
            data[f"{prefix}:{k}"] = entry
    return data


def _journal_trim(data: dict, now: float) -> None:
    """Evict by AGE, not id magnitude. Unresolved records (status
    uncertain, i.e. a send whose external effect may have happened)
    are never evicted - they exist for reconciliation."""
    resolved = [k for k, v in data.items()
                if v.get("status") != "uncertain"]
    for k in resolved:
        if now - data[k].get("ts", now) > JOURNAL_MAX_AGE_S:
            del data[k]
    resolved = [k for k in data if data[k].get("status") != "uncertain"]
    if len(resolved) > JOURNAL_CAP:
        resolved.sort(key=lambda k: data[k].get("ts", 0))
        for k in resolved[: len(resolved) - JOURNAL_CAP]:
            del data[k]


def journal_record_ns(state_dir: Path, namespace: str, ident: int,
                      command: str, result: str) -> None:
    """Record under an explicit namespace (e.g. publish:<clip_id> for
    per-clip publication dedup) - a different id space than the
    Telegram-update command journal."""

    def apply(data):
        import time as _t
        now = _t.time()
        _journal_migrate(data, now)
        data[f"{namespace}:{int(ident)}"] = {
            "command": command, "result": result, "ts": now}
        _journal_trim(data, now)
        return data, None

    locked_update(state_dir / JOURNAL, {}, apply)


def journal_result_ns(state_dir: Path, namespace: str, ident: int,
                      command: str):
    """Exact-key lookup in an explicit namespace. No legacy fallback:
    a same-numbered key from another namespace must never resolve."""
    entry = _journal_read(state_dir).get(f"{namespace}:{int(ident)}")
    if entry and entry.get("command") == command:
        return entry.get("result")
    return None


def journal_result(state_dir: Path, update_id: int, command: str):
    """Stored result of an already-executed side-effecting command, or
    None. Gives replayed commands operation-level idempotency: a drain
    replay returns the recorded result instead of repeating the side
    effect (a second Twitch clip, a second restream)."""
    if not update_id:
        return None
    data = _journal_read(state_dir)
    entry = (data.get(_jkey(command, update_id))
             or data.get(f"tg:{int(update_id)}")  # brief typed format
             or data.get(str(update_id)))  # legacy bare-numeric
    # Fallback hits are still guarded by the command equality check
    # below, so a cross-namespace hit can never resolve.
    if entry and entry.get("command") == command:
        return entry.get("result")
    return None


def journal_mark_uncertain(state_dir: Path, update_id: int,
                           command: str, text: str) -> None:
    """A side-effecting command stranded WITHOUT a journal record is
    uncertain: its external effect may or may not have happened (the
    success-before-journal-commit ambiguity). Blind retry can duplicate
    the effect, so the operation is preserved for reconciliation and
    reported, not re-executed."""
    if not update_id:
        return

    def apply(data):
        import time as _t
        now = _t.time()
        _journal_migrate(data, now)
        data[_jkey(command, update_id)] = {
            "command": command, "status": "uncertain",
            "text": text, "ts": now}
        return data, None

    locked_update(state_dir / JOURNAL, {}, apply)



def inbox_quarantine(state_dir: Path, update_id: int, text: str) -> None:
    """Preserve a repeatedly failing command for operator
    reconciliation. Command NAME only, 0600, bounded - same discipline
    as the overflow file."""
    command = (text.lstrip("/").split() or [""])[0][:32]

    def apply(rows):
        rows.append({"update_id": int(update_id), "command": command})
        return rows[-100:], None

    locked_update(state_dir / "quarantine.json", [], apply)



def journal_uncertain(state_dir: Path) -> list[dict]:
    out = []
    for k, v in _journal_read(state_dir).items():
        if v.get("status") != "uncertain":
            continue
        raw = k.split(":", 1)[1] if ":" in k else k
        if raw.isdigit():
            out.append({"update_id": int(raw), **v})
    return out


def journal_record(state_dir: Path, update_id: int, command: str,
                   result: str) -> None:
    if not update_id:
        return

    def apply(data):
        import time as _t
        now = _t.time()
        _journal_migrate(data, now)
        data[_jkey(command, update_id)] = {
            "command": command, "result": result, "ts": now}
        _journal_trim(data, now)
        return data, None

    locked_update(state_dir / JOURNAL, {}, apply)



def drain_inbox(state_dir: Path, router, watermark: UpdateWatermark,
                inbox: PreAckInbox, reply=lambda _uid, _text: None,
                admin_id: int | None = None) -> dict:
    """Retry updates stranded by a crash between confirmation and
    handling. Idempotent via the watermark: entries already handled are
    just dropped. Entries that fail again STAY for the next drain.

    AUTHORIZATION IS NEVER TAKEN FROM THE ENTRY: a stored sender id is
    untrusted input. Only entries whose sender matches the paired
    administrator (PairingStore) are dispatched, and the router is
    authorized with that paired id - anything else is dropped and
    marked handled so it can never execute."""
    import logging
    log = logging.getLogger("bot.ops")
    replayed = dropped = failed = uncertain = 0
    for entry in inbox.pending():
        uid = int(entry["update_id"])
        if watermark.already_handled(uid):
            inbox.remove(uid)
            dropped += 1
            continue
        if admin_id is None or int(entry.get("user_id", 0)) != admin_id:
            # not from the paired admin (or pairing not complete):
            # never dispatch; drop so it can never execute later
            log.warning("inbox drain dropping non-admin entry update=%s",
                        uid)
            inbox.remove(uid)
            watermark.mark_handled(uid)
            dropped += 1
            continue
        command = (entry["text"].lstrip("/").split() or [""])[0].lower()
        if command == "ingest":
            # media uploads cannot be re-driven from the drain (the
            # Telegram download needs the live update): a journaled
            # result means the pipeline completed - drop; otherwise
            # preserve for reconciliation and ask the admin to re-send
            if journal_result(state_dir, uid, "ingest") is not None:
                inbox.remove(uid)
                watermark.mark_handled(uid)
                dropped += 1
            else:
                journal_mark_uncertain(state_dir, uid, "ingest",
                                       entry["text"])
                watermark.mark_handled(uid)
                inbox.remove(uid)
                reply(admin_id,
                      "Your video upload was interrupted by a restart "
                      "and was NOT processed. Please re-send the video.")
                uncertain += 1
            continue
        if (command in SIDE_EFFECTING
                and journal_result(state_dir, uid, command) is None):
            # crash window: the side effect may already have executed
            # without a journal record - preserve for reconciliation,
            # never blind-retry
            journal_mark_uncertain(state_dir, uid, command,
                                   entry["text"])
            watermark.mark_handled(uid)
            inbox.remove(uid)
            reply(admin_id,
                  f"Command {entry['text']} was interrupted by a restart "
                  "and may already have run. It was NOT repeated. "
                  "Re-send it if you want it executed again.")
            uncertain += 1
            continue
        try:
            router.admin_ids = {admin_id}
            reply_text = router.dispatch(entry["text"],
                                         user_id=admin_id,
                                         update_id=uid)
            if reply_text:
                reply(admin_id, reply_text)
            watermark.mark_handled(uid)
            inbox.remove(uid)
            replayed += 1
        except Exception:
            log.exception("inbox drain failed for update %s", uid)
            failed += 1
            attempts = int(entry.get("attempts", 0)) + 1
            if attempts >= 3:
                # quarantine: a repeatedly failing entry must not stall
                # the inbox forever; preserve it for reconciliation
                log.error("quarantining update %s after %d attempts",
                          uid, attempts)
                inbox.remove(uid)
                watermark.mark_handled(uid)
                inbox_quarantine(state_dir, uid, entry["text"])
            else:
                inbox.set_attempts(uid, attempts)
    if replayed or dropped or failed or uncertain:
        log.info("inbox drain: %d replayed, %d already-handled, %d failed,"
                 " %d uncertain", replayed, dropped, failed, uncertain)
    return {"replayed": replayed, "dropped": dropped, "failed": failed,
            "uncertain": uncertain}


AUTO_DELIVER_DEFAULT = True


def auto_deliver_enabled(state_dir: Path) -> bool:
    """Whether session clips are sent to the paired admin as soon as
    they pass QC. The owner's stated preference wins; the default
    matches the product's automatic-delivery goal and can be paused
    with '/deliveries hold'."""
    from ..intelligence.memory import Memory
    row = Memory(str(state_dir / "memory.json")).get(
        "preference", "auto_deliver")
    if row is None:
        return AUTO_DELIVER_DEFAULT
    return str(row.get("value", "")).lower() not in ("off", "hold",
                                                     "false", "0")


def set_auto_deliver(state_dir: Path, enabled: bool) -> None:
    from ..intelligence.memory import Memory
    Memory(str(state_dir / "memory.json")).remember(
        "preference", "auto_deliver", "on" if enabled else "off",
        "owner /deliveries toggle", confidence="stated")


def deliver_one_clip(state_dir: Path, clip_id: int,
                     deliver_fn=None) -> str:
    """Deliver one HELD clip to the paired admin with per-clip
    durability. Returns one of: delivered, reconciled, uncertain,
    skipped_uncertain, not_held, missing.

    The send is journaled and committed INDIVIDUALLY: a crash right
    after keeps the clip delivered. A clip whose send raised is marked
    UNCERTAIN (it may already have reached Telegram) and is never
    blind-resent; it stays visible for reconciliation. A clip journaled
    delivered but still HELD in the DB (crash between send and update)
    is reconciled to DELIVERED without resending. No exactly-once
    claim: the uncertain state is the honest answer for a send
    interrupted mid-flight."""
    import os as _os
    import time as _time
    from ..core.database import connect
    conn = connect(str(state_dir / "nova.db"))
    row = conn.execute(
        "SELECT file_path, duration_seconds, status FROM clips"
        " WHERE id=?", (clip_id,)).fetchone()
    if row is None:
        conn.close()
        return "missing"
    path, dur, status = row
    if status != "HELD":
        conn.close()
        return "not_held"
    if journal_result(state_dir, clip_id, "deliver") is not None:
        # sent earlier, crash before DB update: reconcile, no resend
        conn.execute(
            "UPDATE clips SET status='DELIVERED', delivered_at=?"
            " WHERE id=?", (_time.time(), clip_id))
        conn.commit()
        conn.close()
        return "reconciled"
    prior = [e for e in journal_uncertain(state_dir)
             if e["update_id"] == clip_id
             and e.get("command") == "deliver"]
    if prior:
        conn.close()
        return "skipped_uncertain"  # may already be sent; never resend
    if deliver_fn is None:
        from ..distribution.telegram_delivery import deliver_clip_sync
        token_path = _os.environ["NOVA_TOKEN_FILE"]
        deliver_fn = lambda p, caption: deliver_clip_sync(
            token_path, str(state_dir), p, caption)
    try:
        mid = deliver_fn(path, f"NOVA clip #{clip_id} ({dur:.0f}s)")
        journal_record(state_dir, clip_id, "deliver", f"message {mid}")
        conn.execute(
            "UPDATE clips SET status='DELIVERED', delivered_at=?"
            " WHERE id=?", (_time.time(), clip_id))
        conn.commit()
        conn.close()
        return "delivered"
    except Exception:
        journal_mark_uncertain(state_dir, clip_id, "deliver", str(path))
        conn.close()
        return "uncertain"


def auto_deliver_clips(state_dir: Path, clip_ids,
                       deliver_fn=None) -> dict:
    """Auto-deliver freshly finished session clips when the preference
    is on. Never raises: a delivery problem marks the clip uncertain
    and leaves it HELD for /deliveries, it must not fail the clip."""
    if not auto_deliver_enabled(state_dir):
        return {"enabled": False, "delivered": 0, "uncertain": 0}
    delivered = uncertain = 0
    for cid in clip_ids:
        try:
            result = deliver_one_clip(state_dir, cid,
                                      deliver_fn=deliver_fn)
        except Exception as exc:
            # setup errors (e.g. token file missing) happen BEFORE any
            # send: not an uncertain clip, just an undelivered one
            log.warning("auto-delivery of clip %s failed before send:"
                        " %s", cid, exc)
            continue
        if result in ("delivered", "reconciled"):
            delivered += 1
        elif result in ("uncertain", "skipped_uncertain"):
            uncertain += 1
    return {"enabled": True, "delivered": delivered,
            "uncertain": uncertain}


def deliveries_reply(state_dir: Path, args: str,
                     deliver_fn=None) -> str:
    """List HELD clips; '/deliveries flush' sends them to the admin on
    explicit request; '/deliveries auto' and '/deliveries hold' toggle
    automatic delivery of session clips as they pass QC. Mode C
    (one-off owner upload) never delivers media by itself.

    Per-clip durability lives in deliver_one_clip: journaled per clip,
    never blind-resent when uncertain."""
    arg = args.strip().lower()
    if arg == "auto":
        set_auto_deliver(state_dir, True)
        return ("Auto-delivery ON: new session clips are sent to you "
                "as soon as they pass quality checks.")
    if arg == "hold":
        set_auto_deliver(state_dir, False)
        return ("Auto-delivery OFF: new clips are held until "
                "'/deliveries flush'.")
    from ..core.database import connect
    conn = connect(str(state_dir / "nova.db"))
    rows = conn.execute(
        "SELECT id, file_path, duration_seconds FROM clips"
        " WHERE status='HELD' ORDER BY id").fetchall()
    conn.close()
    if arg != "flush":
        uncertain = {e["update_id"] for e in journal_uncertain(state_dir)
                     if e.get("command") == "deliver"}
        state = "on" if auto_deliver_enabled(state_dir) else "off"
        if not rows:
            return (f"No held clips (auto-delivery {state}). "
                    "Send a video to make one.")
        lines = []
        for cid, _path, dur in rows:
            note = " (send uncertain - may already be sent)" \
                if cid in uncertain else ""
            lines.append(f"#{cid} ({dur:.0f}s){note}")
        return ("Held clips: " + ", ".join(lines) +
                ". /deliveries flush sends them to you. "
                f"Auto-delivery: {state}.")
    if not rows:
        return "Nothing held."
    sent = failed = uncertain = 0
    for cid, _path, _dur in rows:
        result = deliver_one_clip(state_dir, cid, deliver_fn=deliver_fn)
        if result in ("delivered", "reconciled"):
            sent += 1
        elif result == "uncertain":
            failed += 1
        elif result == "skipped_uncertain":
            uncertain += 1
    parts = [f"Delivered {sent} clip(s)"]
    if failed:
        parts.append(f"{failed} uncertain (may already be sent; held "
                     "for reconciliation, NOT resent)")
    if uncertain:
        parts.append(f"{uncertain} previously uncertain, skipped")
    return "; ".join(parts) + "."


def clipnow_reply(state_dir: Path, channel: str, update_id: int = 0) -> str:
    """Manual official-route clip. Fails loudly when unconfigured -
    never pretends a clip exists. Journaled: a replayed update returns
    the recorded result instead of creating a second clip."""
    cached = journal_result(state_dir, update_id, "clipnow")
    if cached is not None:
        return "Already processed (replay guard): " + cached
    token_file = state_dir / "twitch_user_token"
    client_file = state_dir / "twitch_client_id"
    if not token_file.exists() or not client_file.exists():
        return UNCONFIGURED_CLIP
    channel = channel.strip()
    if not channel:
        return "Usage: /clipnow <channel-login>"
    client = TwitchClipsClient(client_file.read_text().strip(),
                               token_file.read_text().strip())
    try:
        bid = client.resolve_broadcaster_id(channel)
        clip_id = client.create_clip(bid)
        info = client.wait_for_clip(clip_id)
    except ClipFailed as exc:
        return f"Clip creation failed: {exc}"
    from ..intelligence.memory import Memory
    from ..intelligence.planner import plan_clip
    mem = Memory(str(state_dir / "memory.json"))
    plan = plan_clip("manual", channel, mem,
                     game=str(info.get("game_id", "")))
    mem.remember("clip", f"twitch-clip:{clip_id}",
                 f"manual clip of {channel} via /clipnow",
                 "twitch api", confidence="observed")
    plan_note = (f"\nPlan: {plan.target_seconds}s {plan.preset}, "
                 f"title \"{plan.title}\"")
    try:
        url = client.download_url(info)
    except ClipFailed:
        result = (f"Clip created ({info.get('url', 'pending')}) but file "
                  "download is restricted to the broadcaster or channel "
                  "editors - no file was pulled." + plan_note)
        journal_record(state_dir, update_id, "clipnow", result)
        return result
    result = (f"Clip created: {info.get('url', '')} "
              f"(download: {url})" + plan_note)
    journal_record(state_dir, update_id, "clipnow", result)
    return result


def destinations_reply(state_dir: Path, args: str) -> str:
    store = DestinationStore(str(state_dir / "destinations.json"))
    parts = args.split()
    if not parts:
        rows = store.list()
        if not rows:
            return "No delivery destinations registered."
        return "Destinations:\n" + "\n".join(
            f"{r['chat_id']} - {r['label']}" for r in rows)
    if parts[0] == "add" and len(parts) >= 3:
        try:
            chat_id = int(parts[1])
        except ValueError:
            return "chat_id must be a number"
        store.add(chat_id, " ".join(parts[2:]))
        return f"Destination {chat_id} registered."
    if parts[0] == "remove" and len(parts) == 2:
        try:
            chat_id = int(parts[1])
        except ValueError:
            return "chat_id must be a number"
        return (f"Destination {chat_id} removed." if store.remove(chat_id)
                else f"No destination {chat_id}.")
    return USAGE_DESTINATIONS
