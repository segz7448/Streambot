"""Phase 010/009: automatic event-driven clipping.

Consumes PENDING candidate_events, scores them through the configured
intelligence provider (default: $0 heuristic fusion, never labeled AI),
extracts clips for moments above threshold via the real extractor, and
records clips rows. Events are marked SCORED/CLIPPED/REJECTED so each is
processed exactly once; extraction failures mark the event FAILED for
operator visibility rather than silently dropping it.
"""
from __future__ import annotations

import contextlib
from pathlib import Path

import time
from typing import Callable

from ..core.database import connect
from ..core.logging import get_logger
from ..intelligence.providers import IntelligenceProvider, QuotaExhausted
from ..recording.buffer import RollingBuffer
from .extractor import extract_clip
from .manual import MANUAL_MARK_KIND, MANUAL_MARK_SCORE
from .quota import GENERATION_DAILY_CAP, check_quota

log = get_logger("clipping.auto")


class AutoClipper:
    """Extract candidate events under the owner's source/day clip limit.

    The default generation cap is 8 durable clip rows per Twitch source per
    America/Los_Angeles calendar day. It is enforced atomically at clip-row
    creation and is separate from the later upload-admission cap.
    """

    def __init__(self, buffer: RollingBuffer, db_path: str,
                 provider: IntelligenceProvider, ffmpeg: str,
                 out_dir: str, min_score: float = 0.6,
                 pre_seconds: float = 20.0, post_seconds: float = 10.0,
                 merge_seconds: float = 5.0,
                 extract_fn: Callable = extract_clip,
                 max_session_clips: int | None = None,
                 event_kinds: tuple[str, ...] | None = None,
                 clock: Callable[[], float] = time.time,
                 generation_cap: int | None = GENERATION_DAILY_CAP):
        if max_session_clips is not None and max_session_clips != 1:
            raise ValueError("only opt-in single-clip bound is supported")
        self.max_session_clips = max_session_clips
        self.event_kinds = event_kinds
        self.clock = clock
        self.generation_cap = generation_cap
        self.buffer = buffer
        self.db_path = db_path
        self.provider = provider
        self.ffmpeg = ffmpeg
        self.out_dir = out_dir
        self.min_score = min_score
        self.pre_seconds = pre_seconds
        self.post_seconds = post_seconds
        self.merge_seconds = merge_seconds
        self.extract_fn = extract_fn

    def _pending_events(self, conn) -> list[dict]:
        rows = conn.execute(
            "SELECT id, detected_at, kind, confidence FROM candidate_events"
            " WHERE session_id=? AND status IN ('PENDING','QUEUED')"
            " ORDER BY detected_at",
            (self.buffer.session_id,)).fetchall()
        events = [{"id": r[0], "at": r[1], "kind": r[2],
                   "conf": r[3] or 0.0}
                  for r in rows]
        if self.event_kinds is not None:
            wanted = set(self.event_kinds)
            events = [e for e in events if e["kind"] in wanted]
        return events

    def _gate_manual_marks(self, conn, events):
        """Hold manual marks until durable footage covers the marked moment
        plus post-roll. Returns (ready events, ids with truncated post-roll).

        A mark whose wait deadline passed, or whose session is no longer
        recording (drained), is decided now: if durable footage contains the
        marked moment the clip is made and flagged post_window_truncated;
        otherwise the event fails explicitly (MARK_UNCOVERED), never a clip
        that lacks the moment."""
        import json
        marks = [e for e in events if e["kind"] == MANUAL_MARK_KIND]
        if not marks:
            return events, set()
        sid = self.buffer.session_id
        end = conn.execute(
            "SELECT MAX(created_at) FROM recording_segments"
            " WHERE session_id=?", (sid,)).fetchone()[0]
        state = conn.execute(
            "SELECT state FROM stream_sessions WHERE id=?",
            (sid,)).fetchone()
        recording = state is not None and state[0] == "RECORDING"
        now = self.clock()
        drop: set = set()
        truncated: set = set()
        for e in marks:
            need = e["at"] + self.post_seconds
            if end is not None and end >= need:
                continue                      # fully covered: ready
            row = conn.execute(
                "SELECT metadata_json FROM candidate_events WHERE id=?",
                (e["id"],)).fetchone()
            try:
                deadline = json.loads(row[0] or "{}").get("wait_deadline_at")
            except ValueError:
                deadline = None
            expired = (not recording) or (
                deadline is not None and now >= float(deadline))
            if not expired:
                drop.add(e["id"])             # keep PENDING, retry next tick
                continue
            if end is not None and end >= e["at"]:
                truncated.add(e["id"])        # moment present, post-roll short
                continue
            conn.execute(
                "UPDATE candidate_events SET status='MARK_UNCOVERED'"
                " WHERE id=?", (e["id"],))
            log.warning("manual mark %s not covered by durable footage "
                        "(moment %.1f, durable end %s); failing explicitly",
                        e["id"], e["at"], end)
            drop.add(e["id"])
        return [e for e in events if e["id"] not in drop], truncated

    def process_pending(self) -> int:
        """Score pending events and extract clips above threshold.
        Returns the number of clips produced."""
        with contextlib.closing(connect(self.db_path)) as conn:
            events = self._pending_events(conn)
            if not events:
                conn.close()
                return 0
            events, truncated_ids = self._gate_manual_marks(conn, events)
            if not events:
                conn.commit()
                conn.close()
                return 0
            # A heuristic queued before the owner's mark finished must not
            # later extract a duplicate of that same moment. Processed
            # manual marks are the stronger selection evidence and win the
            # existing merge window across passes, retries and recovery.
            clipped_manual = [r[0] for r in conn.execute(
                "SELECT detected_at FROM candidate_events"
                " WHERE session_id=? AND kind=? AND status='CLIPPED'",
                (self.buffer.session_id, MANUAL_MARK_KIND))]
            if clipped_manual:
                duplicate_ids = [
                    e["id"] for e in events
                    if e["kind"] != MANUAL_MARK_KIND
                    and any(abs(e["at"] - at) < self.merge_seconds
                            for at in clipped_manual)
                ]
                if duplicate_ids:
                    conn.executemany(
                        "UPDATE candidate_events SET status='MERGED'"
                        " WHERE id=?", [(i,) for i in duplicate_ids])
                    events = [e for e in events
                              if e["id"] not in set(duplicate_ids)]
                    if not events:
                        conn.commit()
                        conn.close()
                        return 0
            # Heuristic-first ordering: a manual mark near an ALREADY
            # CLIPPED non-manual event is the same moment; no second clip.
            # The owner hears about it (MERGED notification names the clip).
            clipped_other = [(r[0], r[1]) for r in conn.execute(
                "SELECT detected_at, id FROM candidate_events"
                " WHERE session_id=? AND kind<>? AND status='CLIPPED'",
                (self.buffer.session_id, MANUAL_MARK_KIND))]
            if clipped_other:
                dup = []
                for e in events:
                    if e["kind"] != MANUAL_MARK_KIND:
                        continue
                    hit = [i for at, i in clipped_other
                           if abs(e["at"] - at) < self.merge_seconds]
                    if hit:
                        dup.append((e["id"], hit[0]))
                for eid, other in dup:
                    conn.execute(
                        "UPDATE candidate_events SET status='MERGED',"
                        " metadata_json=json_set(COALESCE(metadata_json,"
                        "'{}'), '$.merged_into_event', ?) WHERE id=?",
                        (other, eid))
                if dup:
                    gone = {d[0] for d in dup}
                    events = [e for e in events if e["id"] not in gone]
                    if not events:
                        conn.commit()
                        conn.close()
                        return 0
            # Unit bridge: candidate_events.confidence is normalized 0..1;
            # HeuristicProvider expects audio loudness in LUFS (its mapping
            # (lufs + 60) / 60 recovers the normalized value). Convert here
            # so a 0.1-confidence event scores 0.1, not ~1.0. Scene
            # confidences pass through unchanged (provider uses 0.5 * score).
            provider_events = [e for e in events
                               if e["kind"] != MANUAL_MARK_KIND]
            spikes = [(e["at"], e["conf"] * 60 - 60)
                      for e in provider_events
                      if e["kind"] == "audio_spike"]
            scenes = [(e["at"], e["conf"]) for e in provider_events
                      if e["kind"] == "scene_change"]
            moments = []
            provider_down = False
            if provider_events:
                try:
                    moments = self.provider.score_moments(
                        self.buffer.segment_seconds, spikes, scenes)
                except QuotaExhausted:
                    # A manual mark is owner command evidence, not model
                    # output: it must not wait behind an AI/provider quota.
                    # Only provider-scored events are queued for retry.
                    provider_down = True
                    log.warning(
                        "intelligence OFFLINE: provider quota exhausted; "
                        "%d provider-scored job(s) queued, manual marks "
                        "continue - no silent substitution",
                        len(provider_events))
                    conn.executemany(
                        "UPDATE candidate_events SET status='QUEUED'"
                        " WHERE id=?",
                        [(e["id"],) for e in provider_events])
            by_ts = {m.timestamp: m for m in moments}
            if provider_down:
                events = [e for e in events
                          if e["kind"] == MANUAL_MARK_KIND]
                if not events:
                    conn.commit()
                    conn.close()
                    return 0
            # Cross-kind merge: one moment can fire both an audio_spike and a
            # scene_change; clipping both yields two clips of the same content.
            # Within merge_seconds keep the higher-scored event and mark the
            # other MERGED (visible in the DB, never silently dropped). Distinct
            # moments further apart are unaffected.
            scored = sorted(
                ((e, MANUAL_MARK_SCORE if e["kind"] == MANUAL_MARK_KIND
                  else (by_ts[e["at"]].score if e["at"] in by_ts
                        else e["conf"]))
                 for e in events),
                key=lambda es: es[0]["at"])
            merged_ids: set[int] = set()
            kept: list[tuple[dict, float]] = []
            for event, score in scored:
                if kept and event["at"] - kept[-1][0]["at"] < self.merge_seconds:
                    prev, pscore = kept[-1]
                    if score > pscore:
                        conn.execute(
                            "UPDATE candidate_events SET status='MERGED' WHERE id=?",
                            (prev["id"],))
                        merged_ids.add(prev["id"])
                        kept[-1] = (event, score)
                    else:
                        conn.execute(
                            "UPDATE candidate_events SET status='MERGED' WHERE id=?",
                            (event["id"],))
                        merged_ids.add(event["id"])
                    continue
                kept.append((event, score))
            scores = {e["id"]: s for e, s in kept}
            produced = 0
            for event in events:
                if event["id"] in merged_ids:
                    continue
                score = scores.get(event["id"], event["conf"])
                if score < self.min_score:
                    conn.execute(
                        "UPDATE candidate_events SET status='REJECTED' WHERE id=?",
                        (event["id"],))
                    continue
                if self.max_session_clips is not None:
                    count = conn.execute(
                        "SELECT count(*) FROM clips WHERE session_id=?",
                        (self.buffer.session_id,)).fetchone()[0]
                    if count >= self.max_session_clips:
                        break   # no extra extraction, edit, or private send
                # Cheap read-only precheck BEFORE the expensive extraction.
                # The authoritative gate stays the atomic one at insert.
                if self.generation_cap is not None:
                    has_row = conn.execute(
                        "SELECT 1 FROM clips WHERE event_id=?",
                        (event["id"],)).fetchone()
                    if has_row is None:
                        pre = check_quota(conn, self.buffer.session_id,
                                          self.clock(),
                                          cap=self.generation_cap)
                        if not pre["allowed"]:
                            conn.execute(
                                "UPDATE candidate_events"
                                " SET status='QUOTA_EXCEEDED' WHERE id=?",
                                (event["id"],))
                            log.warning(
                                "clip generation quota reached (%d/%d, "
                                "scope %s); event %s not extracted",
                                pre["count"], pre["cap"], pre["scope"],
                                event["id"])
                            continue
                # unique, never-reused raw path, RESERVED with
                # O_CREAT|O_EXCL before the extractor child is spawned:
                # this run exclusively owns the path it extracts into
                from .seal import seal_file, reserve_output
                out = str(reserve_output(
                    self.out_dir,
                    f"clip-{self.buffer.session_id}-{event['id']}", ".mp4"))
                try:
                    # a manual mark made near the recording start has no
                    # full pre-roll because it never existed (not rotated)
                    xkw = ({"allow_stream_start": True}
                           if event["kind"] == MANUAL_MARK_KIND else {})
                    res = self.extract_fn(self.ffmpeg, self.buffer,
                                          event["at"], out,
                                          pre_seconds=self.pre_seconds,
                                          post_seconds=self.post_seconds,
                                          **xkw)
                except Exception as exc:
                    log.warning("clip extraction failed for event %s: %s",
                                event["id"], exc)
                    Path(out).unlink(missing_ok=True)
                    conn.execute(
                        "UPDATE candidate_events SET status='FAILED' WHERE id=?",
                        (event["id"],))
                    continue
                if res is None:
                    # window rotated out of the buffer; cannot clip this event
                    Path(out).unlink(missing_ok=True)
                    conn.execute(
                        "UPDATE candidate_events SET status='EXPIRED' WHERE id=?",
                        (event["id"],))
                    continue
                if isinstance(res, tuple):
                    # the real extractor reports the planned window duration
                    path, clip_seconds = res
                else:
                    # legacy/test extractors return only the path; the
                    # intended pre+post window is the honest estimate then
                    # (never the segment length, which understates it).
                    path, clip_seconds = res, (
                        self.pre_seconds + self.post_seconds)
                # durable sealed completion evidence committed WITH the
                # PENDING row, computed after the extractor child exited;
                # recovery eligibility requires it. A seal failure fails
                # the event rather than publishing unsealed input.
                try:
                    seal_sha, seal_bytes = seal_file(path)
                except Exception as exc:
                    log.warning("clip seal failed for event %s: %s",
                                event["id"], exc)
                    Path(path).unlink(missing_ok=True)
                    conn.execute(
                        "UPDATE candidate_events SET status='FAILED'"
                        " WHERE id=?", (event["id"],))
                    continue

                # The owner's max-8 generation rule is source/day scoped
                # and atomic: the existing-row check, source/day count,
                # clips insert and event transition all share one write
                # transaction. A retried event reuses its existing row;
                # concurrent different events serialize at BEGIN IMMEDIATE.
                now = self.clock()
                conn.execute("BEGIN IMMEDIATE")
                try:
                    existing = conn.execute(
                        "SELECT id FROM clips WHERE event_id=?",
                        (event["id"],)).fetchone()
                    if existing is not None:
                        # Same event retried after its clip row committed:
                        # no second slot is consumed. This pass's output is
                        # an exclusively owned duplicate path, so remove it.
                        Path(path).unlink(missing_ok=True)
                        conn.execute(
                            "UPDATE candidate_events SET status='CLIPPED'"
                            " WHERE id=?", (event["id"],))
                        conn.commit()
                        continue
                    if self.generation_cap is not None:
                        quota = check_quota(conn, self.buffer.session_id,
                                            now, cap=self.generation_cap)
                        if not quota["allowed"]:
                            Path(path).unlink(missing_ok=True)
                            conn.execute(
                                "UPDATE candidate_events"
                                " SET status='QUOTA_EXCEEDED' WHERE id=?",
                                (event["id"],))
                            conn.commit()
                            log.warning(
                                "clip generation quota reached for %s on %s"
                                " (%d/%d); event %s not clipped",
                                quota["source"], quota["day"],
                                quota["count"], quota["cap"], event["id"])
                            continue
                    cur = conn.execute(
                        "INSERT INTO clips (event_id, session_id,"
                        " file_path, duration_seconds, status, created_at,"
                        " sealed_sha256, sealed_bytes, sealed_at)"
                        " VALUES (?,?,?,?,?,?,?,?,?)",
                        (event["id"], self.buffer.session_id, str(path),
                         float(clip_seconds), "PENDING", now,
                         seal_sha, seal_bytes, now))
                    if cur.rowcount != 1:
                        raise RuntimeError(
                            "clip reservation row was not created")
                    conn.execute(
                        "UPDATE candidate_events SET status='CLIPPED'"
                        " WHERE id=?", (event["id"],))
                    if event["id"] in truncated_ids:
                        import json
                        m = conn.execute(
                            "SELECT metadata_json FROM candidate_events"
                            " WHERE id=?", (event["id"],)).fetchone()[0]
                        md = json.loads(m or "{}")
                        md["post_window_truncated"] = True
                        conn.execute(
                            "UPDATE candidate_events SET metadata_json=?"
                            " WHERE id=?", (json.dumps(md, sort_keys=True),
                                            event["id"]))
                    conn.commit()
                    produced += 1
                except Exception:
                    conn.rollback()
                    raise
            conn.commit()
            conn.close()
            return produced
