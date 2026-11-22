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

import time
from typing import Callable

from ..core.database import connect
from ..core.logging import get_logger
from ..intelligence.providers import IntelligenceProvider, QuotaExhausted
from ..recording.buffer import RollingBuffer
from .extractor import extract_clip

log = get_logger("clipping.auto")


class AutoClipper:
    def __init__(self, buffer: RollingBuffer, db_path: str,
                 provider: IntelligenceProvider, ffmpeg: str,
                 out_dir: str, min_score: float = 0.6,
                 pre_seconds: float = 20.0, post_seconds: float = 10.0,
                 merge_seconds: float = 5.0,
                 extract_fn: Callable = extract_clip):
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
        return [{"id": r[0], "at": r[1], "kind": r[2], "conf": r[3] or 0.0}
                for r in rows]

    def process_pending(self) -> int:
        """Score pending events and extract clips above threshold.
        Returns the number of clips produced."""
        with contextlib.closing(connect(self.db_path)) as conn:
            events = self._pending_events(conn)
            if not events:
                conn.close()
                return 0
            # Unit bridge: candidate_events.confidence is normalized 0..1;
            # HeuristicProvider expects audio loudness in LUFS (its mapping
            # (lufs + 60) / 60 recovers the normalized value). Convert here
            # so a 0.1-confidence event scores 0.1, not ~1.0. Scene
            # confidences pass through unchanged (provider uses 0.5 * score).
            spikes = [(e["at"], e["conf"] * 60 - 60) for e in events
                      if e["kind"] == "audio_spike"]
            scenes = [(e["at"], e["conf"]) for e in events
                      if e["kind"] == "scene_change"]
            try:
                moments = self.provider.score_moments(
                    self.buffer.segment_seconds, spikes, scenes)
            except QuotaExhausted:
                # AI understanding was requested; a spent quota means the
                # pipeline is DEGRADED/OFFLINE, not silently heuristic. Queue
                # every pending job and surface it; nothing is scored or
                # evicted while the provider is down.
                log.warning(
                    "intelligence OFFLINE: provider quota exhausted; "
                    "%d pending jobs queued, retry next pass - no silent "
                    "heuristic substitution", len(events))
                conn.executemany(
                    "UPDATE candidate_events SET status='QUEUED' WHERE id=?",
                    [(e["id"],) for e in events])
                conn.commit()
                conn.close()
                return 0
            by_ts = {m.timestamp: m for m in moments}
            # Cross-kind merge: one moment can fire both an audio_spike and a
            # scene_change; clipping both yields two clips of the same content.
            # Within merge_seconds keep the higher-scored event and mark the
            # other MERGED (visible in the DB, never silently dropped). Distinct
            # moments further apart are unaffected.
            scored = sorted(
                ((e, by_ts[e["at"]].score if e["at"] in by_ts else e["conf"])
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
                # unique, never-reused raw path: a deterministic path
                # would let a concurrent or stale producer clobber the
                # input another finalizer is about to seal
                from .seal import seal_file, unique_path
                out = str(unique_path(
                    self.out_dir,
                    f"clip-{self.buffer.session_id}-{event['id']}", ".mp4"))
                try:
                    res = self.extract_fn(self.ffmpeg, self.buffer,
                                          event["at"], out,
                                          pre_seconds=self.pre_seconds,
                                          post_seconds=self.post_seconds)
                except Exception as exc:
                    log.warning("clip extraction failed for event %s: %s",
                                event["id"], exc)
                    conn.execute(
                        "UPDATE candidate_events SET status='FAILED' WHERE id=?",
                        (event["id"],))
                    continue
                if res is None:
                    # window rotated out of the buffer; cannot clip this event
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
                    conn.execute(
                        "UPDATE candidate_events SET status='FAILED'"
                        " WHERE id=?", (event["id"],))
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO clips (event_id, session_id,"
                    " file_path, duration_seconds, status, created_at,"
                    " sealed_sha256, sealed_bytes, sealed_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?)",
                    (event["id"], self.buffer.session_id, str(path),
                     float(clip_seconds), "PENDING", time.time(),
                     seal_sha, seal_bytes, time.time()))
                conn.execute(
                    "UPDATE candidate_events SET status='CLIPPED' WHERE id=?",
                    (event["id"],))
                produced += 1
            conn.commit()
            conn.close()
            return produced
