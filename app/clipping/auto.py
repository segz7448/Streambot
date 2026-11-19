"""Phase 010/009: automatic event-driven clipping.

Consumes PENDING candidate_events, scores them through the configured
intelligence provider (default: $0 heuristic fusion, never labeled AI),
extracts clips for moments above threshold via the real extractor, and
records clips rows. Events are marked SCORED/CLIPPED/REJECTED so each is
processed exactly once; extraction failures mark the event FAILED for
operator visibility rather than silently dropping it.
"""
from __future__ import annotations

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
                 extract_fn: Callable = extract_clip):
        self.buffer = buffer
        self.db_path = db_path
        self.provider = provider
        self.ffmpeg = ffmpeg
        self.out_dir = out_dir
        self.min_score = min_score
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
        conn = connect(self.db_path)
        events = self._pending_events(conn)
        if not events:
            conn.close()
            return 0
        spikes = [(e["at"], e["conf"]) for e in events
                  if e["kind"] == "audio_spike"]
        scenes = [(e["at"], e["conf"]) for e in events
                  if e["kind"] == "scene_change"]
        try:
            moments = self.provider.score_moments(
                self.buffer.session_seconds, spikes, scenes)
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
        produced = 0
        for event in events:
            moment = by_ts.get(event["at"])
            score = moment.score if moment else event["conf"]
            if score < self.min_score:
                conn.execute(
                    "UPDATE candidate_events SET status='REJECTED' WHERE id=?",
                    (event["id"],))
                continue
            out = (f"{self.out_dir}/clip-{self.buffer.session_id}-"
                   f"{event['id']}.mp4")
            try:
                path = self.extract_fn(self.ffmpeg, self.buffer, event["at"],
                                       out)
            except Exception as exc:
                log.warning("clip extraction failed for event %s: %s",
                            event["id"], exc)
                conn.execute(
                    "UPDATE candidate_events SET status='FAILED' WHERE id=?",
                    (event["id"],))
                continue
            if path is None:
                # window rotated out of the buffer; cannot clip this event
                conn.execute(
                    "UPDATE candidate_events SET status='EXPIRED' WHERE id=?",
                    (event["id"],))
                continue
            conn.execute(
                "INSERT INTO clips (event_id, session_id, file_path,"
                " duration_seconds, status, created_at)"
                " VALUES (?,?,?,?,?,?)",
                (event["id"], self.buffer.session_id, str(path),
                 self.buffer.segment_seconds, "PENDING", time.time()))
            conn.execute(
                "UPDATE candidate_events SET status='CLIPPED' WHERE id=?",
                (event["id"],))
            produced += 1
        conn.commit()
        conn.close()
        return produced
