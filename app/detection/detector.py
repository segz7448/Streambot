"""Candidate detector: folds heuristic signals into candidate_events rows.

Consumes finalized buffer segments in order, runs audio + visual analysis,
and records candidates with kind/confidence. Deliberately cheap and local;
no AI claims - Phase 009 evaluates these candidates.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Callable

from ..core.database import connect
from ..core.lifecycle import ServiceContext
from ..core.logging import get_logger
from ..recording.buffer import RollingBuffer
from . import audio, visual

log = get_logger("detection.detector")


def ensure_state_table(db_path: str) -> None:
    conn = connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS detection_state (
            session_id INTEGER PRIMARY KEY,
            last_analyzed_sequence INTEGER NOT NULL DEFAULT -1,
            baseline_lufs REAL
        )""")
    # older databases predate the baseline column
    try:
        conn.execute("ALTER TABLE detection_state ADD COLUMN baseline_lufs REAL")
    except Exception:
        pass
    conn.commit()
    conn.close()


class CandidateDetector:
    def __init__(self, ffmpeg: str, buffer: RollingBuffer, db_path: str,
                 audio_fn: Callable = audio.loudness_samples,
                 scene_fn: Callable = visual.scene_changes):
        self.ffmpeg = ffmpeg
        self.buffer = buffer
        self.db_path = db_path
        self.audio_fn = audio_fn
        self.scene_fn = scene_fn

    def analyze_segment(self, path: str,
                        baseline: float | None = None
                        ) -> tuple[list[dict], float | None]:
        """Heuristic candidates for one segment plus its median loudness,
        so the caller can carry a cross-segment baseline (a loud segment
        that starts exactly on a segment boundary has no internal rise)."""
        candidates: list[dict] = []
        seg_median: float | None = None
        try:
            samples = self.audio_fn(self.ffmpeg, path)
            for t, lufs in audio.loudness_spikes(samples, baseline):
                candidates.append({"kind": "audio_spike", "offset": t,
                                   "confidence": min(1.0, (lufs + 60) / 60)})
            vals = sorted(s.momentary for s in samples if s.momentary > -60.0)
            if vals:
                seg_median = vals[len(vals) // 2]
        except Exception as exc:  # heuristic layer must not kill recording
            log.warning("audio analysis failed for %s: %s", path, exc)
        try:
            for t, score in self.scene_fn(self.ffmpeg, path):
                candidates.append({"kind": "scene_change", "offset": t,
                                   "confidence": min(1.0, score)})
        except Exception as exc:
            log.warning("visual analysis failed for %s: %s", path, exc)
        return candidates, seg_median

    def process_new_segments(self) -> int:
        """Analyze finalized segments newer than the last processed one.
        The newest segment may still be written by ffmpeg; skip it."""
        ensure_state_table(self.db_path)
        conn = connect(self.db_path)
        row = conn.execute(
            "SELECT last_analyzed_sequence, baseline_lufs FROM"
            " detection_state WHERE session_id=?",
            (self.buffer.session_id,)).fetchone()
        last = row[0] if row else -1
        baseline = row[1] if row else None
        segs = [s for s in self.buffer.segments() if s.sequence > last]
        finalized = segs[:-1] if segs else []
        written = 0
        for seg in finalized:
            cands, seg_median = self.analyze_segment(str(seg.path), baseline)
            for cand in cands:
                conn.execute(
                    "INSERT INTO candidate_events"
                    " (session_id, detected_at, kind, confidence, metadata_json)"
                    " VALUES (?,?,?,?,?)",
                    (self.buffer.session_id,
                     seg.created_at + cand["offset"],
                     cand["kind"], cand["confidence"],
                     json.dumps({"segment": seg.sequence,
                                 "offset": cand["offset"]})))
                written += 1
            last = seg.sequence
            if seg_median is not None:
                # slow adaptation: the spike is the rise, not the level;
                # sustained loudness becomes the new normal
                baseline = seg_median if baseline is None else (
                    baseline + 0.2 * (seg_median - baseline))
        conn.execute(
            "INSERT INTO detection_state"
            " (session_id, last_analyzed_sequence, baseline_lufs)"
            " VALUES (?,?,?) ON CONFLICT(session_id)"
            " DO UPDATE SET last_analyzed_sequence"
            "=excluded.last_analyzed_sequence,"
            " baseline_lufs=excluded.baseline_lufs",
            (self.buffer.session_id,
             last if finalized else (row[0] if row else -1),
             baseline))
        conn.commit()
        conn.close()
        return written


class DetectionWorker:
    def __init__(self, detector: CandidateDetector, poll_seconds: float = 6.0):
        self.detector = detector
        self.poll_seconds = poll_seconds

    async def run(self, ctx: ServiceContext) -> None:
        while not ctx.stopping:
            ctx.heartbeat()
            n = await asyncio.to_thread(self.detector.process_new_segments)
            if n:
                log.info("recorded %d candidate events", n)
            await asyncio.sleep(self.poll_seconds)
