"""Candidate detector: folds heuristic signals into candidate_events rows.

Consumes finalized buffer segments in order, runs audio + visual analysis,
and records candidates with kind/confidence. Deliberately cheap and local;
no AI claims - Phase 009 evaluates these candidates.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
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
    # older databases predate the episode-continuity column
    try:
        conn.execute("ALTER TABLE detection_state"
                     " ADD COLUMN episode_active INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    # older databases predate the last-segment-mtime column
    try:
        conn.execute("ALTER TABLE detection_state"
                     " ADD COLUMN last_seg_mtime REAL")
    except Exception:
        pass
    # Per-segment analysis outcome: detection_state advancing proves the
    # segment was VISITED, never that analysis succeeded - analyzer
    # exceptions are swallowed so heuristics can't kill recording. Without
    # this row a zero-candidate segment is indistinguishable from a
    # silently failed analysis (session-8 evidence gap).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS segment_analysis (
            session_id INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            audio_ok INTEGER NOT NULL,
            visual_ok INTEGER NOT NULL,
            analyzed_at REAL NOT NULL,
            PRIMARY KEY (session_id, sequence))""")
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
        # the buffer's timeline honors this database's recorded segment
        # identity (legacy rows are never reinterpreted)
        self.buffer.bind_db(db_path)

    def analyze_segment(self, path: str,
                        baseline: float | None = None,
                        episode_active: bool = False
                        ) -> tuple[list[dict], float | None, bool, set]:
        """Heuristic candidates for one segment plus its median loudness,
        so the caller can carry a cross-segment baseline (a loud segment
        that starts exactly on a segment boundary has no internal rise),
        plus the episode flag so one sustained moment spanning a segment
        boundary records one candidate, not one per segment.
        The last return value names the analyzers that FAILED (closed
        enum, subset of {"audio", "visual"}); failures are swallowed so
        the heuristic layer can't kill recording, but they must reach the
        caller so silence is recorded, not mistaken for clean analysis."""
        candidates: list[dict] = []
        seg_median: float | None = None
        failed: set = set()
        try:
            samples = self.audio_fn(self.ffmpeg, path)
            spikes, episode_active = audio.spikes_episode(
                samples, baseline, 6.0, -60.0, episode_active)
            for t, lufs in spikes:
                candidates.append({"kind": "audio_spike", "offset": t,
                                   "confidence": min(1.0, (lufs + 60) / 60)})
            vals = sorted(s.momentary for s in samples if s.momentary > -60.0)
            if vals:
                seg_median = vals[len(vals) // 2]
        except Exception as exc:  # heuristic layer must not kill recording
            log.warning("audio analysis failed for %s: %s", path, exc)
            failed.add("audio")
        try:
            for t, score in self.scene_fn(self.ffmpeg, path):
                candidates.append({"kind": "scene_change", "offset": t,
                                   "confidence": min(1.0, score)})
        except Exception as exc:
            log.warning("visual analysis failed for %s: %s", path, exc)
            failed.add("visual")
        return candidates, seg_median, episode_active, failed

    # One in-process owner per (db_path, session_id): concurrent passes
    # double-record candidates while the audit row collapses (reviewer
    # repro). Cross-PROCESS exactly-once is NOT claimed: the app runs a
    # single instance per state dir (leader discipline), and a foreign
    # duplicate writer surfaces as extra rows, never hidden ones.
    _PASS_LOCKS: dict = {}
    _PASS_LOCKS_GUARD = threading.Lock()

    @classmethod
    def _pass_lock(cls, db_path: str, session_id: int) -> threading.Lock:
        key = (str(db_path), session_id)
        with cls._PASS_LOCKS_GUARD:
            return cls._PASS_LOCKS.setdefault(key, threading.Lock())

    def process_new_segments(self, include_newest: bool = False) -> int:
        """Analyze finalized segments newer than the last processed one.
        The newest segment may still be written by ffmpeg, so it is
        skipped during the recording loop. Pass include_newest=True ONLY
        from session finalize, after every writer has attested drained:
        without it the capture's final segment is never analyzed and a
        moment in the last seconds of a recording is silently lost.
        Each segment's audit row, candidate rows and state advance commit
        ATOMICALLY, so a crash mid-pass resumes at the last fully
        recorded segment with no partial truth."""
        with self._pass_lock(self.db_path, self.buffer.session_id):
            return self._process_locked(include_newest)

    def _process_locked(self, include_newest: bool) -> int:
        ensure_state_table(self.db_path)
        conn = connect(self.db_path)
        try:
            if conn.execute(
                    "SELECT 1 FROM detection_state WHERE session_id=?",
                    (self.buffer.session_id,)).fetchone() is None:
                # initialize the state row OUTSIDE the per-segment
                # transactions: a rolled-back segment must leave the
                # pre-pass state visible (-1), not delete the row
                conn.execute(
                    "INSERT INTO detection_state (session_id,"
                    " last_analyzed_sequence) VALUES (?,-1)",
                    (self.buffer.session_id,))
            row = conn.execute(
                "SELECT last_analyzed_sequence, baseline_lufs, episode_active,"
                " last_seg_mtime FROM detection_state WHERE session_id=?",
                (self.buffer.session_id,)).fetchone()
            last = row[0] if row else -1
            baseline = row[1] if row else None
            episode_active = bool(row[2]) if row else False
            prev_mtime: float | None = row[3] if row else None
            segs = [s for s in self.buffer.segments() if s.sequence > last]
            finalized = segs if include_newest else (segs[:-1] if segs else [])
            written = 0
            for seg in finalized:
                # A recording interruption (reconnect gap) produces a real
                # wall-time gap between segment files that sequence-derived
                # content time hides. A burst right after such a gap is a NEW
                # moment, not a continuation: reset the carried episode.
                try:
                    mt = seg.path.stat().st_mtime
                except OSError:
                    mt = None
                if (mt is not None and prev_mtime is not None
                        and mt - prev_mtime > 2 * self.buffer.segment_seconds):
                    episode_active = False
                if mt is not None:
                    prev_mtime = mt
                cands, seg_median, episode_active, failed = (
                    self.analyze_segment(
                        str(seg.path), baseline, episode_active))
                # created_at is the segment's content END; the event time
                # is the segment START plus the analyzer's local offset
                seg_start = seg.created_at - seg.duration_seconds
                # audit + candidates + state advance commit in ONE REAL
                # transaction: connect() is autocommit (isolation_level
                # = None), so `with conn` would not BEGIN and each
                # statement would commit alone. Explicit BEGIN IMMEDIATE
                # also serializes writers across processes; ROLLBACK on
                # any failure leaves no partial audit/state truth.
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute(
                        "INSERT OR REPLACE INTO segment_analysis"
                        " (session_id, sequence, audio_ok, visual_ok,"
                        " analyzed_at) VALUES (?,?,?,?,?)",
                        (self.buffer.session_id, seg.sequence,
                         0 if "audio" in failed else 1,
                         0 if "visual" in failed else 1,
                         time.time()))
                    for cand in cands:
                        conn.execute(
                            "INSERT INTO candidate_events"
                            " (session_id, detected_at, kind, confidence,"
                            " metadata_json) VALUES (?,?,?,?,?)",
                            (self.buffer.session_id,
                             seg_start + cand["offset"],
                             cand["kind"], cand["confidence"],
                             json.dumps({"segment": seg.sequence,
                                         "offset": cand["offset"]})))
                        written += 1
                    last = seg.sequence
                    if seg_median is not None:
                        # slow adaptation: the spike is the rise, not the
                        # level; sustained loudness becomes the new normal
                        baseline = seg_median if baseline is None else (
                            baseline + 0.2 * (seg_median - baseline))
                    conn.execute(
                        "INSERT INTO detection_state"
                        " (session_id, last_analyzed_sequence, baseline_lufs,"
                        " episode_active, last_seg_mtime)"
                        " VALUES (?,?,?,?,?) ON CONFLICT(session_id)"
                        " DO UPDATE SET last_analyzed_sequence"
                        "=excluded.last_analyzed_sequence,"
                        " baseline_lufs=excluded.baseline_lufs,"
                        " episode_active=excluded.episode_active,"
                        " last_seg_mtime=excluded.last_seg_mtime",
                        (self.buffer.session_id, last, baseline,
                         1 if episode_active else 0, prev_mtime))
                    conn.execute("COMMIT")
                except BaseException:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    raise
            return written
        finally:
            conn.close()


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
