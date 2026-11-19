"""Rolling segmented media buffer.

FFmpeg writes fixed-duration segments; the buffer keeps enough footage
before and after detected events and enforces a storage cap with safe
rotation. Segment metadata lives in the database so a restarted process can
reconcile on-disk segments instead of duplicating recordings.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path

from ..core.database import connect
from ..core.logging import get_logger

log = get_logger("recording.buffer")


@dataclass
class Segment:
    session_id: int
    sequence: int
    path: Path
    duration_seconds: float
    created_at: float


class RollingBuffer:
    def __init__(self, root: str, session_id: int, segment_seconds: int = 5,
                 pre_event_seconds: int = 30, post_event_seconds: int = 30,
                 max_storage_mb: int = 2048):
        self.root = Path(root)
        self.session_id = session_id
        self.segment_seconds = segment_seconds
        self.pre_event_segments = max(1, pre_event_seconds // segment_seconds)
        self.post_event_segments = max(1, post_event_seconds // segment_seconds)
        self.max_storage_bytes = max_storage_mb * 1024 * 1024
        self.dir.mkdir(parents=True, exist_ok=True)

    @property
    def dir(self) -> Path:
        return self.root / f"session-{self.session_id}"

    def segment_args(self, input_url: str) -> list[str]:
        """FFmpeg args producing numbered segments for this session."""
        pattern = str(self.dir / "seg-%06d.ts")
        return ["-hide_banner", "-loglevel", "warning",
                "-i", input_url, "-c", "copy",
                "-f", "segment", "-segment_time", str(self.segment_seconds),
                "-reset_timestamps", "1", pattern]

    def segments(self) -> list[Segment]:
        out = []
        for path in sorted(self.dir.glob("seg-*.ts")):
            seq = int(path.stem.split("-")[1])
            out.append(Segment(self.session_id, seq, path,
                               float(self.segment_seconds),
                               path.stat().st_mtime))
        return out

    def event_window(self, event_time: float) -> list[Path]:
        """Segments covering pre/post-event retention around an event."""
        segs = self.segments()
        if not segs:
            return []
        start = event_time - self.pre_event_segments * self.segment_seconds
        end = event_time + self.post_event_segments * self.segment_seconds
        return [s.path for s in segs if start <= s.created_at <= end]

    def enforce_storage_cap(self, keep_after: float | None = None,
                            protect_newest: int = 2) -> int:
        """Delete oldest segments until under the cap. The newest segments
        (one may be actively written by ffmpeg) and segments inside the
        retention window of an event (keep_after) are preserved."""
        segs = self.segments()
        total = sum(s.path.stat().st_size for s in segs)
        removed = 0
        protected = {s.sequence for s in segs[-protect_newest:]} if segs else set()
        for seg in segs:
            if total <= self.max_storage_bytes:
                break
            if seg.sequence in protected:
                continue
            if keep_after is not None and seg.created_at >= keep_after:
                continue
            try:
                size = seg.path.stat().st_size
                seg.path.unlink()
                total -= size
                removed += 1
            except FileNotFoundError:
                pass
        if removed:
            log.info("buffer rotation removed %d segments (session %s)",
                     removed, self.session_id)
        return removed

    def reconcile(self, db_path: str) -> int:
        """Record on-disk segments missing from the database (post-restart)."""
        conn = connect(db_path)
        known = {r["sequence"] for r in conn.execute(
            "SELECT sequence FROM recording_segments WHERE session_id = ?",
            (self.session_id,))}
        added = 0
        with conn:
            for seg in self.segments():
                if seg.sequence in known:
                    continue
                conn.execute(
                    "INSERT INTO recording_segments"
                    " (session_id, file_path, sequence, duration_seconds,"
                    "  created_at) VALUES (?, ?, ?, ?, ?)",
                    (self.session_id, str(seg.path), seg.sequence,
                     seg.duration_seconds, seg.created_at))
                added += 1
        return added
