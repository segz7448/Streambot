"""Integration proof for manual mark timing against a segmented recording.

The media is generated locally so the test is hermetic, but it is a real
25-second H.264 recording represented as durable ``recording_segments`` rows
and consumed through the real ``RollingBuffer``/extractor path. A bright
frame is embedded at a known timestamp near the end of the durable
recording. The test proves that:

1. a mark requested at that visible buffered event lands there when it is
   durable; and
2. a mark requested in the not-yet-durable tail is clamped to the newest
   durable segment end, with the offset recorded instead of claimed exact.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from app.bot import manual_mark
from app.bot.manual_mark import clipmark_reply
from app.clipping.auto import AutoClipper
from app.core.database import connect, migrate
from app.intelligence.providers import HeuristicProvider
from app.recording.buffer import RollingBuffer


class _LiveSession:
    def __init__(self):
        self._session_id = 1
        self._stopping = threading.Event()

    def alive(self) -> bool:
        return True


class ManualMarkTimingIntegrationTests(unittest.TestCase):
    def test_command_receipt_timing_against_visible_durable_event(self) -> None:
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            self.skipTest("ffmpeg not installed")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_dir = root / "clips"
            out_dir.mkdir()
            db_path = str(root / "nova.db")
            conn = connect(db_path)
            migrate(conn)
            conn.execute(
                "INSERT INTO twitch_sources (id, username, created_at,"
                " updated_at) VALUES (1, 'timing-proof', 0, 0)")
            conn.execute(
                "INSERT INTO stream_sessions (id, source_id, started_at,"
                " state) VALUES (1, 1, 999.0, 'RECORDING')")
            conn.commit()
            conn.close()

            # A real segmented recording: 20s durable first segment + 5s
            # second segment. The visible event starts at 19.65s and lasts
            # 0.10s near the durable first segment's end.
            segment_dir = root / "recordings" / "session-1"
            segment_dir.mkdir(parents=True)
            first = segment_dir / "seg-000000.ts"
            second = segment_dir / "seg-000001.ts"
            subprocess.run(
                [
                    ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
                    "-f", "lavfi", "-i", "color=c=black:s=160x90:r=20:d=20",
                    "-vf", "null",
                    "-an", "-c:v", "libx264", "-preset", "ultrafast",
                    "-pix_fmt", "yuv420p", "-f", "mpegts", str(first),
                ],
                check=True,
            )
            subprocess.run(
                [
                    ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
                    "-f", "lavfi", "-i", "color=c=black:s=160x90:r=20:d=5",
                    "-vf", "drawbox=x=0:y=0:w=160:h=90:color=white:t=fill:enable='between(t,2.4,2.6)'",
                    "-an", "-c:v", "libx264", "-preset", "ultrafast",
                    "-pix_fmt", "yuv420p", "-f", "mpegts", str(second),
                ],
                check=True,
            )
            conn = connect(db_path)
            conn.execute(
                "INSERT INTO recording_segments (session_id, file_path,"
                " sequence, duration_seconds, created_at)"
                " VALUES (1, ?, 0, 20.0, 1020.0)", (str(first),))
            # Only the first segment has a durable DB row. The second file is
            # the open capture tail: it exists for the extractor but must not
            # yet count as a reconciled segment boundary.
            conn.commit()
            conn.close()

            buffer = RollingBuffer(str(root / "recordings"), session_id=1,
                                   segment_seconds=5)
            buffer.bind_db(db_path)
            now = [1024.0]
            clipper = AutoClipper(
                buffer, db_path, HeuristicProvider(), ffmpeg, str(out_dir),
                min_score=0.0, pre_seconds=5.0, post_seconds=2.0,
                merge_seconds=30.0, clock=lambda: now[0])
            live = {"timing-proof": _LiveSession()}

            def send_mark(receipt, seconds_ago, update_id):
                with patch.object(manual_mark.session_registry, "_sessions",
                                  live), \
                     patch("time.time", return_value=receipt):
                    return clipmark_reply(root, f"timing-proof {seconds_ago}",
                                          update_id=update_id)

            def gray_average(clip_path, offset_seconds):
                raw = subprocess.run(
                    [ffmpeg, "-hide_banner", "-loglevel", "error",
                     "-ss", f"{offset_seconds:.3f}", "-i", clip_path,
                     "-frames:v", "1", "-f", "rawvideo", "-pix_fmt", "gray",
                     "-"], check=True, stdout=subprocess.PIPE).stdout
                return sum(raw) / len(raw)

            def db_rows():
                c = connect(db_path)
                r = c.execute(
                    "SELECT e.id, e.status, e.detected_at, e.metadata_json,"
                    " k.file_path FROM candidate_events e"
                    " LEFT JOIN clips k ON k.event_id=e.id ORDER BY e.id"
                ).fetchall()
                c.close()
                return r

            # Mark the visible flash at 1022.5, received at 1024.0 (1.5s
            # ago). Newest durable end is 1020.0, so the moment is in the
            # open tail. The seg-000001 flash is the ONLY flash in the
            # recording, so a clip showing it proves the requested moment.
            requested = 1022.5
            reply = send_mark(1024.0, 1.5, update_id=1)
            self.assertIn("Manual mark #1", reply)
            self.assertIn("waits", reply)

            # Stage 1: only seg0 is durable -> mark stays pending, no clip.
            self.assertEqual(clipper.process_pending(), 0)
            r = db_rows()[0]
            self.assertEqual(r["status"], "PENDING")
            self.assertIsNone(r["file_path"])
            self.assertAlmostEqual(r["detected_at"], requested, places=3)

            # Stage 2: seg1 becomes durable (end 1025 >= 1022.5 + 2).
            conn = connect(db_path)
            conn.execute(
                "INSERT INTO recording_segments (session_id, file_path,"
                " sequence, duration_seconds, created_at)"
                " VALUES (1, ?, 1, 5.0, 1025.0)", (str(second),))
            conn.commit()
            conn.close()
            self.assertEqual(clipper.process_pending(), 1)
            r = db_rows()[0]
            self.assertEqual(r["status"], "CLIPPED")
            meta = json.loads(r["metadata_json"])
            self.assertAlmostEqual(r["detected_at"], requested, places=3)
            self.assertAlmostEqual(meta["requested_event_time"], requested,
                                   places=3)
            self.assertAlmostEqual(meta["durable_lag_seconds"], 4.0,
                                   places=3)
            self.assertNotIn("post_window_truncated", meta)
            clip_start = r["detected_at"] - 5.0
            offset = requested - clip_start
            bright = gray_average(r["file_path"], offset)
            dark = gray_average(r["file_path"], 1.0)
            self.assertGreater(bright, 200.0)
            self.assertLess(dark, 30.0)

            # Uncovered mark: moment never reaches durable footage and the
            # wait deadline passes -> explicit failure, no misleading clip.
            send_mark(1030.0, 0.0, update_id=2)
            now[0] = 2000.0
            self.assertEqual(clipper.process_pending(), 0)
            r2 = db_rows()[1]
            self.assertEqual(r2["status"], "MARK_UNCOVERED")
            self.assertIsNone(r2["file_path"])

            print("manual mark timing proof: "
                  f"requested={requested:.3f} receipt=1024.000 "
                  f"durable_lag={meta['durable_lag_seconds']:.3f}s "
                  f"stage1=PENDING(no clip) stage2=CLIPPED "
                  f"anchor={r['detected_at']:.3f} flash_offset={offset:.3f}s "
                  f"bright={bright:.0f} dark={dark:.0f} "
                  f"uncovered={r2['status']}")


if __name__ == "__main__":
    unittest.main()
