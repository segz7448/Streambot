"""Live-cadence proof: durable recording_segments rows must arrive WHILE a
real ffmpeg segmenter is still recording, through the production tick
helpers (reconcile_durable_segments + AutoClipper + outcome notifier), not
staged DB rows. Media is a synthetic lavfi source written in real time."""
from __future__ import annotations

import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from app.bot import manual_mark, ops
from app.bot.manual_mark import clipmark_reply
from app.clipping.auto import AutoClipper
from app.core.database import connect, migrate
from app.intelligence.providers import HeuristicProvider
from app.pipeline.session import (notify_mark_outcomes,
                                  reconcile_durable_segments)
from app.recording.buffer import RollingBuffer


class _Live:
    _session_id = 1

    def __init__(self):
        self._stopping = threading.Event()

    def alive(self):
        return True


class LiveCadenceTests(unittest.TestCase):
    def test_durable_rows_arrive_while_recording_and_mark_completes(self):
        self._run_live(3.0)

    def test_early_mark_with_longer_preroll_than_recording_clips(self):
        """Phone failure class: mark ~2-3s into the recording with a 10s
        pre-roll. Nothing was rotated; the footage never existed. Before
        the fix this ended EXPIRED; it must clip from the recording start."""
        self._run_live(10.0)

    def _run_live(self, pre):
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            self.skipTest("ffmpeg not installed")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = str(root / "nova.db")
            conn = connect(db)
            migrate(conn)
            conn.execute("INSERT INTO twitch_sources (id, username,"
                         " created_at, updated_at) VALUES (1,'live',0,0)")
            conn.execute("INSERT INTO stream_sessions (id, source_id,"
                         " started_at, state) VALUES (1,1,0,'RECORDING')")
            conn.commit()
            conn.close()
            work = root / "sessions"
            buf = RollingBuffer(str(work), session_id=1, segment_seconds=2,
                                pre_event_seconds=10, post_event_seconds=10)
            buf.bind_db(db)
            out = root / "clips"
            out.mkdir()
            proc = subprocess.Popen(
                [ffmpeg, "-hide_banner", "-loglevel", "error", "-re",
                 "-f", "lavfi", "-i", "testsrc=size=160x90:rate=10",
                 "-t", "16", "-c:v", "libx264", "-preset", "ultrafast",
                 "-pix_fmt", "yuv420p", "-g", "10",
                 "-force_key_frames", "expr:gte(t,n_forced*2)",
                 "-f", "segment", "-segment_time", "2",
                 "-reset_timestamps", "1",
                 str(buf.dir / "seg-%06d.ts")])
            clipper = AutoClipper(buf, db, HeuristicProvider(), ffmpeg,
                                  str(out), min_score=0.0, pre_seconds=pre,
                                  post_seconds=2.0, merge_seconds=30.0)
            live = {"live": _Live()}
            sent = []
            rows_over_time = []
            marked = None
            clip_done_while_recording = False
            t0 = time.time()
            try:
                while time.time() - t0 < 40:
                    time.sleep(1.0)
                    running = proc.poll() is None
                    reconcile_durable_segments(buf, db)
                    c = connect(db)
                    n = c.execute("SELECT count(*) FROM recording_segments"
                                  ).fetchone()[0]
                    c.close()
                    rows_over_time.append((round(time.time() - t0, 1), n,
                                           running))
                    if marked is None and n >= 1 and running:
                        with patch.object(manual_mark.session_registry,
                                          "_sessions", live):
                            reply = clipmark_reply(root, "live 0",
                                                   update_id=1)
                        self.assertIn("Manual mark #1", reply)
                        marked = time.time() - t0
                    clipper.process_pending()
                    c = connect(db)
                    st = c.execute("SELECT status FROM candidate_events"
                                   " WHERE id=1").fetchone()
                    c.close()
                    if st and st[0] == "CLIPPED":
                        clip_done_while_recording = running
                        break
                    if not running and st is None:
                        break
            finally:
                if proc.poll() is None:
                    proc.kill()
                proc.wait()
            c = connect(db)
            status = c.execute("SELECT status FROM candidate_events"
                               " WHERE id=1").fetchone()[0]
            nclips = c.execute("SELECT count(*) FROM clips").fetchone()[0]
            c.close()
            rows_while_recording = [r for r in rows_over_time
                                    if r[2] and r[1] > 0]
            print("live cadence proof: marked_at=%.1fs rows_over_time=%s "
                  "final=%s clip_done_while_recording=%s"
                  % (marked or -1, rows_over_time, status,
                     clip_done_while_recording))
            # durable rows exist while the recorder is still running
            self.assertTrue(rows_while_recording,
                            "no recording_segments rows while recording")
            self.assertIsNotNone(marked)
            self.assertEqual(status, "CLIPPED")
            self.assertEqual(nclips, 1)

    def test_control_without_tick_reconcile_no_rows_during_recording(self):
        """Control: the recording workers alone never write rows mid-run,
        which is why the session tick must reconcile."""
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            self.skipTest("ffmpeg not installed")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = str(root / "nova.db")
            conn = connect(db)
            migrate(conn)
            conn.execute("INSERT INTO twitch_sources (id, username,"
                         " created_at, updated_at) VALUES (1,'live',0,0)")
            conn.execute("INSERT INTO stream_sessions (id, source_id,"
                         " started_at, state) VALUES (1,1,0,'RECORDING')")
            conn.commit()
            conn.close()
            buf = RollingBuffer(str(root / "s"), session_id=1,
                                segment_seconds=2)
            buf.bind_db(db)
            proc = subprocess.Popen(
                [ffmpeg, "-hide_banner", "-loglevel", "error", "-re",
                 "-f", "lavfi", "-i", "testsrc=size=160x90:rate=10",
                 "-t", "7", "-c:v", "libx264", "-preset", "ultrafast",
                 "-g", "10", "-force_key_frames", "expr:gte(t,n_forced*2)",
                 "-f", "segment", "-segment_time", "2",
                 "-reset_timestamps", "1", str(buf.dir / "seg-%06d.ts")])
            try:
                time.sleep(5.5)
                on_disk = len(list(buf.dir.glob("seg-*.ts")))
                running = proc.poll() is None
                c = connect(db)
                n = c.execute("SELECT count(*) FROM recording_segments"
                              ).fetchone()[0]
                c.close()
            finally:
                proc.kill()
                proc.wait()
            self.assertTrue(running)
            self.assertGreaterEqual(on_disk, 2)
            self.assertEqual(n, 0)


if __name__ == "__main__":
    unittest.main()
