"""Mode A command-path acceptance: /startclip over a registered local
recording runs the supervised chain (record -> detect -> auto-clip ->
edit -> QC -> HELD) with NO smoke-script orchestration, then an
explicit /deliveries flush delivers. Real ffmpeg, real detection,
real editor, real QC - nothing injected except the delivery transport.
"""
import functools
import shutil
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

FFMPEG = shutil.which("ffmpeg")
FFPROBE = shutil.which("ffprobe")


@unittest.skipUnless(FFMPEG and FFPROBE, "ffmpeg not available")
class ModeACommandPathTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        feeds = self.dir / "feeds"
        feeds.mkdir()
        self.src = feeds / "tester.mp4"
        # 12s h264/aac: quiet, 2.5s loud moment at t=6, quiet
        subprocess.run(
            [FFMPEG, "-y", "-f", "lavfi", "-i",
             "testsrc2=size=320x240:rate=15:duration=12",
             "-f", "lavfi", "-i",
             "sine=frequency=600:duration=12,"
             "volume='if(between(t,6,8.5),1.0,0.03)':eval=frame",
             "-c:v", "libx264", "-pix_fmt", "yuv420p",
             "-g", "6",  # regular keyframes so -c copy segmentation can cut
             "-c:a", "aac", "-shortest", str(self.src)],
            capture_output=True, timeout=120, check=True)
        from app.core.database import connect, migrate
        conn = connect(str(self.dir / "nova.db"))
        migrate(conn)
        conn.execute(
            "INSERT INTO twitch_sources (username, created_at, updated_at)"
            " VALUES ('tester', 0, 0)")
        conn.commit()
        conn.close()

    def test_startclip_produces_held_clip_then_flush_delivers(self):
        from app.bot import ops, sessions
        from app.pipeline.session import ClipSession
        fast = functools.partial(ClipSession, segment_seconds=2,
                                 tick_seconds=1.0,
                                 clip_pre_seconds=4, clip_post_seconds=4)
        reply = sessions.startclip_reply(self.dir, "tester", FFMPEG,
                                         FFPROBE, session_factory=fast)
        self.assertIn("started", reply.lower(), reply)
        session = sessions._sessions["tester"]
        # wait for a HELD clip (bounded)
        deadline = time.time() + 60
        held = None
        while time.time() < deadline:
            conn = sqlite3.connect(str(self.dir / "nova.db"))
            held = conn.execute(
                "SELECT id, file_path, status FROM clips"
                " WHERE status='HELD'").fetchone()
            conn.close()
            if held:
                break
            if session.error:
                break
            time.sleep(1.0)
        sessions.stopclip_reply(self.dir)
        session.join(timeout=30)
        self.assertIsNone(session.error, session.error)
        self.assertIsNotNone(held, "no HELD clip produced by the "
                                   "supervised session")
        path = Path(held[1])
        self.assertTrue(path.exists())
        self.assertGreater(path.stat().st_size, 10000)
        # real vertical edit applied
        probe = subprocess.run(
            [FFPROBE, "-v", "quiet", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-of", "csv=p=0",
             str(path)], capture_output=True, text=True, timeout=60)
        w, h = probe.stdout.strip().split(",")[:2]
        self.assertEqual((int(w), int(h)), (1080, 1920))
        # session closed honestly
        conn = sqlite3.connect(str(self.dir / "nova.db"))
        state = conn.execute(
            "SELECT state FROM stream_sessions ORDER BY id DESC"
            " LIMIT 1").fetchone()[0]
        conn.close()
        self.assertIn(state, ("ENDED", "INTERRUPTED"))
        # explicit flush delivers every HELD clip (the loud window spans
        # two segments, so two candidates clip and deliver)
        conn = sqlite3.connect(str(self.dir / "nova.db"))
        n_held = conn.execute(
            "SELECT count(*) FROM clips WHERE status='HELD'").fetchone()[0]
        conn.close()
        sent = []
        out = ops.deliveries_reply(
            self.dir, "flush",
            deliver_fn=lambda p, c: sent.append(p) or 7)
        self.assertIn(f"Delivered {n_held}", out)
        self.assertEqual(len(sent), n_held)

    def test_startclip_without_feed_fails_loudly(self):
        from app.bot import sessions
        from app.core.database import connect
        conn = connect(str(self.dir / "nova.db"))
        conn.execute(
            "INSERT INTO twitch_sources (username, created_at,"
            " updated_at) VALUES ('nofeed', 0, 0)")
        conn.commit()
        conn.close()
        reply = sessions.startclip_reply(self.dir, "nofeed", FFMPEG,
                                         FFPROBE)
        self.assertIn("No authorized feed", reply)
        self.assertNotIn("nofeed", sessions._sessions)

    def test_crash_recovery_marks_interrupted(self):
        from app.pipeline.session import recover_interrupted_sessions
        from app.core.database import connect
        conn = connect(str(self.dir / "nova.db"))
        conn.execute(
            "INSERT INTO stream_sessions (source_id, state, started_at,"
            " ingest_mode) VALUES (1, 'RECORDING', 0, 'feed')")
        conn.commit()
        conn.close()
        self.assertEqual(recover_interrupted_sessions(self.dir), 1)
        conn = connect(str(self.dir / "nova.db"))
        state = conn.execute(
            "SELECT state FROM stream_sessions").fetchone()[0]
        conn.close()
        self.assertEqual(state, "INTERRUPTED")


if __name__ == "__main__":
    unittest.main()
