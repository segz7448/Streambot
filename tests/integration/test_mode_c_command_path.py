"""Command-path integration: mode C drives the REAL chain (real ffmpeg
detection -> extraction -> edit -> QC -> held clip -> explicit flush
delivery) with no smoke-script orchestration. Skips without ffmpeg."""
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path

FFMPEG = shutil.which("ffmpeg")
FFPROBE = shutil.which("ffprobe")


@unittest.skipUnless(FFMPEG and FFPROBE, "ffmpeg not available")
class ModeCCommandPathTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.src = self.dir / "recording.mp4"
        # 8s h264/aac recording: quiet, 2s loud moment at t=4, quiet
        subprocess.run(
            [FFMPEG, "-y", "-f", "lavfi", "-i",
             "testsrc2=size=320x240:rate=15:duration=8",
             "-f", "lavfi", "-i",
             "sine=frequency=600:duration=8,"
             "volume='if(between(t,4,6),1.0,0.03)':eval=frame",
             "-c:v", "libx264", "-pix_fmt", "yuv420p",
             "-c:a", "aac", "-shortest", str(self.src)],
            capture_output=True, timeout=120, check=True)

    def test_recording_to_held_clip_to_explicit_delivery(self):
        from app.bot import ops
        from app.clipping import retrospective
        from app.intelligence.memory import Memory
        mem = Memory(str(self.dir / "memory.json"))
        result = retrospective.process_recording(
            FFMPEG, FFPROBE, str(self.src), str(self.dir / "work"),
            str(self.dir / "nova.db"), mem, "owner-upload:test",
            runtime_dir=str(self.dir / "runtime"))
        self.assertEqual(result["status"], "held", result)
        self.assertEqual(result["event_kind"], "audio_spike")
        self.assertTrue(result["qc_passed"])
        # content-sized: the moment is ~2s; pre-roll + tail => well
        # under the 180s cap, never padded to it
        self.assertLessEqual(result["clip_duration_seconds"], 30.0)
        import sqlite3 as _sq
        _c = _sq.connect(str(self.dir / "nova.db"))
        _row = _c.execute("SELECT file_path FROM clips WHERE id=?",
                          (result["clip_id"],)).fetchone()
        _c.close()
        edited = Path(_row[0])
        self.assertTrue(edited.exists())
        self.assertGreater(edited.stat().st_size, 10000)
        # vertical preset actually applied
        probe = subprocess.run(
            [FFPROBE, "-v", "quiet", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-of", "csv=p=0",
             str(edited)], capture_output=True, text=True, timeout=60)
        w, h = probe.stdout.strip().split(",")[:2]
        self.assertEqual((int(w), int(h)), (1080, 1920))

        # /deliveries: listed as held, then explicit flush delivers
        listing = ops.deliveries_reply(self.dir, "")
        self.assertIn("Held clips", listing)
        sent = []
        flush = ops.deliveries_reply(
            self.dir, "flush",
            deliver_fn=lambda path, caption: sent.append((path, caption)) or 42)
        self.assertIn("Delivered 1", flush)
        self.assertEqual(len(sent), 1)
        conn = sqlite3.connect(str(self.dir / "nova.db"))
        status, delivered_at = conn.execute(
            "SELECT status, delivered_at FROM clips").fetchone()
        conn.close()
        self.assertEqual(status, "DELIVERED")
        self.assertIsNotNone(delivered_at)
        self.assertEqual(ops.deliveries_reply(self.dir, ""),
                         "No held clips (auto-delivery on). "
                         "Send a video to make one.")

    def test_ingest_journal_replay_guard(self):
        from app.bot import ops
        ops.journal_record(self.dir, 77, "ingest", "Clip ready (HELD)")
        cached = ops.journal_result(self.dir, 77, "ingest")
        self.assertEqual(cached, "Clip ready (HELD)")

    def test_drain_ingest_strand_is_uncertain_not_executed(self):
        from app.bot.commands import CommandRouter
        from app.bot.inbox import PreAckInbox
        from app.bot.ops import drain_inbox, journal_uncertain
        from app.bot.watermark import UpdateWatermark
        inbox = PreAckInbox(str(self.dir / "inbox.json"))
        wm = UpdateWatermark(str(self.dir / "updates.json"))
        inbox.add(88, 111, "/ingest")
        sent = []
        result = drain_inbox(self.dir, CommandRouter(admin_ids=[111]), wm,
                             inbox,
                             reply=lambda uid, text: sent.append(text),
                             admin_id=111)
        self.assertEqual(result["uncertain"], 1)
        self.assertEqual(inbox.pending(), [])
        self.assertTrue(any("re-send" in s for s in sent))
        self.assertEqual(journal_uncertain(self.dir)[0]["command"], "ingest")


if __name__ == "__main__":
    unittest.main()
