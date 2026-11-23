"""Actual-path integration: a sealed interrupted-session clip finished
through finalize_interrupted_sessions (the production live/recovery
finish) preserves ALL source edges under the default full framing and
keeps its audio track. Uses real ffmpeg end to end, no mocks of the
edit itself."""
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.bot import restartproof as rp          # noqa: E402
from app.bot import source_auth                 # noqa: E402
from app.clipping.seal import seal_file         # noqa: E402
from app.core.database import connect, migrate  # noqa: E402
from app.pipeline import session as ps          # noqa: E402

FFMPEG = os.environ.get("NOVA_FFMPEG", "ffmpeg")
FFPROBE = os.environ.get("NOVA_FFPROBE", "ffprobe")

CORNERS = {  # name -> (source drawbox, expected rgb, src x, src y)
    "red_tl": ("drawbox=x=0:y=0:w=64:h=64:c=red:t=fill", (255, 0, 0),
               0, 0),
    "green_tr": ("drawbox=x=1216:y=0:w=64:h=64:c=green:t=fill",
                 (0, 128, 0), 1216, 0),
    "blue_bl": ("drawbox=x=0:y=656:w=64:h=64:c=blue:t=fill", (0, 0, 255),
                0, 656),
    "yellow_br": ("drawbox=x=1216:y=656:w=64:h=64:c=yellow:t=fill",
                  (255, 255, 0), 1216, 656),
}
# 1280x720 scaled by min(1080/1280, 1920/720) = 0.84375 -> 1080x608,
# centered on the 1080x1920 canvas at y offset (1920-608)/2 = 656
SCALE = 1080 / 1280
YOFF = (1920 - 608) // 2


def _mean_rgb(video: str, x: int, y: int, w: int = 24, h: int = 24):
    out = subprocess.run(
        [FFMPEG, "-hide_banner", "-loglevel", "error", "-ss", "1",
         "-i", video, "-frames:v", "1",
         "-vf", f"crop={w}:{h}:{x}:{y}", "-f", "rawvideo",
         "-pix_fmt", "rgb24", "-"],
        capture_output=True, check=True).stdout
    n = len(out) // 3
    return tuple(sum(out[c::3]) / n for c in range(3))


class FullFramingPathTests(unittest.TestCase):
    def test_finalize_preserves_all_edges_and_audio(self):
        d = Path(tempfile.mkdtemp())
        raw_src = d / "gen.mp4"
        vf = ",".join(v[0] for v in CORNERS.values())
        subprocess.run(
            [FFMPEG, "-hide_banner", "-loglevel", "error", "-y",
             "-f", "lavfi", "-i", "testsrc=size=1280x720:rate=15",
             "-f", "lavfi", "-i", "sine=frequency=440",
             "-t", "4", "-vf", vf,
             "-c:v", "libx264", "-g", "15", "-pix_fmt", "yuv420p",
             "-c:a", "aac", str(raw_src)], check=True)

        conn = connect(str(d / "nova.db"))
        migrate(conn)
        conn.execute("INSERT INTO twitch_sources (username, created_at,"
                     " updated_at) VALUES ('chan',0,0)")
        src_id = conn.execute("SELECT id FROM twitch_sources").fetchone()[0]
        conn.execute("INSERT INTO stream_sessions (source_id, state,"
                     " ingest_mode, notes) VALUES (?,?,?,?)",
                     (src_id, "INTERRUPTED", "feed", "{}"))
        sid = conn.execute("SELECT id FROM stream_sessions").fetchone()[0]
        conn.execute("INSERT INTO candidate_events (session_id,"
                     " detected_at, kind) VALUES (?,0,'moment')", (sid,))
        ev = conn.execute("SELECT id FROM candidate_events").fetchone()[0]
        work = d / "sessions" / "chan"
        work.mkdir(parents=True)
        (d / "runtime").mkdir()
        raw = work / "raw-1.mp4"
        raw.write_bytes(raw_src.read_bytes())
        sha, nbytes = seal_file(raw)
        conn.execute(
            "INSERT INTO clips (event_id, session_id, file_path,"
            " duration_seconds, status, created_at, sealed_sha256,"
            " sealed_bytes, sealed_at) VALUES (?,?,?,?,?,0,?,?,1.0)",
            (ev, sid, str(raw), 4.0, "PENDING", sha, nbytes))
        cid = conn.execute("SELECT id FROM clips").fetchone()[0]
        conn.commit()
        conn.close()
        source_auth.set_scope(d, "chan", "record", True)
        registry = work / f"writers-{sid}.json"
        self.assertTrue(rp.init_registry(registry))
        # age every file so the writer-quiet proof does not fence us
        old = time.time() - 400
        for root, _, files in os.walk(d):
            for f in files:
                os.utime(Path(root) / f, (old, old))

        held = ps.finalize_interrupted_sessions(d, FFMPEG, FFPROBE)
        self.assertEqual(held, 1)
        conn = connect(str(d / "nova.db"))
        row = conn.execute("SELECT status, file_path, qc_report_json"
                           " FROM clips WHERE id=?", (cid,)).fetchone()
        conn.close()
        self.assertEqual(row[0], "HELD")
        out = row[1]
        self.assertTrue(Path(out).exists())

        # every corner of the source survives at its scaled position
        for name, (spec, rgb, sx, sy) in CORNERS.items():
            ox = int((sx + 32) * SCALE) - 12
            oy = int((sy + 32) * SCALE) + YOFF - 12
            mean = _mean_rgb(out, ox, oy)
            for got, want in zip(mean, rgb):
                self.assertAlmostEqual(got, want, delta=40,
                                       msg=f"{name} lost: {mean}")

        # audio preserved and normalized through the real path
        streams = subprocess.run(
            [FFPROBE, "-v", "error", "-show_entries",
             "stream=codec_type,codec_name", "-of", "csv=p=0", out],
            capture_output=True, text=True, check=True).stdout
        self.assertIn("aac,audio", streams)
        dims = subprocess.run(
            [FFPROBE, "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-of", "csv=p=0",
             out], capture_output=True, text=True, check=True).stdout
        self.assertEqual(dims.strip(), "1080,1920")
        meta = json.loads(row[2]) if row[2] else {}
        self.assertIn("full framing",
                      " ".join(meta.get("plan_reasons", [])))


if __name__ == "__main__":
    unittest.main()
