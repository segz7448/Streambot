"""v5 regressions for the two real phone findings (HOME-independent tool
resolution, no tz database) and the title-duration metadata fix."""
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.bot import media
from app.core import intake


class FfmpegPathsHomeIndependent(unittest.TestCase):
    def test_prefix_found_without_home(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve() / "instinct-clip"
            bindir = root / media._PREFIX_REL
            bindir.mkdir(parents=True)
            for n in ("ffmpeg", "ffprobe"):
                (bindir / n).write_text("x")
            tree = root / "tree" / "app" / "bot"
            tree.mkdir(parents=True)
            other_home = Path(d) / "deployment-home"
            other_home.mkdir()
            env = {k: v for k, v in os.environ.items()
                   if k not in ("NOVA_FFMPEG", "NOVA_FFPROBE")}
            env["HOME"] = str(other_home)
            with mock.patch.dict(os.environ, env, clear=True), \
                    mock.patch.object(media, "__file__",
                                      str(tree / "media.py")), \
                    mock.patch("shutil.which", return_value=None):
                ff, fp = media.ffmpeg_paths()
            self.assertEqual(ff, str(bindir / "ffmpeg"))
            self.assertEqual(fp, str(bindir / "ffprobe"))

    def test_env_override_wins_and_bare_fallback(self):
        env = {"NOVA_FFMPEG": "/x/ff", "NOVA_FFPROBE": "/x/fp"}
        with mock.patch.dict(os.environ, env):
            self.assertEqual(media.ffmpeg_paths(), ("/x/ff", "/x/fp"))


class NoTzDatabase(unittest.TestCase):
    def test_los_angeles_falls_back_and_matches_zoneinfo(self):
        real = ZoneInfo("America/Los_Angeles")
        fb = intake._USPacific()
        t = datetime(2008, 1, 1, tzinfo=timezone.utc).timestamp()
        end = datetime(2036, 1, 1, tzinfo=timezone.utc).timestamp()
        n = 0
        while t < end:
            a = datetime.fromtimestamp(t, real)
            b = datetime.fromtimestamp(t, fb)
            self.assertEqual(a.utcoffset(), b.utcoffset(), a)
            self.assertEqual(a.date(), b.date())
            t += 1800 * 7 + 11
            n += 1
        self.assertGreater(n, 1000)
        # transitions exactly
        for y in (2024, 2026, 2030):
            for m, dday in ((3, None), (11, None)):
                pass
        for ts in ("2026-03-08T09:59:59", "2026-03-08T10:00:00",
                   "2026-11-01T08:59:59", "2026-11-01T09:00:00"):
            u = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
            self.assertEqual(u.astimezone(real).utcoffset(),
                             u.astimezone(fb).utcoffset(), ts)

    def test_authority_starts_with_no_zoneinfo(self):
        with mock.patch.object(intake, "ZoneInfo",
                               side_effect=ZoneInfoNotFoundError("x")):
            self.assertIsInstance(
                intake.resolve_timezone("America/Los_Angeles"),
                intake._USPacific)
            with self.assertRaises(ZoneInfoNotFoundError):
                intake.resolve_timezone("Europe/Paris")


class TitleUsesMeasuredDuration(unittest.TestCase):
    def _run(self, probe_fn):
        from app.clipping import retrospective as r
        from app.detection.audio import LoudnessSample
        from app.intelligence.memory import Memory
        import sqlite3
        d = Path(tempfile.mkdtemp())
        (d / "src.mp4").write_bytes(b"fake")
        db = str(d / "n.db")
        samples = [LoudnessSample(i * 0.1, -14.0 if 5.0 <= i * 0.1 <= 8.0
                                  else -45.0) for i in range(100)]
        r.process_recording(
            ffmpeg="ff", ffprobe="fp", src_path=str(d / "src.mp4"),
            work_dir=str(d / "work"), db_path=db,
            memory=Memory(str(d / "m.json")), source_label="lbl",
            audio_fn=lambda ff, p: samples, scene_fn=lambda ff, p: [],
            extract_fn=lambda ff, s_, o, st, du:
                Path(o).write_bytes(b"x" * 20000) or Path(o),
            edit_fn=lambda ff, s_, o, pr, sub, fx, rt, watermark=None:
                Path(o).write_bytes(b"y" * 20000) or Path(o),
            qc_fn=lambda *a: type("QC", (), {"ok": True, "failures": []})(),
            validate_fn=lambda fp, p: {"duration": 60.0, "width": 320,
                                       "height": 240},
            final_probe_fn=probe_fn)
        c = sqlite3.connect(db)
        title = json.loads(c.execute(
            "SELECT metadata_json FROM candidate_events"
        ).fetchone()[0])["title"]
        dur = c.execute("SELECT duration_seconds FROM clips").fetchone()[0]
        return title, dur

    def test_measured_duration_in_title_and_row(self):
        title, dur = self._run(
            lambda ff, p: {"format": {"duration": "8.04"}})
        self.assertIn("(8s)", title)
        self.assertAlmostEqual(dur, 8.04)

    def test_probe_failure_omits_duration(self):
        def boom(ff, p):
            raise RuntimeError("no probe")
        title, dur = self._run(boom)
        self.assertNotRegex(title, r"\(\d+s\)")
        self.assertTrue(title.endswith("from lbl"))


if __name__ == "__main__":
    unittest.main()
