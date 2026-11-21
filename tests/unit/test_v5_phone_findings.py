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

    def _prod_layout(self, d):
        files = Path(d).resolve() / "files"
        bindir = files / "home" / "instinct-clip" / media._PREFIX_REL
        bindir.mkdir(parents=True)
        for n in ("ffmpeg", "ffprobe"):
            (bindir / n).write_text("x")
        dep = files / "agt" / "deployments" / "abc"
        (dep / "app" / "bot").mkdir(parents=True)
        return bindir, dep

    def _clean_env(self, **extra):
        env = {k: v for k, v in os.environ.items()
               if not k.startswith("NOVA_")}
        env.update(extra)
        return env

    def test_production_topology_sibling_install_root(self):
        # code, cwd and HOME all under files/agt/deployments/<id>; the
        # install root is the SIBLING files/home/instinct-clip.
        with tempfile.TemporaryDirectory() as d:
            bindir, dep = self._prod_layout(d)
            env = self._clean_env(HOME=str(dep))
            cwd = os.getcwd()
            os.chdir(dep)
            try:
                with mock.patch.dict(os.environ, env, clear=True), \
                        mock.patch.object(media, "__file__",
                                          str(dep / "app/bot/media.py")), \
                        mock.patch("shutil.which", return_value=None):
                    ff, fp = media.ffmpeg_paths()
            finally:
                os.chdir(cwd)
            self.assertEqual((ff, fp), (str(bindir / "ffmpeg"),
                                        str(bindir / "ffprobe")))

    def test_explicit_absolute_tool_dir_wins_over_discovery(self):
        with tempfile.TemporaryDirectory() as d:
            bindir, dep = self._prod_layout(d)
            alt = Path(d).resolve() / "alt"
            alt.mkdir()
            for n in ("ffmpeg", "ffprobe"):
                (alt / n).write_text("x")
            env = self._clean_env(HOME=str(dep), NOVA_TOOL_BIN_DIR=str(alt))
            with mock.patch.dict(os.environ, env, clear=True), \
                    mock.patch("shutil.which", return_value=None):
                self.assertEqual(media.ffmpeg_paths()[0], str(alt / "ffmpeg"))
            env["NOVA_TOOL_BIN_DIR"] = "relative/dir"  # ignored: not absolute
            with mock.patch.dict(os.environ, env, clear=True), \
                    mock.patch.object(media, "__file__",
                                      str(dep / "app/bot/media.py")), \
                    mock.patch("shutil.which", return_value=None):
                self.assertEqual(media.ffmpeg_paths()[0],
                                 str(bindir / "ffmpeg"))

    def test_env_override_wins_and_bare_fallback(self):
        env = {"NOVA_FFMPEG": "/x/ff", "NOVA_FFPROBE": "/x/fp"}
        with mock.patch.dict(os.environ, env):
            self.assertEqual(media.ffmpeg_paths(), ("/x/ff", "/x/fp"))


class NoTzDatabase(unittest.TestCase):
    """Host with NO tz database: ZoneInfo search path is an empty dir and
    tzdata package lookup is blocked; the vendored IANA file is used."""

    def setUp(self):
        import zoneinfo
        self.empty = tempfile.mkdtemp()
        self._old = zoneinfo.TZPATH
        zoneinfo.reset_tzpath(to=[self.empty])
        ZoneInfo.clear_cache()
        self._p = mock.patch.dict("sys.modules", {"tzdata": None, "tzdata.zoneinfo": None})
        self._p.start()
        self.addCleanup(self._p.stop)
        self.addCleanup(zoneinfo.reset_tzpath, to=list(self._old))
        self.addCleanup(ZoneInfo.clear_cache)

    def test_host_really_has_no_zone(self):
        with self.assertRaises(ZoneInfoNotFoundError):
            ZoneInfo("America/Los_Angeles")

    def test_vendored_matches_host_data_incl_far_future_and_folds(self):
        import zoneinfo
        zoneinfo.reset_tzpath(to=list(self._old))
        ZoneInfo.clear_cache()
        try:
            real = ZoneInfo("America/Los_Angeles")
        except ZoneInfoNotFoundError:
            self.skipTest("no host tz database to compare against")
        zoneinfo.reset_tzpath(to=[self.empty])
        ZoneInfo.clear_cache()
        fb = intake.resolve_timezone("America/Los_Angeles")
        self.assertNotEqual(fb.key, None)
        t = datetime(1990, 1, 1, tzinfo=timezone.utc).timestamp()
        end = datetime(2100, 1, 1, tzinfo=timezone.utc).timestamp()
        n = 0
        while t < end:
            a = datetime.fromtimestamp(t, real)
            b = datetime.fromtimestamp(t, fb)
            self.assertEqual((a.utcoffset(), a.dst(), a.tzname()),
                             (b.utcoffset(), b.dst(), b.tzname()), a)
            t += 86400 * 3 + 3571
            n += 1
        self.assertGreater(n, 3000)
        # exact transitions and the ambiguous/nonexistent local hours
        for ts in ("2026-03-08T09:59:59", "2026-03-08T10:00:00",
                   "2026-11-01T08:59:59", "2026-11-01T09:00:00"):
            u = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
            self.assertEqual(u.astimezone(real).utcoffset(),
                             u.astimezone(fb).utcoffset(), ts)
        for fold in (0, 1):
            x = datetime(2026, 11, 1, 1, 30, fold=fold)
            self.assertEqual(x.replace(tzinfo=real).utcoffset(),
                             x.replace(tzinfo=fb).utcoffset())
        x = datetime(2026, 3, 8, 2, 30)
        self.assertEqual(x.replace(tzinfo=real).utcoffset(),
                         x.replace(tzinfo=fb).utcoffset())
        # before the first transition behaves like the host (LMT), no raise
        old = datetime(1800, 1, 1)
        self.assertEqual(old.replace(tzinfo=real).utcoffset(),
                         old.replace(tzinfo=fb).utcoffset())

    def test_authority_initializes_with_no_host_tz_data(self):
        from app.core import database
        d = Path(tempfile.mkdtemp())
        db = str(d / "n.db")
        conn = database.connect(db)
        database.migrate(conn)
        conn.close()
        auth = intake.ClipIntakeAuthority(db)
        self.assertEqual(auth._tz.key, "America/Los_Angeles")

    def test_unknown_and_hostile_names_fail_fast(self):
        for bad in ("Europe/Paris", "../../etc/passwd", "/etc/passwd",
                    "America//Los_Angeles", "", "America/Nowhere"):
            with self.assertRaises((ZoneInfoNotFoundError, ValueError),
                                   msg=bad):
                intake.resolve_timezone(bad)

    def test_vendored_file_pinned(self):
        import hashlib
        f = intake._VENDORED_TZ_DIR / "America" / "Los_Angeles"
        self.assertEqual(
            hashlib.sha256(f.read_bytes()).hexdigest(),
            "200d05754f6d83a371cf408d7085125797657b3b0bebeba1e508cffe86a3e5c8")


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
