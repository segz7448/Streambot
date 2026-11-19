import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app.subtitles.fonts import ensure_fontconfig_env


class FontconfigEnvTests(unittest.TestCase):
    def test_conf_written_for_prefix_fonts(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            ff = root / "usr" / "bin"
            (ff.parent / "share" / "fonts").mkdir(parents=True)
            ff.mkdir(parents=True)
            ffmpeg = ff / "ffmpeg"
            ffmpeg.touch()
            cache = root / "cache"
            env = ensure_fontconfig_env(str(ffmpeg), str(cache))
            self.assertIn("FONTCONFIG_FILE", env)
            conf = Path(env["FONTCONFIG_FILE"]).read_text()
            self.assertIn(str(ff.parent / "share" / "fonts"), conf)
            self.assertIn(str(cache / "fc-cache"), conf)

    def test_no_fonts_dir_returns_empty(self):
        with TemporaryDirectory() as td:
            ffmpeg = Path(td) / "bin" / "ffmpeg"
            ffmpeg.parent.mkdir(parents=True)
            ffmpeg.touch()
            import app.subtitles.fonts as f
            orig = f._font_dirs
            f._font_dirs = lambda _p: []
            try:
                self.assertEqual(
                    ensure_fontconfig_env(str(ffmpeg), str(Path(td) / "c")),
                    {})
            finally:
                f._font_dirs = orig


if __name__ == "__main__":
    unittest.main()
