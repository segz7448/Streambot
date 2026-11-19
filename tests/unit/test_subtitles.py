"""Subtitle style tests: geometry derived from output frame."""
import unittest

from app.editing.presets import VERTICAL
from app.subtitles.style import Cue, default_ass


class AssTests(unittest.TestCase):
    def test_playres_matches_output(self):
        doc = default_ass(VERTICAL, [Cue(0.5, 3.0, "Hello")])
        self.assertIn("PlayResX: 1080", doc)
        self.assertIn("PlayResY: 1920", doc)

    def test_fontsize_proportional(self):
        doc = default_ass(VERTICAL, [Cue(0, 1, "x")])
        self.assertIn(",63,", doc)  # 1920 * 0.033 = 63.36 -> 63

    def test_safe_area_margins(self):
        doc = default_ass(VERTICAL, [Cue(0, 1, "x")])
        self.assertIn(",115", doc)  # MarginV = 1920 * 0.06

    def test_timestamps(self):
        doc = default_ass(VERTICAL, [Cue(0.5, 3.25, "Hi")])
        self.assertIn("0:00:00.50,0:00:03.25", doc)

    def test_newlines_become_ass_wraps(self):
        doc = default_ass(VERTICAL, [Cue(0, 1, "a\nb")])
        self.assertIn("a\\Nb", doc)


if __name__ == "__main__":
    unittest.main()
