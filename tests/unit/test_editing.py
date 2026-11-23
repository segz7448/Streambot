"""Phase 011 editing tests: graph/args construction, no ffmpeg execution."""
import unittest

from app.editing.editor import build_filter_graph, edit_args
from app.editing.presets import HORIZONTAL, PRESETS, SQUARE, VERTICAL


class PresetTests(unittest.TestCase):
    def test_presets_registered(self):
        self.assertEqual(set(PRESETS), {"vertical", "square", "horizontal"})
        self.assertAlmostEqual(VERTICAL.width / VERTICAL.height, 9 / 16)

    def test_vertical_graph_scales_to_target(self):
        g = build_filter_graph(VERTICAL)
        self.assertIn("scale=1080:1920", g)
        self.assertIn("crop=", g)

    def test_square_uses_one_to_one(self):
        g = build_filter_graph(SQUARE)
        self.assertIn("scale=1080:1080", g)

    def test_subtitles_burned_last(self):
        g = build_filter_graph(VERTICAL, subtitle_path="/tmp/captions.ass")
        self.assertTrue(g.endswith("subtitles='/tmp/captions.ass'"))

    def test_subtitle_path_colon_escaped(self):
        g = build_filter_graph(VERTICAL, subtitle_path="/data/a:b.ass")
        self.assertIn("a\\:b.ass", g)

    def test_focus_shifts_crop(self):
        left = build_filter_graph(VERTICAL, focus_x=0.0)
        right = build_filter_graph(VERTICAL, focus_x=1.0)
        self.assertIn("*0.000", left)
        self.assertIn("*1.000", right)

    def test_full_framing_preserves_all_source_pixels(self):
        g = build_filter_graph(VERTICAL, framing="full")
        # no blind crop of the source: the foreground branch only
        # scales the full frame; the crop applies to the blurred
        # background fill only
        self.assertIn("split=2", g)
        self.assertIn("[src_fg]scale=1080:-2[fg]", g)
        self.assertIn("overlay=(W-w)/2:(H-h)/2", g)
        self.assertNotIn("[src_fg]crop", g)

    def test_full_framing_threads_through_edit_args(self):
        args = edit_args("ffmpeg", "in.mp4", "out.mp4", VERTICAL,
                         framing="full")
        self.assertIn("split=2", args[args.index("-vf") + 1])

    def test_default_framing_unchanged(self):
        self.assertEqual(build_filter_graph(VERTICAL),
                         build_filter_graph(VERTICAL, framing="crop"))

    def test_edit_args_full(self):
        args = edit_args("ffmpeg", "in.mp4", "out.mp4", HORIZONTAL)
        self.assertIn("libx264", args)
        self.assertIn("loudnorm=I=-16:TP=-1.5:LRA=11", args)
        self.assertIn("-crf", args)
        self.assertEqual(args[-1], "out.mp4")
        i = args.index("-t")
        self.assertEqual(args[i + 1], str(HORIZONTAL.max_duration_seconds))


if __name__ == "__main__":
    unittest.main()
