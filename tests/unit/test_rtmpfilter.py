import logging
import unittest

from app.bot.rtmpfilter import RtmpKeyRedactor, redact_rtmp


class RtmpRedactTests(unittest.TestCase):
    def test_url_redacted_to_host(self):
        self.assertEqual(
            redact_rtmp("pushing to rtmp://live.twitch.tv/app/live_12345 now"),
            "pushing to rtmp://live.twitch.tv/<redacted> now")

    def test_rtmps_and_multiple(self):
        out = redact_rtmp("a rtmps://h1/k1 b rtmp://h2/app/k2")
        self.assertNotIn("k1", out)
        self.assertNotIn("k2", out)
        self.assertIn("rtmps://h1", out)

    def test_filter_redacts_formatted_record(self):
        rec = logging.LogRecord("x", logging.INFO, __file__, 1,
                                "dest %s failed", ("rtmp://h/secret",), None)
        RtmpKeyRedactor().filter(rec)
        self.assertNotIn("secret", rec.getMessage())

    def test_non_rtmp_untouched(self):
        rec = logging.LogRecord("x", logging.INFO, __file__, 1,
                                "hello %s", ("world",), None)
        RtmpKeyRedactor().filter(rec)
        self.assertEqual(rec.getMessage(), "hello world")


if __name__ == "__main__":
    unittest.main()
