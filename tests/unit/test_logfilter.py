import logging
import unittest

from app.bot.logfilter import NetworkRetryCondenser


class FakeNetworkError(Exception):
    pass


class CondenserTests(unittest.TestCase):
    def _record(self, exc_info=None):
        return logging.LogRecord("telegram", logging.ERROR, __file__, 1,
                                 "polling failed", (), exc_info)

    def test_network_error_condensed(self):
        import app.bot.logfilter as lf
        original = lf.NetworkError
        lf.NetworkError = FakeNetworkError
        try:
            rec = self._record((FakeNetworkError, FakeNetworkError("x"), None))
            NetworkRetryCondenser().filter(rec)
            self.assertIsNone(rec.exc_info)
            self.assertEqual(rec.levelno, logging.WARNING)
            self.assertIn("recoverable", rec.msg % rec.args)
            self.assertIn("FakeNetworkError", rec.msg % rec.args)
        finally:
            lf.NetworkError = original

    def test_other_exceptions_untouched(self):
        rec = self._record((ValueError, ValueError("boom"), None))
        NetworkRetryCondenser().filter(rec)
        self.assertIsNotNone(rec.exc_info)
        self.assertEqual(rec.levelno, logging.ERROR)
        self.assertEqual(rec.msg, "polling failed")

    def test_plain_records_untouched(self):
        rec = self._record()
        NetworkRetryCondenser().filter(rec)
        self.assertEqual(rec.msg, "polling failed")


if __name__ == "__main__":
    unittest.main()
