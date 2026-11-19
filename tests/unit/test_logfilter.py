import logging
import unittest

from app.bot.logfilter import NetworkRetryCondenser


class FakeNetworkError(Exception):
    pass


def _record(name="telegram.ext.Updater",
            msg="Exception happened while polling for updates.",
            exc_info=None):
    return logging.LogRecord(name, logging.ERROR, __file__, 1, msg, (),
                             exc_info)


class CondenserTests(unittest.TestCase):
    def setUp(self):
        import app.bot.logfilter as lf
        self._original = lf.NetworkError
        lf.NetworkError = FakeNetworkError
        self.addCleanup(setattr, lf, "NetworkError", self._original)
        self.exc = (FakeNetworkError, FakeNetworkError("x"), None)

    def test_known_recoverable_record_condensed(self):
        rec = _record(exc_info=self.exc)
        NetworkRetryCondenser().filter(rec)
        self.assertIsNone(rec.exc_info)
        self.assertEqual(rec.levelno, logging.WARNING)
        out = rec.msg % rec.args
        self.assertIn("recoverable", out)
        self.assertIn("FakeNetworkError", out)
        self.assertNotIn("x", out)  # exception text never carried over

    def test_other_exceptions_untouched(self):
        rec = _record(exc_info=(ValueError, ValueError("boom"), None))
        NetworkRetryCondenser().filter(rec)
        self.assertIsNotNone(rec.exc_info)
        self.assertEqual(rec.levelno, logging.ERROR)

    def test_fatal_message_same_exception_untouched(self):
        # InvalidToken/fatal paths log different messages; never condensed
        rec = _record(msg="Network Retry Loop (Polling Updates): Invalid "
                          "token. Aborting retry loop.", exc_info=self.exc)
        NetworkRetryCondenser().filter(rec)
        self.assertIsNotNone(rec.exc_info)
        self.assertEqual(rec.levelno, logging.ERROR)

    def test_other_logger_untouched(self):
        rec = _record(name="telegram.ext.Application", exc_info=self.exc)
        NetworkRetryCondenser().filter(rec)
        self.assertIsNotNone(rec.exc_info)

    def test_plain_records_untouched(self):
        rec = _record()
        NetworkRetryCondenser().filter(rec)
        self.assertEqual(rec.getMessage(),
                         "Exception happened while polling for updates.")


if __name__ == "__main__":
    unittest.main()
