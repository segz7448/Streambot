import json
import logging
import os
import tempfile
import unittest

import app.core.logging as nl


class ServiceLoggingTest(unittest.TestCase):
    def setUp(self):
        nl._CONFIGURED = False
        self._saved = {n: (list(logging.getLogger(n).handlers),
                           logging.getLogger(n).level)
                       for n in ("nova", "telegram", "httpx", "httpcore", "")}
        for n in ("nova", "telegram", ""):
            logging.getLogger(n).handlers = []

    def tearDown(self):
        for n, (h, lvl) in self._saved.items():
            lg = logging.getLogger(n)
            for x in list(lg.handlers):
                if x not in h:
                    x.close()
            lg.handlers = h
            lg.setLevel(lvl)
        nl._CONFIGURED = False

    def test_writes_info_and_framework_records_redacted(self):
        with tempfile.TemporaryDirectory() as d:
            nl.setup_service_logging(os.path.join(d, "logs"))
            tok = "123456789:" + "A" * 35
            nl.get_logger("x").info("hello %s", tok)
            logging.getLogger("telegram.ext.Application").info("polling up")
            for h in logging.getLogger("nova").handlers:
                h.flush()
            self.assertEqual(logging.getLogger("httpx").level,
                             logging.WARNING)
            with open(os.path.join(d, "logs", "nova.log")) as fh:
                lines = [json.loads(x) for x in fh]
            msgs = " ".join(x["msg"] for x in lines)
            self.assertIn("polling up", msgs)
            self.assertIn("[redacted-token]", msgs)
            self.assertNotIn("A" * 35, msgs)
            for h in logging.getLogger("nova").handlers:
                h.close()

    def test_args_exception_and_http_url_redacted(self):
        with tempfile.TemporaryDirectory() as d:
            nl.setup_service_logging(os.path.join(d, "logs"))
            tok = "987654321:" + "Zz9_-" * 7
            url = "https://api.telegram.org/bot%s/getUpdates" % tok
            nl.get_logger("x").warning("args %s and %r", tok, (tok,))
            try:
                raise RuntimeError("failed GET " + url)
            except RuntimeError:
                logging.getLogger("telegram.ext.Updater").error(
                    "boom", exc_info=True)
            logging.getLogger("httpx").info("HTTP Request: GET %s", url)
            logging.getLogger("httpx").warning("HTTP Request: GET %s", url)
            logging.getLogger("asyncio").error("x %s", url)
            for h in logging.getLogger("nova").handlers:
                h.flush()
            with open(os.path.join(d, "logs", "nova.log")) as fh:
                text = fh.read()
            self.assertNotIn("Zz9_-", text)
            self.assertNotIn("987654321", text)
            self.assertIn("[redacted-token]", text)
            self.assertIn("HTTP Request", text)  # warning kept, info dropped
            self.assertEqual(text.count("HTTP Request"), 1)
            for h in logging.getLogger("nova").handlers:
                h.close()


if __name__ == "__main__":
    unittest.main()
