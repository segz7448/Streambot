import json
import logging
import os
import tempfile
import unittest

import app.core.logging as nl


class ServiceLoggingTest(unittest.TestCase):
    def setUp(self):
        import sys
        nl._SERVICE_HANDLERS = None
        import threading
        self._thook = threading.excepthook
        self._hook, self._lr = sys.excepthook, logging.lastResort
        self._child = logging.getLogger("telegram.ext.Updater")
        self._child_state = (list(self._child.handlers),
                             self._child.propagate)
        self._saved = {n: (list(logging.getLogger(n).handlers),
                           logging.getLogger(n).level,
                           logging.getLogger(n).propagate)
                       for n in ("nova", "telegram", "httpx", "httpcore", "")}

    def tearDown(self):
        import sys
        for x in nl._SERVICE_HANDLERS or []:
            x.close()
        for n, (h, lvl, prop) in self._saved.items():
            lg = logging.getLogger(n)
            lg.handlers = h
            lg.setLevel(lvl)
            lg.propagate = prop
        nl._SERVICE_HANDLERS = None
        import threading
        threading.excepthook = self._thook
        self._child.handlers, self._child.propagate = self._child_state
        sys.excepthook, logging.lastResort = self._hook, self._lr

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

    def test_headers_params_encoded_and_excepthook(self):
        import sys
        with tempfile.TemporaryDirectory() as d:
            nl.setup_service_logging(os.path.join(d, "logs"))
            secrets = ["SYNTH-bearer-abcdef123456", "SYNTH-cookie-777",
                       "SYNTHsig999aaa", "SYNTHrefresh555", "SYNTHkey321",
                       "SYNTHamz444", "SYNTHbasic8888xx", "SYNTHhdr222",
                       "SYNTHpw000"]
            enc = "555444333%3A" + "Qq1" * 10
            lg = logging.getLogger("httpx")
            lg.warning("h Authorization: Bearer %s", secrets[0])
            lg.warning("h Cookie: sid=%s", secrets[1])
            lg.error("u https://x.example/a?sig=%s&ok=1&refresh_token=%s"
                     "&key=%s&X-Amz-Signature=%s", secrets[2], secrets[3],
                     secrets[4], secrets[5])
            lg.error("b Authorization: Basic %s", secrets[6])
            lg.error("hdr {'X-Api-Token': '%s'}", secrets[7])
            lg.error("form password=%s&user=u", secrets[8])
            lg.error("enc https://a/bot%s/x", enc)
            try:
                raise ValueError("t " + enc)
            except ValueError:
                sys.excepthook(*sys.exc_info())
            for h in logging.getLogger("nova").handlers:
                h.flush()
            with open(os.path.join(d, "logs", "nova.log")) as fh:
                text = fh.read()
            for sec in secrets + ["Qq1Qq1", "555444333"]:
                self.assertNotIn(sec, text)
            self.assertIn("ok=1", text)
            self.assertIn("uncaught exception", text)
            self.assertIsNone(logging.lastResort)
            for h in logging.getLogger("nova").handlers:
                h.close()

    def test_preexisting_handlers_replaced_and_singleton(self):
        pre = logging.StreamHandler()
        logging.getLogger("telegram").addHandler(pre)
        logging.getLogger().addHandler(logging.StreamHandler())
        with tempfile.TemporaryDirectory() as d:
            nl.setup_service_logging(os.path.join(d, "logs"))
            self.assertNotIn(pre, logging.getLogger("telegram").handlers)
            self.assertEqual(len(logging.getLogger().handlers), 2)
            n = len(logging.getLogger("nova").handlers)
            nl.setup_service_logging(os.path.join(d, "other"))
            self.assertEqual(len(logging.getLogger("nova").handlers), n)
            self.assertFalse(os.path.exists(os.path.join(d, "other")))
            self.assertEqual(os.stat(os.path.join(d, "logs")).st_mode & 0o777,
                             0o700)
            p = os.path.join(d, "logs", "nova.log")
            self.assertEqual(os.stat(p).st_mode & 0o777, 0o600)
            h = [x for x in logging.getLogger("nova").handlers
                 if hasattr(x, "doRollover")][0]
            h.doRollover()
            logging.getLogger("nova.x").warning("after")
            for f in os.listdir(os.path.join(d, "logs")):
                self.assertEqual(
                    os.stat(os.path.join(d, "logs", f)).st_mode & 0o777,
                    0o600, f)

    def test_failure_leaves_logging_untouched(self):
        with tempfile.TemporaryDirectory() as d:
            blocker = os.path.join(d, "f")
            open(blocker, "w").close()
            before = {n: list(logging.getLogger(n).handlers)
                      for n in ("nova", "telegram", "")}
            with self.assertRaises(OSError):
                nl.setup_service_logging(os.path.join(blocker, "logs"))
            self.assertIsNone(nl._SERVICE_HANDLERS)
            for n, hs in before.items():
                self.assertEqual(logging.getLogger(n).handlers, hs)
            # retry on a good dir works and does not duplicate
            nl.setup_service_logging(os.path.join(d, "logs"))
            self.assertEqual(len(logging.getLogger("nova").handlers), 2)

    def test_framework_child_handler_thread_hook_and_malformed_format(self):
        import io
        import sys
        import threading
        leaky = io.StringIO()
        self._child.addHandler(logging.StreamHandler(leaky))
        self._child.propagate = False
        tok = "SYNTH-secret-child-1"
        with tempfile.TemporaryDirectory() as d:
            nl.setup_service_logging(os.path.join(d, "logs"))
            self.assertEqual(self._child.handlers, [])
            self.assertTrue(self._child.propagate)
            self._child.error("child key=%s", tok)

            def boom():
                raise RuntimeError("thread url ?token=" + tok)
            t = threading.Thread(target=boom, name="clip-synth")
            t.start()
            t.join()

            err = io.StringIO()
            real = sys.stderr
            sys.stderr = err
            try:
                logging.getLogger("nova.x").error(
                    "bad %s %d key=" + tok, "only-one-arg-" + tok)
                logging.getLogger("nova.x").error("ok-after")
            finally:
                sys.stderr = real
            for h in logging.getLogger("nova").handlers:
                h.flush()
            with open(os.path.join(d, "logs", "nova.log")) as fh:
                text = fh.read()
            self.assertEqual(leaky.getvalue(), "")
            self.assertNotIn(tok, text)
            self.assertNotIn(tok, err.getvalue())
            self.assertIn("uncaught thread exception in clip-synth", text)
            self.assertIn("unformattable log message", text)
            self.assertIn("ok-after", text)
            # the file handler's own failure path prints only a summary
            fhdl = [x for x in logging.getLogger("nova").handlers
                    if hasattr(x, "doRollover")][0]
            sys.stderr = err
            try:
                try:
                    raise OSError("x " + tok)
                except OSError:
                    fhdl.handleError(logging.makeLogRecord({}))
            finally:
                sys.stderr = real
            self.assertNotIn(tok, err.getvalue())
            self.assertIn("logging failure: OSError", err.getvalue())


if __name__ == "__main__":
    unittest.main()
