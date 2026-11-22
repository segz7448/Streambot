"""Per-request deadline semantics for the fetch child (measured live class).

Session 7 (agent00, deployed 6071717) died with the fixed-schema diag
"capture refused kind=error stage=segment via=deadline" after ~18.6s:
~3.6s launch/resolve/playlist + the 15s whole-request cap (t_hard) of one
segment GET. The cap is PROGRESS-BLIND: a transfer that is actively
receiving bytes is killed at `timeout` wall seconds exactly like a hung
one. On a slow link a multi-hundred-KB live-edge segment cannot fit in
15s, so the single bounded attempt can never capture.

Fix under test: `timeout` is an INACTIVITY cap - the request dies after
`timeout` seconds without a byte (anti-stall preserved, hung servers still
die) while a progressing transfer may run until the absolute `deadline`.
Too-large and absolute-deadline guards are unchanged.

These tests use the REAL fetch child process against a local server.
"""
import http.server
import threading
import time
import urllib.request
import unittest

from app.ingestion import twitch_playback as tp

CHUNK = 4096
CHUNKS = 12                     # 48 KiB total
GAP = 0.25                      # ~3s trickled transfer


class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path == "/trickle":
            self.send_response(200)
            self.send_header("Content-Type", "video/mp2t")
            self.end_headers()
            for _ in range(CHUNKS):
                self.wfile.write(b"\x47" * CHUNK)
                self.wfile.flush()
                time.sleep(GAP)
        elif self.path == "/hang":
            self.send_response(200)
            self.send_header("Content-Type", "video/mp2t")
            self.end_headers()
            time.sleep(30)      # headers only, never a byte of body
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"\x47" * CHUNK)


class FetchDeadlineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        cls.srv.daemon_threads = True
        cls.th = threading.Thread(target=cls.srv.serve_forever, daemon=True)
        cls.th.start()
        cls.base = f"http://127.0.0.1:{cls.srv.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()
        cls.srv.server_close()

    def _get(self, path, timeout, deadline=None):
        return tp.default_opener(
            urllib.request.Request(self.base + path), timeout,
            1_000_000, deadline)

    def test_progressing_transfer_completes_past_idle_cap(self):
        # transfer takes ~3s > timeout=1.0: the old whole-request cap kills
        # it at ~1s ("deadline reached") despite steady progress; the
        # inactivity cap lets it finish well before the absolute deadline
        t0 = time.monotonic()
        code, body = self._get("/trickle", 1.0,
                               time.monotonic() + 30)
        self.assertEqual(code, 200)
        self.assertEqual(body, b"\x47" * (CHUNK * CHUNKS))
        self.assertGreaterEqual(time.monotonic() - t0, GAP * (CHUNKS - 2))

    def test_hung_server_still_dies_at_idle_cap(self):
        # anti-stall unchanged: zero bytes for `timeout` -> refused,
        # bounded near the cap (never the 30s server sleep)
        t0 = time.monotonic()
        with self.assertRaises(tp.PlaybackRefused) as cm:
            self._get("/hang", 1.0, time.monotonic() + 30)
        # the parent's idle cap wins: the child's own socket timeout is
        # deliberately wider (cap + 10s) so stalls classify as deadline
        self.assertEqual(cm.exception.transport, "deadline")
        self.assertLess(time.monotonic() - t0, 10)

    def test_absolute_deadline_still_bounds_progress(self):
        # a progressing transfer may not outlive the absolute deadline
        t0 = time.monotonic()
        with self.assertRaises(tp.PlaybackRefused) as cm:
            self._get("/trickle", 30.0, time.monotonic() + 1.5)
        self.assertEqual(cm.exception.transport, "deadline")
        self.assertLess(time.monotonic() - t0, 10)

    def test_full_pump_capture_over_trickled_segment(self):
        # the session-7 path end to end: real playlist parse, real fetch
        # child, real segment GET trickled slower than the old 15s-style
        # whole-request cap, fake ffmpeg sink. Old code: PlaybackRefused
        # stage=segment via=deadline, zero bytes to the writer. New code:
        # the segment completes and is piped.
        import os
        import stat
        import tempfile
        import types
        from app.ingestion import hls_pump
        playlist = ("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n"
                    "#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2.0,\n/seg.ts\n"
                    "#EXT-X-ENDLIST\n")

        class H(_Handler):
            def do_GET(self):
                if self.path == "/playlist.m3u8":
                    body = playlist.encode()
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.path = "/trickle"  # the segment, served trickled
                    super().do_GET()

        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        srv.daemon_threads = True
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.shutdown)
        self.addCleanup(srv.server_close)
        base = f"http://127.0.0.1:{srv.server_port}"
        with tempfile.TemporaryDirectory() as td:
            fake_ff = os.path.join(td, "fake-ffmpeg")
            with open(fake_ff, "w") as f:
                f.write("#!" + os.sys.executable + "\n"
                        "import sys\n"
                        "open(sys.argv[-1], 'wb').write(sys.stdin.buffer.read())\n")
            os.chmod(fake_ff, os.stat(fake_ff).st_mode | stat.S_IXUSR)
            out = os.path.join(td, "out.ts")
            resolved = types.SimpleNamespace(
                ffmpeg_input=lambda: base + "/playlist.m3u8")
            # the https-only host gate is orthogonal to deadline semantics;
            # the local server speaks http, so gate it off for this scope
            from unittest import mock
            with mock.patch.object(hls_pump.tp, "_host_ok",
                                   return_value=True):
                res = hls_pump.capture(
                    resolved, fake_ff, out, max_media_seconds=1500,
                    deadline_sec=25, opener=tp.default_opener,
                    host_ok=lambda h: True, timeout=1.0,
                    output_args=None, grace=1.0)
        self.assertEqual(res.reason, "ended")
        self.assertEqual(res.segments, 1)
        self.assertEqual(res.bytes_written, CHUNK * CHUNKS)

    def test_fast_request_unchanged(self):
        code, body = self._get("/fast", 5.0, time.monotonic() + 30)
        self.assertEqual((code, body), (200, b"\x47" * CHUNK))


if __name__ == "__main__":
    unittest.main()
