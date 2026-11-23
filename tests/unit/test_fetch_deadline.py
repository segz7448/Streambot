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
from unittest import mock
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
        if self.path == "/cl-truncated":
            self.send_response(200)
            self.send_header("Content-Length", "10000")
            self.end_headers()
            try:
                self.wfile.write(b"\x47" * 100)  # 100 of 10000, then close
            except (BrokenPipeError, ConnectionResetError):
                pass
        elif self.path == "/cl-exact":
            self.send_response(200)
            self.send_header("Content-Length", "500")
            self.end_headers()
            self.wfile.write(b"\x47" * 500)
        elif self.path == "/cl-truncated-403":
            self.send_response(403)
            self.send_header("Content-Length", "10000")
            self.end_headers()
            self.wfile.write(b"denied")
        elif self.path == "/te-chunked-with-cl":
            # Transfer-Encoding governs; the contradictory CL must be
            # ignored - the decoded body is complete
            body = b"\x47" * 6
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Content-Length", "99")
            self.end_headers()
            self.wfile.write(b"%x\r\n" % len(body) + body + b"\r\n0\r\n\r\n")
        elif self.path == "/cl-absurd":
            self.send_response(200)
            self.send_header("Content-Length", "99999999999999999999999")
            self.end_headers()
            self.wfile.write(b"\x47" * 5)
        elif self.path == "/cl-absurd-403":
            self.send_response(403)
            self.send_header("Content-Length", "99999999999999999999999")
            self.end_headers()
            self.wfile.write(b"denied")
        elif self.path == "/cl-negative":
            self.send_response(200)
            self.send_header("Content-Length", "-5")
            self.end_headers()
            self.wfile.write(b"\x47" * 5)
        elif self.path == "/trickle":
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

    def test_truncated_content_length_refused_not_partial(self):
        # advertised 10000, delivered 100: must refuse, never return media
        with self.assertRaises(tp.PlaybackRefused) as cm:
            self._get("/cl-truncated", 5.0, time.monotonic() + 10)
        self.assertEqual(cm.exception.transport, "truncated")

    def test_exact_content_length_accepted(self):
        code, body = self._get("/cl-exact", 5.0, time.monotonic() + 10)
        self.assertEqual((code, body), (200, b"\x47" * 500))

    def test_chunked_with_contradictory_cl_accepted(self):
        # Transfer-Encoding governs: 6 decoded bytes with CL:99 is complete
        code, body = self._get("/te-chunked-with-cl", 5.0,
                               time.monotonic() + 10)
        self.assertEqual((code, body), (200, b"\x47" * 6))

    def test_absurd_cl_treated_as_unknown_status_preserved(self):
        # un packable/unusable CL must not break framing or mask status
        code, body = self._get("/cl-absurd", 5.0, time.monotonic() + 10)
        self.assertEqual((code, body), (200, b"\x47" * 5))
        code, body = self._get("/cl-absurd-403", 5.0, time.monotonic() + 10)
        self.assertEqual((code, body), (403, b"denied"))

    def test_negative_cl_treated_as_absent(self):
        code, body = self._get("/cl-negative", 5.0, time.monotonic() + 10)
        self.assertEqual((code, body), (200, b"\x47" * 5))

    def test_truncated_error_page_keeps_http_classification(self):
        # completeness is enforced on media-carrying 200s only; a short
        # error page still surfaces its HTTP status for kind mapping
        code, body = self._get("/cl-truncated-403", 5.0,
                               time.monotonic() + 10)
        self.assertEqual((code, body), (403, b"denied"))

    def test_fast_request_unchanged(self):
        code, body = self._get("/fast", 5.0, time.monotonic() + 30)
        self.assertEqual((code, body), (200, b"\x47" * CHUNK))


class ExpectedStopTests(unittest.TestCase):
    """Session 8 semantics: the bounded acceptance window expiring (or an
    owner stop) MID-FETCH is an expected stop, not a network failure. The
    cycle must end cleanly (stopped/deadline) with already-piped media
    preserved; unexplained cancellations still raise."""

    def _capture(self, opener, should_stop=lambda: False, clock=None):
        import os
        import stat
        import tempfile
        import types
        from app.ingestion import hls_pump
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        fake_ff = os.path.join(td.name, "fake-ffmpeg")
        with open(fake_ff, "w") as f:
            f.write("#!" + os.sys.executable + "\n"
                    "import sys\n"
                    "open(sys.argv[-1], 'wb').write(sys.stdin.buffer.read())\n")
        os.chmod(fake_ff, os.stat(fake_ff).st_mode | stat.S_IXUSR)
        resolved = types.SimpleNamespace(
            ffmpeg_input=lambda: "https://cdn.example/pl.m3u8")
        kw = dict(max_media_seconds=1500, deadline_sec=25, opener=opener,
                  host_ok=lambda h: True, timeout=1.0, output_args=None,
                  grace=1.0, sleep=lambda s: None, should_stop=should_stop)
        if clock is not None:
            kw["clock"] = clock
        return hls_pump.capture(resolved, fake_ff,
                                os.path.join(td.name, "out.ts"), **kw)

    def test_cancel_mid_playlist_refetch_is_clean_stop(self):
        from app.ingestion import twitch_playback as tp
        state = {"playlists": 0, "stop": False}
        playlist = ("#EXTM3U\n#EXT-X-TARGETDURATION:2\n"
                    "#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2.0,\nseg-0.ts\n")

        def op(req, timeout, limit, deadline, should_stop):
            if req.full_url.endswith(".ts"):
                return 200, b"\x47" * 1000
            state["playlists"] += 1
            if state["playlists"] == 1:
                return 200, playlist.encode()
            # the window expires DURING the refetch (child-side cancel)
            state["stop"] = True
            raise tp.PlaybackRefused("error", "cancelled",
                                     transport="cancelled")
        res = self._capture(op, should_stop=lambda: state["stop"])
        self.assertEqual(res.reason, "stopped")
        self.assertEqual(res.segments, 1)
        self.assertEqual(res.bytes_written, 1000)

    def test_cancel_mid_segment_fetch_is_clean_stop(self):
        from app.ingestion import twitch_playback as tp
        state = {"stop": False}
        playlist = ("#EXTM3U\n#EXT-X-TARGETDURATION:2\n"
                    "#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2.0,\nseg-0.ts\n"
                    "#EXTINF:2.0,\nseg-1.ts\n")

        def op(req, timeout, limit, deadline, should_stop):
            if req.full_url.endswith("seg-0.ts"):
                return 200, b"\x47" * 1000
            if req.full_url.endswith("seg-1.ts"):
                state["stop"] = True
                raise tp.PlaybackRefused("error", "cancelled",
                                         transport="cancelled")
            return 200, playlist.encode()
        res = self._capture(op, should_stop=lambda: state["stop"])
        self.assertEqual(res.reason, "stopped")
        self.assertEqual(res.segments, 1)
        self.assertEqual(res.bytes_written, 1000)

    def test_unexplained_cancel_still_raises(self):
        from app.ingestion import twitch_playback as tp

        def op(req, timeout, limit, deadline, should_stop):
            raise tp.PlaybackRefused("error", "cancelled",
                                     transport="cancelled")
        with self.assertRaises(tp.PlaybackRefused):
            self._capture(op, should_stop=lambda: False)

    def test_mid_fetch_deadline_maps_only_when_window_expired(self):
        from app.ingestion import twitch_playback as tp
        now = [1000.0]

        def op(req, timeout, limit, deadline, should_stop):
            now[0] = 1026.0     # past t_end = 1000 + deadline_sec(25)
            raise tp.PlaybackRefused("error", "deadline reached",
                                     transport="deadline")
        res = self._capture(op, clock=lambda: now[0],
                            should_stop=lambda: False)
        # capture's own deadline really passed -> clean deadline outcome
        self.assertEqual(res.reason, "deadline")
        self.assertEqual(res.segments, 0)

        now2 = [1000.0]

        def op2(req, timeout, limit, deadline, should_stop):
            raise tp.PlaybackRefused("error", "deadline reached",
                                     transport="deadline")
        # window NOT expired: an unexplained deadline refusal propagates
        with self.assertRaises(tp.PlaybackRefused):
            self._capture(op2, clock=lambda: now2[0],
                          should_stop=lambda: False)

    def test_single_cycle_stopped_outcome_ends_clean_with_media(self):
        import asyncio
        import types
        from app.ingestion import hls_pump
        from tests.unit.test_capture_diagnostics import _worker
        w = _worker()
        w.buffer.reconcile.return_value = 0
        w.buffer.enforce_storage_cap.return_value = None
        w._cycle = mock.Mock(return_value="stopped")
        w._closed_media_ready = lambda: False
        ctx = types.SimpleNamespace(stopping=False, heartbeat=lambda: None)
        asyncio.run(w.run(ctx))          # no error: expected bounded stop
        self.assertEqual(w.end_reason, "single_cycle_complete")
        self.assertIsNone(w.error_text)


if __name__ == "__main__":
    unittest.main()
