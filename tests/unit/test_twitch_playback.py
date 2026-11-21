import urllib.request
import functools
import http.server
import json
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path

from app.ingestion import twitch_playback as tp

MASTER = (
    '#EXTM3U\n'
    '#EXT-X-STREAM-INF:BANDWIDTH=8000000,RESOLUTION=1920x1080,NAME="1080p60"\n'
    'https://video-weaver.x.hls.ttvnw.net/v1/a1080.m3u8?sig=SECRETSIG&token=SECRETTOK\n'
    '#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720,NAME="720p60"\n'
    'https://video-weaver.x.hls.ttvnw.net/v1/a720.m3u8?sig=SECRETSIG&token=SECRETTOK\n'
    '#EXT-X-STREAM-INF:BANDWIDTH=160000,NAME="audio_only"\n'
    'https://video-weaver.x.hls.ttvnw.net/v1/aud.m3u8\n'
    '#EXT-X-STREAM-INF:BANDWIDTH=900000,RESOLUTION=640x360,NAME="evil"\n'
    'https://evil.example.com/v.m3u8\n')


def fake(gql=None, usher=None, calls=None):
    gql = gql or (200, json.dumps({"data": {"streamPlaybackAccessToken": {
        "value": json.dumps({"authorization": {"forbidden": False}}),
        "signature": "SECRETSIG"}}}).encode())
    usher = usher or (200, MASTER.encode())

    def op(req, timeout, limit=None, deadline=None, should_stop=None):
        if calls is not None:
            calls.append(req.full_url)
        return gql if req.full_url.startswith(tp.GQL_URL) else usher
    return op


class Base(unittest.TestCase):
    def setUp(self):
        tp._last.clear()

    def res(self, **kw):
        kw.setdefault("authorized", True)
        kw.setdefault("min_interval", 0)
        return tp.resolve("SomeChannel", **kw)


class ResolveTests(Base):
    def test_default_is_unauthorized_and_makes_no_network_call(self):
        calls = []
        with self.assertRaises(tp.PlaybackRefused) as c:
            tp.resolve("abc", opener=fake(calls=calls))
        self.assertEqual(c.exception.kind, "unauthorized")
        self.assertEqual(calls, [])

    def test_picks_best_variant_under_cap_and_skips_audio_and_offlist(self):
        r = self.res(opener=fake(), max_height=720)
        self.assertEqual(r.variant.name, "720p")
        self.assertEqual([v.name for v in r.variants], ["1080p", "720p"])
        self.assertEqual(self.res(opener=fake(), max_height=1080).variant.name, "1080p")

    def test_repr_and_errors_never_leak_signed_values(self):
        r = self.res(opener=fake())
        for s in (repr(r), str(r), repr(r.variant)):
            self.assertNotIn("SECRET", s)
        self.assertTrue(r.ffmpeg_input().startswith("https://"))
        self.assertNotIn("SECRET", tp.redact("see https://x.ttvnw.net/a?sig=SECRETSIG&token=SECRETTOK failed"))

    def test_invalid_channel_names_refused(self):
        for bad in ("", "a", "x/../y", "a b", "a" * 40, "-flag", "x;rm"):
            tp._last.clear()
            with self.assertRaises(tp.PlaybackRefused) as c:
                tp.resolve(bad, authorized=True, opener=fake(), min_interval=0)
            self.assertEqual(c.exception.kind, "invalid", bad)

    def test_typed_refusals_fail_closed(self):
        cases = [
            (dict(usher=(404, b"[]")), "offline"),
            (dict(usher=(403, b"")), "restricted"),
            (dict(usher=(429, b"")), "blocked"),
            (dict(usher=(500, b"")), "error"),
            (dict(gql=(401, b"")), "restricted"),
            (dict(gql=(200, json.dumps({"errors": [{"message": "failed integrity check"}]}).encode())), "challenge"),
            (dict(gql=(200, json.dumps({"errors": [{"message": "other"}]}).encode())), "error"),
            (dict(gql=(200, json.dumps({"data": {"streamPlaybackAccessToken": None}}).encode())), "offline"),
            (dict(gql=(200, json.dumps({"data": {"streamPlaybackAccessToken": {
                "value": json.dumps({"authorization": {"forbidden": True}}), "signature": "s"}}}).encode())), "restricted"),
            (dict(usher=(200, b"#EXTM3U\n")), "error"),
            (dict(gql=(200, b"not json")), "error"),
        ]
        for kw, kind in cases:
            tp._last.clear()
            with self.assertRaises(tp.PlaybackRefused) as c:
                self.res(opener=fake(**kw))
            self.assertEqual(c.exception.kind, kind, kw)

    def test_network_exception_text_is_not_propagated(self):
        def op(req, timeout, limit=None, deadline=None, should_stop=None):
            raise OSError("boom https://usher.ttvnw.net/x?sig=SECRETSIG")
        with self.assertRaises(tp.PlaybackRefused) as c:
            self.res(opener=op)
        self.assertNotIn("SECRET", str(c.exception))
        self.assertNotIn("usher", str(c.exception))

    def test_rate_limit_blocks_rapid_repeat_without_network(self):
        calls = []
        tp.resolve("abcde", authorized=True, opener=fake(calls=calls), min_interval=60)
        n = len(calls)
        with self.assertRaises(tp.PlaybackRefused) as c:
            tp.resolve("abcde", authorized=True, opener=fake(calls=calls), min_interval=60)
        self.assertEqual(c.exception.kind, "blocked")
        self.assertEqual(len(calls), n)

    def test_redirects_are_not_followed(self):
        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302); self.send_header("Location", "http://127.0.0.1:1/x"); self.end_headers()
            def log_message(self, *a): pass
        srv = http.server.HTTPServer(("127.0.0.1", 0), H)
        t = threading.Thread(target=srv.serve_forever, daemon=True); t.start()
        try:
            import urllib.request
            code, _ = tp.default_opener(urllib.request.Request(f"http://127.0.0.1:{srv.server_port}/"), 5, 1000, None)
            self.assertEqual(code, 302)
        finally:
            srv.shutdown(); srv.server_close(); t.join(5)


    def test_malformed_token_metadata_fails_closed(self):
        def g(value):
            return (200, json.dumps({"data": {"streamPlaybackAccessToken": {
                "value": value, "signature": "SECRETSIG"}}}).encode())
        cases = [("not-json", "error"), ("[]", "error"), ("{}", "restricted"),
                 (json.dumps({"authorization": "x"}), "restricted"),
                 (json.dumps({"authorization": {"forbidden": None}}), "restricted"),
                 (json.dumps({"authorization": {"forbidden": False}, "blackout_enabled": True}), "restricted"),
                 (json.dumps({"authorization": {"forbidden": False}, "geoblock_reason": "region"}), "restricted")]
        for v, kind in cases:
            tp._last.clear()
            with self.assertRaises(tp.PlaybackRefused) as c:
                self.res(opener=fake(gql=g(v)))
            self.assertEqual(c.exception.kind, kind, v)

    def test_max_height_is_a_cap(self):
        only1080 = MASTER.split("#EXT-X-STREAM-INF:BANDWIDTH=3000000")[0]
        with self.assertRaises(tp.PlaybackRefused) as c:
            self.res(opener=fake(usher=(200, only1080.encode())), max_height=720)
        self.assertEqual(c.exception.kind, "quality")

    def test_external_name_never_reaches_repr_log_or_traceback(self):
        import traceback
        evil = ('#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720,'
                'NAME="https://x.ttvnw.net/a?sig=SECRETSIG&token=SECRETTOK"\n'
                'https://video-weaver.x.hls.ttvnw.net/v1/a.m3u8?sig=SECRETSIG\n')
        r = self.res(opener=fake(usher=(200, evil.encode())))
        self.assertEqual(r.variant.name, "720p")
        for s in (repr(r), str(r), repr(r.variants), repr(r.variant)):
            self.assertNotIn("SECRET", s)

    def test_full_traceback_has_no_signed_values(self):
        import traceback
        def op(req, timeout, limit=None, deadline=None, should_stop=None):
            raise OSError("boom https://usher.ttvnw.net/x?sig=SECRETSIG&token=SECRETTOK")
        try:
            self.res(opener=op)
        except tp.PlaybackRefused as e:
            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        self.assertNotIn("SECRET", tb)
        def bad_json(req, timeout, limit=None, deadline=None, should_stop=None):
            return 200, b'{"data": SECRETTOK'
        tp._last.clear()
        try:
            self.res(opener=bad_json)
        except tp.PlaybackRefused as e:
            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        self.assertNotIn("SECRET", tb)


from app.ingestion import hls_pump


@unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg unavailable")
class PumpTests(Base):
    """Owned generated HLS fixture; nothing third-party is fetched. The
    fixture URLs sit on a *.ttvnw.net-shaped name and are answered by an
    in-process opener, so the production host allowlist is exercised
    unmodified (a fixture does not prove Twitch's real nested hosts)."""
    BASE = "https://video-weaver.fixture.ttvnw.net/v1/"

    @classmethod
    def setUpClass(cls):
        cls.d = Path(tempfile.mkdtemp())
        subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
                        "-i", "testsrc=size=320x180:rate=15", "-f", "lavfi",
                        "-i", "sine=frequency=440", "-t", "8", "-c:v", "libx264",
                        "-pix_fmt", "yuv420p", "-force_key_frames", "expr:gte(t,n_forced*2)", "-c:a", "aac", "-f", "hls",
                        "-hls_time", "2", "-hls_list_size", "0",
                        str(cls.d / "v.m3u8")], check=True)
        cls.playlist = (cls.d / "v.m3u8").read_text()

    def opener(self, playlist=None, seen=None, overrides=None):
        pl = (playlist or self.playlist).encode()
        overrides = overrides or {}

        def op(req, timeout, limit=None, deadline=None, should_stop=None):
            u = req.full_url
            if seen is not None:
                seen.append(u)
            if u in overrides:
                return overrides[u]
            name = u[len(self.BASE):].split("?")[0]
            if name == "v.m3u8":
                return 200, pl
            f = self.d / name
            return (200, f.read_bytes()) if f.exists() else (404, b"")
        return op

    def resolved(self):
        master = ('#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=500000,RESOLUTION=320x180\n'
                  + self.BASE + 'v.m3u8?sig=SECRETSIG\n')
        return tp.resolve("fixturech", authorized=True, min_interval=0,
                          opener=fake(usher=(200, master.encode())))

    def probe(self, path):
        pr = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                             "-of", "default=nw=1:nk=1", str(path)], capture_output=True, text=True)
        return float(pr.stdout.strip() or 0)

    def alive(self, pid):
        try:
            import os
            os.killpg(pid, 0)
            return True
        except ProcessLookupError:
            return False

    def test_ended_playlist_captured_through_pipe_with_network_less_ffmpeg(self):
        out = self.d / "cap1.ts"
        seen = []
        r = hls_pump.capture(self.resolved(), "ffmpeg", str(out), max_media_seconds=60,
                             deadline_sec=60, opener=self.opener(seen=seen))
        self.assertEqual(r.reason, "ended")
        self.assertEqual(r.ffmpeg_rc, 0)
        self.assertGreaterEqual(r.segments, 2)
        self.assertGreater(self.probe(out), 3.0)
        self.assertFalse(self.alive(r.ffmpeg_pid))
        self.assertTrue(all(u.startswith(self.BASE) for u in seen))

    def test_media_cap_stops_early(self):
        out = self.d / "cap2.ts"
        r = hls_pump.capture(self.resolved(), "ffmpeg", str(out), max_media_seconds=2,
                             deadline_sec=60, opener=self.opener())
        self.assertEqual(r.reason, "media_cap")
        self.assertFalse(self.alive(r.ffmpeg_pid))

    def test_off_allowlist_segment_uri_blocked_and_never_requested(self):
        evil = self.playlist.replace("v1.ts", "https://evil.example.com/x.ts") \
            if "v1.ts" in self.playlist else self.playlist + "#EXTINF:2.0,\nhttps://evil.example.com/x.ts\n"
        seen = []
        with self.assertRaises(tp.PlaybackRefused) as c:
            hls_pump.capture(self.resolved(), "ffmpeg", str(self.d / "cap3.ts"),
                             max_media_seconds=60, deadline_sec=60,
                             opener=self.opener(playlist=evil, seen=seen))
        self.assertEqual(c.exception.kind, "blocked")
        self.assertFalse([u for u in seen if "evil.example.com" in u])

    def test_encrypted_or_foreign_key_and_http_uri_refused(self):
        for extra, kind in (('#EXT-X-KEY:METHOD=AES-128,URI="https://evil.example.com/k"\n', "restricted"),
                            ('#EXT-X-MAP:URI="http://video-weaver.fixture.ttvnw.net/init"\n', "error")):
            pl = self.playlist.replace("#EXTINF", extra + "#EXTINF", 1)
            with self.assertRaises(tp.PlaybackRefused) as c:
                hls_pump.capture(self.resolved(), "ffmpeg", str(self.d / "cap4.ts"),
                                 max_media_seconds=60, deadline_sec=60,
                                 opener=self.opener(playlist=pl))
            self.assertEqual(c.exception.kind, kind)

    def test_strict_parsing_regressions(self):
        base = "https://x.ttvnw.net/p.m3u8"
        cases = [
            ('#EXT-X-KEY:METHOD=AES-128,URI="https://k/NONE"\n#EXTINF:2.0,\na.ts\n', "restricted"),
            ('#EXTINF:nan,\na.ts\n', "error"),
            ('#EXTINF:inf,\na.ts\n', "error"),
            ('#EXTINF:-1,\na.ts\n', "error"),
            ('#EXT-X-BYTERANGE:10@0\n#EXTINF:2.0,\na.ts\n', "error"),
            ('#EXT-X-DISCONTINUITY\n#EXTINF:2.0,\na.ts\n', "error"),
            ('#EXT-X-TARGETDURATION:nan\n#EXTINF:2.0,\na.ts\n', "error"),
        ]
        for body, kind in cases:
            with self.assertRaises(tp.PlaybackRefused) as c:
                hls_pump.parse_media("#EXTM3U\n" + body, base)
            self.assertEqual(c.exception.kind, kind, body)
        with self.assertRaises(tp.PlaybackRefused):
            hls_pump.parse_media("#EXTINF:2.0,\na.ts\n", base)  # no header
        segs, _, _ = hls_pump.parse_media(
            "#EXTM3U\n#EXT-X-KEY:METHOD=NONE\n#EXTINF:2.0,\na.ts\n", base)
        self.assertEqual(len(segs), 1)

    def test_real_opener_no_truncation_overflow_and_deadline(self):
        import http.server, socketserver
        big = b"x" * 1_500_000
        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a): pass
            def do_GET(self):
                if self.path == "/slow":
                    self.send_response(200); self.send_header("Content-Length", "100"); self.end_headers()
                    for _ in range(100):
                        try:
                            self.wfile.write(b"y"); self.wfile.flush()
                        except OSError: return
                        time.sleep(0.2)
                    return
                self.send_response(200); self.send_header("Content-Length", str(len(big))); self.end_headers()
                self.wfile.write(big)
        class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True
        srv = S(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            u = f"http://127.0.0.1:{srv.server_port}"
            code, body = tp.default_opener(urllib.request.Request(u + "/big"), 5, 20_000_000, None)
            self.assertEqual((code, len(body)), (200, 1_500_000))      # not cut to 1MB
            with self.assertRaises(tp.PlaybackRefused):
                tp.default_opener(urllib.request.Request(u + "/big"), 5, 1_000_000, None)
            t0 = time.time()
            with self.assertRaises(tp.PlaybackRefused):
                tp.default_opener(urllib.request.Request(u + "/slow"), 5, 1000,
                                  time.monotonic() + 1.0)
            self.assertLess(time.time() - t0, 3)
        finally:
            srv.shutdown(); srv.server_close()

    def test_error_body_trickle_and_cancel_and_blackhole_bounded(self):
        import http.server, socketserver
        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a): pass
            def do_GET(self):
                self.send_response(403); self.send_header("Content-Length", "400"); self.end_headers()
                for _ in range(400):
                    try:
                        self.wfile.write(b"z"); self.wfile.flush()
                    except OSError: return
                    time.sleep(0.05)
        class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True
        srv = S(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            u = f"http://127.0.0.1:{srv.server_port}/"
            t0 = time.time()
            with self.assertRaises(tp.PlaybackRefused):
                tp.default_opener(urllib.request.Request(u), 5, 1000, time.monotonic() + 0.3)
            self.assertLess(time.time() - t0, 1.0)            # was 1.2s+ with in-process reads
            ev = threading.Event()
            threading.Timer(0.3, ev.set).start()
            t0 = time.time()
            with self.assertRaises(tp.PlaybackRefused):
                tp.default_opener(urllib.request.Request(u), 30, 1000, None, ev.is_set)
            self.assertLess(time.time() - t0, 1.5)
        finally:
            srv.shutdown(); srv.server_close()
        # unroutable address: connect blocks; absolute deadline still wins
        t0 = time.time()
        with self.assertRaises(tp.PlaybackRefused):
            tp.default_opener(urllib.request.Request("http://10.255.255.1/"), 30, 1000,
                              time.monotonic() + 0.5)
        self.assertLess(time.time() - t0, 1.5)

    def test_group_cleanup_after_leader_exit_and_spawn_failure(self):
        script = self.d / "leaderexit.sh"
        script.write_text("#!/bin/sh\nsleep 300 &\necho $! > %s\nexit 0\n" % (self.d / "child.pid"))
        script.chmod(0o755)
        r = hls_pump.capture(self.resolved(), str(script), str(self.d / "cap9.ts"),
                             max_media_seconds=1, deadline_sec=5, opener=self.opener(), grace=1)
        time.sleep(0.3)
        child = int((self.d / "child.pid").read_text())
        self.assertFalse(self.alive(child))
        with self.assertRaises(OSError):
            hls_pump.capture(self.resolved(), "/nonexistent/ffmpeg", str(self.d / "c10.ts"),
                             max_media_seconds=1, deadline_sec=5, opener=self.opener())

    def test_stderr_is_bounded_ring(self):
        script = self.d / "noisy.sh"
        script.write_text("#!/bin/sh\nhead -c 3000000 /dev/zero | tr '\\0' 'e' >&2\ncat >/dev/null\n")
        script.chmod(0o755)
        r = hls_pump.capture(self.resolved(), str(script), str(self.d / "cap11.ts"),
                             max_media_seconds=1, deadline_sec=10, opener=self.opener(), grace=2)
        self.assertLessEqual(len(r.stderr_tail), 500)

    def test_restriction_on_segment_fails_closed_without_retry_and_reaps(self):
        seen = []
        first = [l for l in self.playlist.splitlines() if l.endswith(".ts")][0]
        with self.assertRaises(tp.PlaybackRefused) as c:
            hls_pump.capture(self.resolved(), "ffmpeg", str(self.d / "cap5.ts"),
                             max_media_seconds=60, deadline_sec=60,
                             opener=self.opener(seen=seen, overrides={self.BASE + first: (403, b"")}))
        self.assertEqual(c.exception.kind, "restricted")
        self.assertEqual(len([u for u in seen if u.endswith(first)]), 1)

    def test_deadline_and_stop_with_endless_live_playlist(self):
        live = "\n".join(l for l in self.playlist.splitlines() if l != "#EXT-X-ENDLIST") + "\n"
        t0 = time.time()
        r = hls_pump.capture(self.resolved(), "ffmpeg", str(self.d / "cap6.ts"),
                             max_media_seconds=3600, deadline_sec=3, opener=self.opener(playlist=live))
        self.assertEqual(r.reason, "deadline")
        self.assertLess(time.time() - t0, 20)
        self.assertFalse(self.alive(r.ffmpeg_pid))
        ev = threading.Event(); ev.set()
        r = hls_pump.capture(self.resolved(), "ffmpeg", str(self.d / "cap7.ts"),
                             max_media_seconds=60, deadline_sec=60, opener=self.opener(playlist=live),
                             should_stop=ev.is_set)
        self.assertEqual(r.reason, "stopped")

    def test_hung_ffmpeg_is_killed_with_its_process_group(self):
        script = self.d / "fakeffmpeg.sh"
        script.write_text("#!/bin/sh\nsleep 300 &\nwait\n"); script.chmod(0o755)
        t0 = time.time()
        r = hls_pump.capture(self.resolved(), str(script), str(self.d / "cap8.ts"),
                             max_media_seconds=1, deadline_sec=5, opener=self.opener(),
                             grace=1)
        self.assertLess(time.time() - t0, 25)
        self.assertFalse(self.alive(r.ffmpeg_pid))

    def test_stderr_tail_is_redacted_and_bounded(self):
        script = self.d / "noisyffmpeg.sh"
        script.write_text('#!/bin/sh\necho "failed https://x.ttvnw.net/a?sig=SECRETSIG&token=SECRETTOK" >&2\nexit 1\n')
        script.chmod(0o755)
        r = hls_pump.capture(self.resolved(), str(script), str(self.d / "cap9.ts"),
                             max_media_seconds=1, deadline_sec=5, opener=self.opener(), grace=1)
        self.assertNotIn("SECRET", r.stderr_tail)
        self.assertLessEqual(len(r.stderr_tail), 500)

    def test_pump_exception_traceback_is_clean(self):
        import traceback
        def op(req, timeout, limit=None, deadline=None, should_stop=None):
            raise OSError("boom https://x.ttvnw.net/a?sig=SECRETSIG&token=SECRETTOK")
        try:
            hls_pump.capture(self.resolved(), "ffmpeg", str(self.d / "capA.ts"),
                             max_media_seconds=1, deadline_sec=5, opener=op)
        except tp.PlaybackRefused as e:
            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        self.assertNotIn("SECRET", tb)


if __name__ == "__main__":
    unittest.main()
