import functools
import http.server
import json
import shutil
import subprocess
import tempfile
import threading
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

    def op(req, timeout):
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
        self.assertEqual(r.variant.name, "720p60")
        self.assertEqual([v.name for v in r.variants], ["1080p60", "720p60"])
        self.assertEqual(self.res(opener=fake(), max_height=1080).variant.name, "1080p60")

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
        def op(req, timeout):
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
            code, _ = tp.default_opener(urllib.request.Request(f"http://127.0.0.1:{srv.server_port}/"), 5)
            self.assertEqual(code, 302)
        finally:
            srv.shutdown(); srv.server_close(); t.join(5)

    def test_ffmpeg_args_bounded(self):
        r = self.res(opener=fake())
        a = tp.ffmpeg_args("ffmpeg", r, "/tmp/o.ts", 30)
        self.assertIn("-t", a); self.assertEqual(a[a.index("-t") + 1], "30")
        self.assertIn("-nostdin", a)
        for bad in (0, -1, 99999):
            with self.assertRaises(ValueError):
                tp.ffmpeg_args("ffmpeg", r, "/tmp/o.ts", bad)


@unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg unavailable")
class LocalFixtureIngest(Base):
    """Our own generated HLS fixture served from 127.0.0.1: proves the
    adapter's output feeds ffmpeg end to end. No third-party stream."""

    def test_generated_hls_through_adapter_to_ffmpeg(self):
        d = Path(tempfile.mkdtemp())
        subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
                        "-i", "testsrc=size=320x180:rate=15", "-f", "lavfi",
                        "-i", "sine=frequency=440", "-t", "6", "-c:v", "libx264",
                        "-pix_fmt", "yuv420p", "-c:a", "aac", "-f", "hls",
                        "-hls_time", "2", "-hls_list_size", "0",
                        str(d / "v.m3u8")], check=True)
        class Quiet(http.server.SimpleHTTPRequestHandler):
            def log_message(self, *a, **k):
                pass
        handler = functools.partial(Quiet, directory=str(d))
        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        t = threading.Thread(target=srv.serve_forever, daemon=True); t.start()
        try:
            port = srv.server_port
            master = ('#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=500000,RESOLUTION=320x180,NAME="180p"\n'
                      f'https://fixture.test/v.m3u8\n')
            r = tp.resolve("fixturech", authorized=True, min_interval=0,
                           opener=fake(usher=(200, master.encode())),
                           host_ok=lambda h: h == "fixture.test" or h == "usher.ttvnw.net")
            # point the fixture host at the local server
            local = r.ffmpeg_input().replace("https://fixture.test", f"http://127.0.0.1:{port}")
            lr = tp.Resolved(r.channel, tp.Variant(r.variant.name, r.variant.height,
                                                    r.variant.bandwidth, local), r.variants, r.resolved_at)
            out = d / "cap.ts"
            args = tp.ffmpeg_args("ffmpeg", lr, str(out), 4, protocols="http,tcp,file,crypto")
            p = subprocess.run(args, capture_output=True, timeout=60)
            self.assertEqual(p.returncode, 0, p.stderr[-300:])
            pr = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                                 "-of", "default=nw=1:nk=1", str(out)], capture_output=True, text=True)
            self.assertGreater(float(pr.stdout.strip()), 1.5)
        finally:
            srv.shutdown(); srv.server_close(); t.join(5)


if __name__ == "__main__":
    unittest.main()
