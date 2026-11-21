"""Owned-fixture lifecycle for playback sessions. Everything is served by a
local endpoint from a generated fixture; the REAL fetch worker, hls_pump,
ffmpeg and segmenter run. Only scheme/host policy and endpoint constants are
patched to reach loopback. No third-party host is contacted."""
import asyncio
import http.server
import json
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from app.bot import sessions as bot_sessions, source_auth
from app.core import database
from app.ingestion import twitch_playback as tp
from app.pipeline.session import ClipSession, recover_interrupted_sessions
from app.recording.playback_worker import PlaybackWorker


def make_fixture(d):
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
                    "-i", "testsrc=size=320x180:rate=15", "-f", "lavfi", "-i", "sine=frequency=440",
                    "-t", "12", "-c:v", "libx264", "-pix_fmt", "yuv420p",
                    "-force_key_frames", "expr:gte(t,n_forced*2)", "-c:a", "aac", "-f", "hls",
                    "-hls_time", "2", "-hls_list_size", "0", str(d / "v.m3u8")], check=True)


class Endpoint:
    def __init__(self, d, live: bool, token_status=200, usher_status=200):
        self.d, self.live = d, live
        self.token_status, self.usher_status = token_status, usher_status
        ep = self
        self.hits = []

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a): pass

            def _send(self, code, body):
                self.send_response(code); self.send_header("Content-Length", str(len(body)))
                self.end_headers(); self.wfile.write(body)

            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                ep.hits.append("token")
                if ep.token_status != 200:
                    return self._send(ep.token_status, b"{}")
                self._send(200, json.dumps({"data": {"streamPlaybackAccessToken": {
                    "value": json.dumps({"authorization": {"forbidden": False}}),
                    "signature": "s"}}}).encode())

            def do_GET(self):
                path = self.path.split("?")[0]
                base = f"http://127.0.0.1:{self.server.server_port}"
                if path.startswith("/api/channel/hls/"):
                    ep.hits.append("usher")
                    if ep.usher_status != 200:
                        return self._send(ep.usher_status, b"")
                    self._send(200, ('#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=320x180\n'
                                     f'{base}/media/v.m3u8\n').encode())
                elif path == "/media/v.m3u8":
                    txt = (ep.d / "v.m3u8").read_text()
                    if ep.live:
                        txt = "\n".join(l for l in txt.splitlines() if l != "#EXT-X-ENDLIST") + "\n"
                    self._send(200, txt.encode())
                elif path.startswith("/media/") and (ep.d / path[7:]).exists():
                    self._send(200, (ep.d / path[7:]).read_bytes())
                else:
                    self._send(404, b"")
        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.base = f"http://127.0.0.1:{self.srv.server_port}"

    def patches(self):
        base = self.base
        ok = lambda url, host_ok=None: url.startswith(base + "/")
        return [mock.patch.object(tp, "GQL_URL", base + "/gql"),
                mock.patch.object(tp, "USHER_BASE", base),
                mock.patch.object(tp, "_host_ok", ok)]

    def close(self):
        self.srv.shutdown(); self.srv.server_close()


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fix = Path(tempfile.mkdtemp()); make_fixture(cls.fix)

    def setUp(self):
        tp._last.clear()
        self.sd = Path(tempfile.mkdtemp())
        conn = database.connect(str(self.sd / "nova.db")); database.migrate(conn); conn.close()
        self.addCleanup(shutil.rmtree, self.sd, True)

    def endpoint(self, **kw):
        ep = Endpoint(self.fix, **kw)
        for p in ep.patches():
            p.start(); self.addCleanup(p.stop)
        self.addCleanup(ep.close)
        return ep

    def session_row(self):
        with closing(sqlite3.connect(str(self.sd / "nova.db"))) as c:
            return c.execute("SELECT id,state,ingest_mode,notes FROM stream_sessions ORDER BY id DESC LIMIT 1").fetchone()

    def seg_count(self):
        with closing(sqlite3.connect(str(self.sd / "nova.db"))) as c:
            return c.execute("SELECT count(*) FROM recording_segments").fetchone()[0]

    def new_session(self, **pb):
        return ClipSession("ffmpeg", "ffprobe", "", "streamerx", self.sd, segment_seconds=2,
                           tick_seconds=0.5, playback=pb)


class SessionLifecycle(Base):
    def test_finite_stream_records_with_durable_provenance_and_ends(self):
        self.endpoint(live=False)
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        s = self.new_session(); s.start(); s.join(90)
        self.assertFalse(s.alive())
        sid, state, mode, notes = self.session_row()
        self.assertEqual((state, mode), ("ENDED", "playback"))
        n = json.loads(notes)
        self.assertEqual(n["origin"], "twitch_public_playback")
        self.assertTrue(n["record_authorized_at_start"])
        self.assertFalse(n["repost_authorized_at_start"])
        self.assertEqual(n["end_reason"], "ended")
        self.assertGreaterEqual(self.seg_count(), 2)

    def test_record_revocation_stops_active_capture(self):
        self.endpoint(live=True)
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        s = self.new_session(); s.start()
        t0 = time.time()
        # segments are reconciled on stop; wait for recording files instead
        files = lambda: list((self.sd / "sessions" / "streamerx").rglob("seg-*.ts"))
        while not files() and time.time() - t0 < 30:
            time.sleep(0.3)
        self.assertTrue(files(), "capture never produced segments")
        self.t_files = time.time() - t0
        print("files after %.1fs" % self.t_files)
        source_auth.set_scope(self.sd, "streamerx", "record", False)
        t1 = time.time(); s.join(40)
        self.assertFalse(s.alive())
        print("stop latency %.1fs" % (time.time() - t1))
        self.assertLess(time.time() - t1, 15)
        sid, state, mode, notes = self.session_row()
        self.assertEqual(state, "ENDED")
        self.assertEqual(json.loads(notes)["end_reason"], "revoked")

    def test_restricted_token_is_terminal_session_interrupted_no_retry(self):
        ep = self.endpoint(live=False, token_status=403)
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        s = self.new_session(); s.start(); s.join(60)
        self.assertEqual(self.session_row()[1], "INTERRUPTED")
        self.assertIn("restricted", s.error)
        self.assertEqual(ep.hits.count("token"), 1)       # no retry, no other route

    def test_record_off_never_hits_network(self):
        ep = self.endpoint(live=False)
        s = self.new_session(); s.start(); s.join(60)
        self.assertEqual(ep.hits, [])
        self.assertEqual(json.loads(self.session_row()[3])["end_reason"], "revoked")

    def test_interrupted_playback_session_recovered_never_resumed(self):
        with closing(database.connect(str(self.sd / "nova.db"))) as c, c:
            n = time.time()
            c.execute("INSERT INTO twitch_sources (username,created_at,updated_at) VALUES ('streamerx',?,?)", (n, n))
            c.execute("INSERT INTO stream_sessions (source_id,state,started_at,ingest_mode,notes) VALUES (1,'RECORDING',?,'playback','{}')", (n,))
        self.assertEqual(recover_interrupted_sessions(self.sd), 1)
        self.assertEqual(self.session_row()[1], "INTERRUPTED")


class FakeCtx:
    def __init__(self): self.stopping = False
    def heartbeat(self): pass


class FakeBuf:
    session_id = 9
    def __init__(self, d): self.d = d
    def reconcile(self, db): return 0
    def enforce_storage_cap(self): pass
    def next_seq(self): return 0
    def segmenter_output_args(self, n=0): return ["x"]


class WorkerPolicy(Base):
    def worker(self, resolve, capture, **kw):
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        kw.setdefault("resolve_interval", 0.0); kw.setdefault("reconnect_base_seconds", 0.01)
        kw.setdefault("auth_poll_seconds", 0.0)
        return PlaybackWorker(FakeBuf(self.sd), "streamerx", str(self.sd / "nova.db"), self.sd,
                              "ffmpeg", resolve_fn=resolve, capture_fn=capture, **kw)

    def run_w(self, w):
        return asyncio.run(w.run(FakeCtx()))

    @staticmethod
    def res(*a, **k):
        return object()

    def test_offline_at_start_terminal_offline_later_clean_end(self):
        def off(*a, **k): raise tp.PlaybackRefused("offline", "x")
        with self.assertRaises(RuntimeError):
            self.run_w(self.worker(off, None))
        n = {"c": 0}
        def cap(*a, **k):
            n["c"] += 1
            return mock.Mock(reason="media_cap" if n["c"] == 1 else "ended", segments=3)
        calls = []
        def resolve(*a, **k):
            calls.append(1)
            if len(calls) > 1: raise tp.PlaybackRefused("offline", "x")
            return object()
        w = self.worker(resolve, cap); self.run_w(w)
        self.assertEqual((w.end_reason, len(calls)), ("ended", 2))     # re-resolved, then clean end

    def test_restriction_or_challenge_on_reconnect_is_terminal_no_more_resolves(self):
        for kind in ("restricted", "challenge", "blocked"):
            calls = []
            def resolve(*a, **k):
                calls.append(1)
                if len(calls) > 1: raise tp.PlaybackRefused(kind, "x")
                return object()
            cap = lambda *a, **k: mock.Mock(reason="ffmpeg_exit", segments=1)
            w = self.worker(resolve, cap)
            with self.assertRaises(RuntimeError) as c:
                self.run_w(w)
            self.assertIn(kind, str(c.exception))
            self.assertEqual(len(calls), 2)

    def test_transient_errors_bounded(self):
        def resolve(*a, **k): raise tp.PlaybackRefused("error", "x")
        w = self.worker(resolve, None, max_reconnects=2)
        with self.assertRaises(RuntimeError):
            self.run_w(w)
        self.assertEqual(w.reconnects, 3)

    def test_revocation_between_cycles_ends_without_another_resolve(self):
        calls = []
        def resolve(*a, **k):
            calls.append(1); return object()
        def cap(*a, **k):
            source_auth.set_scope(self.sd, "streamerx", "record", False)
            return mock.Mock(reason="media_cap", segments=1)
        w = self.worker(resolve, cap); self.run_w(w)
        self.assertEqual((w.end_reason, len(calls)), ("revoked", 1))


class StartPlayback(Base):
    def reply(self, user, resolve=None, **kw):
        started = []
        class F:
            def __init__(self, *a, **k): started.append((a, k)); self._stopping = threading.Event()
            def start(self): pass
            def alive(self): return False
        out = bot_sessions.startplayback_reply(self.sd, user, "ffmpeg", "ffprobe",
                                               session_factory=F, resolve_fn=resolve)
        return out, started

    def setUp(self):
        super().setUp()
        bot_sessions._sessions.clear()
        with closing(sqlite3.connect(str(self.sd / "nova.db"))) as c, c:
            n = time.time()
            c.execute("INSERT INTO twitch_sources (username,created_at,updated_at) VALUES ('streamerx',?,?)", (n, n))

    def test_refusals_and_started_reply_states_repost(self):
        out, st = self.reply("ghost"); self.assertIn("not registered", out); self.assertEqual(st, [])
        out, st = self.reply("streamerx"); self.assertIn("not authorized", out); self.assertEqual(st, [])
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        def off(*a, **k): raise tp.PlaybackRefused("offline", "x")
        out, st = self.reply("streamerx", off); self.assertIn("offline", out); self.assertEqual(st, [])
        out, st = self.reply("streamerx", lambda *a, **k: object())
        self.assertEqual(len(st), 1)
        self.assertIn("Repost to YouTube is OFF", out)
        self.assertIn("HELD", out)
        source_auth.set_scope(self.sd, "streamerx", "repost", True)
        bot_sessions._sessions.clear()
        out, st = self.reply("streamerx", lambda *a, **k: object())
        self.assertIn("Repost to YouTube is ON", out)


if __name__ == "__main__":
    unittest.main()
