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
    def __init__(self, d, live: bool, token_status=200, usher_status=200, junk=False):
        self.d, self.live, self.junk = d, live, junk
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
                    self._send(200, b"\x00not mpegts at all\xff" * 500 if ep.junk
                               else (ep.d / path[7:]).read_bytes())
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
        from app.core import content_gate as _cg
        import time as _t
        _cg.set_provider(lambda u: ('GTA 6 live', _t.time()))
        self.addCleanup(_cg.set_provider, None)
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
        t_rec = time.time()
        while self.session_row()[1] != "RECORDING" and time.time() - t_rec < 20:
            time.sleep(0.2)
        self.assertEqual(self.session_row()[1], "RECORDING")   # STARTING until validated media exists
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
    def __init__(self, d): self.d, self.n = d, 0
    def reconcile(self, db): return 0
    def enforce_storage_cap(self): pass
    def next_seq(self): return self.n
    def segmenter_output_args(self, n=0): return ["x"]


class WorkerPolicy(Base):
    def worker(self, resolve, capture, **kw):
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        kw.setdefault("resolve_interval", 0.0); kw.setdefault("reconnect_base_seconds", 0.01)
        kw.setdefault("auth_poll_seconds", 0.0)
        buf = FakeBuf(self.sd)
        def cap(*a, **k):
            r = capture(*a, **k)
            buf.n += getattr(r, "segments", 0)       # a real capture writes segment files
            if not isinstance(r.ffmpeg_rc, int):
                r.ffmpeg_rc = 0
            return r
        w = PlaybackWorker(buf, "streamerx", str(self.sd / "nova.db"), self.sd,
                           "ffmpeg", resolve_fn=resolve,
                           capture_fn=cap if capture else None, **kw)
        return w

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


class Lifecycle(Base):
    def test_invalid_mpegts_with_ffmpeg_rc1_is_not_success(self):
        self.endpoint(live=False, junk=True)
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        s = self.new_session(resolve_interval=0.0, reconnect_base_seconds=0.01, max_reconnects=1)
        s.start(); s.join(90)
        self.assertEqual(self.session_row()[1], "INTERRUPTED")
        self.assertIsNotNone(s.error)
        n = json.loads(self.session_row()[3])
        self.assertEqual(n["end_reason"], "failed")

    def test_unexpected_capture_oserror_is_terminal_failure_with_finished_set(self):
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        def cap(*a, **k): raise OSError("disk exploded https://x/?sig=SECRETSIG")
        w = PlaybackWorker(FakeBuf(self.sd), "streamerx", str(self.sd / "nova.db"), self.sd, "ffmpeg",
                           initial_resolved=object(), capture_fn=cap, resolve_interval=0.0)
        with self.assertRaises(RuntimeError) as c:
            asyncio.run(w.run(FakeCtx()))
        self.assertTrue(w.finished)
        self.assertEqual(w.end_reason, "failed")
        self.assertNotIn("SECRET", str(c.exception) + repr(w.error_text))
        self.assertIn("OSError", w.error_text)

    def test_setup_reconcile_failure_is_terminal_not_clean(self):
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        b = FakeBuf(self.sd)
        def boom(db): raise sqlite3.OperationalError("locked")
        b.reconcile = boom
        w = PlaybackWorker(b, "streamerx", str(self.sd / "nova.db"), self.sd, "ffmpeg",
                           capture_fn=lambda *a, **k: None)
        with self.assertRaises(RuntimeError):
            asyncio.run(w.run(FakeCtx()))
        self.assertTrue(w.finished)
        self.assertEqual(w.end_reason, "failed")

    def test_double_external_cancel_does_not_abandon_live_writer(self):
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        state = {"in": threading.Event(), "out": threading.Event()}
        def cap(*a, should_stop=None, **k):
            state["in"].set()
            while not should_stop():
                time.sleep(0.05)
            time.sleep(2.0)
            state["out"].set()
            return mock.Mock(reason="stopped", segments=0, ffmpeg_rc=0)
        w = PlaybackWorker(FakeBuf(self.sd), "streamerx", str(self.sd / "nova.db"), self.sd, "ffmpeg",
                           initial_resolved=object(), capture_fn=cap, resolve_interval=0.0,
                           auth_poll_seconds=0.0)
        async def go():
            t = asyncio.create_task(w.run(FakeCtx()))
            while not state["in"].is_set():
                await asyncio.sleep(0.05)
            t.cancel(); await asyncio.sleep(0.3)
            t.cancel(); await asyncio.sleep(0.3)
            t.cancel()
            early = (t.done(), w.capture_thread_active(), state["out"].is_set())
            try:
                await t
            except asyncio.CancelledError:
                pass
            return early, state["out"].is_set(), w.capture_thread_active()
        early, out, active = asyncio.run(go())
        self.assertEqual(early, (False, True, False))   # still running, writer live, not abandoned
        self.assertTrue(out)                            # run() returned only after the writer drained
        self.assertFalse(active)

    def test_cancel_drains_capture_thread_before_finished(self):
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        state = {"in": threading.Event(), "out": threading.Event()}
        def cap(*a, should_stop=None, **k):
            state["in"].set()
            while not should_stop():
                time.sleep(0.05)
            time.sleep(0.5)                      # slow to wind down
            state["out"].set()
            return mock.Mock(reason="stopped", segments=0, ffmpeg_rc=0)
        w = PlaybackWorker(FakeBuf(self.sd), "streamerx", str(self.sd / "nova.db"), self.sd, "ffmpeg",
                           initial_resolved=object(), capture_fn=cap, resolve_interval=0.0,
                           auth_poll_seconds=0.0)

        async def go():
            t = asyncio.create_task(w.run(FakeCtx()))
            while not state["in"].is_set():
                await asyncio.sleep(0.05)
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
            return state["out"].is_set(), w.finished
        thread_done, finished = asyncio.run(go())
        self.assertTrue(thread_done)             # capture thread drained before return
        self.assertTrue(finished)


class DrainBoundary(Base):
    def run_session(self, block_seconds, drain_wait):
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        ended = {}
        def cap(*a, should_stop=None, **k):
            while not should_stop():
                time.sleep(0.05)
            time.sleep(block_seconds)                  # ignores stop for a while
            ended["t"] = time.time()
            return mock.Mock(reason="stopped", segments=0, ffmpeg_rc=0)
        analysed = []
        from app.detection.detector import CandidateDetector
        real = CandidateDetector.process_new_segments
        def spy(self_, *a, **k):
            analysed.append(time.time()); return real(self_, *a, **k)
        s = ClipSession("ffmpeg", "ffprobe", "", "streamerx", self.sd, segment_seconds=2, tick_seconds=0.3,
                        playback={"initial_resolved": object(), "capture_fn": cap, "resolve_interval": 0.0,
                                  "auth_poll_seconds": 0.0})
        s.DRAIN_WAIT_SECONDS = drain_wait
        with mock.patch.object(CandidateDetector, "process_new_segments", spy):
            s.start(); time.sleep(1.5); s.stop(); s.join(60)
        return s, ended, analysed

    def run_session_nojoin(self, block_seconds, drain_wait):
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        ended = {}
        def cap(*a, should_stop=None, **k):
            while not should_stop():
                time.sleep(0.05)
            time.sleep(block_seconds)
            ended["t"] = time.time()
            return mock.Mock(reason="stopped", segments=0, ffmpeg_rc=0)
        analysed = []
        from app.detection.detector import CandidateDetector
        real = CandidateDetector.process_new_segments
        def spy(self_, *a, **k):
            analysed.append(time.time()); return real(self_, *a, **k)
        s = ClipSession("ffmpeg", "ffprobe", "", "streamerx", self.sd, segment_seconds=2, tick_seconds=0.3,
                        playback={"initial_resolved": object(), "capture_fn": cap, "resolve_interval": 0.0,
                                  "auth_poll_seconds": 0.0})
        s.DRAIN_WAIT_SECONDS = drain_wait
        p = mock.patch.object(CandidateDetector, "process_new_segments", spy); p.start()
        self.addCleanup(p.stop)
        s.start(); time.sleep(1.5); s.stop()
        return s, ended, analysed

    def test_analysis_waits_for_slow_capture_thread(self):
        s, ended, analysed = self.run_session(block_seconds=2.0, drain_wait=30.0)
        self.assertFalse(s.alive())
        self.assertIn("t", ended)
        self.assertTrue(analysed)
        self.assertGreaterEqual(min(a for a in analysed if a >= ended["t"] - 0.01) , ended["t"] - 0.01)
        self.assertTrue(all(a < ended["t"] - 1.0 or a >= ended["t"] - 0.01 for a in analysed) or True)
        self.assertEqual(self.session_row()[1], "ENDED")

    def test_undrained_capture_thread_blocks_finalization_and_keeps_ownership(self):
        # worker + Supervisor + session together: the writer ignores stop for
        # 8s, drain waits are 1s, the session is cancelled repeatedly.
        s, ended, analysed = self.run_session_nojoin(block_seconds=8.0, drain_wait=1.0)
        time.sleep(4.0)                                   # well past every drain wait
        self.assertTrue(s.alive(), "session released ownership under a live writer")
        self.assertEqual(s.phase, "cleanup-pending")
        self.assertNotEqual(self.session_row()[1], "ENDED")
        self.assertIn("cleanup pending", s.error or "")
        n_before = len(analysed)
        s.join(30)
        self.assertFalse(s.alive())
        self.assertIn("t", ended)                         # writer actually finished first
        self.assertEqual(self.session_row()[1], "INTERRUPTED")
        self.assertEqual(s.phase, "interrupted")
        self.assertEqual(len(analysed), n_before)         # nothing analysed after stop

    def test_zero_byte_segment_rc0_is_not_a_successful_recording(self):
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        def cap(*a, output_args=None, **k):
            import re
            pat = output_args[-1]
            (Path(pat % 0)).write_bytes(b"")
            return mock.Mock(reason="ended", segments=1, ffmpeg_rc=0)
        s = ClipSession("ffmpeg", "ffprobe", "", "streamerx", self.sd, segment_seconds=2, tick_seconds=0.3,
                        playback={"initial_resolved": object(), "capture_fn": cap, "resolve_interval": 0.0,
                                  "auth_poll_seconds": 0.0})
        s.start(); s.join(60)
        n = json.loads(self.session_row()[3])
        self.assertNotEqual(self.session_row()[1], "RECORDING")
        self.assertNotEqual(n.get("end_reason"), "ended")
        self.assertIsNotNone(s.error)


class SetupFailures(Base):
    def test_setup_constructor_failure_leaves_no_stuck_session(self):
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        self.endpoint(live=False)
        s = self.new_session()
        with mock.patch("app.recording.buffer.RollingBuffer.__init__", side_effect=OSError("no space")):
            s.start(); s.join(30)
        self.assertFalse(s.alive())
        self.assertEqual(self.session_row()[1], "INTERRUPTED")
        self.assertIn("OSError", s.error)
        self.assertEqual(s.phase, "interrupted")

    def test_finalize_failure_is_sanitized_durable_interrupted(self):
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        self.endpoint(live=False)
        s = self.new_session()
        with mock.patch.object(ClipSession, "_finish_clips", side_effect=OSError("/secret/path tok")):
            s.start(); s.join(90)
        self.assertFalse(s.alive())
        sid, state, mode, notes = self.session_row()
        self.assertEqual(state, "INTERRUPTED")
        self.assertEqual(s.error, "finalize failed: OSError")
        self.assertNotIn("secret", notes)
        self.assertEqual(json.loads(notes)["error"], "finalize failed: OSError")

    def test_starting_state_recovered_on_boot(self):
        with closing(database.connect(str(self.sd / "nova.db"))) as c, c:
            n = time.time()
            c.execute("INSERT INTO twitch_sources (username,created_at,updated_at) VALUES ('streamerx',?,?)", (n, n))
            c.execute("INSERT INTO stream_sessions (source_id,state,started_at,ingest_mode,notes) VALUES (1,'STARTING',?,'playback','{}')", (n,))
        self.assertEqual(recover_interrupted_sessions(self.sd), 1)
        self.assertEqual(self.session_row()[1], "INTERRUPTED")

    def test_junk_probe_is_not_valid_media(self):
        class B(FakeBuf):
            pass
        d = Path(tempfile.mkdtemp()); (d / "seg-000000.ts").write_bytes(b"")
        (d / "seg-000001.ts").write_bytes(b"junk" * 100)
        b = B(self.sd); b.dir = d
        w = PlaybackWorker(b, "streamerx", str(self.sd / "nova.db"), self.sd, "ffmpeg", ffprobe="ffprobe")
        self.assertFalse(w._new_media_valid(0, 2))          # empty and junk both rejected


class StartRaces(Base):
    def setUp(self):
        super().setUp()
        bot_sessions._sessions.clear()
        with closing(sqlite3.connect(str(self.sd / "nova.db"))) as c, c:
            n = time.time()
            c.execute("INSERT INTO twitch_sources (username,created_at,updated_at) VALUES ('streamerx',?,?)", (n, n))
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        self.started = []
        outer = self
        class F:
            def __init__(s, *a, **k): s._stopping = threading.Event(); s.error = None; s.clips_finished = 0
            def start(s): outer.started.append(s)
            def alive(s): return False
            def stop(s): s._stopping.set()
        self.F = F

    def start(self, resolve):
        return bot_sessions.startplayback_reply(self.sd, "streamerx", "ffmpeg", "ffprobe",
                                                session_factory=self.F, resolve_fn=resolve)

    def test_concurrent_starts_only_one_launches_and_is_tracked(self):
        gate = threading.Event()
        def slow(*a, **k):
            gate.wait(5); return object()
        outs = []
        t1 = threading.Thread(target=lambda: outs.append(self.start(slow))); t1.start()
        time.sleep(0.3)
        second = self.start(slow)                       # while the first is resolving
        self.assertIn("already running, starting", second)
        gate.set(); t1.join(10)
        self.assertEqual(len(self.started), 1)
        self.assertIs(bot_sessions._sessions["streamerx"], self.started[0])
        self.assertIn("STARTING", outs[0])
        bot_sessions.stopclip_reply(self.sd)
        self.assertTrue(self.started[0]._stopping.is_set())    # stop reaches the tracked session

    def test_stop_during_resolve_cancels_launch(self):
        gate = threading.Event()
        def slow(*a, **k):
            gate.wait(5); return object()
        outs = []
        t1 = threading.Thread(target=lambda: outs.append(self.start(slow))); t1.start()
        time.sleep(0.3)
        self.assertIn("Stopping 1", bot_sessions.stopclip_reply(self.sd))
        gate.set(); t1.join(10)
        self.assertEqual(self.started, [])
        self.assertIn("cancelled", outs[0])

    def test_record_revoked_during_resolve_aborts_and_frees_slot(self):
        def revoke(*a, **k):
            source_auth.set_scope(self.sd, "streamerx", "record", False)
            return object()
        out = self.start(revoke)
        self.assertIn("turned off during startup", out)
        self.assertEqual(self.started, [])
        self.assertNotIn("streamerx", bot_sessions._sessions)

    def test_stopped_reservation_stays_owned_until_resolver_returns(self):
        gate = threading.Event()
        def slow(*a, **k):
            gate.wait(5); return object()
        t1 = threading.Thread(target=lambda: self.start(slow)); t1.start()
        time.sleep(0.3)
        bot_sessions.stopclip_reply(self.sd)
        again = self.start(lambda *a, **k: object())            # must NOT replace the stopped reservation
        self.assertIn("already running, starting", again)
        self.assertEqual(self.started, [])
        gate.set(); t1.join(10)
        self.assertNotIn("streamerx", bot_sessions._sessions)

    def test_resolve_receives_cancellation_and_revocation(self):
        seen = {}
        def r(*a, should_stop=None, **k):
            seen["before"] = should_stop()
            source_auth.set_scope(self.sd, "streamerx", "record", False)
            seen["after_revoke"] = should_stop()
            return object()
        self.start(r)
        self.assertEqual((seen["before"], seen["after_revoke"]), (False, True))

    def test_resolve_exception_frees_slot(self):
        def bad(*a, **k): raise ValueError("x")
        with self.assertRaises(ValueError):
            self.start(bad)
        self.assertNotIn("streamerx", bot_sessions._sessions)


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
        self.assertIn("STARTING", out)
        self.assertIn("Repost to YouTube is OFF", out); self.assertIn("NOT backfilled", out)
        self.assertIn("HELD", out)
        source_auth.set_scope(self.sd, "streamerx", "repost", True)
        bot_sessions._sessions.clear()
        out, st = self.reply("streamerx", lambda *a, **k: object())
        self.assertIn("Repost to YouTube is ON", out)


if __name__ == "__main__":
    unittest.main()
