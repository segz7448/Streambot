"""NOVA_PLAYBACK_MAX_HEIGHT threading for public playback recording."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import time
from pathlib import Path
from unittest import mock

from app.bot import sessions as ss
from app.bot import source_auth
from app.core import content_gate as cg
from app.core.database import migrate


def test_default_cap_is_720(monkeypatch):
    monkeypatch.delenv("NOVA_PLAYBACK_MAX_HEIGHT", raising=False)
    assert ss._playback_max_height() == 720


def test_env_raises_cap(monkeypatch):
    monkeypatch.setenv("NOVA_PLAYBACK_MAX_HEIGHT", "1080")
    assert ss._playback_max_height() == 1080


def test_env_unparsable_falls_back(monkeypatch):
    monkeypatch.setenv("NOVA_PLAYBACK_MAX_HEIGHT", "best")
    assert ss._playback_max_height() == 720


def test_env_out_of_range_falls_back(monkeypatch):
    monkeypatch.setenv("NOVA_PLAYBACK_MAX_HEIGHT", "9999")
    assert ss._playback_max_height() == 720
    monkeypatch.setenv("NOVA_PLAYBACK_MAX_HEIGHT", "12")
    assert ss._playback_max_height() == 720


def _prepared_state_dir():
    sd = Path(tempfile.mkdtemp())
    c = sqlite3.connect(str(sd / "nova.db"))
    migrate(c)
    c.execute("INSERT INTO twitch_sources(username, created_at, updated_at)"
              " VALUES ('streamerx', 1.0, 1.0)")
    c.commit()
    c.close()
    (sd / "feeds").mkdir()
    (sd / "feeds" / "streamerx.url").write_text("https://relay.example/x.m3u8")
    source_auth.set_scope(sd, "streamerx", "record", True)
    return sd


def _match_provider(u):
    return cg.Evidence(u, "GTA 6", time.time(), True)


def test_startplayback_threads_max_height(monkeypatch):
    monkeypatch.setenv("NOVA_PLAYBACK_MAX_HEIGHT", "1080")
    sd = _prepared_state_dir()
    cg.set_provider(_match_provider)
    ss._sessions.clear()
    seen = {}

    def resolve(*a, **k):
        seen["resolve_max_height"] = k.get("max_height")
        return object()

    factory = mock.Mock()
    try:
        with mock.patch.object(ss, "validate_feed", lambda f, d: f):
            reply = ss.startplayback_reply(sd, "streamerx", "ff", "fp",
                                            session_factory=factory,
                                            resolve_fn=resolve)
        assert seen["resolve_max_height"] == 1080
        assert factory.call_count == 1
        pb = factory.call_args.kwargs["playback"]
        assert pb["max_height"] == 1080
        assert "STARTING" in reply or "tarting" in reply
    finally:
        ss._sessions.clear()


def test_startplayback_default_threads_720(monkeypatch):
    monkeypatch.delenv("NOVA_PLAYBACK_MAX_HEIGHT", raising=False)
    sd = _prepared_state_dir()
    cg.set_provider(_match_provider)
    ss._sessions.clear()
    seen = {}

    def resolve(*a, **k):
        seen["resolve_max_height"] = k.get("max_height")
        return object()

    factory = mock.Mock()
    try:
        with mock.patch.object(ss, "validate_feed", lambda f, d: f):
            ss.startplayback_reply(sd, "streamerx", "ff", "fp",
                                    session_factory=factory,
                                    resolve_fn=resolve)
        assert seen["resolve_max_height"] == 720
        pb = factory.call_args.kwargs["playback"]
        assert pb["max_height"] == 720
    finally:
        ss._sessions.clear()


# ---------------------------------------------------------------------------
# Real propagation pin: env -> startplayback_reply -> REAL ClipSession ->
# REAL PlaybackWorker -> real tp.resolve -> the 1080 child playlist is what
# gets fetched. No session-factory or worker mocks in the chain. Loopback
# endpoint only; no third-party host is contacted.
# ---------------------------------------------------------------------------

def _two_variant_endpoint(tmp_path, hits):
    import http.server
    import json as _json
    import shutil
    import subprocess
    import threading
    from app.ingestion import twitch_playback as tp

    fix = tmp_path / "fix"
    fix.mkdir()
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
                    "-i", "testsrc=size=320x180:rate=15", "-f", "lavfi",
                    "-i", "sine=frequency=440", "-t", "8", "-c:v", "libx264",
                    "-pix_fmt", "yuv420p", "-force_key_frames", "expr:gte(t,n_forced*2)",
                    "-c:a", "aac", "-f", "hls", "-hls_time", "2", "-hls_list_size", "0",
                    str(fix / "v.m3u8")], check=True)
    shutil.copy(fix / "v.m3u8", fix / "v1080.m3u8")

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body):
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            self._send(200, _json.dumps({"data": {"streamPlaybackAccessToken": {
                "value": _json.dumps({"authorization": {"forbidden": False}}),
                "signature": "s"}}}).encode())

        def do_GET(self):
            path = self.path.split("?")[0]
            base = f"http://127.0.0.1:{self.server.server_port}"
            if path.startswith("/api/channel/hls/"):
                self._send(200, (
                    "#EXTM3U\n"
                    "#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720\n"
                    f"{base}/media/v.m3u8\n"
                    "#EXT-X-STREAM-INF:BANDWIDTH=6000000,RESOLUTION=1920x1080\n"
                    f"{base}/media/v1080.m3u8\n").encode())
            elif path in ("/media/v.m3u8", "/media/v1080.m3u8"):
                hits.append(path)
                self._send(200, (fix / path[7:]).read_bytes())
            elif path.startswith("/media/") and (fix / path[7:]).exists():
                self._send(200, (fix / path[7:]).read_bytes())
            else:
                self._send(404, b"")

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_port}"
    return srv, base


def test_real_chain_threads_1080_to_worker_resolve(monkeypatch, tmp_path):
    import time
    from app.ingestion import twitch_playback as tp
    from app.pipeline.session import ClipSession

    monkeypatch.setenv("NOVA_PLAYBACK_MAX_HEIGHT", "1080")
    hits = []
    srv, base = _two_variant_endpoint(tmp_path, hits)
    monkeypatch.setattr(tp, "GQL_URL", base + "/gql")
    monkeypatch.setattr(tp, "USHER_BASE", base)
    monkeypatch.setattr(tp, "_host_ok", lambda url, host_ok=None: url.startswith(base + "/"))

    sd = _prepared_state_dir()
    cg.set_provider(_match_provider)
    ss._sessions.clear()
    tp._last.clear()

    def factory(*a, **kw):
        kw.setdefault("segment_seconds", 2)
        kw.setdefault("tick_seconds", 0.3)
        return ClipSession(*a, **kw)

    sess = None
    try:
        reply = ss.startplayback_reply(sd, "streamerx", "ffmpeg", "ffprobe",
                                        session_factory=factory)
        assert "STARTING" in reply or "tarting" in reply
        sess = list(ss._sessions.values())[0]
        deadline = time.time() + 30
        while time.time() < deadline and "/media/v1080.m3u8" not in hits:
            time.sleep(0.1)
        # the REAL PlaybackWorker was built with the raised cap...
        assert sess._worker.max_height == 1080
        # ...and its real resolve fetched the 1080 child playlist
        assert "/media/v1080.m3u8" in hits
        assert "/media/v.m3u8" not in hits  # 720 variant never selected
    finally:
        if sess is not None:
            sess.stop()
            sess.join(60)
        ss._sessions.clear()
        srv.shutdown()
        srv.server_close()
