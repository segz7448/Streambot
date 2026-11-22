"""Shared GTA6 title gate across live-capture paths (synthetic only)."""
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from app.bot import sessions as ss
from app.core import content_gate as cg


class _Net(Exception):
    pass


class GateUnit(unittest.TestCase):
    def tearDown(self):
        cg.set_provider(None)

    def test_matching(self):
        for t in ("GTA 6 LIVE", "gta6 day 1", "Grand Theft Auto VI!", "GTA-VI hype"):
            self.assertTrue(cg.title_matches(t), t)
        for t in ("GTA 5 stream", "gta66", "vi", "", None, "Grand Theft Auto V"):
            self.assertFalse(cg.title_matches(t), t)

    def test_fail_closed_reasons(self):
        now = time.time()
        E = cg.Evidence
        self.assertEqual(cg.check("a").reason, "no_source")
        cases = [
            (lambda u: 1 / 0, "unavailable"),
            (lambda u: ("GTA 6", now), "invalid"),                  # untyped
            (lambda u: None, "invalid"),
            (lambda u: E(u, "GTA 6", float("nan"), True), "invalid"),
            (lambda u: E(u, "GTA 6", float("inf"), True), "invalid"),
            (lambda u: E(u, "GTA 6", True, True), "invalid"),       # bool
            (lambda u: E(u, "GTA 6", "123", True), "invalid"),
            (lambda u: E(u, "GTA 6", now, 1), "invalid"),           # live not bool
            (lambda u: E(u, None, now, True), "invalid"),
            (lambda u: E("someoneelse", "GTA 6", now, True), "wrong_channel"),
            (lambda u: E(u, "GTA 6", now, False), "not_live"),
            (lambda u: E(u, "GTA 6", now - 999, True), "stale"),
            (lambda u: E(u, "GTA 6", now + 999, True), "stale"),
            (lambda u: E(u, "just chatting", now, True), "mismatch"),
        ]
        for fn, want in cases:
            cg.set_provider(fn)
            self.assertEqual(cg.check("a").reason, want, want)
        cg.set_provider(lambda u: E(u, "gta 6", now, True))
        self.assertTrue(cg.check("a").ok)
        self.assertTrue(cg.check("@A").ok)       # identity is normalized

    def test_cached_never_calls_provider_and_keeps_last_false(self):
        calls = []
        now = time.time()
        titles = iter(["gta 6", "just chatting"])

        def prov(u):
            calls.append(u)
            return cg.Evidence(u, next(titles), now, True)
        cg.set_provider(prov)
        self.assertFalse(cg.check_cached("a").ok)      # nothing recorded yet
        self.assertTrue(cg.check("a").ok)
        self.assertTrue(cg.check_cached("a").ok)
        self.assertEqual(len(calls), 1)               # cached: no provider call
        self.assertFalse(cg.check("a").ok)
        self.assertFalse(cg.check_cached("a").ok)     # last False is retained
        self.assertEqual(len(calls), 2)
        cg.set_provider(None)
        self.assertFalse(cg.check_cached("a").ok)

    def test_cached_goes_stale(self):
        t0 = time.time()
        cg.set_provider(lambda u: cg.Evidence(u, "gta 6", t0, True))
        self.assertTrue(cg.check("a", now=lambda: t0).ok)
        self.assertTrue(cg.check_cached("a", now=lambda: t0 + 60).ok)
        self.assertEqual(cg.check_cached("a", now=lambda: t0 + 500).reason, "stale")


class ManualPaths(unittest.TestCase):
    def setUp(self):
        import sqlite3
        from app.core.database import migrate
        self.sd = Path(tempfile.mkdtemp())
        c = sqlite3.connect(str(self.sd / "nova.db")); migrate(c)
        c.execute("INSERT INTO twitch_sources(username, created_at, updated_at) VALUES ('streamerx', 1.0, 1.0)")
        c.commit(); c.close()
        (self.sd / "feeds").mkdir()
        (self.sd / "feeds" / "streamerx.url").write_text("https://relay.example/x.m3u8")
        from app.bot import source_auth
        source_auth.set_scope(self.sd, "streamerx", "record", True)
        ss._sessions.clear()
        self.addCleanup(ss._sessions.clear)
        self.addCleanup(cg.set_provider, None)

    def _boom(self, *a, **k):
        raise _Net("network or media touched")

    def test_manual_startclip_live_refused_before_validation(self):
        factory = mock.Mock()
        with mock.patch.object(ss, "validate_feed", self._boom):
            out = ss.startclip_reply(self.sd, "streamerx", "ff", "fp",
                                     session_factory=factory)
        self.assertIn("Not started", out)
        factory.assert_not_called()
        self.assertEqual(ss._sessions, {})

    def test_manual_startclip_live_allowed_with_current_match(self):
        cg.set_provider(lambda u: cg.Evidence(u, "GTA 6 day", time.time(), True))
        started = []

        class F:
            def __init__(self, *a, **k): pass
            def start(self): started.append(1)
            def alive(self): return False
        with mock.patch.object(ss, "validate_feed", lambda f, d: f):
            out = ss.startclip_reply(self.sd, "streamerx", "ff", "fp",
                                     session_factory=F)
        self.assertEqual(started, [1], out)

    def test_manual_startplayback_refused_before_any_network(self):
        with mock.patch("socket.create_connection", self._boom), \
                mock.patch("urllib.request.urlopen", self._boom):
            out = ss.startplayback_reply(self.sd, "streamerx", "ff", "fp")
        self.assertIn("Not started", out)
        self.assertEqual(ss._sessions, {})

    def test_mismatch_and_stale_refused(self):
        for title, age in (("just chatting", 0), ("GTA 6", 999)):
            cg.set_provider(lambda u, t=title, a=age: cg.Evidence(u, t, time.time() - a, True))
            out = ss.startplayback_reply(self.sd, "streamerx", "ff", "fp")
            self.assertIn("Not started", out)

    def test_service_gate_delegates(self):
        from app.bot.service import BotService
        svc = BotService.__new__(BotService)
        self.assertFalse(svc._content_qualified("x"))
        cg.set_provider(lambda u: cg.Evidence(u, "GTA6", time.time(), True))
        self.assertTrue(svc._content_qualified("x"))


class RunningRecheck(unittest.TestCase):
    def tearDown(self):
        cg.set_provider(None)

    def _s(self, feed="https://relay.example/x.m3u8"):
        from app.pipeline.session import ClipSession
        return ClipSession("ff", "fp", feed, "streamerx", tempfile.mkdtemp())

    def test_live_session_gate_closes_and_stays_closed_between_polls(self):
        s = self._s()
        cg.set_provider(lambda u: cg.Evidence(u, "GTA 6", time.time(), True))
        self.assertTrue(s._gate_still_ok(poll=0))
        cg.set_provider(lambda u: cg.Evidence(u, "just chatting", time.time(), True))
        self.assertFalse(s._gate_still_ok(poll=0))
        cg.set_provider(lambda u: cg.Evidence(u, "GTA 6", time.time(), True))
        # within the poll window the retained False is returned, not True
        self.assertFalse(s._gate_still_ok(poll=3600))

    def test_local_recording_is_not_gated_and_scheme_is_normalized(self):
        self.assertFalse(self._s("/tmp/rec.mp4")._live_gated)
        self.assertTrue(self._s("  HTTPS://relay.example/x")._live_gated)
        self.assertTrue(self._s("rtmp://relay.example/x")._live_gated)

    def test_terminal_kind_is_qualification_not_unauthorized(self):
        s = self._s()
        self.assertIsNone(s.terminal_kind)
        s._gate_closed = True
        self.assertEqual(s.terminal_kind, "qualification")


class WorkerReason(unittest.TestCase):
    def test_playback_gate_closure_is_qualification_and_revocation_unauthorized(self):
        import types
        from app.pipeline.session import ClipSession
        s = ClipSession("ff", "fp", "https://r.example/x", "streamerx", tempfile.mkdtemp())
        s._worker = types.SimpleNamespace(end_reason="gate_closed", terminal_kind=None)
        self.assertEqual(s.terminal_kind, "qualification")
        s._worker = types.SimpleNamespace(end_reason="revoked", terminal_kind=None)
        self.assertEqual(s.terminal_kind, "unauthorized")


class LockAndScheduler(unittest.TestCase):
    def setUp(self):
        self.addCleanup(cg.set_provider, None)

    def test_blocking_provider_does_not_stall_registry_lock(self):
        import sqlite3
        import threading
        from app.bot import source_auth
        from app.core.database import migrate
        sd = Path(tempfile.mkdtemp())
        c = sqlite3.connect(str(sd / "nova.db")); migrate(c)
        c.execute("INSERT INTO twitch_sources(username, created_at, updated_at) VALUES ('streamerx', 1.0, 1.0)")
        c.commit(); c.close()
        source_auth.set_scope(sd, "streamerx", "record", True)
        gate, entered = threading.Event(), threading.Event()

        def slow(u):
            entered.set(); gate.wait(5)
            return cg.Evidence(u, "GTA 6", time.time(), True)
        cg.set_provider(slow)
        ss._sessions.clear()
        starter = threading.Thread(
            target=lambda: ss.startplayback_reply(
                sd, "streamerx", "ff", "fp", resolve_fn=lambda *a, **k: (_ for _ in ()).throw(OSError("fixture"))),
            daemon=True)
        starter.start()
        self.assertTrue(entered.wait(2))
        done = []
        w = threading.Thread(target=lambda: done.append(ss.stopclip_reply(sd)), daemon=True)
        w.start(); w.join(2)
        stalled = w.is_alive()
        gate.set(); starter.join(5); ss._sessions.clear()
        self.assertFalse(stalled, "stop/session controls stalled behind the provider")

    def test_start_scheduled_maps_gate_refusal_to_qualification(self):
        import sqlite3
        from app.core.database import migrate
        sd = Path(tempfile.mkdtemp())
        c = sqlite3.connect(str(sd / "nova.db")); migrate(c)
        c.execute("INSERT INTO twitch_sources(username, created_at, updated_at) VALUES ('streamerx', 1.0, 1.0)")
        c.commit(); c.close()
        from app.bot import source_auth
        source_auth.set_scope(sd, "streamerx", "record", True)
        kind, reply = ss.start_scheduled(sd, "streamerx", "ff", "fp", "a" * 32)
        self.assertEqual(kind, "qualification", reply)
        self.assertIn("live-title", reply)
        self.assertNotIn("cannot confirm", reply)
        # rights off stays a rights refusal
        source_auth.set_scope(sd, "streamerx", "record", False)
        kind, _ = ss.start_scheduled(sd, "streamerx", "ff", "fp", "b" * 32)
        self.assertEqual(kind, "unauthorized")


if __name__ == "__main__":
    unittest.main()
