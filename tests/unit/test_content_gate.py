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
        self.assertEqual(cg.check("a").reason, "no_source")
        cg.set_provider(lambda u: 1 / 0)
        self.assertEqual(cg.check("a").reason, "unavailable")
        cg.set_provider(lambda u: ("GTA 6", now - 999))
        self.assertEqual(cg.check("a").reason, "stale")
        cg.set_provider(lambda u: ("GTA 6", now + 999))
        self.assertEqual(cg.check("a").reason, "stale")
        cg.set_provider(lambda u: ("just chatting", now))
        self.assertEqual(cg.check("a").reason, "mismatch")
        cg.set_provider(lambda u: ("gta 6", now))
        self.assertTrue(cg.check("a").ok)


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
        cg.set_provider(lambda u: ("GTA 6 day", time.time()))
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
            cg.set_provider(lambda u, t=title, a=age: (t, time.time() - a))
            out = ss.startplayback_reply(self.sd, "streamerx", "ff", "fp")
            self.assertIn("Not started", out)

    def test_service_gate_delegates(self):
        from app.bot.service import BotService
        svc = BotService.__new__(BotService)
        self.assertFalse(svc._content_qualified("x"))
        cg.set_provider(lambda u: ("GTA6", time.time()))
        self.assertTrue(svc._content_qualified("x"))


class RunningRecheck(unittest.TestCase):
    def tearDown(self):
        cg.set_provider(None)

    def test_live_session_gate_closes_when_title_changes(self):
        from app.pipeline.session import ClipSession
        s = ClipSession("ff", "fp", "https://relay.example/x.m3u8", "streamerx",
                        tempfile.mkdtemp())
        cg.set_provider(lambda u: ("GTA 6", time.time()))
        self.assertTrue(s._gate_still_ok(poll=0))
        cg.set_provider(lambda u: ("just chatting", time.time()))
        self.assertFalse(s._gate_still_ok(poll=0))

    def test_local_recording_is_not_gated(self):
        from app.pipeline.session import ClipSession
        s = ClipSession("ff", "fp", "/tmp/rec.mp4", "streamerx", tempfile.mkdtemp())
        self.assertFalse(s._live_gated)


if __name__ == "__main__":
    unittest.main()
