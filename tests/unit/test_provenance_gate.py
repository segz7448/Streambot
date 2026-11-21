import tempfile, time, unittest
from contextlib import closing
from pathlib import Path
from app.core import database
from app.distribution import publish_records as pr
from app.core import provenance
repost_allowed = lambda c, i: provenance.repost_allowed(c, i)


class Gate(unittest.TestCase):
    def setUp(self):
        from unittest import mock
        pp = mock.patch.object(provenance, 'repost_allowed', provenance._repost_allowed_real)
        pp.start(); self.addCleanup(pp.stop)
        self.d = tempfile.mkdtemp()
        with closing(pr._connect(self.d)) as c:
            database.migrate(c)
            n = time.time()
            c.execute("INSERT INTO twitch_sources (username,created_at,updated_at) VALUES ('streamerx',?,?)", (n, n))
            c.execute("INSERT INTO stream_sessions (source_id,ingest_mode,notes) VALUES (1,'playback','{}')")
            c.execute("INSERT INTO stream_sessions (source_id,ingest_mode) VALUES (1,'feed')")
            c.execute("INSERT INTO clips (id,session_id,status,created_at) VALUES (10,1,'HELD',?)", (n,))
            c.execute("INSERT INTO clips (id,session_id,status,created_at) VALUES (11,2,'HELD',?)", (n,))
            c.commit()

    def setting(self, v):
        with closing(pr._connect(self.d)) as c:
            c.execute("INSERT OR REPLACE INTO settings (key,value) VALUES ('srcauth:streamerx:repost',?)", (v,))
            c.commit()

    def check(self, clip):
        with closing(pr._connect(self.d)) as c:
            return repost_allowed(c, clip).allowed

    def test_default_off_then_on_then_revoked(self):
        self.assertFalse(self.check(10))
        self.setting("1"); self.assertTrue(self.check(10))
        self.setting("0"); self.assertFalse(self.check(10))
        self.setting("garbage"); self.assertFalse(self.check(10))

    def test_non_playback_clip_unaffected(self):
        self.assertTrue(self.check(11))

    def test_reserve_denies_and_writes_nothing_then_allows_when_on(self):
        with pr.ClipLease(self.d, 10) as lease:
            dec, _ = pr.reserve(self.d, 10, "t", lease=lease)
            self.assertEqual(dec, "repost_denied")
            with closing(pr._connect(self.d)) as c:
                self.assertEqual(c.execute("SELECT count(*) FROM publish_records").fetchone()[0], 0)
            self.setting("1")
            dec, rec = pr.reserve(self.d, 10, "t", lease=lease)
            self.assertNotEqual(dec, "repost_denied")
