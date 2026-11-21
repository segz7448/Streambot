"""Real-gate coverage (no legacy shim) for playback-origin repost control
across native upload, manual /publish, admission and handoff jobs, with
missing-grant vs unreadable-provenance outcomes and owner retry."""
import time
import unittest
from contextlib import closing
from unittest import mock

from app.bot import commands_full, handoff, source_auth
from app.core import database, provenance
from app.core.intake import HANDOFF_JOB_KIND
from app.core.jobs import JobQueue
from app.distribution import publish_records
from tests.unit import test_native_upload as nu


class Paths(unittest.TestCase):
    def setUp(self):
        p = mock.patch.object(provenance, "repost_allowed",
                              provenance._repost_allowed_real)
        p.start(); self.addCleanup(p.stop)
        e = mock.patch.dict("os.environ", {"NOVA_YOUTUBE_CLIENT_ID": "x",
                                           "NOVA_YOUTUBE_CLIENT_SECRET": "x",
                                           "NOVA_YOUTUBE_REFRESH_TOKEN": "x"})
        e.start(); self.addCleanup(e.stop)
        self.h = nu.NativeUploadTests("test_gate_default_off")
        self.h.setUp(); self.addCleanup(self.h.tearDown)
        self.sd, self.db = self.h.state_dir, self.h.db
        with closing(database.connect(self.db)) as c, c:
            n = time.time()
            c.execute("INSERT INTO twitch_sources (username,created_at,updated_at) VALUES ('streamerx',?,?)", (n, n))
            self.sid = c.execute("INSERT INTO stream_sessions (source_id,state,ingest_mode,notes) VALUES (1,'ENDED','playback','{}')").lastrowid
        self.cid = self.h._add_held_clip()
        self.sql("UPDATE clips SET session_id=? WHERE id=?", (self.sid, self.cid))

    def sql(self, q, a=()):
        with closing(database.connect(self.db)) as c:
            c.execute("PRAGMA foreign_keys=OFF")
            with c:
                return c.execute(q, a).fetchall()

    def job(self):
        return self.sql("SELECT state,last_error FROM job_queue WHERE kind=?", (HANDOFF_JOB_KIND,))[0]

    def test_native_denied_then_owner_grant_retries_and_revoke_denies_again(self):
        self.h._admit(self.cid)
        self.h._run_once()
        st, err = self.job()
        self.assertEqual(st, "FAILED")
        self.assertTrue(err.startswith(provenance.DENIED_PREFIX), err)
        self.assertEqual(self.h.publisher.calls, [])
        self.assertIsNone(publish_records.get(self.sd, self.cid))
        self.assertEqual(self.sql("SELECT status FROM clips WHERE id=?", (self.cid,))[0][0], "HELD")
        source_auth.set_scope(self.sd, "streamerx", "repost", True)   # owner grants: requeue
        self.assertEqual(self.job()[0], "PENDING")
        source_auth.set_scope(self.sd, "streamerx", "repost", False)  # revoked before it runs
        self.h._run_once()
        self.assertEqual(self.job()[0], "FAILED")
        self.assertEqual(self.h.publisher.calls, [])
        source_auth.set_scope(self.sd, "streamerx", "repost", True)
        self.h._run_once()
        self.assertEqual(len(self.h.publisher.calls), 1)

    def test_orphan_source_is_unreadable_and_not_auto_retried(self):
        self.h._admit(self.cid)
        source_auth.set_scope(self.sd, "streamerx", "repost", True)
        self.sql("DELETE FROM twitch_sources WHERE id=1") if False else None
        with closing(database.connect(self.db)) as c, c:
            c.execute("PRAGMA foreign_keys=OFF"); c.execute("DELETE FROM twitch_sources WHERE id=1")
        self.assertFalse(provenance.repost_allowed(self._c(), self.cid).allowed)
        self.h._run_once()
        st, err = self.job()
        self.assertEqual(st, "FAILED")
        self.assertTrue(err.startswith(provenance.UNREADABLE_PREFIX), err)
        self.assertEqual(self.h.publisher.calls, [])
        self.assertEqual(provenance.requeue_denied(self._c(), 'streamerx'), 0)

    def _c(self):
        c = database.connect(self.db); self.addCleanup(c.close); return c

    def test_unknown_mode_null_session_and_missing_schema_denied(self):
        c = self._c()
        self.sql("UPDATE stream_sessions SET ingest_mode='weird' WHERE id=?", (self.sid,))
        self.assertEqual(provenance.repost_allowed(c, self.cid).reason, "unknown ingest mode")
        self.sql("UPDATE stream_sessions SET ingest_mode=NULL WHERE id=?", (self.sid,))
        self.assertFalse(provenance.repost_allowed(c, self.cid).allowed)
        self.sql("UPDATE clips SET session_id=NULL WHERE id=?", (self.cid,))
        self.assertEqual(provenance.repost_allowed(c, self.cid).reason, "clip has no session origin")
        self.sql("UPDATE clips SET session_id=999 WHERE id=?", (self.cid,))
        self.assertEqual(provenance.repost_allowed(c, self.cid).reason, "session row missing")
        import sqlite3
        bare = sqlite3.connect(":memory:")
        self.assertEqual(provenance.repost_allowed(bare, 1).reason, "provenance schema missing")
        self.assertEqual(provenance.repost_allowed(c, 12345).reason, "clip row missing")
        for m in ("feed", "upload"):
            self.sql("UPDATE stream_sessions SET ingest_mode=? WHERE id=?", (m, self.sid))
            self.sql("UPDATE clips SET session_id=? WHERE id=?", (self.sid, self.cid))
            self.assertTrue(provenance.repost_allowed(c, self.cid).allowed)

    def test_grant_requeues_only_that_source_and_is_atomic(self):
        # second playback source with its own denied job
        with closing(database.connect(self.db)) as c, c:
            n = time.time()
            c.execute("INSERT INTO twitch_sources (username,created_at,updated_at) VALUES ('othery',?,?)", (n, n))
            s2 = c.execute("INSERT INTO stream_sessions (source_id,state,ingest_mode,notes) VALUES (2,'ENDED','playback','{}')").lastrowid
        cid2 = self.h._add_held_clip()
        self.sql("UPDATE clips SET session_id=? WHERE id=?", (s2, cid2))
        self.h._admit(self.cid); self.h._admit(cid2)
        self.h._run_once(); self.h._run_once()
        self.assertEqual({r[0] for r in self.sql("SELECT state FROM job_queue WHERE kind=?", (HANDOFF_JOB_KIND,))}, {"FAILED"})
        # atomicity: requeue failure rolls the grant back and the reply says nothing changed
        import sqlite3
        with mock.patch.object(provenance, "requeue_denied", side_effect=sqlite3.OperationalError("boom")):
            out = source_auth.sourceauth_reply(self.sd, "streamerx repost on")
        self.assertIn("Nothing changed", out)
        self.assertFalse(source_auth.allowed(self.sd, "streamerx", "repost"))
        out = source_auth.sourceauth_reply(self.sd, "streamerx repost on")
        self.assertIn("1 earlier-refused", out)
        states = {}
        for jid, st in self.sql("SELECT payload_json, state FROM job_queue WHERE kind=?", (HANDOFF_JOB_KIND,)):
            states[provenance._job_clip(HANDOFF_JOB_KIND, jid)] = st
        self.assertEqual(states[self.cid], "PENDING")
        self.assertEqual(states[cid2], "FAILED")

    def test_manual_publish_replies_visibly_and_writes_nothing(self):
        out = commands_full.publish_reply(self.sd, str(self.cid))
        self.assertIn("repost is not authorized", out)
        self.assertIn("HELD", out)
        self.assertIsNone(publish_records.get(self.sd, self.cid))
        with closing(database.connect(self.db)) as c, c:
            c.execute("PRAGMA foreign_keys=OFF"); c.execute("DELETE FROM twitch_sources WHERE id=1")
        out = commands_full.publish_reply(self.sd, str(self.cid))
        self.assertIn("could not be verified", out)
        self.assertIsNone(publish_records.get(self.sd, self.cid))

    def test_admission_job_denied_visibly_not_completed(self):
        with closing(database.connect(self.db)) as c:
            JobQueue(c).enqueue(handoff.ADMIT_JOB_KIND, {"clip_row_id": self.cid})
        handoff._process_one_admission(self._c(), self.sd)
        r = self.sql("SELECT state,last_error FROM job_queue WHERE kind=?", (handoff.ADMIT_JOB_KIND,))[0]
        self.assertEqual(r[0], "FAILED")
        self.assertTrue(r[1].startswith(provenance.DENIED_PREFIX))
        self.assertEqual(self.sql("SELECT status FROM clips WHERE id=?", (self.cid,))[0][0], "HELD")
        source_auth.set_scope(self.sd, "streamerx", "repost", True)
        self.assertEqual(self.sql("SELECT state FROM job_queue WHERE kind=?", (handoff.ADMIT_JOB_KIND,))[0][0], "PENDING")


if __name__ == "__main__":
    unittest.main()
