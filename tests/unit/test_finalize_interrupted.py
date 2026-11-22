"""Boot recovery of PENDING clips from interrupted sessions: narrow,
writer-safe, idempotent, no flush. Existing PENDING rows are finished
only with positive writer-death proof and current record authorization;
nothing else is touched and no auto-delivery happens."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.bot import source_auth
from app.core.database import connect, migrate
from app.pipeline import session as ps


class FinalizeTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        conn = connect(str(self.dir / "nova.db"))
        migrate(conn)
        conn.execute("INSERT INTO twitch_sources (username, created_at,"
                     " updated_at) VALUES (?,0,0)", ("someuser",))
        self.src = conn.execute("SELECT id FROM twitch_sources").fetchone()[0]
        conn.execute(
            "INSERT INTO stream_sessions (source_id, state, ingest_mode,"
            " notes) VALUES (?,?,?,?)", (self.src, "INTERRUPTED", "feed",
                               json.dumps({"sched_attempt": "att-1"})))
        self.sid = conn.execute("SELECT id FROM stream_sessions").fetchone()[0]
        conn.execute(
            "INSERT INTO candidate_events (session_id, detected_at, kind)"
            " VALUES (?,0,'moment')", (self.sid,))
        self.ev = conn.execute("SELECT id FROM candidate_events").fetchone()[0]
        conn.commit()
        conn.close()
        source_auth.set_scope(self.dir, "someuser", "record", True)
        # raw media for the PENDING clip
        self.work = self.dir / "sessions" / "someuser"
        self.work.mkdir(parents=True)
        (self.dir / "runtime").mkdir()
        self.raw = self.work / "raw-1.ts"
        self.raw.write_bytes(b"\x47" * 4096)

    def _add_clip(self, status="PENDING"):
        conn = connect(str(self.dir / "nova.db"))
        cur = conn.execute(
            "INSERT INTO clips (event_id, session_id, file_path,"
            " duration_seconds, status, created_at)"
            " VALUES (?,?,?,?,?,0)",
            (self.ev, self.sid, str(self.raw), 30.0, status))
        conn.commit()
        cid = cur.lastrowid
        conn.close()
        return cid

    def _clip(self, cid):
        conn = connect(str(self.dir / "nova.db"))
        r = conn.execute("SELECT status, duration_seconds, file_path"
                         " FROM clips WHERE id=?", (cid,)).fetchone()
        conn.close()
        return r

    def _jobs(self):
        conn = connect(str(self.dir / "nova.db"))
        rows = conn.execute("SELECT kind, payload_json FROM job_queue"
                            ).fetchall()
        conn.close()
        return rows

    def _patches(self):
        edited = self.work / "edited.mp4"
        def fake_edit(ffmpeg, src, dst, preset, a, b, runtime, watermark=None):
            Path(dst).write_bytes(b"\x00" * 2048)
        qc = mock.Mock(ok=True)
        return [mock.patch("app.editing.editor.edit_clip", fake_edit),
                mock.patch("app.qc.verify.check_media", lambda *a, **k: qc),
                mock.patch("app.qc.verify.probe",
                           lambda *a, **k: {"format": {"duration": "28.5"}})]

    def test_pending_clip_finished_with_proof_and_auth(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        with mock.patch("app.bot.ops.auto_deliver_clips") as ad:
            n = ps.finalize_interrupted_sessions(
                self.dir, "ffmpeg", "ffprobe",
                writer_proof=lambda d, u, s: (True, "proven"))
        self.assertEqual(n, 1)
        status, dur, path = self._clip(cid)
        self.assertEqual(status, "HELD")
        self.assertAlmostEqual(dur, 28.5)   # ffprobe-measured, not guessed
        self.assertTrue(Path(path).exists())
        ad.assert_not_called()              # no boot-time flush
        jobs = self._jobs()
        self.assertEqual(len(jobs), 1)      # admission via normal outbox

    def test_no_proof_leaves_clip_pending(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        n = ps.finalize_interrupted_sessions(
            self.dir, "ffmpeg", "ffprobe",
            writer_proof=lambda d, u, s: (False, "leader still alive"))
        self.assertEqual(n, 0)
        self.assertEqual(self._clip(cid)[0], "PENDING")
        self.assertEqual(self._jobs(), [])

    def test_revoked_authorization_leaves_clip_pending(self):
        cid = self._add_clip()
        source_auth.set_scope(self.dir, "someuser", "record", False)
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        n = ps.finalize_interrupted_sessions(
            self.dir, "ffmpeg", "ffprobe",
            writer_proof=lambda d, u, s: (True, "proven"))
        self.assertEqual(n, 0)
        self.assertEqual(self._clip(cid)[0], "PENDING")

    def test_idempotent_second_run(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        proof = lambda d, u, s: (True, "proven")
        n1 = ps.finalize_interrupted_sessions(self.dir, "ffmpeg", "ffprobe",
                                              writer_proof=proof)
        n2 = ps.finalize_interrupted_sessions(self.dir, "ffmpeg", "ffprobe",
                                              writer_proof=proof)
        self.assertEqual((n1, n2), (1, 0))
        self.assertEqual(len(self._jobs()), 1)   # no duplicate admission

    def test_non_pending_clips_untouched(self):
        held = self._add_clip(status="HELD")
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        n = ps.finalize_interrupted_sessions(
            self.dir, "ffmpeg", "ffprobe",
            writer_proof=lambda d, u, s: (True, "proven"))
        self.assertEqual(n, 0)
        self.assertEqual(self._clip(held)[0], "HELD")
        self.assertEqual(self._jobs(), [])


if __name__ == "__main__":
    unittest.main()
