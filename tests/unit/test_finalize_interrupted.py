"""Sealed-input recovery of PENDING clips from interrupted sessions.

Eligibility is sealed PENDING rows only; input is a private snapshot
verified against the producer's committed digest; claims are owner-token
CAS; terminal state is HELD with zero admission jobs; legacy/unsealed
and interrupted RECOVERING rows stay fenced.
"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.bot import source_auth
from app.clipping.seal import seal_file
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
        self.work = self.dir / "sessions" / "someuser"
        self.work.mkdir(parents=True)
        (self.dir / "runtime").mkdir()
        self.raw = self.work / "raw-1.ts"
        self.raw.write_bytes(b"\x47" * 4096)
        self.seal_sha, self.seal_bytes = seal_file(self.raw)

    def _add_clip(self, status="PENDING", sealed=True, owner=None):
        conn = connect(str(self.dir / "nova.db"))
        cur = conn.execute(
            "INSERT INTO candidate_events (session_id, detected_at, kind)"
            " VALUES (?,0,'moment')", (self.sid,))
        ev = cur.lastrowid
        cur = conn.execute(
            "INSERT INTO clips (event_id, session_id, file_path,"
            " duration_seconds, status, created_at, sealed_sha256,"
            " sealed_bytes, sealed_at, recovery_owner)"
            " VALUES (?,?,?,?,?,0,?,?,?,?)",
            (ev, self.sid, str(self.raw), 30.0, status,
             self.seal_sha if sealed else None,
             self.seal_bytes if sealed else None,
             1.0 if sealed else None, owner))
        cid = cur.lastrowid
        conn.commit()
        conn.close()
        return cid

    def _clip(self, cid):
        conn = connect(str(self.dir / "nova.db"))
        r = conn.execute(
            "SELECT status, duration_seconds, file_path, recovery_owner"
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
        def fake_edit(ffmpeg, src, dst, preset, a, b, runtime, framing=None,
                      watermark=None, pre_spawn=None, on_spawn=None):
            Path(dst).write_bytes(b"\x00" * 2048)
        qc = mock.Mock(ok=True)
        return [mock.patch("app.editing.editor.edit_clip", fake_edit),
                mock.patch("app.qc.verify.check_media", lambda *a, **k: qc),
                mock.patch("app.qc.verify.probe",
                           lambda *a, **k: {"format": {"duration": "28.5"}})]

    def test_sealed_clip_recovered_to_held_no_admission(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        with mock.patch("app.bot.ops.auto_deliver_clips") as ad:
            n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
        self.assertEqual(n, 1)
        status, dur, path, owner = self._clip(cid)
        self.assertEqual(status, "HELD")
        self.assertAlmostEqual(dur, 28.5)
        self.assertIsNone(owner)             # claim released by the CAS
        self.assertIn("clip-recovery-", Path(path).name)  # unique output
        self.assertTrue(Path(path).exists())
        self.assertEqual(self.raw.read_bytes(), b"\x47" * 4096)  # untouched
        ad.assert_not_called()
        self.assertEqual(self._jobs(), [])   # HOLD-only, no admission

    def test_unsealed_pending_stays_fenced(self):
        cid = self._add_clip(sealed=False)
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg", "ffprobe")
        self.assertEqual(n, 0)
        self.assertEqual(self._clip(cid)[0], "PENDING")

    def test_interrupted_recovering_stays_fenced(self):
        # a dead owner's claim is never reclaimed by a later run
        cid = self._add_clip(status="RECOVERING", owner="recovery-dead00")
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg", "ffprobe")
        self.assertEqual(n, 0)
        status, _d, _p, owner = self._clip(cid)
        self.assertEqual(status, "RECOVERING")
        self.assertEqual(owner, "recovery-dead00")

    def test_tampered_input_fenced_and_claim_released(self):
        cid = self._add_clip()
        self.raw.write_bytes(b"\x00" * 4096)   # no longer matches seal
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg", "ffprobe")
        self.assertEqual(n, 0)
        status, _d, _p, owner = self._clip(cid)
        self.assertEqual(status, "PENDING")  # fenced, claim released
        self.assertIsNone(owner)

    def test_authorization_revoked_mid_edit_returns_pending(self):
        c1 = self._add_clip()
        c2 = self._add_clip()
        edits = []

        def fake_edit(ffmpeg, src, dst, preset, a, b, runtime, framing=None,
                      watermark=None, pre_spawn=None, on_spawn=None):
            edits.append(dst)
            Path(dst).write_bytes(b"\x00" * 2048)
            if len(edits) == 1:
                source_auth.set_scope(self.dir, "someuser", "record",
                                      False)
        qc = mock.Mock(ok=True)
        for p in [mock.patch("app.editing.editor.edit_clip", fake_edit),
                  mock.patch("app.qc.verify.check_media",
                             lambda *a, **k: qc),
                  mock.patch("app.qc.verify.probe",
                             lambda *a, **k:
                                 {"format": {"duration": "28.5"}})]:
            p.start()
        self.addCleanup(mock.patch.stopall)
        n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg", "ffprobe")
        # post-edit auth check inside the final transaction: the edit
        # that SPANNED the revocation must not reach HELD either
        self.assertEqual(n, 0)
        self.assertEqual(self._clip(c1)[0], "PENDING")
        self.assertIsNone(self._clip(c1)[3])
        self.assertFalse(Path(edits[0]).exists())  # our output removed
        self.assertEqual(self._clip(c2)[0], "PENDING")
        self.assertIsNone(self._clip(c2)[3])

    def test_idempotent_second_run(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        n1 = ps.finalize_interrupted_sessions(self.dir, "ffmpeg", "ffprobe")
        n2 = ps.finalize_interrupted_sessions(self.dir, "ffmpeg", "ffprobe")
        self.assertEqual((n1, n2), (1, 0))
        self.assertEqual(self._jobs(), [])

    def test_authorization_off_leaves_pending(self):
        cid = self._add_clip()
        source_auth.set_scope(self.dir, "someuser", "record", False)
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg", "ffprobe")
        self.assertEqual(n, 0)
        self.assertEqual(self._clip(cid)[0], "PENDING")

    def test_held_and_other_states_untouched(self):
        cid = self._add_clip(status="HELD")
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg", "ffprobe")
        self.assertEqual(n, 0)
        self.assertEqual(self._clip(cid)[0], "HELD")


class ClaimRaceTests(unittest.TestCase):
    """Two finalizers claiming the same PENDING clip: exactly one wins;
    the loser never edits, never overwrites, never double-HELDs."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        conn = connect(str(self.dir / "nova.db"))
        migrate(conn)
        conn.execute("INSERT INTO twitch_sources (username, created_at,"
                     " updated_at) VALUES (?,0,0)", ("someuser",))
        src = conn.execute("SELECT id FROM twitch_sources").fetchone()[0]
        conn.execute(
            "INSERT INTO stream_sessions (source_id, state, ingest_mode,"
            " notes) VALUES (?,?,?,?)",
            (src, "RECORDING", "feed", "{}"))
        self.sid = conn.execute("SELECT id FROM stream_sessions").fetchone()[0]
        conn.execute(
            "INSERT INTO candidate_events (session_id, detected_at, kind)"
            " VALUES (?,0,'moment')", (self.sid,))
        ev = conn.execute("SELECT id FROM candidate_events").fetchone()[0]
        raw = self.dir / "raw.ts"
        raw.write_bytes(b"\x47" * 4096)
        from app.clipping.seal import seal_file
        sha, size = seal_file(raw)
        cur = conn.execute(
            "INSERT INTO clips (event_id, session_id, file_path,"
            " duration_seconds, status, created_at, sealed_sha256,"
            " sealed_bytes, sealed_at) VALUES (?,?,?,?,?,0,?,?,1.0)",
            (ev, self.sid, str(raw), 30.0, "PENDING", sha, size))
        self.cid = cur.lastrowid
        conn.commit()
        conn.close()

    def test_exactly_one_claim_wins(self):
        import threading
        results = {}

        def run(name):
            conn = connect(str(self.dir / "nova.db"))
            try:
                results[name] = ps._claim_clip(conn, self.cid, name)
            finally:
                conn.close()

        threads = [threading.Thread(target=run, args=(f"owner-{i}",))
                   for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sum(results.values()), 1)
        winner = [k for k, v in results.items() if v][0]
        conn = connect(str(self.dir / "nova.db"))
        row = conn.execute("SELECT status, recovery_owner FROM clips"
                           " WHERE id=?", (self.cid,)).fetchone()
        conn.close()
        self.assertEqual(row[0], "RECOVERING")
        self.assertEqual(row[1], winner)


if __name__ == "__main__":
    unittest.main()


class PostCommitAncillaryTests(FinalizeTests):
    """The durable HELD commit is the publication point: an ancillary
    failure after it (memory note, admission bookkeeping) must preserve
    the committed output, row state and reported result. Cleanup of the
    owned output is only for unpublished pre-commit failures."""

    def test_recovery_remember_failure_preserves_committed(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        with mock.patch("app.intelligence.memory.Memory.remember",
                        side_effect=OSError("boom")):
            n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
        status, dur, path, owner = self._clip(cid)
        self.assertEqual(n, 1)
        self.assertEqual(status, "HELD")
        self.assertTrue(Path(path).exists())

    def test_live_finalize_with_admission_remember_failure(self):
        # normal finalization path: admission may already be committed
        # in the same transaction when the ancillary failure lands
        source_auth.set_scope(self.dir, "someuser", "repost", True)
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        conn = connect(str(self.dir / "nova.db"))
        with mock.patch("app.intelligence.memory.Memory.remember",
                        side_effect=OSError("boom")):
            held = ps.finish_pending_clips(
                conn, state_dir=self.dir, session_id=self.sid,
                source_label="someuser", out_dir=self.work,
                runtime_dir=self.dir / "runtime",
                ffmpeg="ffmpeg", ffprobe="ffprobe",
                enqueue_admission=True)
        conn.close()
        status, dur, path, owner = self._clip(cid)
        self.assertEqual(held, [cid])
        self.assertEqual(status, "HELD")
        self.assertTrue(Path(path).exists())

    def test_published_guard_holds_even_if_ancillary_handler_raises(self):
        # defense in depth: even if a post-commit ancillary HANDLER
        # itself raises (here the guarded memory-note log call), the
        # outer cleanup must never unlink a published output
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        raised = []
        def boom_once(msg, *a):
            if not raised:
                raised.append(msg)
                raise RuntimeError("logger boom")
        with mock.patch("app.intelligence.memory.Memory.remember",
                        side_effect=OSError("boom")), mock.patch(
                            "app.pipeline.session.log.warning",
                            side_effect=boom_once):
            n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
        self.assertEqual(n, 1)
        status, dur, path, owner = self._clip(cid)
        self.assertEqual(status, "HELD")
        self.assertTrue(Path(path).exists())
