"""Reclaim of interrupted RECOVERING claims (session 9 clip 6 case).

A RECOVERING claim is reset to PENDING only on POSITIVE proof of BOTH
the exact claim-owner process AND the session's full registered writer
set being dead (an edit child runs in its own process group, so owner
death alone never suffices). The owner token must match a known family
(live-<sid>-<8 hex> for this clip's session, recovery-<12 hex>);
anything else is fenced forever. The reset is an exact (clip, owner)
CAS inside one transaction, and a spawn fence re-attests the claim
immediately before any edit child exists. Recovery edit children are
durably registered in the session writer manifest. Real-child tests
use actual subprocesses for the proof legs, not mocks.
"""
import json
import os
import subprocess
import unittest
from pathlib import Path
from unittest import mock

from app.bot import restartproof as rp
from app.bot.restartproof import read_start_time
from app.core.database import connect
from app.pipeline import session as ps

from .test_finalize_interrupted import FinalizeTests


def _dead_pid():
    # a pid/pgid that cannot exist on a normal Linux host
    return 2 ** 22 + 12345


class ReclaimTests(FinalizeTests):
    def setUp(self):
        super().setUp()
        self.live_tok = f"live-{self.sid}-5a3a2c8d"

    def _claims(self, cid, token):
        conn = connect(str(self.dir / "nova.db"))
        r = conn.execute("SELECT pid, start, pgid, finished_at"
                         " FROM clip_claims WHERE clip_id=? AND token=?",
                         (cid, token)).fetchone()
        conn.close()
        return r

    def _add_claim(self, cid, token, pid, start, pgid, finished=None):
        conn = connect(str(self.dir / "nova.db"))
        conn.execute("INSERT INTO clip_claims (clip_id, token, pid,"
                     " start, pgid, claimed_at, finished_at)"
                     " VALUES (?,?,?,?,?,0,?)",
                     (cid, token, pid, start, pgid, finished))
        conn.commit()
        conn.close()

    # ---- claim-time birth record -------------------------------------

    def test_claim_records_owner_birth(self):
        cid = self._add_clip()
        conn = connect(str(self.dir / "nova.db"))
        won = ps._claim_clip(conn, cid, "recovery-tok00000001")
        conn.close()
        self.assertTrue(won)
        row = self._claims(cid, "recovery-tok00000001")
        self.assertIsNotNone(row)
        self.assertEqual(row[0], os.getpid())
        self.assertEqual(row[1], read_start_time(os.getpid()))
        self.assertEqual(row[2], os.getpgrp())
        self.assertIsNone(row[3])

    def test_one_run_token_claims_multiple_clips(self):
        a = self._add_clip()
        b = self._add_clip()
        conn = connect(str(self.dir / "nova.db"))
        try:
            self.assertTrue(ps._claim_clip(conn, a, "recovery-one-run"))
            self.assertTrue(ps._claim_clip(conn, b, "recovery-one-run"),
                            "one recovery run may own multiple clips")
        finally:
            conn.close()
        self.assertIsNotNone(self._claims(a, "recovery-one-run"))
        self.assertIsNotNone(self._claims(b, "recovery-one-run"))

    def test_claim_refused_when_birth_unreadable(self):
        cid = self._add_clip()
        conn = connect(str(self.dir / "nova.db"))
        with mock.patch.object(ps, "read_start_time", lambda pid: None):
            won = ps._claim_clip(conn, cid, "recovery-tok00000002")
        conn.close()
        self.assertFalse(won)                 # fail closed, no claim
        self.assertEqual(self._clip(cid)[0], "PENDING")
        self.assertIsNone(self._claims(cid, "recovery-tok00000002"))

    # ---- combined proof: owner record AND session writer proof --------

    def test_legacy_live_claim_reclaimed_with_session_proof(self):
        # session 9 clip 6 shape: live-<sid>-* claim, no clip_claims row
        cid = self._add_clip(status="RECOVERING", owner=self.live_tok)
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        with mock.patch.object(ps, "proof_for_session",
                               return_value=(True, "all dead")) as pf:
            n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
        self.assertEqual(n, 1)
        status, dur, path, owner = self._clip(cid)
        self.assertEqual(status, "HELD")
        self.assertAlmostEqual(dur, 28.5)     # measured, not stale 30.0
        self.assertIsNone(owner)
        pf.assert_called_once()
        self.assertEqual(self.raw.read_bytes(), b"\x47" * 4096)

    def test_legacy_live_claim_fenced_without_proof(self):
        cid = self._add_clip(status="RECOVERING", owner=self.live_tok)
        edits = []
        patches = self._patches()
        patches[0] = mock.patch(
            "app.editing.editor.edit_clip",
            lambda *a, **k: edits.append(1))
        for p in patches:
            p.start()
        self.addCleanup(mock.patch.stopall)
        with mock.patch.object(ps, "proof_for_session",
                               return_value=(False, "leader alive")):
            n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
        self.assertEqual(n, 0)
        self.assertEqual(self._clip(cid)[0], "RECOVERING")
        self.assertEqual(self._clip(cid)[3], self.live_tok)
        self.assertEqual(edits, [])           # no writer ever spawned

    def test_live_claim_of_other_session_fenced(self):
        cid = self._add_clip(status="RECOVERING", owner="live-9-5a3a2c8d")
        with mock.patch.object(ps, "proof_for_session",
                               return_value=(True, "all dead")):
            r = ps.reclaim_dead_claims(
                connect(str(self.dir / "nova.db")), self.sid,
                "someuser", self.dir)
        self.assertEqual(r["reclaimed"], 0)   # wrong-session live token
        self.assertEqual(self._clip(cid)[0], "RECOVERING")

    def test_unrecognized_owner_never_reclaimed(self):
        cid = self._add_clip(status="RECOVERING",
                             owner="unrecognized-owner")
        with mock.patch.object(ps, "proof_for_session",
                               return_value=(True, "session dead")):
            r = ps.reclaim_dead_claims(
                connect(str(self.dir / "nova.db")), self.sid,
                "someuser", self.dir)
        self.assertEqual(r["reclaimed"], 0)   # no session-proof fallback
        self.assertEqual(self._clip(cid)[0], "RECOVERING")

    def test_recovery_claim_dead_owner_reclaimed(self):
        cid = self._add_clip(status="RECOVERING",
                             owner="recovery-abc123def456")
        self._add_claim(cid, "recovery-abc123def456",
                        _dead_pid(), 1, _dead_pid())
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        with mock.patch.object(ps, "proof_for_session",
                               return_value=(True, "all dead")):
            n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
        self.assertEqual(n, 1)
        self.assertEqual(self._clip(cid)[0], "HELD")
        self.assertIsNotNone(
            self._claims(cid, "recovery-abc123def456")[3])  # finished

    def test_dead_owner_record_never_bypasses_session_proof(self):
        # the owner process is provably dead, but the session writer
        # proof fails (e.g. a separately-grouped exporter may live):
        # the claim must stay fenced
        cid = self._add_clip(status="RECOVERING",
                             owner="recovery-abc123def456")
        self._add_claim(cid, "recovery-abc123def456",
                        _dead_pid(), 1, _dead_pid())
        with mock.patch.object(ps, "proof_for_session",
                               return_value=(False, "writer alive")):
            r = ps.reclaim_dead_claims(
                connect(str(self.dir / "nova.db")), self.sid,
                "someuser", self.dir)
        self.assertEqual(r["reclaimed"], 0)
        self.assertEqual(self._clip(cid)[0], "RECOVERING")

    def test_recovery_claim_live_owner_fenced(self):
        cid = self._add_clip(status="RECOVERING",
                             owner="recovery-ffff00000000")
        self._add_claim(cid, "recovery-ffff00000000", os.getpid(),
                        read_start_time(os.getpid()), os.getpgrp())
        n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                             "ffprobe")
        self.assertEqual(n, 0)
        self.assertEqual(self._clip(cid)[0], "RECOVERING")

    def test_recovery_claim_without_record_fenced_forever(self):
        cid = self._add_clip(status="RECOVERING",
                             owner="recovery-aaaa11110000")
        n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                             "ffprobe")
        self.assertEqual(n, 0)
        self.assertEqual(self._clip(cid)[0], "RECOVERING")

    def test_stale_proof_never_resets_changed_owner(self):
        # proof snapshot says dead, but the owner was replaced before
        # the CAS: the new claim must NOT be reset
        cid = self._add_clip(status="RECOVERING",
                             owner=f"live-{self.sid}-aaa00001")

        def stealing_proof(state_dir, username, sid, proc="/proc"):
            c = connect(str(self.dir / "nova.db"))
            c.execute("UPDATE clips SET recovery_owner=?"
                      " WHERE id=?",
                      (f"live-{self.sid}-bbb00002", cid))
            c.commit()
            c.close()
            return True, "all dead"

        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        with mock.patch.object(ps, "proof_for_session", stealing_proof):
            n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
        self.assertEqual(n, 0)
        self.assertEqual(self._clip(cid)[0], "RECOVERING")
        self.assertEqual(self._clip(cid)[3], f"live-{self.sid}-bbb00002")

    def test_spawn_fence_stops_revived_claim_before_edit(self):
        # owner loses the claim between the claim CAS and the edit
        # spawn: no writer may start for the lost claim
        cid = self._add_clip()
        edits = []

        def stealing_plan(*a, **k):
            c = connect(str(self.dir / "nova.db"))
            c.execute("UPDATE clips SET recovery_owner='thief-token'"
                      " WHERE id=?", (cid,))
            c.commit()
            c.close()
            m = mock.Mock()
            m.preset = "default"
            m.reasons = []
            return m

        with mock.patch("app.editing.editor.edit_clip",
                        lambda *a, **k: edits.append(1)), \
             mock.patch("app.intelligence.planner.plan_clip",
                        stealing_plan), \
             mock.patch("app.qc.verify.check_media") as qc, \
             mock.patch("app.qc.verify.probe",
                        lambda *a, **k: {"format": {"duration": "28.5"}}):
            qc.return_value = mock.Mock(ok=True)
            n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
        self.assertEqual(n, 0)
        self.assertEqual(edits, [])           # spawn fence held
        self.assertEqual(self._clip(cid)[3], "thief-token")

    def test_interrupted_partial_output_preserved(self):
        # the dead owner's truncated export is never deleted; recovery
        # writes a fresh unique path
        cid = self._add_clip(status="RECOVERING", owner=self.live_tok)
        partial = self.work / "clip-recovery-old-partial.mp4"
        partial.write_bytes(b"\x00" * 100)    # truncated, no moov
        before = partial.read_bytes()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        with mock.patch.object(ps, "proof_for_session",
                               return_value=(True, "all dead")):
            n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
        self.assertEqual(n, 1)
        self.assertEqual(partial.read_bytes(), before)
        new_path = Path(self._clip(cid)[2])
        self.assertNotEqual(new_path, partial)
        self.assertTrue(new_path.exists())

    # ---- missing-manifest narrowing (explicit regression) --------------

    @staticmethod
    def _honest_edit(ffmpeg, src, dst, preset, a, b, runtime,
                     watermark=None, pre_spawn=None, on_spawn=None):
        # mimics the real edit_clip contract: intent before spawn,
        # bind-or-refuse, then produce output
        from app.editing.editor import SpawnRefused
        token = pre_spawn() if pre_spawn is not None else None
        if pre_spawn is not None and token is None:
            raise SpawnRefused(
                "edit spawn intent not recordable; not spawning")
        if on_spawn is not None and not on_spawn(os.getpid(),
                                                 os.getpgrp(), token):
            raise SpawnRefused(
                "edit writer identity not recorded; killed")
        Path(dst).write_bytes(b"\x00" * 2048)

    def _manifest(self):
        return self.work / f"writers-{self.sid}.json"

    def test_missing_manifest_refusal_leaves_pending(self):
        # sealed PENDING clip in a session with NO writer manifest
        # (legacy/hand-built): no editor spawn, no manifest forged,
        # clip back to PENDING (never FAILED), raw untouched, retry-safe
        cid = self._add_clip()
        outcomes = []
        with mock.patch("app.editing.editor.edit_clip",
                        self._honest_edit):
            n = ps.finalize_interrupted_sessions(
                self.dir, "ffmpeg", "ffprobe",
                outcome_cb=lambda c, o: outcomes.append((c, o)))
            again = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                     "ffprobe")
        self.assertEqual((n, again), (0, 0))
        status, dur, path, owner = self._clip(cid)
        self.assertEqual(status, "PENDING")        # never burned FAILED
        self.assertIsNone(owner)
        self.assertEqual(path, str(self.raw))      # no output adopted
        self.assertFalse(self._manifest().exists())  # no forged history
        self.assertEqual(self.raw.read_bytes(), b"\x47" * 4096)
        self.assertIn((cid, "interrupted"), outcomes)
        # the claim's birth record exists and is marked finished
        rows = connect(str(self.dir / "nova.db")).execute(
            "SELECT finished_at FROM clip_claims WHERE clip_id=?",
            (cid,)).fetchall()
        self.assertEqual(len(rows), 2)             # one per attempt
        self.assertTrue(all(r[0] is not None for r in rows))

    def test_manifest_registered_edit_child_held(self):
        # with an initialized manifest the recovery edit child is
        # durably registered: intent bound with pid/pgid identity
        cid = self._add_clip()
        self.assertTrue(rp.init_registry(self._manifest()))
        qc = mock.Mock(ok=True)
        with mock.patch("app.editing.editor.edit_clip",
                        self._honest_edit), \
             mock.patch("app.qc.verify.check_media", lambda *a, **k: qc), \
             mock.patch("app.qc.verify.probe",
                        lambda *a, **k: {"format": {"duration": "28.5"}}):
            n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
        self.assertEqual(n, 1)
        self.assertEqual(self._clip(cid)[0], "HELD")
        doc = json.loads(self._manifest().read_text())
        self.assertEqual(len(doc["writers"]), 1)
        w = doc["writers"][0]
        self.assertEqual((w["pid"], w["pgid"]),
                         (os.getpid(), os.getpgrp()))  # bound, resolved
        self.assertTrue(w["start"] > 0)

    def test_bind_failure_confirmed_kill_is_retry_safe(self):
        # bind failure with a best-effort _kill_group that does
        # nothing: edit_clip must STILL only raise retry-safe
        # SpawnRefused after positively confirming the child's death
        # (direct kill + reap + /proc group scan)
        import tempfile
        from app.editing import editor
        from app.editing.presets import PRESETS
        with tempfile.TemporaryDirectory() as d:
            fake = Path(d) / "fake"
            fake.write_text("#!/usr/bin/python3\nimport time\n"
                            "time.sleep(20)\n")
            fake.chmod(0o755)
            children = []
            real_popen = subprocess.Popen

            def pop(*a, **k):
                q = real_popen([str(fake)], **k)
                children.append(q)
                return q

            try:
                with mock.patch.object(editor.subprocess, "Popen",
                                       side_effect=pop), \
                     mock.patch.object(editor, "_kill_group",
                                       return_value=None):
                    with self.assertRaises(editor.SpawnRefused):
                        editor.edit_clip(
                            str(fake), "in", d + "/out",
                            PRESETS["vertical"],
                            pre_spawn=lambda: "intent",
                            on_spawn=lambda *a: False)
                alive = [q.pid for q in children if q.poll() is None]
                self.assertEqual(alive, [],
                                 "retry-safe refusal requires a dead"
                                 " writer")
            finally:
                for q in children:
                    if q.poll() is None:
                        q.kill()
                    q.wait()
                    if q.stdout:
                        q.stdout.close()
                    if q.stderr:
                        q.stderr.close()

    def test_bind_failure_unconfirmed_death_raises_uncertain(self):
        # the /proc death proof says not-dead (e.g. a denied read):
        # SpawnUncertain, never retry-safe SpawnRefused, regardless of
        # the escalation kill's outcome
        import tempfile
        from app.editing import editor
        from app.editing.presets import PRESETS
        with tempfile.TemporaryDirectory() as d:
            fake = Path(d) / "fake"
            fake.write_text("#!/usr/bin/python3\nimport time\n"
                            "time.sleep(20)\n")
            fake.chmod(0o755)
            children = []
            real_popen = subprocess.Popen

            def pop(*a, **k):
                q = real_popen([str(fake)], **k)
                children.append(q)
                return q

            try:
                with mock.patch.object(editor.subprocess, "Popen",
                                       side_effect=pop), \
                     mock.patch.object(editor, "_kill_group",
                                       return_value=None), \
                     mock.patch.object(editor, "claim_owner_dead",
                                       return_value=(False, "alive")):
                    with self.assertRaises(editor.SpawnUncertain):
                        editor.edit_clip(
                            str(fake), "in", d + "/out",
                            PRESETS["vertical"],
                            pre_spawn=lambda: "intent",
                            on_spawn=lambda *a: False)
            finally:
                for q in children:
                    if q.poll() is None:
                        q.kill()
                    q.wait()
                    if q.stdout:
                        q.stdout.close()
                    if q.stderr:
                        q.stderr.close()

    def test_uncertain_spawn_stays_recovering_no_retry_no_delete(self):
        # finish level: an unconfirmed-dead spawn keeps the claim
        # (RECOVERING + owner + unfinished claim record), keeps the
        # reserved output, and is never retried or deleted
        from app.editing.editor import SpawnUncertain
        cid = self._add_clip()
        created = []

        def uncertain_edit(ffmpeg, src, dst, preset, a, b, runtime,
                           watermark=None, pre_spawn=None, on_spawn=None):
            Path(dst).write_bytes(b"\x00" * 512)   # partial, uncertain
            created.append(dst)
            raise SpawnUncertain("death unconfirmed")

        with mock.patch("app.editing.editor.edit_clip", uncertain_edit):
            n = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
            again = ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                     "ffprobe")
        self.assertEqual((n, again), (0, 0))
        status, dur, path, owner = self._clip(cid)
        self.assertEqual(status, "RECOVERING")     # claim retained
        self.assertIsNotNone(owner)
        self.assertEqual(len(created), 1)          # never retried
        self.assertEqual(Path(created[0]).read_bytes(), b"\x00" * 512)
        claims = connect(str(self.dir / "nova.db")).execute(
            "SELECT finished_at FROM clip_claims WHERE clip_id=?",
            (cid,)).fetchall()
        self.assertEqual(len(claims), 1)
        self.assertIsNone(claims[0][0])            # unfinished

    def test_confirmation_channel_errors_mean_uncertain(self):
        # ordinary failures of the wait/kill/proof channel are
        # uncertainty (False), never confirmation, never a propagated
        # generic error; cancellation always propagates
        from app.editing import editor as e
        p = subprocess.Popen(["sleep", "10"], start_new_session=True)
        try:
            with mock.patch.object(p, "wait",
                                   side_effect=OSError("fixed")):
                self.assertFalse(e._confirm_dead(p, 1))
        finally:
            p.kill()
            p.wait()
        q = mock.Mock()
        q.wait.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            e._confirm_dead(q, 1)
        q2 = mock.Mock()
        q2.pid = 100
        q2.wait.return_value = 0
        self.assertFalse(e._confirm_dead(q2, None))  # unreadable birth
        with mock.patch.object(e, "claim_owner_dead",
                               side_effect=OSError("denied")):
            self.assertFalse(e._confirm_dead(q2, 1))  # proof error
        with mock.patch.object(e, "claim_owner_dead",
                               return_value=(False, "survivor")):
            self.assertFalse(e._confirm_dead(q2, 1))  # survivor

    def test_cancellation_preserves_owner_and_output(self):
        # BaseException mid-edit: no PENDING release, no output delete -
        # the row keeps its owner and the reserved output for review
        cid = self._add_clip()
        created = []

        def cancelled_edit(ffmpeg, src, dst, preset, a, b, runtime,
                           watermark=None, pre_spawn=None,
                           on_spawn=None):
            Path(dst).write_bytes(b"\x00" * 256)
            created.append(dst)
            raise KeyboardInterrupt()

        with mock.patch("app.editing.editor.edit_clip",
                        cancelled_edit):
            with self.assertRaises(KeyboardInterrupt):
                ps.finalize_interrupted_sessions(self.dir, "ffmpeg",
                                                 "ffprobe")
        status, dur, path, owner = self._clip(cid)
        self.assertEqual(status, "RECOVERING")
        self.assertIsNotNone(owner)
        self.assertEqual(Path(created[0]).read_bytes(), b"\x00" * 256)

    def test_edit_clip_spawn_refusal_is_distinct(self):
        # the real editor raises SpawnRefused before Popen when the
        # intent cannot be recorded - never a generic edit failure
        from app.editing.editor import edit_clip, SpawnRefused
        from app.editing.presets import PRESETS
        with mock.patch("app.editing.editor.subprocess.Popen") as pop:
            with self.assertRaises(SpawnRefused):
                edit_clip("ffmpeg", "in", "out", PRESETS["vertical"],
                          runtime_dir=str(self.dir / "runtime"),
                          pre_spawn=lambda: None)
        pop.assert_not_called()

    # ---- real-child proof legs (no process mocks) ----------------------

    def test_real_child_surviving_exporter_blocks_reclaim(self):
        """Actual processes: a killed real owner + a LIVE registered
        exporter child in its own session/group must block the reclaim;
        killing and reaping the exporter must unblock it. Only the
        writer-quiet backstop (a separate proof leg covered elsewhere)
        is stubbed."""
        cid = self._add_clip(status="RECOVERING",
                             owner="recovery-abc123def456")
        children = []
        try:
            owner = subprocess.Popen(["sleep", "30"],
                                     start_new_session=True)
            children.append(owner)
            self._add_claim(cid, "recovery-abc123def456", owner.pid,
                            rp.read_start_time(owner.pid), owner.pid)
            leader = subprocess.Popen(["sleep", "30"],
                                      start_new_session=True)
            children.append(leader)
            conn = connect(str(self.dir / "nova.db"))
            notes = json.loads(conn.execute(
                "SELECT notes FROM stream_sessions WHERE id=?",
                (self.sid,)).fetchone()[0])
            conn.execute(
                "UPDATE stream_sessions SET notes=? WHERE id=?",
                (json.dumps(rp.record_leader(notes, leader.pid)),
                 self.sid))
            conn.commit()
            conn.close()
            owner.kill()
            owner.wait()
            leader.kill()
            leader.wait()                     # owner and leader dead
            reg = self.work / f"writers-{self.sid}.json"
            self.assertTrue(rp.init_registry(reg))
            tok = rp.record_spawn_intent(reg)
            self.assertIsNotNone(tok)
            expo = subprocess.Popen(["sleep", "30"],
                                    start_new_session=True)
            children.append(expo)
            self.assertTrue(
                rp.record_spawned(reg, tok, expo.pid, expo.pid))
            conn = connect(str(self.dir / "nova.db"))
            with mock.patch("app.bot.sessions.writer_active",
                            return_value=False):
                r = ps.reclaim_dead_claims(conn, self.sid, "someuser",
                                           self.dir)
            self.assertEqual(r["reclaimed"], 0,
                             "live exporter in its own group must fence")
            self.assertEqual(self._clip(cid)[0], "RECOVERING")
            expo.kill()
            expo.wait()
            with mock.patch("app.bot.sessions.writer_active",
                            return_value=False):
                r = ps.reclaim_dead_claims(conn, self.sid, "someuser",
                                           self.dir)
            self.assertEqual(r["reclaimed"], 1)
            self.assertEqual(self._clip(cid)[0], "PENDING")
        finally:
            for c in children:
                if c.poll() is None:
                    c.kill()
                    c.wait()

    def test_real_child_unresolved_intent_blocks_forever(self):
        """A spawn intent whose binding was lost (crash between intent
        and record_spawned) fences the claim even with every known
        process dead."""
        cid = self._add_clip(status="RECOVERING",
                             owner="recovery-abc123def456")
        self._add_claim(cid, "recovery-abc123def456",
                        _dead_pid(), 1, _dead_pid())
        conn = connect(str(self.dir / "nova.db"))
        notes = json.loads(conn.execute(
            "SELECT notes FROM stream_sessions WHERE id=?",
            (self.sid,)).fetchone()[0])
        notes["leader_pid"], notes["leader_start"] = _dead_pid(), 1
        conn.execute("UPDATE stream_sessions SET notes=? WHERE id=?",
                     (json.dumps(notes), self.sid))
        conn.commit()
        conn.close()
        reg = self.work / f"writers-{self.sid}.json"
        self.assertTrue(rp.init_registry(reg))
        self.assertIsNotNone(rp.record_spawn_intent(reg))  # never bound
        conn = connect(str(self.dir / "nova.db"))
        with mock.patch("app.bot.sessions.writer_active",
                        return_value=False):
            r = ps.reclaim_dead_claims(conn, self.sid, "someuser",
                                       self.dir)
        conn.close()
        self.assertEqual(r["reclaimed"], 0)
        self.assertEqual(self._clip(cid)[0], "RECOVERING")


if __name__ == "__main__":
    unittest.main()
