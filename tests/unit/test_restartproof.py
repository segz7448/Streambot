"""restartproof: positive writer-death proof for restart-fenced attempts.
Quiet alone never proves. Proof requires: a recorded session leader
verifiably dead (start-time check, PID-reuse safe), a durable registry
of the session's writer processes with every entry verifiably dead, no
surviving member of any recorded process group (orphan check), complete
process visibility (denied/hidden is never "absent"), and a clean
fail-closed advisory scan. Sessions without records stay fenced."""
import json
import tempfile
import unittest
from pathlib import Path

from app.bot import restartproof
from app.core.database import connect, migrate


def _stat_bytes(pid, start, pgrp=0):
    fields = ["S", "1", str(pgrp)] + ["0"] * 16 + [str(start)]
    return f"{pid} (fake proc) ".encode() + " ".join(fields).encode()


def _fake_proc(entries):
    """entries: {pid: (start, pgrp)}. Builds a fake proc root."""
    root = tempfile.mkdtemp()
    for pid, (start, pgrp) in entries.items():
        d = Path(root) / str(pid)
        d.mkdir()
        (d / "stat").write_bytes(_stat_bytes(pid, start, pgrp))
    return root


class _Base(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        conn = connect(str(self.dir / "nova.db"))
        migrate(conn)
        conn.execute("INSERT INTO twitch_sources (username, created_at,"
                     " updated_at) VALUES (?,0,0)", ("someuser",))
        self.src = conn.execute("SELECT id FROM twitch_sources").fetchone()[0]
        conn.commit()
        conn.close()
        self.work = self.dir / "sessions" / "someuser"
        self.work.mkdir(parents=True)

    def _session(self, notes):
        conn = connect(str(self.dir / "nova.db"))
        cur = conn.execute(
            "INSERT INTO stream_sessions (source_id, state, notes)"
            " VALUES (?,?,?)", (self.src, "INTERRUPTED", json.dumps(notes)))
        conn.commit()
        sid = cur.lastrowid
        conn.close()
        return sid

    def _session_with_leader(self, pid=4242, start=111, proc=None):
        proc = proc or _fake_proc({pid: (start, 0)})
        return self._session({"sched_attempt": "att-1",
                              **restartproof.record_leader({}, pid=pid,
                                                           proc=proc)})


class ProofTests(_Base):
    def test_no_session_row_is_not_proof(self):
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1", proc="/nonexistent",
            writer_active=lambda u: False)
        self.assertFalse(ok)
        self.assertIn("no unique session row", why)

    def test_row_without_recorded_leader_is_not_proof(self):
        self._session({"sched_attempt": "att-1", "drained": False})
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1", proc="/nonexistent",
            writer_active=lambda u: False)
        self.assertFalse(ok)
        self.assertIn("no recorded leader", why)

    def _registered_writer(self, reg, pid=5150, pgid=5150, start=7):
        p0 = _fake_proc({pid: (start, pgid)})
        tok = restartproof.record_spawn_intent(reg)
        self.assertIsNotNone(tok)
        self.assertTrue(restartproof.record_spawned(reg, tok, pid, pgid,
                                                    proc=p0))

    def test_no_writer_registry_stays_closed(self):
        # old sessions predate the registry: leader dead but writers
        # never recorded -> fenced forever, honest status
        self._session_with_leader()
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1", proc=_fake_proc({}),
            writer_active=lambda u: False)
        self.assertFalse(ok)
        self.assertIn("no writer registry", why)

    def test_dead_leader_and_dead_writers_prove(self):
        sid = self._session_with_leader()
        reg = self.work / f"writers-{sid}.json"
        self._registered_writer(reg)
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1", proc=_fake_proc({}),
            writer_active=lambda u: False)
        self.assertTrue(ok, why)

    def test_live_writer_blocks_proof(self):
        sid = self._session_with_leader()
        reg = self.work / f"writers-{sid}.json"
        self._registered_writer(reg)
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1",
            proc=_fake_proc({5150: (7, 5150)}),   # same start: alive
            writer_active=lambda u: False)
        self.assertFalse(ok)
        self.assertIn("still alive", why)

    def test_reused_pids_are_dead(self):
        sid = self._session_with_leader()
        reg = self.work / f"writers-{sid}.json"
        self._registered_writer(reg)
        # leader and writer pids now belong to newer processes
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1",
            proc=_fake_proc({4242: (999, 0), 5150: (888, 5150)}),
            writer_active=lambda u: False)
        self.assertTrue(ok, why)

    def test_orphan_group_member_blocks_proof(self):
        sid = self._session_with_leader()
        reg = self.work / f"writers-{sid}.json"
        self._registered_writer(reg)
        # the recorded writer is gone, but an unrecorded member of its
        # process group survives (child of the writer, orphaned with it)
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1",
            proc=_fake_proc({5199: (9, 5150)}),
            writer_active=lambda u: False)
        self.assertFalse(ok)
        self.assertIn("orphan survives", why)

    def test_same_start_time_means_leader_alive(self):
        self._session_with_leader()
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1",
            proc=_fake_proc({4242: (111, 0)}),
            writer_active=lambda u: False)
        self.assertFalse(ok)
        self.assertIn("still alive", why)

    def test_denied_visibility_is_not_absence(self):
        sid = self._session_with_leader()
        reg = self.work / f"writers-{sid}.json"
        self._registered_writer(reg)
        proc = _fake_proc({})
        # a process entry exists but its stat is permission-denied:
        # on Android a hidden process must never read as "dead"
        d = Path(proc) / "6000"
        d.mkdir()
        st = d / "stat"
        st.write_bytes(_stat_bytes(6000, 5, 0))
        st.chmod(0)
        try:
            ok, why = restartproof.writer_death_proof(
                self.dir, "someuser", "att-1", proc=proc,
                writer_active=lambda u: False)
        finally:
            st.chmod(0o644)
        self.assertFalse(ok)
        self.assertIn("visibility", why)

    def test_writer_evidence_blocks_proof_even_with_dead_leader(self):
        sid = self._session_with_leader()
        reg = self.work / f"writers-{sid}.json"
        self._registered_writer(reg)
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1", proc=_fake_proc({}),
            writer_active=lambda u: True)
        self.assertFalse(ok)
        self.assertIn("writer evidence", why)

    def test_writer_probe_error_is_not_proof(self):
        sid = self._session_with_leader()
        reg = self.work / f"writers-{sid}.json"
        self._registered_writer(reg)
        def boom(u):
            raise OSError("denied")
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1", proc=_fake_proc({}),
            writer_active=boom)
        self.assertFalse(ok)
        self.assertIn("probe failed", why)

    def test_intent_persistence_failure_forbids_spawn(self):
        # the registry path is unwritable: no intent token, and the
        # caller's contract is NOT to spawn at all
        reg = Path("/nonexistent-dir/writers-9.json")
        self.assertIsNone(restartproof.record_spawn_intent(reg))
        self.assertFalse(reg.exists())

    def test_unresolved_intent_blocks_proof_forever(self):
        # crash between intent and completion (or lost completion):
        # indistinguishable from a live unrecorded writer
        sid = self._session_with_leader()
        reg = self.work / f"writers-{sid}.json"
        tok = restartproof.record_spawn_intent(reg)
        self.assertIsNotNone(tok)
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1", proc=_fake_proc({}),
            writer_active=lambda u: False)
        self.assertFalse(ok)
        self.assertIn("unresolved spawn intent", why)

    def test_completion_failure_leaves_proof_blocked(self):
        # spawn succeeded but the identity could not be recorded: the
        # caller terminates the child; the leftover intent keeps the
        # fence closed anyway
        sid = self._session_with_leader()
        reg = self.work / f"writers-{sid}.json"
        tok = restartproof.record_spawn_intent(reg)
        self.assertFalse(restartproof.record_spawned(reg, tok, 5150, 5150,
                                                     proc="/nonexistent"))
        self.assertFalse(restartproof.record_spawned(reg, "wrong-token",
                                                     5150, 5150,
                                                     proc=_fake_proc({5150: (7, 5150)})))
        ok, why = restartproof.writer_death_proof(
            self.dir, "someuser", "att-1", proc=_fake_proc({}),
            writer_active=lambda u: False)
        self.assertFalse(ok)
        self.assertIn("unresolved spawn intent", why)

    def test_record_leader_without_start_records_nothing(self):
        notes = restartproof.record_leader({"a": 1}, pid=4242,
                                           proc="/nonexistent")
        self.assertEqual(notes, {"a": 1})


if __name__ == "__main__":
    unittest.main()
