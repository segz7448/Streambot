"""State-integrity regressions (audit @0705670): concurrent JSON writes
must not be lost, a crash mid-write must leave the previous file, and
corrupt state must fail closed. Adapted to unittest from the auditor's
pytest file."""
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import textwrap
import unittest

from app.intelligence.memory import Memory


class StateIntegrityTests(unittest.TestCase):

    def test_concurrent_memory_writes_not_lost(self):
        p = str(pathlib.Path(tempfile.mkdtemp()) / "m.json")

        def w(k):
            m = Memory(p)
            for i in range(40):
                m.remember("channel", f"c{k}-{i}", "x", "t")

        ts = [threading.Thread(target=w, args=(k,)) for k in range(4)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(len(json.loads(pathlib.Path(p).read_text())), 160)

    def test_crash_during_write_leaves_previous_file_intact(self):
        d = pathlib.Path(tempfile.mkdtemp())
        p = d / "m.json"
        Memory(str(p)).remember("channel", "seed", "v", "t")
        before = p.read_text()
        code = textwrap.dedent(f"""
            import json, os
            import app.intelligence.memory as M
            m = M.Memory({str(p)!r})
            def boom(rows, fh, *a, **k):
                fh.write('[{{"kind":'); fh.flush(); os._exit(9)
            json.dump = boom
            m.remember("channel", "new", "v", "t")
        """)
        r = subprocess.run([sys.executable, "-c", code], cwd=os.getcwd(),
                           capture_output=True)
        self.assertEqual(r.returncode, 9)
        self.assertEqual(p.read_text(), before,
                         "state file corrupted by crash mid-write")

    def test_pairing_file_survives_crash_mid_write(self):
        from app.bot.pairing import PairingStore
        d = pathlib.Path(tempfile.mkdtemp())
        p = d / "admin.json"
        s = PairingStore(str(p))
        s.begin_challenge("abcd2345")
        self.assertTrue(s.try_pair("abcd2345", 42))
        code = textwrap.dedent(f"""
            import json, os
            from app.bot.pairing import PairingStore
            json.dump = lambda r, fh, *a, **k: (fh.write('{{'), fh.flush(),
                                               os._exit(9))
            PairingStore({str(p)!r}).begin_challenge("zzzz2222")
        """)
        subprocess.run([sys.executable, "-c", code], cwd=os.getcwd(),
                       capture_output=True)
        self.assertEqual(PairingStore(str(p)).admin_id(), 42,
                         "admin pairing lost after crash")

    def test_corrupt_state_fails_closed_not_empty_reset(self):
        from app.core.state import StateCorruptError, locked_read
        p = pathlib.Path(tempfile.mkdtemp()) / "admin.json"
        p.write_text("{not json")
        with self.assertRaises(StateCorruptError):
            locked_read(p, {})


if __name__ == "__main__":
    unittest.main()


class JournalIntegrityTests(unittest.TestCase):

    def test_journal_120_concurrent_records_not_lost(self):
        from app.bot import ops
        d = pathlib.Path(tempfile.mkdtemp())

        def w(k):
            for i in range(30):
                ops.journal_record(d, k * 100 + i + 1, "ingest", "ok")

        ts = [threading.Thread(target=w, args=(k,)) for k in range(4)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        data = json.loads((d / ops.JOURNAL).read_text())
        self.assertEqual(len(data), 120)

    def test_journal_survives_crash_mid_write(self):
        from app.bot import ops
        d = pathlib.Path(tempfile.mkdtemp())
        ops.journal_record(d, 7, "ingest", "seed")
        before = (d / ops.JOURNAL).read_text()
        code = textwrap.dedent(f"""
            import json, os
            from app.bot import ops
            json.dump = lambda r, fh, *a, **k: (fh.write('{{'), fh.flush(),
                                               os._exit(9))
            ops.journal_record({str(d)!r} and __import__('pathlib').Path({str(d)!r}), 8, "ingest", "x")
        """)
        subprocess.run([sys.executable, "-c", code], cwd=os.getcwd(),
                       capture_output=True)
        self.assertEqual((d / ops.JOURNAL).read_text(), before)


class JournalPolicyTests(unittest.TestCase):

    def test_typed_keys_and_legacy_migration(self):
        from app.bot import ops
        d = pathlib.Path(tempfile.mkdtemp())
        (d / ops.JOURNAL).write_text(
            json.dumps({"42": {"command": "ingest", "result": "ok"}}))
        ops.journal_record(d, 43, "clipnow", "done")
        data = json.loads((d / ops.JOURNAL).read_text())
        self.assertIn("tg:42", data)
        self.assertIn("tg:43", data)
        self.assertNotIn("42", data)
        self.assertEqual(ops.journal_result(d, 42, "ingest"), "ok")
        self.assertEqual(ops.journal_result(d, 43, "clipnow"), "done")

    def test_uncertain_never_evicted_at_cap(self):
        from app.bot import ops
        d = pathlib.Path(tempfile.mkdtemp())
        ops.journal_mark_uncertain(d, 1, "deliveries", "send ambiguous")
        for i in range(ops.JOURNAL_CAP + 20):
            ops.journal_record(d, 100 + i, "ingest", "ok")
        data = json.loads((d / ops.JOURNAL).read_text())
        uncertain = [k for k, v in data.items()
                     if v.get("status") == "uncertain"]
        self.assertEqual(uncertain, ["tg:1"])
        resolved = [k for k in data if data[k].get("status") != "uncertain"]
        self.assertLessEqual(len(resolved), ops.JOURNAL_CAP)

    def test_age_eviction_removes_old_resolved_only(self):
        from app.bot import ops
        d = pathlib.Path(tempfile.mkdtemp())
        old = time.time() - ops.JOURNAL_MAX_AGE_S - 60
        (d / ops.JOURNAL).write_text(json.dumps({
            "tg:1": {"command": "ingest", "result": "ok", "ts": old},
            "tg:2": {"command": "deliveries", "status": "uncertain",
                     "text": "x", "ts": old}}))
        ops.journal_record(d, 3, "ingest", "ok")
        data = json.loads((d / ops.JOURNAL).read_text())
        self.assertNotIn("tg:1", data)
        self.assertIn("tg:2", data)
        self.assertIn("tg:3", data)


class OrphanTempTests(unittest.TestCase):

    def test_dead_writer_tmp_removed_live_kept(self):
        from app.core.state import locked_update
        d = pathlib.Path(tempfile.mkdtemp())
        path = d / "state.json"
        path.write_text("{}")
        # spawn and reap so the pid is certainly dead
        p = subprocess.Popen([sys.executable, "-c", "pass"])
        p.wait()
        orphan = d / f"state.json.tmp.{p.pid}.123"
        orphan.write_text("partial")
        live = d / f"state.json.tmp.{os.getpid()}.456"
        live.write_text("in flight")
        locked_update(path, {}, lambda data: ({"a": 1}, None))
        self.assertFalse(orphan.exists())
        self.assertTrue(live.exists())

    def test_corrupt_file_preserved_and_logged(self):
        from app.core import state as st
        d = pathlib.Path(tempfile.mkdtemp())
        path = d / "bad.json"
        path.write_text("{not json")
        with self.assertLogs("core.state", level="ERROR") as cm:
            with self.assertRaises(st.StateCorruptError):
                st.locked_read(path, {})
        self.assertEqual(path.read_text(), "{not json")
        self.assertIn("operator reconciliation required", cm.output[0])
