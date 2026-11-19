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
