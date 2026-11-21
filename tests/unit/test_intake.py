import json
import os
import subprocess
import sys
import sqlite3
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from app.core import database, intake
from app.core.intake import ClipIntakeAuthority, resolve_timezone
from app.core.jobs import JobQueue

TZ = resolve_timezone("America/Los_Angeles")
# 2026-11-21 12:00:00 in the authority's provisional timezone.
FIXED_EPOCH = datetime(2026, 11, 21, 12, 0, 0, tzinfo=TZ).timestamp()
DATE = "2026-11-21"
NEXT_DATE = "2026-11-22"


def fixed_clock():
    return FIXED_EPOCH


def _admit_one(db_path, clip_id):
    # Separate authority instance per caller, as independent producers do.
    return ClipIntakeAuthority(db_path, _clock=fixed_clock).admit(clip_id)


def _admit_row(db_path, clip_row_id):
    return ClipIntakeAuthority(db_path, _clock=fixed_clock) \
        .admit_clip(clip_row_id)


class ClipIntakeAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "t.db")
        conn = database.connect(self.db)
        database.migrate(conn)  # base schema precondition (bot startup does)
        conn.close()
        self.clock = [FIXED_EPOCH]
        self.auth = ClipIntakeAuthority(
            self.db, _clock=lambda: self.clock[0])

    def tearDown(self):
        self.tmp.cleanup()

    def _fill(self, n, prefix="FILL"):
        for i in range(n):
            v = self.auth.admit(f"{prefix}-{i}")
            self.assertTrue(v["accepted"], v)

    def _add_clip(self, duration=120.0, status="HELD"):
        conn = database.connect(self.db)
        with conn:
            cur = conn.execute(
                "INSERT INTO clips (duration_seconds, file_path, status,"
                " created_at) VALUES (?, '/tmp/c.mp4', ?, 0)",
                (duration, status))
            cid = int(cur.lastrowid)
        conn.close()
        return cid

    def _jobs(self):
        conn = database.connect(self.db)
        rows = conn.execute(
            "SELECT id, kind, payload_json, state FROM job_queue"
            " WHERE kind = 'clip_intake_handoff'").fetchall()
        conn.close()
        return rows

    # ---- exactly-once and replay accounting ----

    def test_accept_then_replay_is_exactly_once(self):
        v1 = self.auth.admit("C-1")
        self.assertTrue(v1["accepted"])
        self.assertFalse(v1["replayed"])
        self.assertEqual(v1["intake_date"], DATE)
        self.assertEqual(v1["daily_count"], 1)
        self.assertIsInstance(v1["handoff_job_id"], int)
        v2 = self.auth.admit("C-1")
        self.assertTrue(v2["accepted"])
        self.assertTrue(v2["replayed"])
        self.assertEqual(v2["daily_count"], 1)
        self.assertEqual(v2["handoff_job_id"], v1["handoff_job_id"])
        self.assertEqual(self.auth.day_count(DATE), 1)

    def test_replay_returns_original_decision_not_current_day(self):
        self.auth.admit("C-1")
        self.clock[0] += 86400  # authority's own clock rolls to next day
        v = self.auth.admit("C-1")
        self.assertTrue(v["accepted"])
        self.assertTrue(v["replayed"])
        self.assertEqual(v["intake_date"], DATE)
        self.assertEqual(self.auth.day_count(NEXT_DATE), 0)

    def test_cap_rejects_ninth_and_records_verdict(self):
        self._fill(8)
        v = self.auth.admit("C-9")
        self.assertFalse(v["accepted"])
        self.assertEqual(v["reason"], "daily_cap")
        self.assertEqual(v["daily_count"], 8)
        self.assertNotIn("handoff_job_id", v)
        self.assertEqual(self.auth.status_of("C-9"), "REJECTED_CAP")

    def test_rejected_clip_admitted_when_capacity_frees(self):
        self._fill(8)
        self.assertFalse(self.auth.admit("C-9")["accepted"])
        self.clock[0] += 86400  # new day frees capacity
        v = self.auth.admit("C-9")
        self.assertTrue(v["accepted"])
        self.assertFalse(v["replayed"])
        self.assertEqual(v["intake_date"], NEXT_DATE)
        self.assertEqual(self.auth.day_count(NEXT_DATE), 1)
        self.assertEqual(self.auth.status_of("C-9"), "ACCEPTED")
        # Exactly one handoff job exists, minted at the flip.
        jobs = [j for j in self._jobs() if f'"clip_id": "C-9"'
                in j["payload_json"]]
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["id"], v["handoff_job_id"])

    # ---- durable handoff ----

    def test_acceptance_and_handoff_are_one_commit(self):
        v = self.auth.admit("C-1")
        # No further action (simulated crash right here): the handoff job
        # is already durable because it committed with the admission.
        jobs = self._jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["id"], v["handoff_job_id"])
        self.assertEqual(jobs[0]["state"], "PENDING")
        # The existing atomic claim path can take it exactly once.
        conn = database.connect(self.db)
        claimed = JobQueue(conn).claim("worker-1",
                                       kinds=["clip_intake_handoff"])
        self.assertIsNotNone(claimed)
        self.assertIsNone(JobQueue(conn).claim("worker-2",
                                               kinds=["clip_intake_handoff"]))
        conn.close()

    def test_replay_never_mints_a_second_handoff(self):
        v1 = self.auth.admit("C-1")
        for _ in range(5):
            v = self.auth.admit("C-1")
            self.assertEqual(v["handoff_job_id"], v1["handoff_job_id"])
        self.assertEqual(len(self._jobs()), 1)

    # ---- metadata immutability contract ----

    def test_conflicting_retry_metadata_ignored_and_flagged(self):
        self.auth.admit("C-1", title="old", video_url="old")
        v = self.auth.admit("C-1", title="new", video_url="new")
        self.assertTrue(v["accepted"])
        self.assertTrue(v["replayed"])
        self.assertTrue(v.get("metadata_ignored"))
        conn = database.connect(self.db)
        row = conn.execute(
            "SELECT title, video_url FROM clip_intake WHERE clip_id='C-1'"
        ).fetchone()
        conn.close()
        self.assertEqual(dict(row), {"title": "old", "video_url": "old"})
        # Matching metadata is not flagged.
        v2 = self.auth.admit("C-1", title="old")
        self.assertNotIn("metadata_ignored", v2)

    # ---- policy ownership: no caller-controlled cap or date ----

    def test_caller_cannot_set_cap_or_date(self):
        with self.assertRaises(TypeError):
            self.auth.admit("C-1", daily_cap=9)
        with self.assertRaises(TypeError):
            self.auth.admit("C-1", "2026-11-21")
        with self.assertRaises(TypeError):
            self.auth.admit_clip(1, daily_cap=9)
        # Nine admissions on the authority's own day hit the cap at 8.
        for i in range(9):
            self.auth.admit(f"POL-{i}")
        self.assertEqual(self.auth.day_count(DATE), 8)
        self.assertEqual(self.auth.status_of("POL-8"), "REJECTED_CAP")

    def test_day_boundary_from_authority_clock(self):
        # 2026-11-21 07:59:59 UTC is still 2026-11-20 in Los_Angeles.
        self.clock[0] = datetime(2026, 11, 21, 7, 59, 59,
                                 tzinfo=timezone.utc).timestamp()
        v1 = self.auth.admit("C-1")
        self.assertEqual(v1["intake_date"], "2026-11-20")
        self.clock[0] += 2  # one second past midnight LA
        v2 = self.auth.admit("C-2")
        self.assertEqual(v2["intake_date"], "2026-11-21")

    def test_unknown_timezone_fails_at_construction(self):
        with self.assertRaises(Exception):
            ClipIntakeAuthority(self.db, timezone="Not/AZone")

    # ---- input validation ----

    def test_input_validation(self):
        with self.assertRaises(ValueError):
            self.auth.admit("")
        with self.assertRaises(ValueError):
            self.auth.admit("   ")
        with self.assertRaises(ValueError):
            self.auth.admit(None)
        with self.assertRaises(ValueError):
            self.auth.admit("x" * 200)
        for bad in (0, -5, 300, float("nan"), float("inf"), True, "120"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                self.auth.admit("C-bad", duration_sec=bad)
        with self.assertRaises(ValueError):
            self.auth.admit("C-bad", title="t" * 201)
        with self.assertRaises(ValueError):
            self.auth.admit("C-bad", video_url="u" * 3000)
        with self.assertRaises(ValueError):
            self.auth.admit_clip(True)
        with self.assertRaises(ValueError):
            self.auth.admit_clip(0)
        with self.assertRaises(ValueError):
            self.auth.admit_clip("5")
        self.assertEqual(self.auth.day_count(DATE), 0)
        self.assertIsNone(self.auth.status_of("C-bad"))
        # Boundary values pass.
        self.assertTrue(self.auth.admit("C-ok", duration_sec=180)["accepted"])

    # ---- admit_clip: DB ground-truth validation ----

    def test_admit_clip_uses_db_ground_truth(self):
        cid = self._add_clip(duration=120.0)
        v = self.auth.admit_clip(cid)
        self.assertTrue(v["accepted"])
        self.assertEqual(v["clip_id"], f"cliprow:{cid}")
        self.assertIsInstance(v["handoff_job_id"], int)

    def test_admit_clip_rejects_over_limit_and_unknown(self):
        long_clip = self._add_clip(duration=300.0)
        v = self.auth.admit_clip(long_clip)
        self.assertEqual(v["reason"], "invalid_duration")
        self.assertFalse(v["accepted"])
        v2 = self.auth.admit_clip(99999)
        self.assertEqual(v2["reason"], "unknown_clip")
        # Validation rejections are not recorded.
        self.assertIsNone(self.auth.status_of(f"cliprow:{long_clip}"))
        self.assertIsNone(self.auth.status_of("cliprow:99999"))

    # ---- migration safety ----

    def test_requires_base_schema(self):
        bare = str(Path(self.tmp.name) / "bare.db")
        with self.assertRaises(RuntimeError):
            ClipIntakeAuthority(bare)
        conn = sqlite3.connect(bare)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        conn.close()
        self.assertNotIn("clip_intake", tables)

    def test_migration_isolation_from_shared_sequence(self):
        before = list(database.MIGRATIONS)
        ClipIntakeAuthority(self.db)
        ClipIntakeAuthority(self.db)
        self.assertEqual(list(database.MIGRATIONS), before)
        conn = database.connect(self.db)
        versions = {r["version"] for r in conn.execute(
            "SELECT version FROM schema_migrations")}
        self.assertEqual(versions, {m[0] for m in database.MIGRATIONS})
        ledger = {r["version"] for r in conn.execute(
            "SELECT version FROM clip_intake_schema_migrations")}
        self.assertEqual(ledger, {1, 2})
        future = list(database.MIGRATIONS) + [
            (99, "CREATE TABLE upstream_future (id INTEGER);")]
        applied = database.migrate(conn, future)
        self.assertIn(99, applied)
        conn.close()

    def test_concurrent_constructors_serialize(self):
        db = str(Path(self.tmp.name) / "cold.db")
        conn = database.connect(db)
        database.migrate(conn)
        conn.close()
        results = []

        def build():
            try:
                ClipIntakeAuthority(db)
                results.append("ok")
            except Exception as exc:  # pragma: no cover - diagnostic
                results.append(f"{type(exc).__name__}: {exc}")

        threads = [threading.Thread(target=build) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(results, ["ok"] * 8, results)
        conn = database.connect(db)
        ledger = {r["version"] for r in conn.execute(
            "SELECT version FROM clip_intake_schema_migrations")}
        conn.close()
        self.assertEqual(ledger, {1, 2})

    def test_interrupted_first_migration_rolls_back_completely(self):
        db = str(Path(self.tmp.name) / "int.db")
        conn = database.connect(db)
        database.migrate(conn)
        conn.close()

        class FailOnLedger(sqlite3.Connection):
            def execute(self, sql, parameters=(), /):
                if sql.startswith("INSERT INTO clip_intake_schema_migrations"):
                    raise sqlite3.OperationalError("injected fault")
                return super().execute(sql, parameters)

        real_connect = intake.connect

        def fault_connect(path):
            c = sqlite3.connect(path, isolation_level=None,
                                factory=FailOnLedger)
            c.row_factory = sqlite3.Row
            return c

        intake.connect = fault_connect
        try:
            with self.assertRaises(sqlite3.OperationalError):
                ClipIntakeAuthority(db)
        finally:
            intake.connect = real_connect
        # Whole version rolled back: neither the registry table nor the
        # ledger row survived, so the retry below starts clean.
        conn = database.connect(db)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("clip_intake", tables)
        self.assertNotIn("clip_intake_schema_migrations", tables)
        conn.close()
        ClipIntakeAuthority(db)  # retry succeeds
        conn = database.connect(db)
        ledger = {r["version"] for r in conn.execute(
            "SELECT version FROM clip_intake_schema_migrations")}
        self.assertEqual(ledger, {1, 2})
        cols = {r[1] for r in conn.execute("PRAGMA table_info(clip_intake)")}
        self.assertIn("handoff_job_id", cols)
        conn.close()

    def test_interrupted_later_migration_recovers_per_version(self):
        db = str(Path(self.tmp.name) / "int2.db")
        conn = database.connect(db)
        database.migrate(conn)
        conn.close()

        class FailOnAlter(sqlite3.Connection):
            def execute(self, sql, parameters=(), /):
                if "ALTER TABLE clip_intake" in sql:
                    raise sqlite3.OperationalError("injected fault")
                return super().execute(sql, parameters)

        real_connect = intake.connect

        def fault_connect(path):
            c = sqlite3.connect(path, isolation_level=None,
                                factory=FailOnAlter)
            c.row_factory = sqlite3.Row
            return c

        intake.connect = fault_connect
        try:
            with self.assertRaises(sqlite3.OperationalError):
                ClipIntakeAuthority(db)
        finally:
            intake.connect = real_connect
        # v1 committed, v2 rolled back: ledger consistent, column absent.
        conn = database.connect(db)
        ledger = {r["version"] for r in conn.execute(
            "SELECT version FROM clip_intake_schema_migrations")}
        self.assertEqual(ledger, {1})
        cols = {r[1] for r in conn.execute("PRAGMA table_info(clip_intake)")}
        self.assertNotIn("handoff_job_id", cols)
        conn.close()
        ClipIntakeAuthority(db)  # retry applies only v2
        conn = database.connect(db)
        ledger = {r["version"] for r in conn.execute(
            "SELECT version FROM clip_intake_schema_migrations")}
        self.assertEqual(ledger, {1, 2})
        cols = {r[1] for r in conn.execute("PRAGMA table_info(clip_intake)")}
        self.assertIn("handoff_job_id", cols)
        conn.close()

    # ---- concurrency ----

    def test_concurrent_same_clip_id_registers_once(self):
        results = []
        threads = [threading.Thread(
            target=lambda: results.append(_admit_one(self.db, "DUP-1")))
            for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        fresh = [r for r in results if not r["replayed"]]
        self.assertEqual(len(fresh), 1, results)
        self.assertTrue(all(r["accepted"] for r in results))
        self.assertEqual(len({r["handoff_job_id"] for r in results}), 1)
        self.assertEqual(self.auth.day_count(DATE), 1)
        self.assertEqual(len(self._jobs()), 1)

    def test_concurrent_boundary_never_over_admits(self):
        self._fill(6)
        results = []
        threads = [threading.Thread(
            target=lambda i=i: results.append(
                ("BND-%d" % i, _admit_one(self.db, "BND-%d" % i))))
            for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        accepted = {cid for cid, r in results if r["accepted"]}
        rejected = {cid for cid, r in results if not r["accepted"]}
        self.assertEqual(len(accepted), 2, results)
        self.assertEqual(len(rejected), 4, results)
        self.assertEqual(self.auth.day_count(DATE), 8)
        for cid in accepted:
            self.assertEqual(self.auth.status_of(cid), "ACCEPTED")
        for cid in rejected:
            self.assertEqual(self.auth.status_of(cid), "REJECTED_CAP")
        # 6 fills + 2 winners = 8 handoff jobs, no more.
        self.assertEqual(len(self._jobs()), 8)

    def test_concurrent_same_id_pairs_at_boundary_keep_acknowledged_data(self):
        self._fill(7)
        results = []
        threads = [threading.Thread(
            target=lambda c=c: results.append((c, _admit_one(self.db, c))))
            for c in ("X-1", "X-1", "Y-1", "Y-1")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        accepted_ids = {cid for cid, r in results if r["accepted"]}
        self.assertEqual(len(accepted_ids), 1, results)
        winner = accepted_ids.pop()
        loser = "Y-1" if winner == "X-1" else "X-1"
        self.assertEqual(self.auth.status_of(winner), "ACCEPTED")
        self.assertEqual(self.auth.status_of(loser), "REJECTED_CAP")
        self.assertEqual(self.auth.day_count(DATE), 8)
        for cid, r in results:
            self.assertEqual(r["accepted"], cid == winner)

    def test_concurrent_admit_clip_same_row(self):
        cid = self._add_clip(duration=90.0)
        results = []
        threads = [threading.Thread(
            target=lambda: results.append(_admit_row(self.db, cid)))
            for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        fresh = [r for r in results if not r["replayed"]]
        self.assertEqual(len(fresh), 1, results)
        self.assertEqual(self.auth.day_count(DATE), 1)

    def test_concurrent_storm_mixed(self):
        calls = [f"S-%02d" % i for i in range(20)] + ["S-%02d" % i for i in range(10)]
        results = []
        threads = [threading.Thread(
            target=lambda c=c: results.append((c, _admit_one(self.db, c))))
            for c in calls]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        accepted_ids = {cid for cid, r in results if r["accepted"]}
        self.assertLessEqual(len(accepted_ids), 8)
        self.assertEqual(self.auth.day_count(DATE), 8)
        for cid in accepted_ids:
            self.assertEqual(self.auth.status_of(cid), "ACCEPTED")
        by_id = {}
        for cid, r in results:
            by_id.setdefault(cid, set()).add(r["accepted"])
        for cid, outcomes in by_id.items():
            self.assertEqual(len(outcomes), 1, (cid, outcomes))
        self.assertEqual(len(self._jobs()), 8)

    def test_multiprocess_boundary(self):
        from tests.process_helpers import track_children
        self._fill(6)
        root = str(Path(__file__).resolve().parents[2])
        children = track_children(self)
        procs = [subprocess.Popen(
            [sys.executable, "-m", "tests.proc_workers", "admit", self.db,
             "MP-%d" % i], cwd=root, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True) for i in range(6)]
        verdicts = []
        for proc in procs:
            out, err = proc.communicate(timeout=60)
            self.assertEqual(proc.returncode, 0, err)
            verdicts.append(json.loads(out.strip().splitlines()[-1]))
        self.assertEqual(len(children), 6)
        self.assertEqual(sum(1 for v in verdicts if v["accepted"]), 2,
                         verdicts)
        self.assertEqual(self.auth.day_count(DATE), 8)

    # ---- legacy rows, convergence, readiness, namespace ----

    def test_legacy_accepted_replay_refused_fail_closed(self):
        for ledgered in (True, False):
            path = str(Path(self.tmp.name) / f"legacy-{ledgered}.db")
            _make_legacy_db(path, ledgered)
            auth = ClipIntakeAuthority(path, _clock=fixed_clock)
            v = auth.admit("legacy-accepted")
            self.assertFalse(v["accepted"], v)
            self.assertTrue(v["replayed"])
            self.assertEqual(v["reason"], "legacy_no_handoff")
            self.assertIsNone(v["handoff_job_id"])
            self.assertTrue(v["legacy"])
            conn = database.connect(path)
            row = conn.execute(
                "SELECT status, handoff_job_id FROM clip_intake"
                " WHERE clip_id = 'legacy-accepted'").fetchone()
            jobs = conn.execute(
                "SELECT COUNT(*) FROM job_queue"
                " WHERE kind = 'clip_intake_handoff'").fetchone()[0]
            versions = sorted(
                r[0] for r in conn.execute(
                    "SELECT version FROM clip_intake_schema_migrations"))
            conn.close()
            # Row preserved untouched, no pending record minted, schema
            # converged to the current version either way.
            self.assertEqual(row["status"], "ACCEPTED")
            self.assertIsNone(row["handoff_job_id"])
            self.assertEqual(jobs, 0)
            self.assertEqual(versions, [1, 2])

    def test_partial_v2_column_without_ledger_converges(self):
        # Simulates a kill by an older non-atomic implementation: the
        # column exists but the ledger knows only version 1.
        path = str(Path(self.tmp.name) / "partial.db")
        conn = database.connect(path)
        database.migrate(conn)
        conn.executescript(_LEGACY_V1_DDL)
        conn.execute(
            "ALTER TABLE clip_intake ADD COLUMN handoff_job_id INTEGER")
        conn.execute(
            "CREATE TABLE clip_intake_schema_migrations"
            " (version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)")
        conn.execute(
            "INSERT INTO clip_intake_schema_migrations VALUES (1, 0)")
        conn.close()
        auth = ClipIntakeAuthority(path, _clock=fixed_clock)  # must converge
        v = auth.admit("fresh")
        self.assertTrue(v["accepted"], v)

    def test_legacy_rejected_cap_row_can_flip(self):
        # A legacy REJECTED_CAP row never left this registry, so a later
        # admission with capacity is safe and mints its first record.
        path = str(Path(self.tmp.name) / "legacyrej.db")
        _make_legacy_db(path, True)
        conn = database.connect(path)
        conn.execute(
            "INSERT INTO clip_intake (clip_id, intake_date, status,"
            " daily_count, created_at, updated_at) VALUES"
            " ('legacy-rejected', '2026-11-20', 'REJECTED_CAP', 8, 0, 0)")
        conn.close()
        auth = ClipIntakeAuthority(path, _clock=fixed_clock)
        v = auth.admit("legacy-rejected")
        self.assertTrue(v["accepted"], v)
        self.assertIsInstance(v["handoff_job_id"], int)

    def test_failed_or_pathless_clip_not_newly_admitted(self):
        failed = self._add_clip(status="FAILED")
        v = self.auth.admit_clip(failed)
        self.assertFalse(v["accepted"], v)
        self.assertEqual(v["reason"], "clip_not_ready")
        qc_failed = self._add_clip(status="QC_FAILED")
        v2 = self.auth.admit_clip(qc_failed)
        self.assertEqual(v2["reason"], "clip_not_ready")
        conn = database.connect(self.db)
        cur = conn.execute(
            "INSERT INTO clips (duration_seconds, file_path, status,"
            " created_at) VALUES (120, NULL, 'HELD', 0)")
        pathless = int(cur.lastrowid)
        cur = conn.execute(
            "INSERT INTO clips (duration_seconds, file_path, status,"
            " created_at) VALUES (120, '   ', 'HELD', 0)")
        blank = int(cur.lastrowid)
        conn.close()
        v3 = self.auth.admit_clip(pathless)
        self.assertEqual(v3["reason"], "clip_not_ready")
        v4 = self.auth.admit_clip(blank)
        self.assertEqual(v4["reason"], "clip_not_ready")
        # Readiness rejections are not recorded; a fixed clip may qualify
        # later.
        self.assertIsNone(self.auth.status_of(f"cliprow:{failed}"))
        self.assertIsNone(self.auth.status_of(f"cliprow:{pathless}"))

    def test_deleted_clip_row_keeps_stable_admission(self):
        cid = self._add_clip()
        v1 = self.auth.admit_clip(cid)
        self.assertTrue(v1["accepted"], v1)
        conn = database.connect(self.db)
        conn.execute("DELETE FROM clips WHERE id = ?", (cid,))
        conn.close()
        v2 = self.auth.admit_clip(cid)
        self.assertTrue(v2["accepted"], v2)
        self.assertTrue(v2["replayed"])
        self.assertEqual(v2["handoff_job_id"], v1["handoff_job_id"])
        # A status change after admission is equally stable.
        cid2 = self._add_clip()
        w1 = self.auth.admit_clip(cid2)
        conn = database.connect(self.db)
        conn.execute("UPDATE clips SET status = 'DELIVERED' WHERE id = ?",
                     (cid2,))
        conn.close()
        w2 = self.auth.admit_clip(cid2)
        self.assertTrue(w2["accepted"], w2)
        self.assertTrue(w2["replayed"])
        self.assertEqual(w2["handoff_job_id"], w1["handoff_job_id"])

    def test_duration_divergence_flags_metadata_ignored(self):
        v1 = self.auth.admit("dur", duration_sec=120)
        self.assertTrue(v1["accepted"], v1)
        v2 = self.auth.admit("dur", duration_sec=150)
        self.assertTrue(v2["replayed"])
        self.assertTrue(v2.get("metadata_ignored"), v2)
        v3 = self.auth.admit("dur", duration_sec=120)
        self.assertNotIn("metadata_ignored", v3)

    def test_reserved_namespace_rejected_for_opaque_ids(self):
        with self.assertRaises(ValueError):
            self.auth.admit("cliprow:1")
        with self.assertRaises(ValueError):
            self.auth.admit("cliprow:999999")


_LEGACY_V1_DDL = """
CREATE TABLE clip_intake (
    clip_id TEXT PRIMARY KEY,
    intake_date TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ACCEPTED', 'REJECTED_CAP')),
    title TEXT,
    streamer TEXT,
    video_url TEXT,
    duration_sec REAL,
    daily_count INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX idx_clip_intake_day ON clip_intake(intake_date, status);
"""


def _make_legacy_db(path, ledgered):
    """A database left by the pre-handoff implementation: v1 clip_intake
    schema, one ACCEPTED row, no handoff column; optionally with the v1
    ledger recorded."""
    conn = database.connect(path)
    database.migrate(conn)
    conn.executescript(_LEGACY_V1_DDL)
    if ledgered:
        conn.execute(
            "CREATE TABLE clip_intake_schema_migrations"
            " (version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)")
        conn.execute(
            "INSERT INTO clip_intake_schema_migrations VALUES (1, 0)")
    conn.execute(
        "INSERT INTO clip_intake (clip_id, intake_date, status,"
        " daily_count, created_at, updated_at) VALUES"
        " ('legacy-accepted', '2026-11-21', 'ACCEPTED', 1, 0, 0)")
    conn.close()


if __name__ == "__main__":
    unittest.main()
