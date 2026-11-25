import json
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from app.bot import manual_mark, ops, sessions
from app.bot.commands import CommandRouter
from app.bot.inbox import PreAckInbox
from app.bot.watermark import UpdateWatermark
from app.clipping.auto import AutoClipper
from app.clipping.manual import MANUAL_MARK_KIND
from app.clipping.quota import GENERATION_DAILY_CAP, local_day_bounds
from app.clipping.seal import seal_file
from app.core.database import connect, migrate
from app.intelligence.providers import HeuristicProvider, QuotaExhausted


class FakeSession:
    def __init__(self, session_id):
        self._session_id = session_id
        self._stopping = threading.Event()

    def alive(self):
        return True


class ManualMarkCommandTests(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.dir = Path(self.td.name)
        self.db = str(self.dir / "nova.db")
        conn = connect(self.db)
        migrate(conn)
        conn.execute("INSERT INTO twitch_sources (id, username, created_at,"
                     " updated_at) VALUES (1, 'chan', 0, 0)")
        conn.execute("INSERT INTO stream_sessions (id, source_id, started_at,"
                     " state, ingest_mode) VALUES (1, 1, 0, 'RECORDING',"
                     " 'playback')")
        conn.execute("INSERT INTO recording_segments"
                     " (session_id, file_path, sequence, duration_seconds,"
                     " created_at) VALUES"
                     " (1, '/tmp/seg-0.ts', 0, 5.0, 1000.0)")
        conn.commit()
        conn.close()
        self._old_sessions = dict(sessions._sessions)
        sessions._sessions.clear()
        sessions._sessions["chan"] = FakeSession(1)

    def tearDown(self):
        sessions._sessions.clear()
        sessions._sessions.update(self._old_sessions)
        self.td.cleanup()

    def _events(self):
        conn = connect(self.db)
        rows = conn.execute(
            "SELECT id, detected_at, kind, confidence, status, metadata_json"
            " FROM candidate_events ORDER BY id").fetchall()
        conn.close()
        return rows

    def test_mark_uses_command_receipt_inside_durable_timeline(self):
        with mock.patch("app.bot.manual_mark.time.time",
                        return_value=1007.0):
            reply = manual_mark.clipmark_reply(self.dir, "chan 10",
                                               update_id=42)
        self.assertIn("Manual mark #1", reply)
        rows = self._events()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["kind"], MANUAL_MARK_KIND)
        self.assertIsNone(row["confidence"])  # owner command, not a score
        self.assertEqual(row["status"], "PENDING")
        self.assertAlmostEqual(row["detected_at"], 997.0)
        meta = json.loads(row["metadata_json"])
        self.assertEqual(meta["origin"], "owner_manual")
        self.assertEqual(meta["mark_basis"],
                         "command_receipt_minus_seconds_ago")
        self.assertEqual(meta["seconds_ago"], 10.0)
        self.assertEqual(meta["command_received_at"], 1007.0)
        self.assertEqual(meta["requested_event_time"], 997.0)
        self.assertEqual(meta["durable_lag_seconds"], 7.0)
        self.assertEqual(meta["wait_deadline_at"], 1007.0 + 45.0)
        self.assertEqual(meta["update_id"], 42)

    def test_open_tail_mark_keeps_requested_moment(self):
        with mock.patch("app.bot.manual_mark.time.time",
                        return_value=1003.5):
            reply = manual_mark.clipmark_reply(self.dir, "chan 3",
                                               update_id=42)
        self.assertIn("past the newest durable segment", reply)
        row = self._events()[0]
        self.assertEqual(row["detected_at"], 1000.5)   # not clamped to 1000
        meta = json.loads(row["metadata_json"])
        self.assertEqual(meta["requested_event_time"], 1000.5)
        self.assertEqual(meta["durable_lag_seconds"], 3.5)
        self.assertNotIn("timing_adjustment_seconds", meta)

    def test_replayed_update_returns_journal_without_second_event(self):
        first = manual_mark.clipmark_reply(self.dir, "chan", update_id=42)
        second = manual_mark.clipmark_reply(self.dir, "chan", update_id=42)
        self.assertIn("Already processed", second)
        self.assertIn(first, second)
        self.assertEqual(len(self._events()), 1)

    def test_no_active_session_fails_loudly(self):
        reply = manual_mark.clipmark_reply(self.dir, "other", update_id=43)
        self.assertIn("No active recording session", reply)
        self.assertEqual(self._events(), [])

    def test_no_durable_segment_fails_loudly(self):
        conn = connect(self.db)
        conn.execute("DELETE FROM recording_segments")
        conn.commit()
        conn.close()
        reply = manual_mark.clipmark_reply(self.dir, "chan", update_id=43)
        self.assertIn("No durable recorded segment", reply)
        self.assertEqual(self._events(), [])

    def test_stale_in_memory_session_does_not_override_ended_row(self):
        conn = connect(self.db)
        conn.execute("UPDATE stream_sessions SET state='ENDED' WHERE id=1")
        conn.commit()
        conn.close()
        reply = manual_mark.clipmark_reply(self.dir, "chan", update_id=43)
        self.assertIn("not recording", reply)
        self.assertEqual(self._events(), [])

    def test_multiple_sessions_require_explicit_username(self):
        conn = connect(self.db)
        conn.execute("INSERT INTO twitch_sources (id, username, created_at,"
                     " updated_at) VALUES (2, 'other', 0, 0)")
        conn.execute("INSERT INTO stream_sessions (id, source_id, started_at,"
                     " state, ingest_mode) VALUES (2, 2, 0, 'RECORDING',"
                     " 'playback')")
        conn.execute("INSERT INTO recording_segments"
                     " (session_id, file_path, sequence, duration_seconds,"
                     " created_at) VALUES"
                     " (2, '/tmp/seg-1.ts', 0, 5.0, 2000.0)")
        conn.commit()
        conn.close()
        sessions._sessions["other"] = FakeSession(2)
        reply = manual_mark.clipmark_reply(self.dir, "2", update_id=44)
        self.assertIn("More than one session", reply)
        with mock.patch("app.bot.manual_mark.time.time",
                        return_value=2010.0):
            reply = manual_mark.clipmark_reply(self.dir, "10 other",
                                               update_id=44)
        self.assertIn("Manual mark #1", reply)
        row = self._events()[0]
        self.assertEqual(row["detected_at"], 2000.0)
        meta = json.loads(row["metadata_json"])
        self.assertEqual(meta["requested_event_time"], 2000.0)

    def test_seconds_ago_is_bounded(self):
        reply = manual_mark.clipmark_reply(self.dir, "chan 21", update_id=45)
        self.assertIn("between 0 and 20", reply)
        self.assertEqual(self._events(), [])

    def test_side_effecting_crash_window_is_uncertain_not_replayed(self):
        self.assertIn("clipmark", ops.SIDE_EFFECTING)
        inbox = PreAckInbox(str(self.dir / "inbox.json"))
        watermark = UpdateWatermark(str(self.dir / "updates.json"))
        inbox.add(50, 42, "/clipmark chan")
        called = []
        router = CommandRouter(admin_ids=[42])
        router.register("clipmark", lambda c: called.append(1) or "ran")
        result = ops.drain_inbox(self.dir, router, watermark, inbox,
                                 admin_id=42)
        self.assertEqual(result["uncertain"], 1)
        self.assertEqual(result["replayed"], 0)
        self.assertEqual(called, [])
        self.assertEqual(ops.journal_uncertain(self.dir)[0]["command"],
                         "clipmark")


class FakeBuffer:
    def __init__(self):
        self.session_id = 1
        self.segment_seconds = 5.0


def _extract_ok(*args, **_kwargs):
    path = Path(args[3])
    path.write_bytes(b"\x00" * 256)
    return path, 30.0


class ManualMarkAutoClipTests(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.db = str(Path(self.td.name) / "t.db")
        conn = connect(self.db)
        migrate(conn)
        conn.execute("INSERT INTO twitch_sources (id, username, created_at,"
                     " updated_at) VALUES (1, 'chan', 0, 0)")
        conn.execute("INSERT INTO stream_sessions (id, source_id, started_at,"
                     " state) VALUES (1, 1, 0, 'RECORDING')")
        conn.execute("INSERT INTO recording_segments (session_id,"
                     " file_path, sequence, duration_seconds, created_at)"
                     " VALUES (1, '/tmp/s0.ts', 0, 5.0, 100000.0)")
        conn.commit()
        conn.close()

    def tearDown(self):
        self.td.cleanup()

    def _add_event(self, event_id_time, kind, confidence=None):
        conn = connect(self.db)
        cur = conn.execute(
            "INSERT INTO candidate_events"
            " (session_id, detected_at, kind, confidence)"
            " VALUES (1,?,?,?)", (event_id_time, kind, confidence))
        conn.commit()
        conn.close()
        return cur.lastrowid

    def _statuses(self):
        conn = connect(self.db)
        rows = dict(conn.execute(
            "SELECT id, status FROM candidate_events").fetchall())
        clips = conn.execute("SELECT count(*) FROM clips").fetchone()[0]
        conn.close()
        return rows, clips

    def _add_clip_row(self, created_at):
        conn = connect(self.db)
        conn.execute(
            "INSERT INTO clips (session_id, file_path, duration_seconds,"
            " status, created_at) VALUES (1, '/tmp/old.mp4', 30.0,"
            " 'HELD', ?)", (created_at,))
        conn.commit()
        conn.close()

    def test_generation_cap_blocks_ninth_manual_clip_same_source_day(self):
        fixed = 1764100800.0  # a stable instant inside one LA policy day
        day, start, end = local_day_bounds(fixed)
        for _ in range(GENERATION_DAILY_CAP):
            self._add_clip_row(start + 60)
        event_id = self._add_event(100.0, MANUAL_MARK_KIND, None)
        clipper = AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                              "ffmpeg", self.td.name,
                              extract_fn=_extract_ok, clock=lambda: fixed)
        self.assertEqual(clipper.process_pending(), 0)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[event_id], "QUOTA_EXCEEDED")
        self.assertEqual(clips, GENERATION_DAILY_CAP)

    def test_generation_cap_reopens_on_next_policy_day(self):
        fixed = 1764100800.0
        day, start, end = local_day_bounds(fixed)
        for _ in range(GENERATION_DAILY_CAP):
            self._add_clip_row(start - 60)
        event_id = self._add_event(100.0, MANUAL_MARK_KIND, None)
        clipper = AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                              "ffmpeg", self.td.name,
                              extract_fn=_extract_ok, clock=lambda: fixed)
        self.assertEqual(clipper.process_pending(), 1)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[event_id], "CLIPPED")
        self.assertEqual(clips, GENERATION_DAILY_CAP + 1)

    def test_generation_cap_also_bounds_automatic_extraction(self):
        fixed = 1764100800.0
        day, start, end = local_day_bounds(fixed)
        for _ in range(GENERATION_DAILY_CAP):
            self._add_clip_row(start + 60)
        event_id = self._add_event(100.0, "audio_spike", 0.9)
        clipper = AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                              "ffmpeg", self.td.name,
                              extract_fn=_extract_ok, clock=lambda: fixed)
        self.assertEqual(clipper.process_pending(), 0)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[event_id], "QUOTA_EXCEEDED")
        self.assertEqual(clips, GENERATION_DAILY_CAP)

    def test_generation_cap_is_concurrency_safe_at_boundary(self):
        fixed = 1764100800.0
        day, start, end = local_day_bounds(fixed)
        for _ in range(GENERATION_DAILY_CAP - 1):
            self._add_clip_row(start + 60)
        first = self._add_event(100.0, MANUAL_MARK_KIND, None)
        second = self._add_event(120.0, MANUAL_MARK_KIND, None)
        barrier = threading.Barrier(2)

        def racing_extract(*args, **kwargs):
            path = Path(args[3])
            path.write_bytes(b"\x00" * 256)
            barrier.wait(5)
            return path, 30.0

        errors = []

        def run():
            try:
                AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                            "ffmpeg", self.td.name,
                            extract_fn=racing_extract,
                            clock=lambda: fixed).process_pending()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=run) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertEqual(errors, [])
        statuses, clips = self._statuses()
        self.assertEqual(clips, GENERATION_DAILY_CAP)
        self.assertEqual(statuses[first], "CLIPPED")
        self.assertEqual(statuses[second], "QUOTA_EXCEEDED")

    def test_manual_mark_needs_no_provider_or_confidence(self):
        class MustNotCall:
            name = "must-not-call"

            def score_moments(self, *args, **kwargs):
                raise AssertionError("manual mark reached provider")

        self._add_event(100.0, MANUAL_MARK_KIND, None)
        clipper = AutoClipper(FakeBuffer(), self.db, MustNotCall(),
                              "ffmpeg", self.td.name,
                              extract_fn=_extract_ok)
        self.assertEqual(clipper.process_pending(), 1)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[1], "CLIPPED")
        self.assertEqual(clips, 1)

    def test_provider_quota_queues_heuristics_but_not_manual_mark(self):
        class BrokeProvider:
            name = "broke"

            def score_moments(self, *args, **kwargs):
                raise QuotaExhausted("429")

        audio_id = self._add_event(100.0, "audio_spike", 0.9)
        manual_id = self._add_event(120.0, MANUAL_MARK_KIND, None)
        clipper = AutoClipper(FakeBuffer(), self.db, BrokeProvider(),
                              "ffmpeg", self.td.name,
                              extract_fn=_extract_ok)
        self.assertEqual(clipper.process_pending(), 1)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[audio_id], "QUEUED")
        self.assertEqual(statuses[manual_id], "CLIPPED")
        self.assertEqual(clips, 1)
        clipper.provider = HeuristicProvider()
        self.assertEqual(clipper.process_pending(), 1)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[audio_id], "CLIPPED")
        self.assertEqual(clips, 2)

    def test_event_kind_filter_limits_recovery_candidates(self):
        class MustNotCall:
            name = "must-not-call"

            def score_moments(self, *args, **kwargs):
                raise AssertionError("filtered heuristic reached provider")

        audio_id = self._add_event(100.0, "audio_spike", 0.9)
        manual_id = self._add_event(120.0, MANUAL_MARK_KIND, None)
        clipper = AutoClipper(FakeBuffer(), self.db, MustNotCall(),
                              "ffmpeg", self.td.name,
                              extract_fn=_extract_ok,
                              event_kinds=(MANUAL_MARK_KIND,))
        self.assertEqual(clipper.process_pending(), 1)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[audio_id], "PENDING")
        self.assertEqual(statuses[manual_id], "CLIPPED")
        self.assertEqual(clips, 1)

    def test_processed_manual_mark_suppresses_queued_nearby_heuristic(self):
        class MustNotCall:
            name = "must-not-call"

            def score_moments(self, *args, **kwargs):
                raise AssertionError("suppressed duplicate reached provider")

        manual_id = self._add_event(100.0, MANUAL_MARK_KIND, None)
        audio_id = self._add_event(102.0, "audio_spike", 0.9)
        conn = connect(self.db)
        conn.execute("UPDATE candidate_events SET status='CLIPPED'"
                     " WHERE id=?", (manual_id,))
        conn.execute("UPDATE candidate_events SET status='QUEUED'"
                     " WHERE id=?", (audio_id,))
        conn.commit()
        conn.close()
        clipper = AutoClipper(FakeBuffer(), self.db, MustNotCall(),
                              "ffmpeg", self.td.name,
                              extract_fn=_extract_ok)
        self.assertEqual(clipper.process_pending(), 0)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[manual_id], "CLIPPED")
        self.assertEqual(statuses[audio_id], "MERGED")
        self.assertEqual(clips, 0)

    def test_manual_mark_wins_same_moment_merge(self):
        audio_id = self._add_event(100.0, "audio_spike", 0.9)
        manual_id = self._add_event(101.0, MANUAL_MARK_KIND, None)
        clipper = AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                              "ffmpeg", self.td.name,
                              extract_fn=_extract_ok)
        self.assertEqual(clipper.process_pending(), 1)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[audio_id], "MERGED")
        self.assertEqual(statuses[manual_id], "CLIPPED")
        self.assertEqual(clips, 1)

    def test_crash_after_extract_leaves_manual_mark_retryable(self):
        self._add_event(100.0, MANUAL_MARK_KIND, None)
        calls = {"n": 0}

        def flaky(*args, **kwargs):
            calls["n"] += 1
            Path(args[3]).write_bytes(b"\x00" * 256)
            if calls["n"] == 1:
                raise SystemExit("simulated crash after extract")
            return Path(args[3]), 30.0

        clipper = AutoClipper(FakeBuffer(), self.db, HeuristicProvider(),
                              "ffmpeg", self.td.name, extract_fn=flaky)
        self.assertRaises(SystemExit, clipper.process_pending)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[1], "PENDING")
        self.assertEqual(clips, 0)
        self.assertEqual(clipper.process_pending(), 1)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[1], "CLIPPED")
        self.assertEqual(clips, 1)


class ManualMarkCoverageGateTests(unittest.TestCase):
    """Staged segment arrival: the mark waits for durable coverage."""
    tearDown = ManualMarkAutoClipTests.tearDown
    _statuses = ManualMarkAutoClipTests._statuses

    def setUp(self):
        self.td = TemporaryDirectory()
        self.db = str(Path(self.td.name) / "t.db")
        conn = connect(self.db)
        migrate(conn)
        conn.execute("INSERT INTO twitch_sources (id, username, created_at,"
                     " updated_at) VALUES (1, 'chan', 0, 0)")
        conn.execute("INSERT INTO stream_sessions (id, source_id, started_at,"
                     " state) VALUES (1, 1, 0, 'RECORDING')")
        conn.execute("INSERT INTO recording_segments (session_id, file_path,"
                     " sequence, duration_seconds, created_at)"
                     " VALUES (1, '/tmp/s0.ts', 0, 5.0, 100.0)")
        conn.commit()
        conn.close()

    def _mark(self, at, deadline):
        import json as _j
        conn = connect(self.db)
        cur = conn.execute(
            "INSERT INTO candidate_events (session_id, detected_at, kind,"
            " confidence, metadata_json) VALUES (1,?,?,NULL,?)",
            (at, MANUAL_MARK_KIND,
             _j.dumps({"wait_deadline_at": deadline})))
        conn.commit()
        conn.close()
        return cur.lastrowid

    def _seg(self, seq, end):
        conn = connect(self.db)
        conn.execute("INSERT INTO recording_segments (session_id, file_path,"
                     " sequence, duration_seconds, created_at)"
                     " VALUES (1, ?, ?, 5.0, ?)", (f"/tmp/s{seq}.ts", seq, end))
        conn.commit()
        conn.close()

    def _clipper(self, now):
        calls = []

        def extract(*a, **k):
            calls.append(a[2])
            return _extract_ok(*a, **k)
        c = AutoClipper(FakeBuffer(), self.db, HeuristicProvider(), "ffmpeg",
                        self.td.name, extract_fn=extract, post_seconds=10.0,
                        clock=lambda: now)
        return c, calls

    def test_waits_then_extracts_at_requested_moment(self):
        ev = self._mark(103.0, 1000.0)       # requested in the open tail
        clipper, calls = self._clipper(110.0)
        self.assertEqual(clipper.process_pending(), 0)
        self.assertEqual(self._statuses()[0][ev], "PENDING")
        self.assertEqual(calls, [])
        self._seg(1, 110.0)                  # covers 103 only up to 110
        self.assertEqual(clipper.process_pending(), 0)
        self.assertEqual(calls, [])          # still short of 113
        self._seg(2, 115.0)                  # now covers 103 + 10
        self.assertEqual(clipper.process_pending(), 1)
        self.assertEqual(calls, [103.0])     # anchor is requested moment
        self.assertEqual(self._statuses()[0][ev], "CLIPPED")

    def test_deadline_with_moment_present_flags_truncated_post_roll(self):
        ev = self._mark(103.0, 120.0)
        self._seg(1, 108.0)                  # moment covered, post-roll short
        clipper, calls = self._clipper(121.0)
        self.assertEqual(clipper.process_pending(), 1)
        self.assertEqual(calls, [103.0])
        conn = connect(self.db)
        import json as _j
        md = _j.loads(conn.execute("SELECT metadata_json FROM"
                                   " candidate_events WHERE id=?",
                                   (ev,)).fetchone()[0])
        conn.close()
        self.assertTrue(md["post_window_truncated"])

    def test_deadline_without_moment_fails_explicitly_no_clip(self):
        ev = self._mark(130.0, 120.0)        # footage never reached 130
        clipper, calls = self._clipper(121.0)
        self.assertEqual(clipper.process_pending(), 0)
        statuses, clips = self._statuses()
        self.assertEqual(statuses[ev], "MARK_UNCOVERED")
        self.assertEqual(clips, 0)
        self.assertEqual(calls, [])

    def test_drained_session_decides_immediately(self):
        ev = self._mark(130.0, 99999.0)
        conn = connect(self.db)
        conn.execute("UPDATE stream_sessions SET state='INTERRUPTED'")
        conn.commit()
        conn.close()
        clipper, calls = self._clipper(121.0)
        clipper.process_pending()
        self.assertEqual(self._statuses()[0][ev], "MARK_UNCOVERED")

    def test_storage_floor_releases_after_wait_deadline(self):
        import tempfile, time as _t
        from app.recording.buffer import RollingBuffer
        with tempfile.TemporaryDirectory() as tmp:
            buf = RollingBuffer(tmp, session_id=1, segment_seconds=5,
                                pre_event_seconds=30)
            buf.bind_db(self.db)
            self._mark(103.0, _t.time() - 1)       # deadline already passed
            self.assertIsNone(buf._pending_mark_floor())
            self._mark(110.0, _t.time() + 600)     # still within wait
            self.assertAlmostEqual(buf._pending_mark_floor(), 80.0)

    def test_pending_mark_protects_preroll_from_rotation(self):
        import tempfile
        from app.recording.buffer import RollingBuffer
        with tempfile.TemporaryDirectory() as tmp:
            buf = RollingBuffer(tmp, session_id=1, segment_seconds=5,
                                pre_event_seconds=30, max_storage_mb=0)
            buf.bind_db(self.db)
            self._mark(103.0, 4e9)
            self.assertAlmostEqual(buf._pending_mark_floor(), 73.0)
            conn = connect(self.db)
            conn.execute("UPDATE candidate_events SET status='CLIPPED'")
            conn.commit()
            conn.close()
            self.assertIsNone(buf._pending_mark_floor())


class ManualMarkOutcomeAndQuotaTests(ManualMarkCoverageGateTests):
    # reuses fixtures; inherited gate tests simply rerun harmlessly
    """Terminal outcomes, cheap precheck, heuristic-first, quota scopes."""

    def test_base_gate_tests_not_rerun_here(self):
        pass

    def _status(self, ev, status):
        conn = connect(self.db)
        conn.execute("UPDATE candidate_events SET status=? WHERE id=?",
                     (status, ev))
        conn.commit()
        conn.close()

    def test_terminal_outcomes_are_announced_once_replay_safe(self):
        from app.bot import ops
        state = Path(self.td.name)
        # nova.db lives at state/nova.db for ops
        import shutil
        shutil.copy(self.db, state / "nova.db")
        self.db = str(state / "nova.db")
        ids = {}
        for st in ("MARK_UNCOVERED", "QUOTA_EXCEEDED", "EXPIRED", "FAILED",
                   "MERGED"):
            ev = self._mark(103.0, 1000.0)
            self._status(ev, st)
            ids[st] = ev
        sent = []
        n = ops.notify_manual_mark_outcomes(state, notify_fn=sent.append)
        self.assertEqual(n, 5)
        self.assertEqual(len(sent), 5)
        self.assertTrue(any("daily clip limit" in m for m in sent))
        self.assertTrue(any("never reached the moment" in m for m in sent))
        # replay / restart: nothing is announced twice
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 0)
        self.assertEqual(len(sent), 5)

    def test_failed_send_is_retried_not_lost(self):
        from app.bot import ops
        import shutil
        state = Path(self.td.name)
        ev = self._mark(103.0, 1000.0)
        self._status(ev, "MARK_UNCOVERED")
        shutil.copy(self.db, state / "nova.db")

        def boom(_t):
            raise RuntimeError("network")
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=boom), 0)
        sent = []
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 1)

    def _prep_state(self):
        import shutil
        state = Path(self.td.name)
        shutil.copy(self.db, state / "nova.db")
        self.db = str(state / "nova.db")
        return state

    def _clip_for(self, ev, status):
        conn = connect(self.db)
        conn.execute("DELETE FROM clips WHERE event_id=?", (ev,))
        conn.execute(
            "INSERT INTO clips (session_id, event_id, file_path,"
            " duration_seconds, status, created_at)"
            " VALUES (1,?,'/tmp/o.mp4',30,?,1.0)", (ev, status))
        conn.commit()
        conn.close()

    def test_recovered_mark_corrects_failure_notice_once(self):
        from app.bot import ops
        state = self._prep_state()
        ev = self._mark(103.0, 1000.0)
        self._status(ev, "FAILED")
        sent = []
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 1)
        self.assertIn("NOT clipped", sent[0])
        # reclaim succeeds
        self._status(ev, "CLIPPED")
        self._clip_for(ev, "HELD")
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 1)
        self.assertIn("recovered", sent[1])
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 0)
        # failing again after recovery is announced again
        self._status(ev, "FAILED")
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 1)
        self.assertIn("NOT clipped", sent[2])

    def test_recovery_notice_only_after_good_clip_state(self):
        from app.bot import ops
        state = self._prep_state()
        ev = self._mark(103.0, 1000.0)
        self._status(ev, "FAILED")
        sent = []
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 1)
        # reclaim in progress: event CLIPPED but clip not good yet
        self._status(ev, "CLIPPED")
        for st in (None, "PENDING", "RECOVERING"):
            if st is None:
                conn = connect(self.db)
                conn.execute("DELETE FROM clips WHERE event_id=?", (ev,))
                conn.commit()
                conn.close()
            else:
                self._clip_for(ev, st)
            self.assertEqual(ops.notify_manual_mark_outcomes(
                state, notify_fn=sent.append), 0, st)
        self.assertEqual(len(sent), 1)
        self._clip_for(ev, "HELD")
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 1)
        self.assertIn("recovered", sent[1])
        # failure again: clip fails after recovery
        self._clip_for(ev, "FAILED")
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 1)
        self.assertIn("failed editing", sent[2])

    def test_false_truncated_flag_is_not_scanned_or_announced(self):
        from app.bot import ops
        state = self._prep_state()
        ev = self._mark(103.0, 1000.0)
        self._status(ev, "CLIPPED")
        conn = connect(self.db)
        conn.execute("UPDATE candidate_events SET metadata_json=? WHERE id=?",
                     ('{"post_window_truncated": false}', ev))
        conn.commit()
        conn.close()
        sent = []
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 0)
        conn = connect(self.db)
        conn.execute("UPDATE candidate_events SET metadata_json=? WHERE id=?",
                     ('{"post_window_truncated": true}', ev))
        conn.commit()
        conn.close()
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 1)

    def test_unsent_failure_is_dropped_when_mark_recovers_first(self):
        from app.bot import ops
        state = self._prep_state()
        ev = self._mark(103.0, 1000.0)
        self._status(ev, "FAILED")

        def boom(_t):
            raise RuntimeError("net")
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=boom), 0)
        self._status(ev, "CLIPPED")
        self._clip_for(ev, "HELD")
        sent = []
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 0)
        self.assertEqual(sent, [])

    def test_crash_between_send_and_record_repeats_one_message(self):
        """At-least-once: documented duplicate window."""
        from app.bot import ops
        state = self._prep_state()
        ev = self._mark(103.0, 1000.0)
        self._status(ev, "MARK_UNCOVERED")
        sent = []
        n1 = ops.notify_manual_mark_outcomes(state, notify_fn=sent.append)
        conn = connect(self.db)
        conn.execute("DELETE FROM mark_notices")   # simulate lost record
        conn.commit()
        conn.close()
        n2 = ops.notify_manual_mark_outcomes(state, notify_fn=sent.append)
        self.assertEqual((n1, n2, len(sent)), (1, 1, 2))

    def test_old_unresolved_notice_not_starved_by_newer_marks(self):
        from app.bot import ops
        state = self._prep_state()
        old = self._mark(103.0, 1000.0)
        self._status(old, "MARK_UNCOVERED")
        conn = connect(self.db)
        conn.execute("CREATE TABLE IF NOT EXISTS mark_notices ("
                     " event_id INTEGER NOT NULL, key TEXT NOT NULL,"
                     " sent_at REAL NOT NULL, PRIMARY KEY (event_id, key))")
        conn.commit()
        conn.close()
        newer = []
        for _ in range(130):
            ev = self._mark(103.0, 1000.0)
            self._status(ev, "EXPIRED")
            newer.append(ev)
        # newer ones are already announced; only the old one is pending
        conn = connect(self.db)
        for ev in newer:
            conn.execute("INSERT INTO mark_notices VALUES (?,?,1.0)",
                         (ev, "final:EXPIRED"))
        conn.commit()
        conn.close()
        sent = []
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 1)
        self.assertIn("never reached the moment", sent[0])

    def test_send_burst_is_bounded_and_drains_oldest_first(self):
        from app.bot import ops
        state = self._prep_state()
        ids = []
        for _ in range(ops.MARK_NOTICE_MAX_SENDS + 5):
            ev = self._mark(103.0, 1000.0)
            self._status(ev, "EXPIRED")
            ids.append(ev)
        sent = []
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append),
            ops.MARK_NOTICE_MAX_SENDS)
        self.assertIn(f"#{ids[0]} ", sent[0])
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 5)
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 0)

    def test_truncated_clip_is_announced(self):
        from app.bot import ops
        import shutil
        state = Path(self.td.name)
        ev = self._mark(103.0, 120.0)
        self._seg(1, 108.0)
        clipper, _ = self._clipper(121.0)
        self.assertEqual(clipper.process_pending(), 1)
        shutil.copy(self.db, state / "nova.db")
        sent = []
        self.assertEqual(
            ops.notify_manual_mark_outcomes(state, notify_fn=sent.append), 1)
        self.assertIn("cut short", sent[0])

    def test_quota_precheck_skips_extraction(self):
        fixed = 1764100800.0
        _d, start, _e = local_day_bounds(fixed)
        conn = connect(self.db)
        for _ in range(GENERATION_DAILY_CAP):
            conn.execute(
                "INSERT INTO clips (session_id, file_path, duration_seconds,"
                " status, created_at) VALUES (1,'/tmp/o.mp4',30,'HELD',?)",
                (start + 60,))
        conn.execute("DELETE FROM recording_segments")
        conn.execute("INSERT INTO recording_segments (session_id, file_path,"
                     " sequence, duration_seconds, created_at)"
                     " VALUES (1,'/tmp/s.ts',0,5,100000.0)")
        conn.commit()
        conn.close()
        ev = self._mark(103.0, 1e12)
        clipper, calls = self._clipper(fixed)
        self.assertEqual(clipper.process_pending(), 0)
        self.assertEqual(calls, [])             # extraction never started
        self.assertEqual(self._statuses()[0][ev], "QUOTA_EXCEEDED")

    def test_quota_scope_and_failed_counting_are_switchable(self):
        from app.clipping.quota import check_quota
        fixed = 1764100800.0
        _d, start, _e = local_day_bounds(fixed)
        conn = connect(self.db)
        conn.execute("INSERT INTO twitch_sources (id, username, created_at,"
                     " updated_at) VALUES (2, 'other', 0, 0)")
        conn.execute("INSERT INTO stream_sessions (id, source_id, started_at,"
                     " state) VALUES (2, 2, 0, 'RECORDING')")
        for sid, st in ((1, "HELD"), (1, "FAILED"), (2, "HELD"),
                        (2, "QC_FAILED")):
            conn.execute(
                "INSERT INTO clips (session_id, file_path, duration_seconds,"
                " status, created_at) VALUES (?,'/tmp/o.mp4',30,?,?)",
                (sid, st, start + 60))
        conn.commit()
        c = lambda **k: check_quota(conn, 1, fixed, cap=100, **k)["count"]
        self.assertEqual(c(scope="source_day", count_failed=True), 2)
        self.assertEqual(c(scope="source_day", count_failed=False), 1)
        self.assertEqual(c(scope="global_day", count_failed=True), 4)
        self.assertEqual(c(scope="global_day", count_failed=False), 2)
        self.assertEqual(c(scope="session", count_failed=True), 2)
        conn.close()

    def test_owner_defaults_global_day_and_failed_do_not_count(self):
        from app.clipping import quota
        self.assertEqual(quota.GENERATION_CAP_SCOPE, "global_day")
        self.assertFalse(quota.GENERATION_COUNT_FAILED)
        fixed = 1764100800.0
        _d, start, _e = local_day_bounds(fixed)
        conn = connect(self.db)
        conn.execute("INSERT INTO twitch_sources (id, username, created_at,"
                     " updated_at) VALUES (2, 'other', 0, 0)")
        conn.execute("INSERT INTO stream_sessions (id, source_id, started_at,"
                     " state) VALUES (2, 2, 0, 'RECORDING')")
        # 8 total across two sources: 4 + 4, plus 3 failed ones
        for sid, st in [(1, "HELD")] * 4 + [(2, "HELD")] * 2 + [
                (2, "PENDING"), (2, "RECOVERING")] + [
                (1, "FAILED"), (2, "QC_FAILED"), (1, "QC_FAILED")]:
            conn.execute(
                "INSERT INTO clips (session_id, file_path, duration_seconds,"
                " status, created_at) VALUES (?,'/tmp/o.mp4',30,?,?)",
                (sid, st, start + 60))
        conn.commit()
        r = quota.check_quota(conn, 1, fixed)
        self.assertEqual((r["count"], r["allowed"], r["scope"]),
                         (8, False, "global_day"))
        # a failure releases the slot of an in-flight clip
        conn.execute("UPDATE clips SET status='QC_FAILED'"
                     " WHERE status='PENDING'")
        conn.commit()
        r = quota.check_quota(conn, 1, fixed)
        self.assertEqual((r["count"], r["allowed"]), (7, True))
        conn.close()

    def test_concurrent_reservations_never_exceed_global_cap(self):
        import threading
        from app.clipping.quota import check_quota
        fixed = 1764100800.0
        _d, start, _e = local_day_bounds(fixed)
        conn = connect(self.db)
        conn.execute("INSERT INTO twitch_sources (id, username, created_at,"
                     " updated_at) VALUES (2, 'other', 0, 0)")
        conn.execute("INSERT INTO stream_sessions (id, source_id, started_at,"
                     " state) VALUES (2, 2, 0, 'RECORDING')")
        conn.commit()
        conn.close()
        won = []
        gate = threading.Barrier(16)

        def worker(i):
            c = connect(self.db)
            try:
                gate.wait()
                c.execute("BEGIN IMMEDIATE")
                q = check_quota(c, 1 + (i % 2), fixed)
                if q["allowed"]:
                    c.execute(
                        "INSERT INTO clips (session_id, file_path,"
                        " duration_seconds, status, created_at)"
                        " VALUES (?,'/tmp/o.mp4',30,'PENDING',?)",
                        (1 + (i % 2), start + 60))
                    won.append(i)
                c.commit()
            finally:
                c.close()
        ts = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(len(won), GENERATION_DAILY_CAP)
        c = connect(self.db)
        n = c.execute("SELECT count(*) FROM clips").fetchone()[0]
        c.close()
        self.assertEqual(n, GENERATION_DAILY_CAP)

    def test_manual_mark_after_clipped_heuristic_is_merged_not_duplicated(self):
        conn = connect(self.db)
        conn.execute("DELETE FROM recording_segments")
        conn.execute("INSERT INTO recording_segments (session_id, file_path,"
                     " sequence, duration_seconds, created_at)"
                     " VALUES (1,'/tmp/s.ts',0,5,100000.0)")
        conn.execute("INSERT INTO candidate_events (session_id, detected_at,"
                     " kind, confidence, status) VALUES"
                     " (1, 103.0, 'audio_spike', 0.9, 'CLIPPED')")
        conn.commit()
        conn.close()
        ev = self._mark(104.0, 1e12)
        clipper, calls = self._clipper(1764100800.0)
        self.assertEqual(clipper.process_pending(), 0)
        self.assertEqual(calls, [])
        self.assertEqual(self._statuses()[0][ev], "MERGED")


class ManualMarkRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.dir = Path(self.td.name)
        self.db = str(self.dir / "nova.db")
        conn = connect(self.db)
        migrate(conn)
        conn.execute("INSERT INTO twitch_sources (id, username, created_at,"
                     " updated_at) VALUES (1, 'chan', 0, 0)")
        conn.execute("INSERT INTO stream_sessions (id, source_id, started_at,"
                     " state, ingest_mode) VALUES (1, 1, 0, 'INTERRUPTED',"
                     " 'playback')")
        conn.execute("INSERT INTO candidate_events"
                     " (session_id, detected_at, kind, confidence)"
                     " VALUES (1, 100.0, ?, NULL)", (MANUAL_MARK_KIND,))
        conn.execute("INSERT INTO settings (key, value) VALUES"
                     " ('srcauth:chan:record', '1')")
        conn.commit()
        conn.close()

    def tearDown(self):
        self.td.cleanup()

    def test_interrupted_manual_mark_reaches_recovery_clipper(self):
        from app.pipeline import session as ps

        class FakeClipper:
            def __init__(self, buf, db_path, pre_spawn, on_spawn):
                self.buf = buf
                self.db_path = db_path

            def process_pending(self):
                conn = connect(self.db_path)
                conn.execute("UPDATE candidate_events SET status='CLIPPED'"
                             " WHERE id=1")
                conn.commit()
                conn.close()
                return 1

        def factory(buf, db_path, pre_spawn, on_spawn):
            return FakeClipper(buf, db_path, pre_spawn, on_spawn)

        with mock.patch("app.bot.restartproof.proof_for_session",
                        return_value=(True, "dead")):
            produced = ps.extract_pending_manual_marks(
                self.dir, "ffmpeg", "ffprobe", clipper_factory=factory)
        self.assertEqual(produced, 1)
        conn = connect(self.db)
        status = conn.execute("SELECT status FROM candidate_events"
                              " WHERE id=1").fetchone()[0]
        conn.close()
        self.assertEqual(status, "CLIPPED")

    def test_recovery_uses_same_generation_quota(self):
        from app.pipeline import session as ps
        fixed = 1764100800.0
        day, start, end = local_day_bounds(fixed)
        conn = connect(self.db)
        for _ in range(GENERATION_DAILY_CAP):
            conn.execute(
                "INSERT INTO clips (session_id, file_path, duration_seconds,"
                " status, created_at) VALUES (1, '/tmp/old.mp4', 30.0,"
                " 'HELD', ?)", (start + 60,))
        conn.execute("INSERT INTO recording_segments (session_id,"
                     " file_path, sequence, duration_seconds, created_at)"
                     " VALUES (1, '/tmp/s0.ts', 0, 5.0, 100000.0)")
        conn.commit()
        conn.close()

        def factory(buf, db_path, pre_spawn, on_spawn):
            return AutoClipper(
                buf, db_path, HeuristicProvider(), "ffmpeg",
                self.td.name, extract_fn=_extract_ok,
                event_kinds=(MANUAL_MARK_KIND,), clock=lambda: fixed)

        with mock.patch("app.bot.restartproof.proof_for_session",
                        return_value=(True, "dead")):
            produced = ps.extract_pending_manual_marks(
                self.dir, "ffmpeg", "ffprobe", clipper_factory=factory)
        self.assertEqual(produced, 0)
        conn = connect(self.db)
        status = conn.execute("SELECT status FROM candidate_events"
                              " WHERE id=1").fetchone()[0]
        clips = conn.execute("SELECT count(*) FROM clips").fetchone()[0]
        conn.close()
        self.assertEqual(status, "QUOTA_EXCEEDED")
        self.assertEqual(clips, GENERATION_DAILY_CAP)

    def test_synchronous_recovery_reports_manual_clip_failure(self):
        raw_dir = self.dir / "sessions" / "chan"
        raw_dir.mkdir(parents=True)
        raw = raw_dir / "manual-raw.ts"
        raw.write_bytes(b"\x47" * 4096)
        seal_sha, seal_bytes = seal_file(raw)

        def factory(buf, db_path, pre_spawn, on_spawn):
            class Clipper:
                def process_pending(self):
                    conn = connect(db_path)
                    conn.execute(
                        "INSERT INTO clips (event_id, session_id, file_path,"
                        " duration_seconds, status, created_at,"
                        " sealed_sha256, sealed_bytes, sealed_at)"
                        " VALUES (1, 1, ?, 30.0, 'PENDING', 0, ?, ?, 1)",
                        (str(raw), seal_sha, seal_bytes))
                    conn.execute("UPDATE candidate_events SET status='CLIPPED'"
                                 " WHERE id=1")
                    conn.commit()
                    conn.close()
                    return 1
            return Clipper()

        with mock.patch("app.bot.restartproof.proof_for_session",
                        return_value=(True, "dead")), mock.patch(
                            "app.editing.editor.edit_clip",
                            side_effect=OSError("injected edit failure")):
            reply = sessions.recoverclips_reply(
                self.dir, "ffmpeg", "ffprobe",
                manual_clipper_factory=factory)
        self.assertIn("could not be finished", reply)
        self.assertNotIn("nothing to do", reply.lower())
        conn = connect(self.db)
        status = conn.execute("SELECT status FROM clips").fetchone()[0]
        conn.close()
        self.assertEqual(status, "FAILED")

    def test_interrupted_manual_mark_stays_fenced_without_death_proof(self):
        from app.pipeline import session as ps
        with mock.patch("app.bot.restartproof.proof_for_session",
                        return_value=(False, "writer alive")):
            produced = ps.extract_pending_manual_marks(
                self.dir, "ffmpeg", "ffprobe",
                clipper_factory=lambda *args: self.fail("clipper spawned"))
        self.assertEqual(produced, 0)
        conn = connect(self.db)
        status = conn.execute("SELECT status FROM candidate_events"
                              " WHERE id=1").fetchone()[0]
        conn.close()
        self.assertEqual(status, "PENDING")


if __name__ == "__main__":
    unittest.main()
