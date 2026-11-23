"""Segment timeline truth: keyframe-cut segments are not 5.0s long, and
the timeline must be IMMUTABLE once observed.

Session 8 (agent00, deployed 040ef82): recording_segments rows stored
duration_seconds=5.0 with created_at spaced exactly 5.000s apart, while
ffprobe of the retained media measured 6+2+4+4=16s. Reviewer repros on
the first fix attempt: rotation rebased retained segments' created_at
(anchor recomputed from the earliest RETAINED mtime), and a growing
file's mtime shifted the in-memory timeline while the DB rows stayed
put.

Fix under test: the timeline (anchor + actual durations) is adopted
once, persisted in .timeline.json, and only CLOSED segments (a higher
sequence exists, or drain attested) are probed/persisted; the open tail
carries nominal timing and is never probed. Nothing rebases: growth,
rotation and restart leave recorded event times pointing at the same
content.
"""
import os
import tempfile
import time
import unittest
import unittest.mock
from pathlib import Path

from app.recording.buffer import RollingBuffer, TimelineError
from app.clipping.extractor import plan_clip


def _files(buf, count, start=0):
    buf.dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i in range(start, start + count):
        p = buf.dir / f"seg-{i:06d}.ts"
        p.write_bytes(b"x" * 1000)
        paths.append(p)
    return paths


class SegmentTimelineTests(unittest.TestCase):
    def _buf(self, tmp, durations):
        buf = RollingBuffer(tmp, session_id=1, segment_seconds=5,
                            pre_event_seconds=30, post_event_seconds=30)
        buf.probe = lambda p: durations[int(p.stem.split("-")[1])]
        return buf

    def test_closed_probed_open_tail_nominal_until_drained(self):
        with tempfile.TemporaryDirectory() as tmp:
            durations = [6.0, 4.0, 6.0, 4.0]
            buf = self._buf(tmp, durations)
            paths = _files(buf, 4)
            m0 = time.time() - 100
            os.utime(paths[0], (m0, m0))
            segs = buf.segments()
            # seg0-2 closed (higher seq exists): actual durations;
            # seg3 open: nominal, never probed
            self.assertEqual([s.duration_seconds for s in segs],
                             [6.0, 4.0, 6.0, 5.0])
            self.assertEqual([s.created_at for s in segs],
                             [m0, m0 + 4.0, m0 + 10.0, m0 + 15.0])
            buf.mark_drained()       # tail closes: true duration persists
            segs = buf.segments()
            self.assertEqual([s.duration_seconds for s in segs], durations)
            self.assertEqual([s.created_at for s in segs],
                             [m0, m0 + 4.0, m0 + 10.0, m0 + 14.0])

    def test_rotation_does_not_rebase(self):
        # reviewer repro: 6/2/4/4, compressed mtimes; deleting seg0 moved
        # retained seq2 from 106 to 104.01 on the held patch
        with tempfile.TemporaryDirectory() as tmp:
            buf = self._buf(tmp, [6.0, 2.0, 4.0, 4.0])
            paths = _files(buf, 4)
            for i, p in enumerate(paths):
                os.utime(p, (100 + i * 0.01, 100 + i * 0.01))
            before = {s.sequence: s.created_at for s in buf.segments()}
            paths[0].unlink()
            after = {s.sequence: s.created_at for s in buf.segments()}
            for seq in after:
                self.assertEqual(after[seq], before[seq],
                                 f"rotation rebased seq{seq}")
            self.assertEqual(after[2], 106.0)

    def test_growth_does_not_shift_timeline_or_db(self):
        # reviewer repro: closed seg0's mtime moved 100 -> 105 while its
        # recording_segments row stayed; memory must agree with the DB
        with tempfile.TemporaryDirectory() as tmp:
            from app.core.database import connect, migrate
            buf = self._buf(tmp, [6.0, 2.0])
            paths = _files(buf, 2)
            for i, p in enumerate(paths):
                os.utime(p, (100 + i, 100 + i))
            db = str(Path(tmp) / "t.db")
            conn = connect(db)
            migrate(conn)
            conn.execute("INSERT INTO twitch_sources"
                         " (id, username, created_at, updated_at)"
                         " VALUES (1, 'fixture', 0, 0)")
            conn.execute("INSERT INTO stream_sessions"
                         " (id, source_id, state)"
                         " VALUES (1, 1, 'RECORDING')")
            conn.commit()
            conn.close()
            buf.reconcile(db)   # only CLOSED seg0 gets a row
            before = [(s.sequence, s.created_at, s.duration_seconds)
                      for s in buf.segments()]
            paths[0].write_bytes(b"xx")   # anomalous growth of a closed
            os.utime(paths[0], (105, 105))  # segment: timeline must not move
            buf.reconcile(db)
            conn = connect(db)
            stored = [tuple(r) for r in conn.execute(
                "SELECT sequence, created_at, duration_seconds"
                " FROM recording_segments ORDER BY sequence")]
            conn.close()
            after = [(s.sequence, s.created_at, s.duration_seconds)
                     for s in buf.segments()]
            self.assertEqual(after, before)
            self.assertEqual(stored, [(0, 100.0, 6.0)])

    def test_timeline_survives_restart_and_rotation(self):
        with tempfile.TemporaryDirectory() as tmp:
            durations = [6.0, 4.0, 6.0]
            buf = self._buf(tmp, durations)
            paths = _files(buf, 3)
            m0 = time.time() - 100
            os.utime(paths[0], (m0, m0))
            before = [(s.sequence, s.created_at, s.duration_seconds)
                      for s in buf.segments()]
            paths[0].unlink()
            fresh = self._buf(tmp, durations)   # "restarted" process
            after = [(s.sequence, s.created_at, s.duration_seconds)
                     for s in fresh.segments()]
            self.assertEqual(after, before[1:])

    def test_sparse_sequence_gap_is_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            durations = {0: 6.0, 100: 3.0}
            buf = self._buf(tmp, 0)
            buf.probe = lambda p: durations[int(p.stem.split("-")[1])]
            _files(buf, 1, start=0)
            _files(buf, 2, start=100)
            m0 = time.time() - 100
            os.utime(buf.dir / "seg-000000.ts", (m0, m0))
            segs = {s.sequence: s for s in buf.segments()}
            # 99 rotated-away segments contribute nominal, summed in
            # O(#known), never an actual range walk over the gap
            self.assertEqual(segs[100].created_at, m0 + 99 * 5.0 + 3.0)
            self.assertEqual(segs[100].duration_seconds, 3.0)

    def test_only_open_tail_stays_provisional_and_unpersisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            buf = self._buf(tmp, [6.0, 4.0])
            paths = _files(buf, 1)
            os.utime(paths[0], (500.0, 500.0))
            segs = buf.segments()
            self.assertEqual(segs[0].created_at, 500.0)   # provisional mtime
            self.assertEqual(segs[0].duration_seconds, 5.0)
            self.assertFalse((buf.dir / ".timeline.json").exists())
            _files(buf, 1, start=1)     # seg0 closes: adopt final anchor
            segs = buf.segments()
            self.assertTrue((buf.dir / ".timeline.json").exists())
            self.assertEqual(segs[0].duration_seconds, 6.0)
            self.assertEqual(segs[0].created_at, 500.0)
            self.assertEqual(segs[1].created_at, 500.0 + 5.0)  # open tail

    def test_event_window_selects_by_interval_overlap(self):
        with tempfile.TemporaryDirectory() as tmp:
            buf = self._buf(tmp, [6.0, 4.0, 6.0, 4.0])
            paths = _files(buf, 4)
            m0 = time.time() - 100
            os.utime(paths[0], (m0, m0))
            buf.mark_drained()
            # content: seg0 [m0-6,m0] seg1 [m0,m0+4] seg2 [m0+4,m0+10]
            # seg3 [m0+10,m0+14]; window [m0-0.5, m0+9.5] (event+/-5):
            # seg2's END (m0+10) is OUTSIDE the window but its interval
            # overlaps - point-in-window membership would drop it
            buf.pre_event_segments = 1   # window = event +/- 5s
            buf.post_event_segments = 1
            window = buf.event_window(m0 + 4.5)   # [m0-0.5, m0+9.5]
            self.assertIn(paths[0], window)       # overlaps [m0-0.5, m0]
            self.assertIn(paths[1], window)
            self.assertIn(paths[2], window)       # end outside, overlaps
            self.assertNotIn(paths[3], window)    # starts at m0+10

    def test_plan_clip_offsets_follow_actual_first_duration(self):
        with tempfile.TemporaryDirectory() as tmp:
            durations = [6.0, 4.0, 6.0, 4.0]
            buf = self._buf(tmp, durations)
            paths = _files(buf, 4)
            m0 = time.time() - 100
            os.utime(paths[0], (m0, m0))
            buf.mark_drained()
            event = m0 + 5.0          # 1s into seg1's content (m0+4..m0+10)
            plan = plan_clip(buf, event, pre_seconds=10.0, post_seconds=5.0)
            self.assertIsNotNone(plan)
            # window starts at seg0's content start m0-6 (actual), so the
            # wanted start m0-5 lands 1.0s into the concat, not 0.0
            self.assertAlmostEqual(plan.start_offset, 1.0, places=3)
            # event+post=m0+10 caps before window_end=m0+14:
            # duration = (m0+10) - (m0-6+1) = 15
            self.assertAlmostEqual(plan.duration, 15.0, places=3)


if __name__ == "__main__":
    unittest.main()


class SidecarFailClosedTests(unittest.TestCase):
    """Persisted-state failure modes never become reinterpreted timing:
    corrupt sidecar != absent, an unpersistable new timeline is never
    exposed, and non-finite/out-of-range values are refused."""

    def _buf(self, tmp, durations=(6.0, 2.0, 4.0, 4.0)):
        buf = RollingBuffer(tmp, session_id=1, segment_seconds=5,
                            pre_event_seconds=30, post_event_seconds=30)
        buf.probe = lambda p: durations[int(p.stem.split("-")[1])]
        paths = _files(buf, len(durations))
        for i, p in enumerate(paths):
            os.utime(p, (100 + i * 0.01, 100 + i * 0.01))
        return buf, paths

    def test_corrupt_sidecar_never_reanchors(self):
        with tempfile.TemporaryDirectory() as tmp:
            buf, paths = self._buf(tmp)
            before = {s.sequence: s.created_at for s in buf.segments()}
            paths[0].unlink()
            (buf.dir / ".timeline.json").write_text("broken")
            after = {s.sequence: s.created_at for s in buf.segments()}
            self.assertEqual(after[2], before[2])

    def test_corrupt_sidecar_fresh_process_exposes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            buf, _ = self._buf(tmp)
            buf.segments()                     # adopt + persist
            (buf.dir / ".timeline.json").write_text("broken")
            fresh = RollingBuffer(tmp, session_id=1, segment_seconds=5)
            fresh.probe = buf.probe
            # no DB bound: no settled identity -> no usable segments,
            # and the corrupt file is not overwritten with a re-anchor
            self.assertEqual(fresh.segments(), [])
            self.assertEqual((fresh.dir / ".timeline.json").read_text(),
                             "broken")

    def test_failed_persist_exposes_no_new_timeline(self):
        with tempfile.TemporaryDirectory() as tmp:
            buf, _ = self._buf(tmp)
            with unittest.mock.patch(
                    "app.recording.buffer.os.replace",
                    side_effect=OSError):
                self.assertEqual(buf.segments(), [])
            self.assertFalse((buf.dir / ".timeline.json").exists())

    def test_nonfinite_and_out_of_range_sidecar_refused(self):
        import json as _json
        for bad in ({"anchor_seq": 0, "anchor_t": float("nan"),
                     "durations": {"0": 6}},
                    {"anchor_seq": 0, "anchor_t": -5,
                     "durations": {"0": 6}},
                    {"anchor_seq": 0, "anchor_t": 100.0,
                     "durations": {"0": float("inf")}},
                    {"anchor_seq": "0", "anchor_t": 100.0,
                     "durations": {"0": 6}},
                    ["not", "a", "dict"]):
            with tempfile.TemporaryDirectory() as tmp:
                buf, _ = self._buf(tmp)
                (buf.dir / ".timeline.json").write_text(_json.dumps(bad))
                segs = buf.segments()
                self.assertEqual(segs, [])

    def test_uncommitted_duration_withheld_in_fresh_instances(self):
        """A closed segment whose duration could not be durably committed
        is withheld - with its dependents - in the failing instance AND
        in any fresh instance while the failure persists; once the commit
        lands, every instance settles the same identity."""
        with tempfile.TemporaryDirectory() as tmp:
            buf, paths = self._buf(tmp)
            for p in paths[2:]:
                p.unlink()
            before = {s.sequence: (s.created_at, s.duration_seconds)
                      for s in buf.segments()}      # commits {0: 6.0}
            self.assertEqual(before[0], (100.0, 6.0))
            p2 = buf.dir / "seg-000002.ts"
            p2.write_bytes(b"x" * 1000)
            os.utime(p2, (102, 102))               # now seq1 closes
            with unittest.mock.patch.object(
                    buf, "_save_timeline",
                    side_effect=[TimelineError, None]):
                a = buf.segments()
            # seq1's duration resolved but NOT committed: seq1 and its
            # dependent tail seq2 are withheld entirely
            self.assertEqual([s.sequence for s in a], [0])
            fresh = RollingBuffer(tmp, session_id=1, segment_seconds=5)
            fresh.probe = buf.probe
            with unittest.mock.patch.object(
                    fresh, "_save_timeline",
                    side_effect=TimelineError):
                # a fresh instance under the same failure exposes the
                # same committed-only view: no divergence
                self.assertEqual([s.sequence for s in fresh.segments()],
                                 [0])
            settled = {s.sequence: (s.created_at, s.duration_seconds)
                       for s in fresh.segments()}
            self.assertEqual(settled[1], (102.0, 2.0))
            self.assertEqual(settled[2], (107.0, 5.0))
