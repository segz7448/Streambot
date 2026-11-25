"""Durable poll-error record: class names + timestamps survive process loss.

The in-memory PollWatch counters die with the process; the appended file is
the only place a post-mortem can distinguish e.g. a getUpdates Conflict
(duplicate poller) from network timeouts. Contract: content-free lines
(iso/ts/kind only), private perms, bounded rotation, nonfatal failures.
"""
import json
import os
import unittest
from tempfile import TemporaryDirectory
from unittest import mock

from app.bot import pollwatch
from app.bot.pollwatch import PollWatch


class DurablePollErrorTests(unittest.TestCase):
    def setUp(self):
        self.td = TemporaryDirectory()
        self.addCleanup(self.td.cleanup)
        self.path = os.path.join(self.td.name, "poll-errors.jsonl")
        self.t = [2000.0]
        self.w = PollWatch(clock=lambda: self.t[0],
                           error_log_path=self.path)

    def _lines(self, path=None):
        with open(path or self.path, encoding="utf-8") as fh:
            return [json.loads(x) for x in fh if x.strip()]

    def test_error_appends_content_free_line(self):
        self.w.note_poll_error("Conflict")
        rows = self._lines()
        self.assertEqual(len(rows), 1)
        self.assertEqual(set(rows[0]), {"iso", "ts", "kind"})
        self.assertEqual(rows[0]["kind"], "Conflict")
        self.assertEqual(rows[0]["ts"], 2000.0)
        from app.core import diagnostics
        self.assertEqual(rows[0]["iso"], diagnostics._iso(2000.0))

    def test_ok_polls_write_nothing(self):
        self.w.note_poll_ok(3)
        self.assertFalse(os.path.exists(self.path))

    def test_multiple_errors_append_in_order(self):
        self.w.note_poll_error("TimedOut")
        self.t[0] += 7
        self.w.note_poll_error("Conflict")
        rows = self._lines()
        self.assertEqual([r["kind"] for r in rows], ["TimedOut", "Conflict"])
        self.assertEqual(rows[1]["ts"], 2007.0)
        # in-memory snapshot still authoritative for counts
        self.assertEqual(self.w.snapshot()["error_count"], 2)

    def test_kind_truncated_and_stringified(self):
        self.w.note_poll_error("X" * 200)
        self.assertEqual(len(self._lines()[0]["kind"]), 64)

    def test_rotation_keeps_one_generation(self):
        with mock.patch.object(pollwatch, "_ERROR_LOG_CAP_BYTES", 120):
            for i in range(20):
                self.w.note_poll_error("E%d" % i)
        self.assertTrue(os.path.exists(self.path + ".1"))
        self.assertLessEqual(os.path.getsize(self.path),
                             os.path.getsize(self.path + ".1") + 200)
        # newest evidence still being appended after rotation
        kinds = [r["kind"] for r in self._lines()]
        self.assertIn("E19", kinds)

    def test_write_failure_is_nonfatal(self):
        with mock.patch.object(pollwatch.diagnostics, "_open_private_append",
                               side_effect=OSError("disk full")):
            self.w.note_poll_error("TimedOut")  # must not raise
        snap = self.w.snapshot()
        self.assertEqual(snap["error_count"], 1)
        self.assertEqual(snap["last_error_kind"], "TimedOut")

    def test_write_failure_warnings_rate_limited(self):
        # persistent disk failure: 50 errors -> 1 warning per 5 minutes,
        # while in-memory counts keep accumulating
        with mock.patch.object(pollwatch.diagnostics, "_open_private_append",
                               side_effect=OSError("disk full")), \
                mock.patch.object(pollwatch.log, "warning") as warn:
            for _ in range(50):
                self.w.note_poll_error("TimedOut")
            self.assertEqual(warn.call_count, 1)
            self.t[0] += 299
            self.w.note_poll_error("TimedOut")
            self.assertEqual(warn.call_count, 1)
            self.t[0] += 1
            self.w.note_poll_error("TimedOut")
            self.assertEqual(warn.call_count, 2)
        self.assertEqual(self.w.snapshot()["error_count"], 52)

    def test_file_is_private(self):
        self.w.note_poll_error("TimedOut")
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_no_path_keeps_memory_only_behavior(self):
        w = PollWatch(clock=lambda: 1.0)
        w.note_poll_error("TimedOut")
        self.assertEqual(w.snapshot()["error_count"], 1)


if __name__ == "__main__":
    unittest.main()
