"""Automatic delivery ergonomics: QC-passed session clips go to the
paired admin without a manual /deliveries flush, under the same
per-clip journaled durability as the manual path."""
import pathlib
import tempfile
import unittest

from app.bot import ops
from app.core.database import connect, migrate


def _db_with_held(d, n=2, status="HELD"):
    conn = connect(str(d / "nova.db"))
    migrate(conn)
    ids = []
    for i in range(n):
        cur = conn.execute(
            "INSERT INTO clips (file_path, preset, duration_seconds,"
            " status, created_at) VALUES (?,?,?,?,0)",
            (f"/x{i}.mp4", "vertical", 5.0, status))
        ids.append(cur.lastrowid)
    conn.commit()
    conn.close()
    return ids


def _status(d, clip_id):
    conn = connect(str(d / "nova.db"))
    row = conn.execute("SELECT status FROM clips WHERE id=?",
                       (clip_id,)).fetchone()
    conn.close()
    return row[0]


class PreferenceTests(unittest.TestCase):
    def test_default_is_on(self):
        d = pathlib.Path(tempfile.mkdtemp())
        self.assertTrue(ops.auto_deliver_enabled(d))

    def test_hold_toggle_disables_and_persists(self):
        d = pathlib.Path(tempfile.mkdtemp())
        out = ops.deliveries_reply(d, "hold")
        self.assertIn("OFF", out)
        self.assertFalse(ops.auto_deliver_enabled(d))
        # survives a fresh process reading the file
        self.assertFalse(ops.auto_deliver_enabled(d))
        out = ops.deliveries_reply(d, "auto")
        self.assertIn("ON", out)
        self.assertTrue(ops.auto_deliver_enabled(d))

    def test_listing_reports_auto_state(self):
        d = pathlib.Path(tempfile.mkdtemp())
        _db_with_held(d, 1)
        self.assertIn("Auto-delivery: on", ops.deliveries_reply(d, ""))
        ops.deliveries_reply(d, "hold")
        self.assertIn("Auto-delivery: off", ops.deliveries_reply(d, ""))


class DeliverOneClipTests(unittest.TestCase):
    def test_held_clip_delivered_and_journaled(self):
        d = pathlib.Path(tempfile.mkdtemp())
        (cid,) = _db_with_held(d, 1)
        calls = []
        result = ops.deliver_one_clip(
            d, cid, deliver_fn=lambda p, c: calls.append((p, c)) or 42)
        self.assertEqual(result, "delivered")
        self.assertEqual(_status(d, cid), "DELIVERED")
        self.assertEqual(ops.journal_result(d, cid, "deliver"),
                         "message 42")
        self.assertEqual(calls[0][0], "/x0.mp4")

    def test_failed_send_uncertain_and_never_resent(self):
        d = pathlib.Path(tempfile.mkdtemp())
        (cid,) = _db_with_held(d, 1)
        def boom(p, c):
            raise RuntimeError("network died mid-send")
        self.assertEqual(ops.deliver_one_clip(d, cid, deliver_fn=boom),
                         "uncertain")
        self.assertEqual(_status(d, cid), "HELD")
        # second attempt does not resend
        calls = []
        result = ops.deliver_one_clip(
            d, cid, deliver_fn=lambda p, c: calls.append(p) or 7)
        self.assertEqual(result, "skipped_uncertain")
        self.assertEqual(calls, [])
        self.assertEqual(_status(d, cid), "HELD")

    def test_journaled_send_reconciles_without_resend(self):
        d = pathlib.Path(tempfile.mkdtemp())
        (cid,) = _db_with_held(d, 1)
        ops.journal_record(d, cid, "deliver", "message 99")
        calls = []
        result = ops.deliver_one_clip(
            d, cid, deliver_fn=lambda p, c: calls.append(p) or 1)
        self.assertEqual(result, "reconciled")
        self.assertEqual(calls, [])
        self.assertEqual(_status(d, cid), "DELIVERED")

    def test_non_held_clip_untouched(self):
        d = pathlib.Path(tempfile.mkdtemp())
        (cid,) = _db_with_held(d, 1, status="DELIVERED")
        calls = []
        result = ops.deliver_one_clip(
            d, cid, deliver_fn=lambda p, c: calls.append(p) or 1)
        self.assertEqual(result, "not_held")
        self.assertEqual(calls, [])

    def test_missing_clip(self):
        d = pathlib.Path(tempfile.mkdtemp())
        _db_with_held(d, 1)
        self.assertEqual(ops.deliver_one_clip(d, 999,
                                              deliver_fn=lambda p, c: 1),
                         "missing")


class AutoDeliverClipsTests(unittest.TestCase):
    def test_delivers_all_when_enabled(self):
        d = pathlib.Path(tempfile.mkdtemp())
        ids = _db_with_held(d, 3)
        sent = []
        out = ops.auto_deliver_clips(
            d, ids, deliver_fn=lambda p, c: sent.append(p) or len(sent))
        self.assertEqual(out, {"enabled": True, "delivered": 3,
                               "uncertain": 0})
        self.assertEqual(len(sent), 3)
        for cid in ids:
            self.assertEqual(_status(d, cid), "DELIVERED")

    def test_no_sends_when_held_off(self):
        d = pathlib.Path(tempfile.mkdtemp())
        ids = _db_with_held(d, 2)
        ops.set_auto_deliver(d, False)
        calls = []
        out = ops.auto_deliver_clips(
            d, ids, deliver_fn=lambda p, c: calls.append(p) or 1)
        self.assertEqual(out["enabled"], False)
        self.assertEqual(calls, [])
        for cid in ids:
            self.assertEqual(_status(d, cid), "HELD")

    def test_one_failure_does_not_stop_the_rest(self):
        d = pathlib.Path(tempfile.mkdtemp())
        ids = _db_with_held(d, 3)
        sent = []
        calls = [0]

        def flaky(p, c):
            calls[0] += 1
            if calls[0] == 2:
                raise RuntimeError("mid-send failure")
            sent.append(p)
            return len(sent)

        out = ops.auto_deliver_clips(d, ids, deliver_fn=flaky)
        self.assertEqual(out["delivered"], 2)
        self.assertEqual(out["uncertain"], 1)
        self.assertEqual(_status(d, ids[1]), "HELD")
        self.assertEqual(_status(d, ids[2]), "DELIVERED")

    def test_setup_error_before_send_is_not_marked_uncertain(self):
        # no deliver_fn and no token env: the failure happens while
        # CONSTRUCTING the sender, before anything could have been
        # sent, so the clip must stay plainly HELD, never uncertain
        import os
        d = pathlib.Path(tempfile.mkdtemp())
        (cid,) = _db_with_held(d, 1)
        saved = os.environ.pop("NOVA_TOKEN_FILE", None)
        try:
            # PTB installed: KeyError on the token env; not installed:
            # ImportError constructing the sender - both pre-send
            with self.assertRaises((KeyError, ImportError)):
                ops.deliver_one_clip(d, cid)
            self.assertEqual(_status(d, cid), "HELD")
            self.assertEqual(
                [e for e in ops.journal_uncertain(d)
                 if e.get("command") == "deliver"], [])
            # and auto_deliver_clips absorbs the setup error per clip
            out = ops.auto_deliver_clips(d, [cid])
            self.assertEqual(out, {"enabled": True, "delivered": 0,
                                   "uncertain": 0})
            self.assertEqual(_status(d, cid), "HELD")
        finally:
            if saved is not None:
                os.environ["NOVA_TOKEN_FILE"] = saved


class FlushRefactorTests(unittest.TestCase):
    """The manual flush keeps its exact reply contract after the
    deliver_one_clip refactor."""

    def test_flush_reply_counts(self):
        d = pathlib.Path(tempfile.mkdtemp())
        _db_with_held(d, 2)
        out = ops.deliveries_reply(d, "flush",
                                   deliver_fn=lambda p, c: 5)
        self.assertIn("Delivered 2", out)

    def test_flush_failed_send_reported_uncertain(self):
        d = pathlib.Path(tempfile.mkdtemp())
        _db_with_held(d, 1)

        def boom(p, c):
            raise RuntimeError("network died mid-send")
        out = ops.deliveries_reply(d, "flush", deliver_fn=boom)
        self.assertIn("uncertain", out.lower())


if __name__ == "__main__":
    unittest.main()
