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
                               "uncertain": 0, "unavailable": 0})
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
        # sent: classified "unavailable", clip plainly HELD, never
        # uncertain
        import os
        d = pathlib.Path(tempfile.mkdtemp())
        (cid,) = _db_with_held(d, 1)
        saved = os.environ.pop("NOVA_TOKEN_FILE", None)
        try:
            self.assertEqual(ops.deliver_one_clip(d, cid),
                             "unavailable")
            self.assertEqual(_status(d, cid), "HELD")
            self.assertEqual(
                [e for e in ops.journal_uncertain(d)
                 if e.get("command") == "deliver"], [])
            # and auto_deliver_clips counts it separately
            out = ops.auto_deliver_clips(d, [cid])
            self.assertEqual(out, {"enabled": True, "delivered": 0,
                                   "uncertain": 0, "unavailable": 1})
            self.assertEqual(_status(d, cid), "HELD")
            # manual flush reports it cleanly instead of crashing
            text = ops.deliveries_reply(d, "flush")
            self.assertIn("delivery unavailable", text.lower())
            self.assertIn("Delivered 0", text)
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


class ModeCAutoDeliveryTests(unittest.TestCase):
    """Owner-upload (mode C) ingest delivers the clip it just made when
    the preference is on, through the same journaled path."""

    def setUp(self):
        from app.clipping import retrospective
        self.retro = retrospective
        self._orig_validate = retrospective.validate_recording
        self._orig_process = retrospective.process_recording
        retrospective.validate_recording = lambda *a, **k: {
            "duration": 5.0, "width": 640, "height": 360}

    def tearDown(self):
        self.retro.validate_recording = self._orig_validate
        self.retro.process_recording = self._orig_process

    def _ingest(self, d, deliver_fn):
        import asyncio
        import types
        from app.bot import media
        video = types.SimpleNamespace(file_id="f", mime_type="video/mp4",
                                      file_size=16)
        msg = types.SimpleNamespace(video=video, document=None)
        upd = types.SimpleNamespace(update_id=920001, message=msg)

        class _F:
            async def download_to_drive(self, dest):
                pathlib.Path(dest).write_bytes(b"fake-video-bytes")

        class _B:
            async def get_file(self, fid):
                return _F()

        return asyncio.run(media.ingest_media(upd, _B(), d,
                                              deliver_fn=deliver_fn))

    def _fake_process(self, d):
        def process(ff, fp, src, work, db_path, memory, source_label,
                    **kw):
            ids = _db_with_held(d, 1)
            return {"status": "held", "clip_id": ids[0],
                    "event_kind": "audio_spike", "event_confidence": 0.9,
                    "clip_duration_seconds": 5.0,
                    "plan": {"preset": "vertical", "title": "T",
                             "tags": [], "watermark": "",
                             "reasons": ["spike"]}}
        return process

    def test_upload_clip_delivered_when_preference_on(self):
        d = pathlib.Path(tempfile.mkdtemp())
        self.retro.process_recording = self._fake_process(d)
        sent = []
        out = self._ingest(d, lambda p, c: sent.append(p) or 11)
        self.assertEqual(len(sent), 1)
        self.assertIn("sent to you", out.lower())
        conn = connect(str(d / "nova.db"))
        status = conn.execute("SELECT status FROM clips").fetchone()[0]
        conn.close()
        self.assertEqual(status, "DELIVERED")
        # journaled per clip
        self.assertEqual(ops.journal_result(d, 1, "deliver"),
                         "message 11")

    def test_upload_clip_held_when_preference_off(self):
        d = pathlib.Path(tempfile.mkdtemp())
        ops.set_auto_deliver(d, False)
        self.retro.process_recording = self._fake_process(d)
        sent = []
        out = self._ingest(d, lambda p, c: sent.append(p) or 11)
        self.assertEqual(sent, [])
        self.assertIn("/deliveries flush", out)
        conn = connect(str(d / "nova.db"))
        status = conn.execute("SELECT status FROM clips").fetchone()[0]
        conn.close()
        self.assertEqual(status, "HELD")

    def test_upload_delivery_failure_reported_uncertain(self):
        d = pathlib.Path(tempfile.mkdtemp())
        self.retro.process_recording = self._fake_process(d)

        def boom(p, c):
            raise RuntimeError("network died mid-send")
        out = self._ingest(d, boom)
        self.assertIn("uncertain", out.lower())
        conn = connect(str(d / "nova.db"))
        status = conn.execute("SELECT status FROM clips").fetchone()[0]
        conn.close()
        self.assertEqual(status, "HELD")


class CrashWindowReconciliationTests(unittest.TestCase):
    """The reviewer-flagged window: a crash between delivery and the
    ingest journal must never reprocess or resend on replay. The ingest
    journal (with the clip binding) is now written BEFORE delivery;
    these tests pin the ordering and both post-crash states."""

    def _update(self, uid):
        import types
        video = types.SimpleNamespace(file_id="f", mime_type="video/mp4",
                                      file_size=16)
        msg = types.SimpleNamespace(video=video, document=None)
        return types.SimpleNamespace(update_id=uid, message=msg)

    class _F:
        async def download_to_drive(self, dest):
            pathlib.Path(dest).write_bytes(b"fake-video-bytes")

    class _B:
        async def get_file(self, fid):
            return CrashWindowReconciliationTests._F()

    def setUp(self):
        from app.clipping import retrospective
        self.retro = retrospective
        self._orig_validate = retrospective.validate_recording
        self._orig_process = retrospective.process_recording
        retrospective.validate_recording = lambda *a, **k: {
            "duration": 5.0, "width": 640, "height": 360}
        self.process_calls = []

    def tearDown(self):
        self.retro.validate_recording = self._orig_validate
        self.retro.process_recording = self._orig_process

    def _fake_process(self, d):
        def process(ff, fp, src, work, db_path, memory, source_label,
                    **kw):
            self.process_calls.append(src)
            ids = _db_with_held(d, 1)
            return {"status": "held", "clip_id": ids[0],
                    "event_kind": "audio_spike", "event_confidence": 0.9,
                    "clip_duration_seconds": 5.0,
                    "plan": {"preset": "vertical", "title": "T",
                             "tags": [], "watermark": "",
                             "reasons": ["spike"]}}
        return process

    def test_ingest_journal_committed_before_delivery(self):
        import asyncio
        from app.bot import media
        d = pathlib.Path(tempfile.mkdtemp())
        self.retro.process_recording = self._fake_process(d)
        seen = {}

        def deliver(p, c):
            # at the moment of send, the ingest journal must ALREADY
            # carry the clip binding
            seen["journal"] = ops.journal_result(d, 930001, "ingest")
            return 77

        asyncio.run(media.ingest_media(self._update(930001), self._B(),
                                       d, deliver_fn=deliver))
        self.assertIsNotNone(seen["journal"])
        self.assertIn("[clip:1]", seen["journal"])

    def test_crash_before_delivery_replay_delivers_once_no_reprocess(
            self):
        import asyncio
        from app.bot import media
        d = pathlib.Path(tempfile.mkdtemp())
        (cid,) = _db_with_held(d, 1)
        # state as if: processing committed, ingest journal written,
        # crash before the delivery attempt
        ops.journal_record(d, 930002, "ingest",
                           "Clip ready (HELD - not delivered). [clip:1]")
        self.retro.process_recording = self._fake_process(d)
        sent = []
        out = asyncio.run(media.ingest_media(
            self._update(930002), self._B(), d,
            deliver_fn=lambda p, c: sent.append(p) or 88))
        self.assertEqual(self.process_calls, [])  # never reprocessed
        self.assertEqual(sent, ["/x0.mp4"])       # delivered once
        self.assertEqual(_status(d, cid), "DELIVERED")
        self.assertIn("Already processed", out)

    def test_crash_after_delivery_replay_never_resends(self):
        import asyncio
        from app.bot import media
        d = pathlib.Path(tempfile.mkdtemp())
        (cid,) = _db_with_held(d, 1)
        # state as if: delivery succeeded and was journaled, crash
        # before the DB flip: the classic ambiguous window
        ops.journal_record(d, 930003, "ingest",
                           "Clip ready and sent to you. [clip:1]")
        ops.journal_record(d, cid, "deliver", "message 55")
        self.retro.process_recording = self._fake_process(d)
        sent = []
        out = asyncio.run(media.ingest_media(
            self._update(930003), self._B(), d,
            deliver_fn=lambda p, c: sent.append(p) or 99))
        self.assertEqual(self.process_calls, [])
        self.assertEqual(sent, [])  # no resend
        self.assertEqual(_status(d, cid), "DELIVERED")  # reconciled
        self.assertIn("Already processed", out)

    def test_replay_without_clip_marker_is_inert(self):
        import asyncio
        from app.bot import media
        d = pathlib.Path(tempfile.mkdtemp())
        ops.journal_record(d, 930004, "ingest",
                           "Processed your video: no gameplay moment "
                           "detected.")
        self.retro.process_recording = self._fake_process(d)
        sent = []
        out = asyncio.run(media.ingest_media(
            self._update(930004), self._B(), d,
            deliver_fn=lambda p, c: sent.append(p) or 1))
        self.assertEqual(self.process_calls, [])
        self.assertEqual(sent, [])
        self.assertIn("Already processed", out)
