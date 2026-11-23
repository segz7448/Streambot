"""Owner-invoked recovery entrypoint: /recoverclips.

The synchronous outcome function (recoverclips_reply) reports ACTUAL
per-clip outcomes - recovered, failed (edit/QC), fenced - and is what
the tracked background worker reports. The RecoveryTracker gives the
service a quick ack + one-at-a-time background run + progress status +
shutdown cancel, so a long edit never blocks the command loop. A
stranded /recoverclips is a side-effecting command: boot drain marks it
UNCERTAIN and asks the owner to re-send, never blind-replays it."""
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from app.bot import sessions
from app.bot.commands import CommandRouter, UnauthorizedError
from tests.unit.test_finalize_interrupted import FinalizeTests


class RecoverclipsReplyTests(FinalizeTests):
    def test_sealed_clip_recovered_and_reported(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        reply = sessions.recoverclips_reply(self.dir, "ffmpeg", "ffprobe")
        self.assertIn("1 clip", reply)
        self.assertIn("HELD", reply)
        self.assertIn("Nothing was delivered", reply)
        self.assertEqual(self._clip(cid)[0], "HELD")
        self.assertEqual(self._jobs(), [])

    def test_nothing_recoverable(self):
        reply = sessions.recoverclips_reply(self.dir, "ffmpeg", "ffprobe")
        self.assertIn("nothing to do", reply.lower())

    def test_edit_failure_is_reported(self):
        cid = self._add_clip()
        with mock.patch("app.editing.editor.edit_clip",
                        side_effect=OSError("injected edit failure")):
            reply = sessions.recoverclips_reply(self.dir, "ffmpeg",
                                                "ffprobe")
        self.assertNotIn("nothing to do", reply)
        self.assertIn("could not be finished", reply)
        self.assertEqual(self._clip(cid)[0], "FAILED")

    def test_qc_failure_is_reported(self):
        import types
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        with mock.patch("app.qc.verify.check_media",
                        return_value=types.SimpleNamespace(ok=False)):
            reply = sessions.recoverclips_reply(self.dir, "ffmpeg",
                                                "ffprobe")
        self.assertNotIn("nothing to do", reply)
        self.assertIn("could not be finished", reply)
        self.assertEqual(self._clip(cid)[0], "QC_FAILED")

    def test_fenced_remainder_reported_honestly(self):
        cid = self._add_clip(sealed=False)
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        reply = sessions.recoverclips_reply(self.dir, "ffmpeg", "ffprobe")
        self.assertIn("fenced", reply.lower())
        self.assertNotIn("not retried", reply)
        self.assertIn("re-checked on each run", reply)
        self.assertEqual(self._clip(cid)[0], "PENDING")
        self.assertEqual(self._jobs(), [])

    def test_second_run_idempotent(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        sessions.recoverclips_reply(self.dir, "ffmpeg", "ffprobe")
        reply = sessions.recoverclips_reply(self.dir, "ffmpeg", "ffprobe")
        self.assertIn("nothing to do", reply.lower())
        self.assertEqual(self._clip(cid)[0], "HELD")


class RecoveryTrackerTests(FinalizeTests):
    def _blocking_edit(self):
        import threading as _t
        entered, release = _t.Event(), _t.Event()
        def edit(ffmpeg, src, dst, preset, a, b, runtime,
                 watermark=None, pre_spawn=None, on_spawn=None, framing=None):
            entered.set()
            release.wait(5)
            Path(dst).write_bytes(b"\x00" * 2048)
        return edit, entered, release

    def test_quick_ack_progress_and_notify(self):
        cid = self._add_clip()
        for p in self._patches()[1:]:
            p.start()
        self.addCleanup(mock.patch.stopall)
        edit, entered, release = self._blocking_edit()
        tracker = sessions.RecoveryTracker()
        sent = []
        with mock.patch("app.editing.editor.edit_clip", edit):
            t0 = time.monotonic()
            ack = tracker.begin(self.dir, "ffmpeg", "ffprobe",
                                sent.append)
            self.assertLess(time.monotonic() - t0, 2)
            self.assertIn("started in the background", ack)
            self.assertIn("running", tracker.status())
            again = tracker.begin(self.dir, "ffmpeg", "ffprobe",
                                  sent.append)
            self.assertIn("already running", again)
            release.set()
            tracker._thread.join(10)
        self.assertFalse(tracker._thread.is_alive())
        self.assertEqual(len(sent), 1)
        self.assertIn("HELD", sent[0])
        self.assertIn("HELD", tracker.status())
        self.assertEqual(self._clip(cid)[0], "HELD")

    def test_cancel_during_run_stops_between_units(self):
        # shutdown cancel does not interrupt the clip mid-edit (each
        # clip is an atomic unit); the NEXT unit is skipped and stays
        # PENDING for a later run
        cid1 = self._add_clip()
        cid2 = self._add_clip()
        for p in self._patches()[1:]:
            p.start()
        self.addCleanup(mock.patch.stopall)
        entered, release = threading.Event(), threading.Event()
        calls = []
        def edit(ffmpeg, src, dst, preset, a, b, runtime,
                 watermark=None, pre_spawn=None, on_spawn=None, framing=None):
            calls.append(dst)
            if len(calls) == 1:
                entered.set()
                release.wait(5)
            Path(dst).write_bytes(b"\x00" * 2048)
        tracker = sessions.RecoveryTracker()
        sent = []
        with mock.patch("app.editing.editor.edit_clip", edit):
            tracker.begin(self.dir, "ffmpeg", "ffprobe", sent.append)
            self.assertTrue(entered.wait(2))
            tracker.request_cancel()
            release.set()
            tracker._thread.join(10)
        self.assertFalse(tracker._thread.is_alive())
        self.assertEqual(len(calls), 1)          # no second edit started
        self.assertEqual(self._clip(cid1)[0], "HELD")
        self.assertEqual(self._clip(cid2)[0], "PENDING")
        self.assertEqual(len(sent), 1)

    def test_mid_edit_cancel_returns_clip_to_pending(self):
        # a shutdown cancel landing DURING an edit is not a clip
        # failure: the row goes back to PENDING, never FAILED
        cid = self._add_clip()
        for p in self._patches()[1:]:
            p.start()
        self.addCleanup(mock.patch.stopall)
        tracker = sessions.RecoveryTracker()
        def edit(ffmpeg, src, dst, preset, a, b, runtime,
                 watermark=None, pre_spawn=None, on_spawn=None, framing=None):
            tracker.request_cancel()
            raise OSError("killed mid-edit")
        sent = []
        with mock.patch("app.editing.editor.edit_clip", edit):
            tracker.begin(self.dir, "ffmpeg", "ffprobe", sent.append)
            tracker._thread.join(10)
        self.assertFalse(tracker._thread.is_alive())
        self.assertEqual(self._clip(cid)[0], "PENDING")

    def test_boot_reconcile_records_interrupted_run_truthfully(self):
        # a run cut off with the service gets a truthful outcome and a
        # queued owner notice; nothing is replayed
        run_id = sessions.recovery_run_start(self.dir)
        self.assertIsNotNone(run_id)
        n = sessions.reconcile_recovery_runs(self.dir)
        self.assertEqual(n, 1)
        from app.core import schedule as _sched
        notices = _sched.pending_notices(self.dir)
        self.assertEqual(len(notices), 1)
        self.assertIn("did not report its result", notices[0][1])
        self.assertIn("Re-send /recoverclips", notices[0][1])
        # idempotent: nothing left unfinished
        self.assertEqual(sessions.reconcile_recovery_runs(self.dir), 0)

    def test_empty_state_still_reports(self):
        tracker = sessions.RecoveryTracker()
        sent = []
        tracker.begin(self.dir, "ffmpeg", "ffprobe", sent.append)
        tracker._thread.join(10)
        self.assertIn("nothing to do", sent[0].lower())


class RecoverclipsRouterTests(FinalizeTests):
    def _router(self, tracker, sent):
        r = CommandRouter(admin_ids=[42])
        r.register(
            "recoverclips",
            lambda c: sessions.recoverclips_reply_tracked(
                self.dir, "ffmpeg", "ffprobe", notify=sent.append,
                _tracker=tracker),
            "finish clips left by interrupted sessions to HELD")
        return r

    def test_non_admin_rejected_nothing_runs(self):
        cid = self._add_clip()
        tracker = sessions.RecoveryTracker()
        r = self._router(tracker, [])
        with self.assertRaises(UnauthorizedError):
            r.dispatch("/recoverclips", user_id=7)
        self.assertEqual(self._clip(cid)[0], "PENDING")
        self.assertIsNone(tracker._thread)

    def test_admin_dispatch_gets_quick_ack(self):
        cid = self._add_clip()
        for p in self._patches():
            p.start()
        self.addCleanup(mock.patch.stopall)
        tracker = sessions.RecoveryTracker()
        sent = []
        reply = self._router(tracker, sent).dispatch("/recoverclips",
                                                     user_id=42)
        self.assertIn("started in the background", reply)
        tracker._thread.join(10)
        self.assertEqual(self._clip(cid)[0], "HELD")
        self.assertEqual(len(sent), 1)


class DrainReplayPolicyTests(unittest.TestCase):
    """A stranded /recoverclips is a side-effecting command: the boot
    drain marks it UNCERTAIN and asks the owner to re-send; it never
    blind-replays a recovery."""

    def test_stranded_recoverclips_not_replayed(self):
        import tempfile
        from app.bot.pairing import PairingStore
        from app.bot.service import BotService
        d = Path(tempfile.mkdtemp())
        env = mock.patch.dict("os.environ", {"NOVA_STATE_DIR": str(d)})
        env.start()
        self.addCleanup(env.stop)
        store = PairingStore(str(d / "admin.json"))
        store._write({"admin_user_id": 42})
        svc = BotService("123456:local-offline-test-value", store)
        svc.inbox.add(30, 42, "/recoverclips")
        with mock.patch(
                "app.pipeline.session.finalize_interrupted_sessions",
                return_value=0) as fn:
            result = svc.drain_inbox()
        self.assertEqual(fn.call_count, 0)
        self.assertEqual(result["uncertain"], 1)
        self.assertEqual(result["replayed"], 0)


if __name__ == "__main__":
    unittest.main()
