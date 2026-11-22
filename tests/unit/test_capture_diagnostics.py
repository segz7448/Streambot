"""Retry-class capture failures must carry SAFE, fixed-schema diagnostics.

Measured gap (deployed fbf1984, acceptance session 6): the single bounded
capture cycle returned retry-class within 28s with zero segments, and the
terminal error was the constant "single capture attempt failed; no retry" -
the pump's reason and ffmpeg exit code existed but were dropped, so the
underlying cause was lost.

Review constraint (independent probe round): the diagnostic surface carries
a FIXED SCHEMA only - stage and refusal kind from closed enums, capture
reason from the pump's closed enum, bounded finite numeric counters. No raw
stderr, refusal detail, session error text, or str() of arbitrary objects:
that path can never leak headers, cookies, tokens, non-HTTP URLs or
api_key-style secrets, and it can never throw or change capture behavior.
"""
import asyncio
import contextlib
import socket
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock

from app.bot import acceptance, sessions
from app.ingestion import hls_pump, twitch_playback as tp
from app.recording.playback_worker import PlaybackWorker

SECRET_BLOB = ("Authorization: Bearer PRIVATE_BEARER\n"
               "Cookie: session=PRIVATE_COOKIE\n"
               "https://cdn.invalid/a?x=1\n"
               "rtmp://private.invalid/live/PRIVATE_RTMP\n"
               "api_key=PRIVATE_API")
SECRET_MARKERS = ["PRIVATE_BEARER", "PRIVATE_COOKIE", "PRIVATE_RTMP",
                  "PRIVATE_API", "cdn.invalid", "private.invalid"]


def _worker(deadline=1.0):
    buf = mock.Mock(session_id=999)
    buf.next_seq.side_effect = lambda: 0
    return PlaybackWorker(buf, "lacy", "unused", Path("/unused"), "ff",
                          initial_resolved=object(),
                          acceptance_deadline=time.monotonic() + deadline,
                          single_cycle=True)


class CycleDiagSchemaTests(unittest.TestCase):
    def test_ffmpeg_exit_cycle_fixed_fields_no_stderr_text(self):
        w = _worker()
        w.capture_fn = lambda *a, **k: hls_pump.PumpResult(
            "ffmpeg_exit", 0, 0.0, 0, 1, 4321, SECRET_BLOB)
        with mock.patch.object(w, "_recheck", return_value=True):
            self.assertEqual(w._cycle_inner(lambda: False), "retry")
        self.assertEqual(w.last_cycle_diag,
                         "capture reason=ffmpeg_exit ffmpeg_rc=1 segments=0 "
                         "media=0.0s bytes=0")
        for marker in SECRET_MARKERS:
            self.assertNotIn(marker, w.last_cycle_diag)
        self.assertNotIn("stderr", w.last_cycle_diag)

    def test_partial_double_uses_placeholders_and_never_throws(self):
        w = _worker()
        w.capture_fn = lambda *a, **k: types.SimpleNamespace(
            segments=0, reason="ended", ffmpeg_rc=0)
        with mock.patch.object(w, "_recheck", return_value=True):
            self.assertEqual(w._cycle_inner(lambda: False), "ended")
        self.assertEqual(w.last_cycle_diag,
                         "capture reason=ended ffmpeg_rc=0 segments=0 "
                         "media=? bytes=?")

    def test_mock_result_never_throws_or_stringifies(self):
        w = _worker()
        w.capture_fn = lambda *a, **k: mock.Mock(reason="media_cap",
                                                 segments=3, ffmpeg_rc=0)
        with mock.patch.object(w, "_recheck", return_value=True):
            # segments>0 but buffer advanced nothing -> failed cycle (retry)
            self.assertEqual(w._cycle_inner(lambda: False), "retry")
        self.assertEqual(w.last_cycle_diag,
                         "capture reason=media_cap ffmpeg_rc=0 segments=3 "
                         "media=? bytes=?")

    def test_string_numeric_and_junk_reason_degrade_bounded(self):
        w = _worker()
        w._note_cycle_diag(types.SimpleNamespace(
            media_seconds="unknown", reason="ended"))
        self.assertIn("media=?", w.last_cycle_diag)
        w._note_cycle_diag(types.SimpleNamespace(
            reason="https://cdn.invalid/SECRET" + "X" * 5000, ffmpeg_rc=1,
            segments=0, media_seconds=0, bytes_written=0))
        self.assertIn("reason=unknown", w.last_cycle_diag)
        self.assertLessEqual(len(w.last_cycle_diag), 200)
        self.assertNotIn("SECRET", w.last_cycle_diag)

    def test_nonfinite_and_out_of_range_numerics_degrade(self):
        w = _worker()
        for bad in (float("nan"), float("inf"), -1.0, 2e9, True):
            w._note_cycle_diag(types.SimpleNamespace(
                reason="ended", ffmpeg_rc=0, segments=0, media_seconds=bad,
                bytes_written=0))
            self.assertIn("media=?", w.last_cycle_diag)
        for bad_rc in (True, "1", 10 ** 12, 1.5):
            w._note_cycle_diag(types.SimpleNamespace(
                reason="ended", ffmpeg_rc=bad_rc, segments=0,
                media_seconds=0, bytes_written=0))
            self.assertIn("ffmpeg_rc=?", w.last_cycle_diag)

    def test_capture_refusal_fixed_kind_only(self):
        w = _worker()

        def boom(*a, **k):
            raise tp.PlaybackRefused("error", "HTTP 500 " + SECRET_BLOB)
        w.capture_fn = boom
        with mock.patch.object(w, "_recheck", return_value=True):
            self.assertEqual(w._cycle_inner(lambda: False), "retry")
        self.assertEqual(w.last_cycle_diag, "capture refused kind=error")
        for marker in SECRET_MARKERS:
            self.assertNotIn(marker, w.last_cycle_diag)

    def test_bogus_refusal_kind_maps_to_error(self):
        w = _worker()
        bogus = tp.PlaybackRefused("error", "x")
        bogus.kind = "https://cdn.invalid/SECRET"
        w.capture_fn = mock.Mock(side_effect=bogus)
        with mock.patch.object(w, "_recheck", return_value=True):
            self.assertEqual(w._cycle_inner(lambda: False), "retry")
        self.assertEqual(w.last_cycle_diag, "capture refused kind=error")

    def test_resolve_refusal_records_stage(self):
        w = _worker()
        w.initial = None

        def refused(*a, **k):
            raise tp.PlaybackRefused("offline", SECRET_BLOB)
        w.resolve_fn = refused
        with mock.patch.object(w, "_recheck", return_value=True):
            with self.assertRaises(RuntimeError):   # offline at start
                w._cycle_inner(lambda: False)
        self.assertEqual(w.last_cycle_diag, "resolve refused kind=offline")

    def test_single_cycle_terminal_error_carries_fixed_diag(self):
        w = _worker()
        w.buffer.reconcile.return_value = 0
        w.buffer.enforce_storage_cap.return_value = None
        w._cycle = mock.Mock(return_value="retry")
        w.last_cycle_diag = ("capture reason=ffmpeg_exit ffmpeg_rc=1 "
                             "segments=0 media=0.0s bytes=0")
        w._closed_media_ready = lambda: False
        ctx = types.SimpleNamespace(stopping=False, heartbeat=lambda: None)
        with self.assertRaises(RuntimeError) as cm:
            asyncio.run(w.run(ctx))
        msg = str(cm.exception)
        self.assertIn("single capture attempt failed; no retry", msg)
        self.assertIn("reason=ffmpeg_exit", msg)
        self.assertIn("ffmpeg_rc=1", msg)


class AcceptanceDiagTests(unittest.TestCase):
    """The runner JSON carries fixed-schema failure fields only."""

    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        self.addCleanup(self.t.cleanup)
        self.sd = Path(self.t.name)
        from app.core.database import connect, migrate
        from app.bot import source_auth
        c = connect(str(self.sd / "nova.db"))
        migrate(c)
        c.execute("INSERT INTO twitch_sources(username,created_at,updated_at)"
                  " VALUES('lacy',0,0)")
        c.commit()
        c.close()
        source_auth.set_scope(self.sd, "lacy", "record", True)

    def _run_with_worker(self, worker_cls):
        from app.pipeline.session import ClipSession
        created = []

        def factory(*args, **kwargs):
            obj = ClipSession(*args, **kwargs)
            obj.tick_seconds = .01
            created.append(obj)
            return obj

        def launch(sd, target, ff, fp, session_factory):
            return sessions.startplayback_reply(
                sd, target, ff, fp, session_factory=session_factory,
                resolve_fn=lambda *a, **k: object())

        with contextlib.ExitStack() as st:
            st.enter_context(mock.patch.object(
                socket.socket, "connect",
                side_effect=AssertionError("network denied in disposable test")))
            st.enter_context(mock.patch(
                "app.recording.playback_worker.PlaybackWorker", worker_cls))
            st.enter_context(mock.patch.object(
                sessions, "_gate_refusal", return_value=None))
            result = acceptance._run(self.sd, "ff", "fp", "lacy", 2,
                                     start_fn=launch, factory_base=factory)
        self.assertFalse(any(s.alive() for s in created))
        return result

    def test_failed_session_fixed_schema_result(self):
        class FailingWorker:
            def __init__(self, *args, **kwargs):
                self.finished = False
                self.end_reason = "failed"
                self.error_text = None
                self._stop_req = False
                self.last_cycle_diag = (
                    "capture reason=ffmpeg_exit ffmpeg_rc=1 segments=0 "
                    "media=0.0s bytes=0")

            async def run(self, ctx):
                self.finished = True
                self.error_text = ("single capture attempt failed; no retry: "
                                   + self.last_cycle_diag + " " + SECRET_BLOB)
                raise RuntimeError(self.error_text)

            def drained(self):
                return True

            async def ensure_stopped(self):
                self._stop_req = True

        result = self._run_with_worker(FailingWorker)
        self.assertTrue(result["terminated"])
        self.assertEqual(result["clips_finished"], 0)
        self.assertTrue(result["session_failed"])
        self.assertEqual(result["capture_diag"],
                         "capture reason=ffmpeg_exit ffmpeg_rc=1 segments=0 "
                         "media=0.0s bytes=0")
        self.assertNotIn("session_error", result)
        blob = repr(sorted(result.items()))
        for marker in SECRET_MARKERS:
            self.assertNotIn(marker, blob)

    def test_clean_session_no_failure_fields(self):
        class OkWorker:
            def __init__(self, *args, **kwargs):
                self.finished = False
                self.end_reason = "stopped"
                self.error_text = None
                self._stop_req = False
                self.last_cycle_diag = None

            async def run(self, ctx):
                while not ctx.stopping and not self._stop_req:
                    await asyncio.sleep(.005)
                self.finished = True

            def drained(self):
                return True

            async def ensure_stopped(self):
                self._stop_req = True

        result = self._run_with_worker(OkWorker)
        self.assertTrue(result["terminated"])
        self.assertFalse(result["session_failed"])
        self.assertIsNone(result["capture_diag"])


if __name__ == "__main__":
    unittest.main()
