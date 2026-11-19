import os
import sqlite3
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from app.bot import commands_full as cf
from app.core.database import migrate


class CommandsFullTests(unittest.TestCase):
    def setUp(self):
        import sqlite3
        self.dir = Path(tempfile.mkdtemp())
        conn = sqlite3.connect(str(self.dir / "nova.db"))
        migrate(conn)
        conn.close()

    def test_addsource_validate_register_dupe_remove(self):
        self.assertTrue(cf.addsource_reply(self.dir, "").startswith("Usage:"))
        self.assertIn("registered", cf.addsource_reply(self.dir, "SomeUser"))
        self.assertIn("already", cf.addsource_reply(self.dir, "someuser"))
        self.assertIn("removed", cf.removesource_reply(self.dir, "someuser"))
        self.assertIn("No source", cf.removesource_reply(self.dir, "someuser"))

    def test_checklive_unconfigured_fails_loudly(self):
        self.assertIn("not configured", cf.checklive_reply(self.dir, "x"))

    def test_startclip_requires_source_then_input(self):
        self.assertIn("/addsource", cf.startclip_reply(self.dir, "ghost"))
        cf.addsource_reply(self.dir, "chan")
        reply = cf.startclip_reply(self.dir, "chan")
        self.assertIn("cannot start", reply)
        self.assertIn("stays registered", reply)

    def test_sessions_clips_empty(self):
        self.assertIn("No sessions", cf.sessions_reply(self.dir, ""))
        self.assertIn("No clips", cf.clips_reply(self.dir, ""))
        self.assertTrue(cf.clipdetails_reply(self.dir, "x").startswith("Usage"))
        self.assertIn("No clip", cf.clipdetails_reply(self.dir, "1"))

    def test_stopclip_idle(self):
        self.assertIn("No clipping session", cf.stopclip_reply(self.dir, ""))

    def test_settings_and_quality(self):
        self.assertIn("No settings", cf.settings_reply(self.dir, ""))
        self.assertIn("saved", cf.settings_reply(self.dir, "note hello"))
        self.assertIn("note = hello", cf.settings_reply(self.dir, ""))
        self.assertIn("vertical", cf.quality_reply(self.dir, ""))
        self.assertIn("set to square", cf.quality_reply(self.dir, "square"))
        self.assertIn("Unknown preset", cf.quality_reply(self.dir, "4k"))
        self.assertIn("square", cf.quality_reply(self.dir, ""))

    def test_subtitles_out_of_scope(self):
        self.assertIn("out of scope", cf.SUBTITLES_REPLY)

    def test_logs_ring(self):
        self.assertIn("No errors", cf.logs_reply(self.dir, ""))
        cf.record_error(self.dir, "boom happened")
        self.assertIn("boom happened", cf.logs_reply(self.dir, ""))
        mode = os.stat(self.dir / "errors.log").st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def test_restart_honest(self):
        self.assertIn("host-controlled", cf.RESTART_REPLY)


if __name__ == "__main__":
    unittest.main()


class PublishCommandTests(unittest.TestCase):

    def _clip(self, d, clip_id=1, status="HELD", outside=False):
        import time
        if not (d / "nova.db").exists():
            conn = sqlite3.connect(str(d / "nova.db"))
            migrate(conn)
            conn.close()
        if outside:
            clip = d.parent / f"escape-{clip_id}.mp4"
        elif status == "RETRO":
            status = "HELD"
            sess = d / "retrospective" / "900001"
            sess.mkdir(parents=True, exist_ok=True)
            clip = sess / f"clip-edited-{clip_id}.mp4"
        else:
            sess = d / "sessions" / "src"
            sess.mkdir(parents=True, exist_ok=True)
            clip = sess / f"clip-{clip_id}.mp4"
        clip.write_bytes(b"mp4data")
        with sqlite3.connect(str(d / "nova.db")) as conn:
            conn.execute(
                "INSERT INTO clips(id, file_path, status, created_at)"
                " VALUES(?,?,?,?)",
                (clip_id, str(clip), status, time.time()))
        return clip

    def test_usage_and_missing_clip(self):
        d = Path(tempfile.mkdtemp())
        self.assertEqual(cf.publish_reply(d, "abc"), cf.USAGE_PUBLISH)
        self.assertEqual(cf.publish_reply(d, ""), cf.USAGE_PUBLISH)
        conn = sqlite3.connect(str(d / "nova.db"))
        migrate(conn)
        conn.close()
        self.assertIn("No clip #9", cf.publish_reply(d, "9"))

    def test_non_qc_passed_statuses_refused(self):
        from app.bot import ops
        for status in ("PENDING", "QC_FAILED", "FAILED", "RAW"):
            d = Path(tempfile.mkdtemp())
            self._clip(d, status=status)
            r = cf.publish_reply(d, "1", 600)
            self.assertIn("only QC-passed", r, status)
            self.assertIsNone(ops.journal_result(d, 600, "publish"))

    def test_path_escape_refused(self):
        d = Path(tempfile.mkdtemp())
        self._clip(d, outside=True)
        r = cf.publish_reply(d, "1", 601)
        self.assertIn("outside the clip storage root", r)

    def test_unconfigured_fails_closed_no_journal(self):
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        r = cf.publish_reply(d, "1", 555)
        self.assertIn("not configured", r)
        from app.bot import ops
        self.assertIsNone(ops.journal_result(d, 555, "publish"))

    def test_success_journaled_replay_guarded_and_deduped(self):
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        fake = mock.Mock()
        fake.publish.return_value = "vid9"
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r = cf.publish_reply(d, "1 My title", 556)
        self.assertIn("PRIVATE", r)
        self.assertIn("vid9", r)
        args, kwargs = fake.publish.call_args
        self.assertEqual(kwargs.get("privacy"), "private")
        self.assertEqual(args[1], "My title")
        self.assertTrue(kwargs.get("tags"))
        self.assertIn("clip #1", kwargs.get("description", ""))
        # replay of the SAME update: recorded result, no second upload
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r2 = cf.publish_reply(d, "1 My title", 556)
        self.assertIn("replay guard", r2)
        # a NEW command for the SAME clip: dedup, no second upload
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r3 = cf.publish_reply(d, "1", 777)
        self.assertIn("already uploaded", r3)
        fake.publish.assert_called_once()

    def test_planner_title_used_and_capped(self):
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        fake = mock.Mock()
        fake.publish.return_value = "vid10"
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            cf.publish_reply(d, "1", 556)
        title = fake.publish.call_args[0][1]
        self.assertTrue(title)
        self.assertLessEqual(len(title), 100)
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            self._clip(d, clip_id=2)
            cf.publish_reply(d, "2 " + "x" * 300, 557)
        self.assertEqual(len(fake.publish.call_args[0][1]), 100)

    def test_definitive_preupload_failure_journaled_failed(self):
        from app.distribution.youtube import YouTubeUploadFailed
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        fake = mock.Mock()
        fake.publish.side_effect = YouTubeUploadFailed(
            "token refresh failed: HTTP 400", stage="auth")
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r = cf.publish_reply(d, "1", 557)
        self.assertIn("failed before any video data was sent", r)
        from app.bot import ops
        self.assertIn("failed", ops.journal_result(d, 557, "publish"))
        self.assertEqual(ops.journal_uncertain(d), [])

    def test_inflight_failure_marked_uncertain_not_retried(self):
        from app.distribution.youtube import YouTubeUploadFailed
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        fake = mock.Mock()
        fake.publish.side_effect = YouTubeUploadFailed(
            "upload interrupted: reset", stage="upload", uncertain=True)
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r = cf.publish_reply(d, "1", 558)
        self.assertIn("uncertain", r)
        from app.bot import ops
        unc = ops.journal_uncertain(d)
        self.assertEqual(len(unc), 1)
        self.assertEqual(unc[0]["command"], "publish")

    def test_retrospective_output_root_accepted(self):
        d = Path(tempfile.mkdtemp())
        self._clip(d, status="RETRO")
        fake = mock.Mock()
        fake.publish.return_value = "vidR"
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r = cf.publish_reply(d, "1", 559)
        self.assertIn("PRIVATE", r)

    def test_uncertain_attempt_blocks_new_command_same_clip(self):
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        from app.bot import ops
        ops.journal_mark_uncertain(d, 900, "publish", "/publish 1")
        fake = mock.Mock()
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r = cf.publish_reply(d, "1", 901)
        self.assertIn("uncertain", r.lower())
        self.assertIn("Reconcile", r)
        fake.publish.assert_not_called()
        # a DIFFERENT clip is not blocked
        self._clip(d, clip_id=2)
        fake.publish.return_value = "vid2"
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            r2 = cf.publish_reply(d, "2", 902)
        self.assertIn("PRIVATE", r2)

    def test_publish_in_side_effecting(self):
        from app.bot import ops
        self.assertIn("publish", ops.SIDE_EFFECTING)


class YouTubeAuthTests(unittest.TestCase):

    def _dir_with_client(self, secret_mode=0o600):
        d = Path(tempfile.mkdtemp())
        (d / "youtube_client_id").write_text("cid")
        sp = d / "youtube_client_secret"
        sp.write_text("cs")
        os.chmod(sp, secret_mode)
        return d

    def tearDown(self):
        # never leak the module-level lock between tests
        cf._YOUTUBEAUTH_LOCK.acquire(blocking=False)
        try:
            cf._YOUTUBEAUTH_LOCK.release()
        except RuntimeError:
            pass

    def test_setup_instructions_without_client(self):
        d = Path(tempfile.mkdtemp())
        r = cf.youtubeauth_reply(d, "")
        self.assertIn("Google OAuth client first", r)
        self.assertIn("Not configured", cf.youtubeauth_reply(d, "status"))

    def test_bind_failure_releases_lock_and_close_runs(self):
        d = self._dir_with_client()
        with mock.patch("app.distribution.youtube.HTTPServer",
                        side_effect=OSError("address in use")):
            r1 = cf.youtubeauth_reply(d, "")
        self.assertIn("Could not start", r1)
        # the lock survived: a second attempt must not say "already
        # waiting" - it must try (and fail the same way) again
        with mock.patch("app.distribution.youtube.HTTPServer",
                        side_effect=OSError("address in use")):
            r2 = cf.youtubeauth_reply(d, "")
        self.assertIn("Could not start", r2)
        self.assertNotIn("already waiting", r2)

    def test_world_readable_secret_refused_before_lock_leak(self):
        d = self._dir_with_client(secret_mode=0o644)
        r1 = cf.youtubeauth_reply(d, "")
        self.assertIn("readable by others", r1)
        r2 = cf.youtubeauth_reply(d, "")
        self.assertIn("readable by others", r2)  # lock not stuck

    def test_success_stores_token_0600_and_closes_server(self):
        d = self._dir_with_client()
        flow = mock.Mock()
        flow.authorization_url = "https://accounts.google.com/x"
        flow.wait_for_code.return_value = "code"
        flow.exchange.return_value = {"refresh_token": "rtok"}
        with mock.patch("app.distribution.youtube.LoopbackAuth",
                        return_value=flow):
            r = cf.youtubeauth_reply(d, "")
        self.assertIn("youtube.upload", r)
        import time as _t
        for _ in range(200):
            # completion ends with lock release; waiting for that also
            # covers the finally (server_close) running first
            if cf._YOUTUBEAUTH_LOCK.acquire(blocking=False):
                cf._YOUTUBEAUTH_LOCK.release()
                break
            _t.sleep(0.02)
        self.assertEqual((d / "youtube_refresh_token").read_text(),
                         "rtok")
        self.assertEqual((d / "youtube_refresh_token").stat().st_mode
                         & 0o777, 0o600)
        flow._server.server_close.assert_called_once()
        # lock free for the next run
        self.assertTrue((d / "youtube_refresh_token").exists())
        r2 = cf.youtubeauth_reply(d, "")
        self.assertIn("already authorized", r2)
