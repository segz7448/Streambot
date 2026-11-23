from contextlib import closing
from tests.unit import _prov
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

    def test_checklive_unconfigured_uses_labelled_public_status_not_a_refusal(self):
        for status, detail, head in (("live", "2 playback variant(s) offered", "LIVE"),
                                     ("offline", "no live playlist", "OFFLINE or not found"),
                                     ("restricted", "r", "RESTRICTED"), ("blocked", "b", "BLOCKED"),
                                     ("unknown", "u", "UNKNOWN")):
            out = cf.checklive_reply(self.dir, "@IShowSpeed", status_fn=lambda u, s=status, d=detail: (s, d))
            self.assertIn("ishowspeed: " + head, out)
            self.assertIn("Unofficial", out)
            self.assertIn("no title, game or viewer count", out)
            self.assertIn("No video or media segments were downloaded or recorded; only playback metadata was checked", out)
            self.assertNotIn("developer application", out)

    def test_checklive_status_function_crash_is_unknown_not_a_traceback_or_secret(self):
        def boom(u): raise OSError("https://x.ttvnw.net/a?sig=SECRETSIG")
        out = cf.checklive_reply(self.dir, "ishowspeed", status_fn=boom)
        self.assertIn("UNKNOWN", out)
        self.assertNotIn("SECRET", out)

    def test_checklive_real_default_path_goes_through_live_status_only(self):
        from unittest import mock
        with mock.patch("app.ingestion.twitch_playback.live_status", return_value=("offline", "x")) as m:
            out = cf.checklive_reply(self.dir, "ishowspeed")
        m.assert_called_once_with("ishowspeed")
        self.assertIn("OFFLINE", out)

    def test_checklive_does_not_require_record_or_repost_authorization(self):
        from app.bot import source_auth
        self.assertFalse(source_auth.allowed(self.dir, "ishowspeed", "record"))
        out = cf.checklive_reply(self.dir, "ishowspeed", status_fn=lambda u: ("live", "ok"))
        self.assertIn("LIVE", out)

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
        with closing(sqlite3.connect(str(d / "nova.db"))) as conn, conn:
            conn.execute(
                "INSERT INTO clips(id, file_path, status, created_at, session_id)"
                " VALUES(?,?,?,?,?)",
                (clip_id, str(clip), status, time.time(),
                 _prov.feed_session_id(conn)))
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
        from app.distribution import publish_records
        with publish_records.ClipLease(d, 1) as lease:
            _, rec = publish_records.reserve(d, 1, "test-setup", lease=lease)
            publish_records.mark_uncertain(d, 1, "test uncertainty", lease=lease,
                                           attempt_token=rec['attempt_token'])
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

    def test_publish_daily_budget_fixed_reply_no_upload(self):
        # TEST ceiling (0,0): the very first reservation of the day is
        # already capped. Not a claim about production limits.
        from app.distribution import publish_records as pr
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        fake = mock.Mock()
        fake.publish.side_effect = AssertionError("publisher must not run")
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake), \
             mock.patch.object(pr, "DEFAULT_BUDGET_LIMITS", (0, 0)):
            out = cf.publish_reply(d, "1", 557)
        self.assertEqual(out, "Daily upload limit reached. This request did "
                              "not upload anything. Try again next local day.")
        fake.publish.assert_not_called()
        self.assertIsNone(pr.get(d, 1))

    def test_publish_budget_unverified_history_distinct_reply(self):
        # bootstrap fails closed: the reply names inspection, never a
        # quota, and never promises "tomorrow".
        from app.distribution import publish_records as pr
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        with closing(sqlite3.connect(str(d / "nova.db"))) as conn, conn:
            conn.execute("INSERT INTO publish_records (clip_id,state,attempts,"
                         "created_at,updated_at) VALUES (9,'RESERVED',1,0,?)",
                         (float("-inf"),))
        fake = mock.Mock()
        fake.publish.side_effect = AssertionError("publisher must not run")
        with mock.patch("app.distribution.youtube.publisher_from_state",
                        return_value=fake):
            out = cf.publish_reply(d, "1", 558)
        self.assertEqual(out, "Upload paused because daily upload history "
                              "could not be verified. This request did not "
                              "upload anything; inspection is needed.")
        fake.publish.assert_not_called()
        self.assertIsNone(pr.get(d, 1))


class YouTubeAuthTests(unittest.TestCase):

    def _dir_with_client(self, secret_mode=0o600):
        d = Path(tempfile.mkdtemp())
        (d / "youtube_client_id").write_text("cid")
        sp = d / "youtube_client_secret"
        sp.write_text("cs")
        os.chmod(sp, secret_mode)
        return d

    def test_setup_instructions_without_client(self):
        d = Path(tempfile.mkdtemp())
        self.assertIn("Google OAuth client first", cf.youtubeauth_reply(d, ""))
        self.assertIn("Not configured", cf.youtubeauth_reply(d, "status"))

    def test_legacy_client_cannot_start_consent_or_write_token(self):
        d = self._dir_with_client()
        with mock.patch("app.distribution.youtube.LoopbackAuth",
                        side_effect=AssertionError("consent started")):
            self.assertIn("operator vault route", cf.youtubeauth_reply(d, ""))
        self.assertFalse((d / "youtube_refresh_token").exists())

    def test_legacy_status_is_explicit_and_read_only(self):
        d = self._dir_with_client()
        (d / "youtube_refresh_token").write_text("old")
        self.assertIn("legacy state-dir", cf.youtubeauth_reply(d, "status"))
        self.assertIn("not checked", cf.youtubeauth_reply(d, "status"))
        cf.youtubeauth_reply(d, "")
        self.assertEqual((d / "youtube_refresh_token").read_text(), "old")

class DurablePublishHandlerTests(unittest.TestCase):
    _clip = PublishCommandTests._clip

    def test_update_zero_success_durable_after_journal_prune(self):
        from app.distribution import publish_records as pr
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        fake = mock.Mock()
        fake.publish.return_value = 'zero-id-video'
        with mock.patch('app.distribution.youtube.publisher_from_env_or_state', return_value=fake):
            self.assertIn('PRIVATE', cf.publish_reply(d, '1'))
            for p in d.glob('*journal*'):
                if p.is_file():
                    p.unlink()
            self.assertIn('already uploaded', cf.publish_reply(d, '1'))
        fake.publish.assert_called_once()
        self.assertEqual(pr.get(d, 1)['state'], 'CONFIRMED')

    def test_update_zero_uncertain_blocks_repeat(self):
        from app.distribution.youtube import YouTubeUploadFailed
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        fake = mock.Mock()
        fake.publish.side_effect = YouTubeUploadFailed('reset', stage='upload', uncertain=True)
        with mock.patch('app.distribution.youtube.publisher_from_env_or_state', return_value=fake):
            self.assertIn('uncertain', cf.publish_reply(d, '1'))
            self.assertIn('Reconcile', cf.publish_reply(d, '1'))
        fake.publish.assert_called_once()

    def test_concurrent_same_clip_handler_one_upload(self):
        import threading
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        entered, release = threading.Event(), threading.Event()
        outputs, errors = [], []
        def upload(*a, **kw):
            entered.set()
            if not release.wait(5):
                raise RuntimeError('test timeout')
            return 'race-video'
        fake = mock.Mock()
        fake.publish.side_effect = upload
        def call():
            try:
                outputs.append(cf.publish_reply(d, '1'))
            except Exception as exc:
                errors.append(exc)
        with mock.patch('app.distribution.youtube.publisher_from_env_or_state', return_value=fake):
            t = threading.Thread(target=call)
            t.start()
            try:
                self.assertTrue(entered.wait(5))
                self.assertIn('in progress', cf.publish_reply(d, '1'))
            finally:
                release.set()
                t.join(10)
        self.assertFalse(t.is_alive())
        self.assertEqual(errors, [])
        self.assertIn('PRIVATE', outputs[0])
        fake.publish.assert_called_once()

    def test_handler_env_only_private_without_files(self):
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        env = {'NOVA_YOUTUBE_CLIENT_ID': 'cid', 'NOVA_YOUTUBE_CLIENT_SECRET': 'sec',
               'NOVA_YOUTUBE_REFRESH_TOKEN': 'rt'}
        with mock.patch.dict(os.environ, env, clear=True), \
             mock.patch('app.distribution.youtube.publisher_from_state', side_effect=AssertionError('legacy read')), \
             mock.patch('app.distribution.youtube.YouTubePublisher.publish', return_value='env-video') as upload:
            self.assertIn('PRIVATE', cf.publish_reply(d, '1'))
        self.assertEqual(upload.call_args.kwargs['privacy'], 'private')
        self.assertFalse(list(d.glob('youtube_*')))


class VaultAuthStatusTests(unittest.TestCase):
    def test_vault_status_and_consent_refusal_without_local_write(self):
        d = Path(tempfile.mkdtemp())
        env = {'NOVA_YOUTUBE_CLIENT_ID': 'cid', 'NOVA_YOUTUBE_CLIENT_SECRET': 'sec',
               'NOVA_YOUTUBE_REFRESH_TOKEN': 'rt'}
        with mock.patch.dict(os.environ, env, clear=True), \
             mock.patch('app.distribution.youtube.LoopbackAuth', side_effect=AssertionError('consent started')):
            self.assertIn('environment', cf.youtubeauth_reply(d, 'status'))
            self.assertIn('disabled', cf.youtubeauth_reply(d, ''))
        self.assertFalse(list(d.glob('youtube_*')))

    def test_incomplete_env_auth_refuses_even_with_legacy_files(self):
        d = Path(tempfile.mkdtemp())
        for name in ('youtube_client_id', 'youtube_client_secret', 'youtube_refresh_token'):
            (d / name).write_text('old')
        env = {'NOVA_YOUTUBE_CLIENT_ID': 'cid', 'NOVA_YOUTUBE_CLIENT_SECRET': ''}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertIn('Refused', cf.youtubeauth_reply(d, 'status'))
            self.assertIn('Refused', cf.youtubeauth_reply(d, ''))
        self.assertEqual((d / 'youtube_refresh_token').read_text(), 'old')

class LegacyPublishUpgradeTests(unittest.TestCase):
    _clip = PublishCommandTests._clip

    def test_legacy_success_promoted_to_durable_record(self):
        from app.bot import ops
        from app.distribution import publish_records as pr
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        ops.journal_record_ns(d, 'publish', 1, 'publish', 'yt:old-video')
        fake = mock.Mock()
        with mock.patch('app.distribution.youtube.publisher_from_env_or_state', return_value=fake):
            self.assertIn('already uploaded', cf.publish_reply(d, '1'))
            (d / ops.JOURNAL).unlink()
            self.assertIn('already uploaded', cf.publish_reply(d, '1'))
        fake.publish.assert_not_called()
        self.assertEqual(pr.get(d, 1)['video_id'], 'old-video')

    def test_legacy_uncertainty_promoted_to_durable_record(self):
        from app.bot import ops
        from app.distribution import publish_records as pr
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        ops.journal_mark_uncertain(d, 123, 'publish', '/publish 1 title')
        fake = mock.Mock()
        with mock.patch('app.distribution.youtube.publisher_from_env_or_state', return_value=fake):
            self.assertIn('Reconcile', cf.publish_reply(d, '1'))
            (d / ops.JOURNAL).unlink()
            self.assertIn('Reconcile', cf.publish_reply(d, '1'))
        fake.publish.assert_not_called()
        self.assertEqual(pr.get(d, 1)['state'], 'UNCERTAIN')

class PublishOwnershipRegressionTests(unittest.TestCase):
    _clip = PublishCommandTests._clip

    def test_confirmation_write_failure_visible_and_repeat_blocked(self):
        from app.distribution import publish_records as pr
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        fake = mock.Mock()
        fake.publish.return_value = 'returned-video'
        with mock.patch('app.distribution.youtube.publisher_from_env_or_state', return_value=fake), \
             mock.patch.object(pr, 'confirm', side_effect=sqlite3.OperationalError('disk full')):
            reply = cf.publish_reply(d, '1')
            self.assertIn('durable confirmation failed', reply)
            self.assertIn('returned-video', reply)
            self.assertNotIn('uploaded PRIVATE', reply)
            self.assertIn('Reconcile', cf.publish_reply(d, '1'))
        fake.publish.assert_called_once()
        self.assertEqual(pr.get(d, 1)['state'], 'UNCERTAIN')

    def test_recovery_during_handler_preserves_live_reservation(self):
        from app.distribution import publish_records as pr
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        fake = mock.Mock()
        def upload(*a, **kw):
            self.assertEqual(pr.recover_interrupted(d), 0)
            self.assertEqual(pr.get(d, 1)['state'], 'RESERVED')
            return 'live-video'
        fake.publish.side_effect = upload
        with mock.patch('app.distribution.youtube.publisher_from_env_or_state', return_value=fake):
            self.assertIn('uploaded PRIVATE', cf.publish_reply(d, '1'))
        self.assertEqual(pr.get(d, 1)['state'], 'CONFIRMED')

    def test_unrelated_audit_write_bulk_promotes_before_age_prune(self):
        from app.bot import ops
        from app.distribution import publish_records as pr
        import json
        d = Path(tempfile.mkdtemp())
        self._clip(d)
        old = {'publish:1': {'command': 'publish', 'result': 'yt:old-video', 'ts': 1},
               'publish:77': {'command': 'publish', 'result': 'yt:other-old', 'ts': 1},
               'tg:4': {'command': 'publish', 'status': 'uncertain', 'text': '/publish 88', 'ts': 1}}
        (d / ops.JOURNAL).write_text(json.dumps(old))
        ops.journal_record(d, 500, 'other', 'done')
        self.assertIsNone(ops.journal_result_ns(d, 'publish', 1, 'publish'))
        self.assertEqual(pr.get(d, 1)['video_id'], 'old-video')
        self.assertEqual(pr.get(d, 77)['video_id'], 'other-old')
        self.assertEqual(pr.get(d, 88)['state'], 'UNCERTAIN')
        fake = mock.Mock()
        with mock.patch('app.distribution.youtube.publisher_from_env_or_state', return_value=fake):
            self.assertIn('already uploaded', cf.publish_reply(d, '1'))
        fake.publish.assert_not_called()
