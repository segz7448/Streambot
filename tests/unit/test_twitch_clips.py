import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from app.ingestion.twitch_clips import (ClipFailed, TwitchClipsClient,
                                        TwitchNotConfigured,
                                        client_from_state)


def _resp(payload):
    m = mock.MagicMock()
    m.read.return_value = json.dumps(payload).encode()
    m.__enter__ = lambda s: s
    m.__exit__ = mock.Mock(return_value=False)
    return m


class ConfigTests(unittest.TestCase):
    def test_refuses_without_credentials(self):
        with self.assertRaises(TwitchNotConfigured):
            TwitchClipsClient("", "")

    def test_state_dir_without_files_not_configured(self):
        with TemporaryDirectory() as td:
            with self.assertRaises(TwitchNotConfigured):
                client_from_state(td)

    def test_state_dir_with_files_builds(self):
        with TemporaryDirectory() as td:
            Path(td, "twitch_client_id").write_text("cid")
            Path(td, "twitch_user_token").write_text("utok")
            client = client_from_state(td)
            self.assertEqual(client.user_token, "utok")


class FlowTests(unittest.TestCase):
    def setUp(self):
        self.client = TwitchClipsClient("cid", "utok", "atok")

    def test_resolve_login(self):
        with mock.patch("urllib.request.urlopen",
                        return_value=_resp({"data": [{"id": "42"}]})):
            self.assertEqual(self.client.resolve_broadcaster_id("chan"), "42")

    def test_resolve_unknown_login_fails_loudly(self):
        with mock.patch("urllib.request.urlopen",
                        return_value=_resp({"data": []})):
            with self.assertRaises(ClipFailed):
                self.client.resolve_broadcaster_id("ghost")

    def test_create_clip_uses_user_token(self):
        captured = {}

        def fake_urlopen(req, timeout=0):
            captured["auth"] = req.headers.get("Authorization")
            captured["method"] = req.get_method()
            return _resp({"data": [{"id": "clip1"}]})

        with mock.patch("urllib.request.urlopen", fake_urlopen):
            clip_id = self.client.create_clip("42")
        self.assertEqual(clip_id, "clip1")
        self.assertEqual(captured["auth"], "Bearer utok")
        self.assertEqual(captured["method"], "POST")

    def test_wait_for_clip_times_out_loudly(self):
        with mock.patch("urllib.request.urlopen",
                        return_value=_resp({"data": []})):
            with self.assertRaises(ClipFailed):
                self.client.wait_for_clip("clip1", timeout=0.1,
                                          interval=0.05)

    def test_download_url_uses_official_endpoint(self):
        info = {"id": "clip1", "broadcaster_id": "42"}
        payload = {"data": [{"clip_id": "clip1",
                             "landscape_download_url": "https://cdn/x.mp4",
                             "portrait_download_url": None}]}
        with mock.patch("urllib.request.urlopen",
                        return_value=_resp(payload)):
            self.assertEqual(self.client.download_url(info),
                             "https://cdn/x.mp4")

    def test_download_url_fails_loudly_without_rights(self):
        # arbitrary channel: official endpoint returns no data and the
        # client refuses rather than using undocumented asset patterns
        info = {"id": "clip1", "broadcaster_id": "999"}
        with mock.patch("urllib.request.urlopen",
                        return_value=_resp({"data": []})):
            with self.assertRaises(ClipFailed):
                self.client.download_url(info)


if __name__ == "__main__":
    unittest.main()
