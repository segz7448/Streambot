import unittest
from unittest import mock

from app.distribution.youtube import (YouTubeNotConfigured,
                                      YouTubePublisher,
                                      YouTubeUploadFailed)


def _resp(payload, headers=None, status=200):
    import io
    import json as _json
    body = _json.dumps(payload).encode()
    r = mock.Mock()
    r.read.return_value = body
    r.headers = headers or {}
    r.__enter__ = lambda s: s
    r.__exit__ = mock.Mock(return_value=False)
    return r


class YouTubePublisherTests(unittest.TestCase):
    def test_missing_credentials_refuses(self):
        with self.assertRaises(YouTubeNotConfigured):
            YouTubePublisher("", "rt")
        with self.assertRaises(YouTubeNotConfigured):
            YouTubePublisher("cid", "")

    def test_loopback_flow_builds_least_privilege_url(self):
        auth = YouTubePublisher.__new__(YouTubePublisher)  # unused
        from app.distribution.youtube import LoopbackAuth
        flow = LoopbackAuth("cid", host="127.0.0.1", port=0)
        self.addCleanup(flow._server.server_close)
        self.assertIn("youtube.upload", flow.authorization_url)
        self.assertNotIn("auth/youtube%20", flow.authorization_url)
        self.assertIn("code_challenge_method=S256", flow.authorization_url)
        self.assertIn("127.0.0.1", flow.redirect_uri)
        self.assertIn("access_type=offline", flow.authorization_url)

    def test_loopback_code_catcher_and_state_check(self):
        import urllib.request as u
        from app.distribution.youtube import (LoopbackAuth,
                                              YouTubeUploadFailed)
        flow = LoopbackAuth("cid", host="127.0.0.1", port=0)
        self.addCleanup(flow._server.server_close)
        import threading
        def visit(url):
            with u.urlopen(url) as response:
                response.read()
        t = threading.Thread(target=visit, args=(
            flow.redirect_uri + f"/?code=abc&state={flow.state}",), daemon=True)
        t.start()
        self.assertEqual(flow.wait_for_code(timeout=10), "abc")
        t.join(timeout=5)
        self.assertFalse(t.is_alive())
        flow2 = LoopbackAuth("cid", host="127.0.0.1", port=0)
        self.addCleanup(flow2._server.server_close)
        t2 = threading.Thread(target=visit, args=(
            flow2.redirect_uri + "/?code=abc&state=WRONG",), daemon=True)
        t2.start()
        with self.assertRaises(YouTubeUploadFailed):
            flow2.wait_for_code(timeout=10)
        t2.join(timeout=5)
        self.assertFalse(t2.is_alive())

    def test_publish_confirmed_returns_video_id(self):
        pub = YouTubePublisher("cid", "rt")
        calls = []

        class FakeResp:
            def __init__(self, payload=b"", headers=None):
                self._payload = payload
                self.headers = headers or {}
            def read(self):
                return self._payload
            def __enter__(self):
                return self
            def __exit__(self, *a):
                return False

        def fake_urlopen(req, timeout=None):
            calls.append(req.full_url)
            if "token" in req.full_url:
                return FakeResp(b'{"access_token": "at"}')
            if "upload/youtube" in req.full_url:
                return FakeResp(headers={"Location": "https://upload/session/1"})
            return FakeResp(b'{"id": "vid123"}')

        with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            with mock.patch("os.path.getsize", return_value=4):
                with mock.patch("builtins.open",
                                mock.mock_open(read_data=b"data")):
                    self.assertEqual(
                        pub.publish("/tmp/v.mp4", "Title"), "vid123")
        self.assertTrue(any("device" not in u for u in calls))

    def test_invalid_privacy_rejected(self):
        pub = YouTubePublisher("cid", "rt")
        with self.assertRaises(ValueError):
            pub.publish("/tmp/v.mp4", "t", privacy="friends")


if __name__ == "__main__":
    unittest.main()


class UploadStageTests(unittest.TestCase):
    def test_auth_failure_is_definitive(self):
        import urllib.error
        pub = YouTubePublisher("cid", "rt")
        with mock.patch("urllib.request.urlopen",
                        side_effect=urllib.error.URLError("down")):
            with self.assertRaises(YouTubeUploadFailed) as cm:
                pub._access_token()
        self.assertEqual(cm.exception.stage, "auth")
        self.assertFalse(cm.exception.uncertain)

    def test_put_streams_file_and_is_uncertain_on_failure(self):
        import io
        pub = YouTubePublisher("cid", "rt")
        sent = []

        class R:
            def __init__(self, payload=b"", headers=None):
                self._p = payload
                self.headers = headers or {}
            def read(self):
                return self._p
            def __enter__(self):
                return self
            def __exit__(self, *a):
                return False

        def fake_urlopen(req, timeout=None):
            if "token" in req.full_url:
                return R(b'{"access_token": "at"}')
            if "upload/youtube" in req.full_url:
                return R(headers={"Location": "https://up/s/1"})
            sent.append(req)
            raise OSError("connection reset")

        data = io.BytesIO(b"x" * 10000)
        with mock.patch("urllib.request.urlopen",
                        side_effect=fake_urlopen):
            with mock.patch("os.path.getsize", return_value=10000):
                with mock.patch("builtins.open",
                                mock.mock_open(read_data=b"")) as m:
                    m.return_value = data
                    with self.assertRaises(YouTubeUploadFailed) as cm:
                        pub.publish("/tmp/v.mp4", "t")
        exc = cm.exception
        self.assertEqual(exc.stage, "upload")
        self.assertTrue(exc.uncertain)
        put = sent[0]
        # streamed: the file object itself, not a bytes copy in RAM
        self.assertTrue(hasattr(put.data, "read"))
        self.assertEqual(put.headers.get("Content-length"), "10000")


class EnvFirstPublisherTests(unittest.TestCase):
    """NOVA_YOUTUBE_* env injection wins as a complete set; partial env
    is refused rather than mixed with state-dir files."""

    def _state(self, tmp, cid="file-cid", rtok="file-rtok", sec="file-sec"):
        import pathlib
        base = pathlib.Path(tmp)
        if cid is not None:
            (base / "youtube_client_id").write_text(cid)
        if rtok is not None:
            (base / "youtube_refresh_token").write_text(rtok)
        if sec is not None:
            (base / "youtube_client_secret").write_text(sec)
        return str(base)

    def test_complete_env_wins_over_files(self):
        import tempfile
        from app.distribution.youtube import publisher_from_env_or_state
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = self._state(tmp)
            env = {"NOVA_YOUTUBE_CLIENT_ID": "env-cid",
                   "NOVA_YOUTUBE_CLIENT_SECRET": "env-sec",
                   "NOVA_YOUTUBE_REFRESH_TOKEN": "env-rtok"}
            with mock.patch.dict("os.environ", env):
                pub = publisher_from_env_or_state(state_dir)
        self.assertEqual(pub.client_id, "env-cid")
        self.assertEqual(pub.refresh_token, "env-rtok")
        self.assertEqual(pub.client_secret, "env-sec")

    def test_partial_env_refused_not_mixed(self):
        import tempfile
        from app.distribution.youtube import publisher_from_env_or_state
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = self._state(tmp)
            # only client id injected: refresh token must NOT be read
            # from the state file to complete the set
            with mock.patch.dict("os.environ",
                                 {"NOVA_YOUTUBE_CLIENT_ID": "env-cid"},
                                 clear=False):
                import os
                os.environ.pop("NOVA_YOUTUBE_REFRESH_TOKEN", None)
                os.environ.pop("NOVA_YOUTUBE_CLIENT_SECRET", None)
                with self.assertRaises(YouTubeNotConfigured):
                    publisher_from_env_or_state(state_dir)

    def test_no_env_falls_back_to_files(self):
        import os
        import tempfile
        from app.distribution.youtube import publisher_from_env_or_state
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = self._state(tmp)
            saved = {k: os.environ.pop(k, None) for k in (
                "NOVA_YOUTUBE_CLIENT_ID", "NOVA_YOUTUBE_CLIENT_SECRET",
                "NOVA_YOUTUBE_REFRESH_TOKEN")}
            try:
                pub = publisher_from_env_or_state(state_dir)
            finally:
                for k, v in saved.items():
                    if v is not None:
                        os.environ[k] = v
        self.assertEqual(pub.client_id, "file-cid")
        self.assertEqual(pub.refresh_token, "file-rtok")

    def test_no_env_no_files_is_unconfigured(self):
        import os
        import tempfile
        from app.distribution.youtube import publisher_from_env_or_state
        with tempfile.TemporaryDirectory() as tmp:
            saved = {k: os.environ.pop(k, None) for k in (
                "NOVA_YOUTUBE_CLIENT_ID", "NOVA_YOUTUBE_CLIENT_SECRET",
                "NOVA_YOUTUBE_REFRESH_TOKEN")}
            try:
                with self.assertRaises(YouTubeNotConfigured):
                    publisher_from_env_or_state(tmp)
            finally:
                for k, v in saved.items():
                    if v is not None:
                        os.environ[k] = v

class FailClosedEnvTests(unittest.TestCase):
    def test_empty_or_missing_each_value_refuses_without_file_fallback(self):
        import tempfile
        from app.distribution.youtube import publisher_from_env_or_state
        keys = ('NOVA_YOUTUBE_CLIENT_ID', 'NOVA_YOUTUBE_CLIENT_SECRET', 'NOVA_YOUTUBE_REFRESH_TOKEN')
        with tempfile.TemporaryDirectory() as d:
            for key in keys:
                for bad in ('', '   ', None):
                    env = dict.fromkeys(keys, 'value')
                    if bad is None:
                        del env[key]
                    else:
                        env[key] = bad
                    with self.subTest(key=key, bad=bad), mock.patch.dict('os.environ', env, clear=True), \
                         mock.patch('app.distribution.youtube.publisher_from_state', side_effect=AssertionError('fallback')):
                        with self.assertRaises(YouTubeNotConfigured):
                            publisher_from_env_or_state(d)

    def test_post_put_without_nonempty_id_is_uncertain(self):
        import io
        import json
        for payload in ({}, {'id': ''}, {'id': None}, {'id': '   '}, {'id': 12}, []):
            responses = [_resp({'access_token': 'token'}),
                         _resp({}, {'Location': 'https://upload/session'}), _resp(payload)]
            with self.subTest(payload=payload), \
                 mock.patch('urllib.request.urlopen', side_effect=responses), \
                 mock.patch('os.path.getsize', return_value=4), \
                 mock.patch('builtins.open', mock.mock_open(read_data=b'data')):
                with self.assertRaises(YouTubeUploadFailed) as exc:
                    YouTubePublisher('cid', 'rt').publish('/tmp/v.mp4', 'title')
                self.assertEqual(exc.exception.stage, 'upload')
                self.assertTrue(exc.exception.uncertain)
