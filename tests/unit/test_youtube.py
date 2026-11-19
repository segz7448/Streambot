import unittest
from unittest import mock

from app.distribution.youtube import (YouTubeNotConfigured,
                                      YouTubePublisher)


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
        import threading
        t = threading.Thread(target=lambda: u.urlopen(
            flow.redirect_uri + f"/?code=abc&state={flow.state}"),
            daemon=True)
        t.start()
        self.assertEqual(flow.wait_for_code(timeout=10), "abc")
        flow2 = LoopbackAuth("cid", host="127.0.0.1", port=0)
        t2 = threading.Thread(target=lambda: u.urlopen(
            flow2.redirect_uri + "/?code=abc&state=WRONG"), daemon=True)
        t2.start()
        with self.assertRaises(YouTubeUploadFailed):
            flow2.wait_for_code(timeout=10)

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
