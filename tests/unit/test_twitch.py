import unittest

from app.integrations.twitch.client import (TwitchAuthError, TwitchClient,
                                            TwitchError)


def make_transport(routes):
    calls = []

    def transport(method, url, headers, body):
        calls.append((method, url))
        for prefix, response in routes.items():
            if url.startswith(prefix):
                status, data = response
                if callable(data):
                    data = data()
                return status, data
        return 404, {}

    transport.calls = calls
    return transport


TOKEN_OK = (200, {"access_token": "tok1", "expires_in": 3600})


class TwitchClientTests(unittest.TestCase):
    def test_requires_credentials(self):
        with self.assertRaises(TwitchAuthError):
            TwitchClient("", "")

    def test_resolve_user(self):
        t = make_transport({
            "https://id.twitch.tv": TOKEN_OK,
            "https://api.twitch.tv/helix/users": (
                200, {"data": [{"id": "42", "login": "someone",
                                "display_name": "Someone"}]}),
        })
        client = TwitchClient("cid", "secret", transport=t)
        user = client.resolve_user("Someone")
        self.assertEqual(user.id, "42")
        self.assertEqual(user.login, "someone")

    def test_resolve_unknown_user_returns_none(self):
        t = make_transport({
            "https://id.twitch.tv": TOKEN_OK,
            "https://api.twitch.tv/helix/users": (200, {"data": []}),
        })
        client = TwitchClient("cid", "secret", transport=t)
        self.assertIsNone(client.resolve_user("nobody_here"))

    def test_invalid_username_rejected(self):
        client = TwitchClient("cid", "secret", transport=make_transport({}))
        with self.assertRaises(TwitchError):
            client.resolve_user("bad name!")

    def test_offline_returns_none(self):
        t = make_transport({
            "https://id.twitch.tv": TOKEN_OK,
            "https://api.twitch.tv/helix/streams": (200, {"data": []}),
        })
        client = TwitchClient("cid", "secret", transport=t)
        self.assertIsNone(client.get_stream("someone"))

    def test_token_refreshed_on_401(self):
        calls = {"n": 0}

        def maybe_401():
            calls["n"] += 1
            return {} if calls["n"] > 1 else {"data": []}

        t = make_transport({
            "https://id.twitch.tv": (200, {"access_token": "tok",
                                           "expires_in": 3600}),
            "https://api.twitch.tv/helix/streams": (
                None, None),
        })

        def transport(method, url, headers, body):
            if url.startswith("https://id.twitch.tv"):
                return 200, {"access_token": "tok", "expires_in": 3600}
            if url.startswith("https://api.twitch.tv/helix/streams"):
                if headers["Authorization"] == "Bearer tok" and calls["n"] == 0:
                    calls["n"] += 1
                    return 401, {}
                return 200, {"data": [{"user_id": "42", "user_login": "x",
                                       "title": "", "game_name": "",
                                       "viewer_count": 1,
                                       "started_at": "now"}]}
            return 404, {}

        client = TwitchClient("cid", "secret", transport=transport)
        info = client.get_stream("x")
        self.assertIsNotNone(info)
        self.assertEqual(info.user_id, "42")


if __name__ == "__main__":
    unittest.main()
