import unittest

from app.bot.commands import CommandRouter, UnauthorizedError, parse_command


class ParseTests(unittest.TestCase):
    def test_parse_plain_command(self):
        cmd = parse_command("/start")
        self.assertEqual(cmd.name, "start")

    def test_parse_with_bot_suffix_and_args(self):
        cmd = parse_command("/addsource@NovaClipBot shroud")
        self.assertEqual(cmd.name, "addsource")
        self.assertEqual(cmd.args, "shroud")

    def test_non_command(self):
        self.assertIsNone(parse_command("hello there"))


class RouterTests(unittest.TestCase):
    def test_admin_only(self):
        router = CommandRouter(admin_ids=[111])
        with self.assertRaises(UnauthorizedError):
            router.dispatch("/start", user_id=999)

    def test_start_and_help(self):
        router = CommandRouter(admin_ids=[111])
        self.assertIn("WELCOME", router.dispatch("/start", user_id=111))
        # help is generated from registered handlers: every advertised
        # command must dispatch, and nothing unregistered is advertised
        help_text = router.dispatch("/help", user_id=111)
        advertised = [ln.split(" - ")[0][1:] for ln in help_text.splitlines()
                      if ln.startswith("/")]
        self.assertEqual(sorted(advertised), sorted(router.handlers))
        for name in advertised:
            self.assertNotIn("Unknown command",
                             router.dispatch("/" + name, user_id=111))

    def test_unknown_command(self):
        router = CommandRouter(admin_ids=[111])
        self.assertIn("Unknown command",
                      router.dispatch("/frobnicate", user_id=111))

    def test_plain_text_guidance(self):
        router = CommandRouter(admin_ids=[111])
        self.assertIn("/help", router.dispatch("hi", user_id=111))


if __name__ == "__main__":
    unittest.main()
