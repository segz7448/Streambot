import tempfile
import unittest
from pathlib import Path

from app.core.config import AppConfig


class ConfigTests(unittest.TestCase):
    def test_defaults_validate(self):
        cfg = AppConfig.load(environ={})
        self.assertEqual(cfg.buffer_segment_seconds, 5)
        self.assertEqual(cfg.max_concurrent_ffmpeg, 1)

    def test_env_override_and_admin_ids(self):
        cfg = AppConfig.load(environ={
            "NOVA_HEALTH_PORT": "9001",
            "NOVA_TELEGRAM_ADMIN_IDS": "111, 222",
            "NOVA_TELEGRAM_BOT_TOKEN": "secret-token",
        })
        self.assertEqual(cfg.health_port, 9001)
        self.assertEqual(cfg.telegram_admin_ids, [111, 222])

    def test_env_file_then_environ_wins(self):
        with tempfile.TemporaryDirectory() as tmp:
            env_file = Path(tmp) / ".env"
            env_file.write_text("NOVA_LOG_LEVEL=DEBUG\nNOVA_DATA_DIR=filedata\n")
            cfg = AppConfig.load(env_file=str(env_file),
                                 environ={"NOVA_DATA_DIR": "envdata"})
            self.assertEqual(cfg.log_level, "DEBUG")
            self.assertEqual(cfg.data_dir, "envdata")

    def test_invalid_values_rejected(self):
        with self.assertRaises(ValueError):
            AppConfig.load(environ={"NOVA_BUFFER_SEGMENT_SECONDS": "0"})
        with self.assertRaises(ValueError):
            AppConfig.load(environ={"NOVA_LOG_LEVEL": "CHATTY"})

    def test_secrets_redacted(self):
        cfg = AppConfig.load(environ={
            "NOVA_TELEGRAM_BOT_TOKEN": "abc123",
            "NOVA_API_AUTH_TOKEN": "",
        })
        view = cfg.redacted()
        self.assertEqual(view["telegram_bot_token"], "set")
        self.assertEqual(view["api_auth_token"], "unset")
        self.assertNotIn("abc123", repr(view))


if __name__ == "__main__":
    unittest.main()
