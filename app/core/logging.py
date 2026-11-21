"""Structured logging setup.

Every record is emitted as one JSON object per line so AGT's 4000-character
job-result window and plain `tail` both stay readable, and so secrets never
appear: handlers are installed once, and the formatter escapes nothing it
does not control because all values are JSON-encoded.
"""
from __future__ import annotations

import json
import logging
import re
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

_CONFIGURED = False


# Telegram bot tokens look like <digits>:<35 url-safe chars>. Any such
# string, and any /bot<token>/ URL segment, is masked before it is written.
_TOKEN_RE = re.compile(r"\d{6,}:[A-Za-z0-9_-]{30,}")


def redact(text: str) -> str:
    return _TOKEN_RE.sub("[redacted-token]", text)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        payload = {k: redact(v) if isinstance(v, str) else v
                   for k, v in payload.items()}
        return json.dumps(payload, separators=(",", ":"))


def setup_logging(log_dir: str, level: str = "INFO") -> logging.Logger:
    global _CONFIGURED
    root = logging.getLogger("nova")
    if _CONFIGURED:
        return root
    root.setLevel(getattr(logging, level, logging.INFO))
    root.propagate = False

    formatter = JsonFormatter()
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        Path(log_dir) / "nova.log", maxBytes=2_000_000, backupCount=3,
        encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    _CONFIGURED = True
    return root


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"nova.{name}")


def setup_service_logging(log_dir: str, level: str = "INFO") -> logging.Logger:
    """Logging for the Telegram service entrypoint (app.bot.service).

    Before this the service never installed handlers, so INFO records were
    dropped and only WARNING+ reached a stderr pipe nobody can read. This
    writes redacted JSON lines to <log_dir>/nova.log (rotating) and stderr,
    and routes the framework's own "telegram" logger the same way so
    polling start-up and failures are visible. httpx/httpcore are held at
    WARNING because their INFO lines embed the request URL (bot token).
    """
    root = setup_logging(log_dir, level)
    tg = logging.getLogger("telegram")
    if not tg.handlers:
        for h in root.handlers:
            tg.addHandler(h)
        tg.setLevel(getattr(logging, level, logging.INFO))
        tg.propagate = False
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return root
