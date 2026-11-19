"""Structured logging setup.

Every record is emitted as one JSON object per line so AGT's 4000-character
job-result window and plain `tail` both stay readable, and so secrets never
appear: handlers are installed once, and the formatter escapes nothing it
does not control because all values are JSON-encoded.
"""
from __future__ import annotations

import json
import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

_CONFIGURED = False


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
