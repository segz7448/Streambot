"""Concise logging for recoverable Telegram network errors.

On mobile networks a long-poll connection is occasionally interrupted
mid-read; python-telegram-bot's retry loop recovers, but logs the full
traceback each time, which reads as a crash in deployment logs. This
filter rewrites records whose exception is a recoverable
telegram.error.NetworkError into a single-line warning, keeping full
tracebacks for everything else (real failures stay visible).
"""
from __future__ import annotations

import logging

try:
    from telegram.error import NetworkError
except ImportError:  # tests without the framework installed
    NetworkError = ()  # type: ignore[assignment]


class NetworkRetryCondenser(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        exc = record.exc_info
        if exc and NetworkError and issubclass(exc[0], NetworkError):
            record.msg = ("recoverable telegram network error "
                          "(%s); polling continues")
            record.args = (exc[0].__name__,)
            record.exc_info = None
            record.exc_text = None
            record.levelno = logging.WARNING
            record.levelname = "WARNING"
        return True


def install(logger_name: str = "telegram") -> None:
    logging.getLogger(logger_name).addFilter(NetworkRetryCondenser())
