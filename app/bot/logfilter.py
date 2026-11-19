"""Concise logging for recoverable Telegram network errors.

On mobile networks a long-poll connection is occasionally interrupted
mid-read; python-telegram-bot recovers via its retry loop, but its
default error callback logs a full traceback for every interruption,
which reads as a crash in deployment logs.

This filter condenses ONLY the known recoverable record: logger
"telegram.ext.Updater", message "Exception happened while polling for
updates.", with a telegram.error.NetworkError attached. Grounded in PTB
22.8 vendored source (telegram/ext/_updater.py default_error_callback,
network_retry_loop with max_retries=-1 for polling). Fatal paths
(InvalidToken, exhausted bootstrap retries, critical records) use
different messages or loggers and always keep their full tracebacks.

The condensed line carries only the exception class name, never the
exception text: httpx error strings can embed the request URL, which
contains the bot token.
"""
from __future__ import annotations

import logging

try:
    from telegram.error import NetworkError
except ImportError:  # tests without the framework installed
    NetworkError = ()  # type: ignore[assignment]

_TARGET_LOGGER = "telegram.ext.Updater"
_TARGET_MESSAGE = "Exception happened while polling for updates."


class NetworkRetryCondenser(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        exc = record.exc_info
        if (exc and NetworkError and issubclass(exc[0], NetworkError)
                and record.name == _TARGET_LOGGER
                and record.getMessage() == _TARGET_MESSAGE):
            record.msg = ("recoverable telegram network error (%s) "
                          "while polling; retrying")
            record.args = (exc[0].__name__,)
            record.exc_info = None
            record.exc_text = None
            record.levelno = logging.WARNING
            record.levelname = "WARNING"
        return True


def install(logger_name: str = _TARGET_LOGGER) -> None:
    # logger (not handler) filters only see records logged directly on
    # that logger, so this must attach to the exact emitting logger
    logging.getLogger(logger_name).addFilter(NetworkRetryCondenser())
