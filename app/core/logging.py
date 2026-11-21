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


# Masking is pattern based, so it is a best-effort boundary for the
# credential shapes this service can actually emit, not generic secret
# safety. Covered: Telegram bot tokens (also with an encoded colon),
# Authorization / Proxy-Authorization / X-*-Token / Cookie header values,
# Bearer and Basic credentials, and credential-like URL or form parameters
# (token, access_token, refresh_token, client_secret, api_key, key,
# password, secret, sig, signature, X-Amz-*/X-Goog-* signed parameters).
_TOKEN_RE = re.compile(r"\d{6,}(?::|%3[Aa]|%253[Aa])[A-Za-z0-9_-]{20,}")
_BEARER_RE = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}")
_HEADER_RE = re.compile(
    r"(?i)(authorization|proxy-authorization|cookie|set-cookie|x-[a-z-]*"
    r"(?:token|secret|key|signature))(['\"]?\s*[:=]\s*['\"]?)[^\s,;'\"}]+"
    r"(?:\s+[^\s,;'\"}]+)?")
_PARAM_RE = re.compile(
    r"(?i)([?&;\s'\"]|^)((?:access_|refresh_|id_|client_|api_|auth_)?"
    r"(?:token|secret|key|password|passwd|sig|signature|credential|"
    r"x-amz-[a-z-]+|x-goog-[a-z-]+))(=|%3[Dd])[^&\s'\"#]+")


def redact(text: str) -> str:
    text = _TOKEN_RE.sub("[redacted-token]", text)
    text = _HEADER_RE.sub(r"\1\2[redacted]", text)
    text = _BEARER_RE.sub(r"\1 [redacted]", text)
    text = _PARAM_RE.sub(r"\1\2\3[redacted]", text)
    return text


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


_SERVICE_HANDLERS: list | None = None


class _PrivateRotatingFileHandler(RotatingFileHandler):
    """Rotating file whose active file is always created mode 0600."""

    def _open(self):
        import os
        fd = os.open(self.baseFilename,
                     os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        os.fchmod(fd, 0o600)
        return os.fdopen(fd, "a", encoding=self.encoding or "utf-8")

    def doRollover(self):
        import glob
        import os
        super().doRollover()
        for f in glob.glob(self.baseFilename + ".*"):
            try:
                os.chmod(f, 0o600)
            except OSError:
                pass


def setup_service_logging(log_dir: str, level: str = "INFO") -> logging.Logger:
    """Logging for the Telegram service entrypoint (app.bot.service).

    Singleton: the first successful call wins; later calls change nothing.
    Handlers are built first and attached only if all of them were created,
    so a failure (unwritable directory) leaves logging untouched. Existing
    handlers on the nova, telegram and Python root loggers are replaced so
    nothing unredacted can coexist, and lastResort is disabled. Output is
    redacted JSON lines to <log_dir>/nova.log (0600 files, 0700 directory,
    rotating) and stderr. httpx/httpcore stay at WARNING: their INFO lines
    carry request URLs. Redaction covers only the shapes listed at redact().
    """
    global _SERVICE_HANDLERS
    import os
    import sys
    root = logging.getLogger("nova")
    if _SERVICE_HANDLERS is not None:
        return root
    lvl = getattr(logging, level, logging.INFO)
    formatter = JsonFormatter()
    handlers: list = []
    try:
        os.makedirs(log_dir, mode=0o700, exist_ok=True)
        os.chmod(log_dir, 0o700)
        fh = _PrivateRotatingFileHandler(
            os.path.join(log_dir, "nova.log"), maxBytes=2_000_000,
            backupCount=3, encoding="utf-8")
        handlers.append(fh)
        ch = logging.StreamHandler()
        handlers.append(ch)
        for h in handlers:
            h.setFormatter(formatter)
    except Exception:
        for h in handlers:
            h.close()
        raise
    for name, lv, prop in (("nova", lvl, False), ("telegram", lvl, False),
                           ("", logging.WARNING, True)):
        lg = logging.getLogger(name)
        for old in list(lg.handlers):
            lg.removeHandler(old)
        for h in handlers:
            lg.addHandler(h)
        lg.setLevel(lv)
        if name:
            lg.propagate = prop
    for noisy in ("httpx", "httpcore"):
        lg = logging.getLogger(noisy)
        lg.setLevel(logging.WARNING)
        for old in list(lg.handlers):
            lg.removeHandler(old)
        lg.propagate = True
    logging.lastResort = None

    def _hook(et, ev, tb):
        root.critical("uncaught exception", exc_info=(et, ev, tb))
    sys.excepthook = _hook
    _SERVICE_HANDLERS = handlers
    return root
