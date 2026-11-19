"""Redaction of stream credentials in logs.

RTMP destinations embed private stream keys (rtmp://host/app/<key>).
Those keys must never reach logs, public files, or any output surface.
This module redacts rtmp(s) URLs down to scheme + host in any log
record. Note the residual boundary: the key still appears in the ffmpeg
process argv, which on Android is visible only to same-UID processes
(app sandbox isolation); the engine's own logs and state files stay
clean. State files holding destinations are 0600 in the state dir and
never committed to the repository.
"""
from __future__ import annotations

import logging
import re

_RTMP = re.compile(r"rtmps?://([^/\s\"']+)(/[^\s\"']*)?")

REDACTED = "redacted"


def redact_rtmp(text: str) -> str:
    """Replace any rtmp(s) URL with scheme://host/<redacted>; safe to
    apply to any string that might carry a destination URL."""
    return _RTMP.sub(lambda m: f"{m.group(0).split('://')[0]}://"
                               f"{m.group(1)}/<{REDACTED}>", text)


class RtmpKeyRedactor(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if "rtmp" in msg:
            # getMessage() is fully formatted; replace template + args
            record.msg = redact_rtmp(msg)
            record.args = ()
        return True


def install() -> None:
    logging.getLogger().addFilter(RtmpKeyRedactor())
