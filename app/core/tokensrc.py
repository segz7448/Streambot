"""Telegram bot token source: vault-injected env first, file fallback.

NOVA_TELEGRAM_TOKEN (injected through the deployment secrets payload from
AGT Vault) is the target source. While it is absent the legacy file named
by NOVA_TOKEN_FILE keeps working, so migration never strands the bot. A
PRESENT-but-empty env value fails closed (no silent fallback to the file):
a half-provisioned vault must be noticed, not masked. source() reports
only which source is in use, never a value or a length.
"""
from __future__ import annotations

import os

ENV_TOKEN = "NOVA_TELEGRAM_TOKEN"
ENV_FILE = "NOVA_TOKEN_FILE"


class TokenUnavailable(RuntimeError):
    pass


def source() -> str:
    """'env', 'file' or 'none'. Raises TokenUnavailable on an empty env."""
    if ENV_TOKEN in os.environ:
        if not os.environ[ENV_TOKEN].strip():
            raise TokenUnavailable(
                f"{ENV_TOKEN} is present but empty; refusing the file fallback")
        return "env"
    path = os.environ.get(ENV_FILE, "")
    return "file" if path and os.path.isfile(path) else "none"


def read_token(token_path: str | None = None) -> str:
    src = source()
    if src == "env":
        return os.environ[ENV_TOKEN].strip()
    path = token_path or os.environ.get(ENV_FILE, "")
    if not path:
        raise TokenUnavailable("no Telegram token source configured")
    with open(path) as fh:
        tok = fh.read().strip()
    if not tok:
        raise TokenUnavailable("token file is empty")
    return tok
