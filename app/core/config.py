"""Configuration loading and validation.

Configuration comes from the process environment, optionally overlaid by a
``.env`` style file (KEY=VALUE lines). Secrets are read from the environment
only and are never logged: ``redacted()`` masks every value whose key looks
sensitive. Nothing secret may be hard-coded here or committed to git.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Mapping, Optional

_SECRET_MARKERS = ("TOKEN", "SECRET", "KEY", "PASSWORD")


def _parse_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def _is_secret(name: str) -> bool:
    upper = name.upper()
    return any(marker in upper for marker in _SECRET_MARKERS)


@dataclass
class AppConfig:
    """Runtime configuration. Durations in seconds, sizes in megabytes."""

    data_dir: str = "data"
    log_dir: str = "logs"
    db_path: str = "data/nova.db"
    log_level: str = "INFO"
    health_host: str = "127.0.0.1"
    health_port: int = 8790
    environment: str = "development"

    buffer_segment_seconds: int = 5
    buffer_pre_event_seconds: int = 30
    buffer_post_event_seconds: int = 30
    buffer_max_storage_mb: int = 2048

    max_concurrent_ffmpeg: int = 1
    max_concurrent_ai_jobs: int = 1
    processing_resolution: str = "640x360"

    telegram_bot_token: str = ""
    telegram_admin_ids: list[int] = field(default_factory=list)

    twitch_client_id: str = ""
    twitch_client_secret: str = ""

    api_auth_token: str = ""

    @classmethod
    def load(cls, env_file: Optional[str] = None,
             environ: Optional[Mapping[str, str]] = None) -> "AppConfig":
        environ = dict(os.environ if environ is None else environ)
        if env_file:
            file_values = _parse_env_file(Path(env_file))
            file_values.update(environ)  # real environment wins
            environ = file_values

        def get(name: str, default: str) -> str:
            return environ.get(f"NOVA_{name}", default)

        admin_raw = get("TELEGRAM_ADMIN_IDS", "").strip()
        admin_ids = [int(x) for x in admin_raw.replace(",", " ").split() if x]

        cfg = cls(
            data_dir=get("DATA_DIR", "data"),
            log_dir=get("LOG_DIR", "logs"),
            db_path=get("DB_PATH", "data/nova.db"),
            log_level=get("LOG_LEVEL", "INFO").upper(),
            health_host=get("HEALTH_HOST", "127.0.0.1"),
            health_port=int(get("HEALTH_PORT", "8790")),
            environment=get("ENVIRONMENT", "development"),
            buffer_segment_seconds=int(get("BUFFER_SEGMENT_SECONDS", "5")),
            buffer_pre_event_seconds=int(get("BUFFER_PRE_EVENT_SECONDS", "30")),
            buffer_post_event_seconds=int(get("BUFFER_POST_EVENT_SECONDS", "30")),
            buffer_max_storage_mb=int(get("BUFFER_MAX_STORAGE_MB", "2048")),
            max_concurrent_ffmpeg=int(get("MAX_CONCURRENT_FFMPEG", "1")),
            max_concurrent_ai_jobs=int(get("MAX_CONCURRENT_AI_JOBS", "1")),
            processing_resolution=get("PROCESSING_RESOLUTION", "640x360"),
            telegram_bot_token=get("TELEGRAM_BOT_TOKEN", ""),
            telegram_admin_ids=admin_ids,
            twitch_client_id=get("TWITCH_CLIENT_ID", ""),
            twitch_client_secret=get("TWITCH_CLIENT_SECRET", ""),
            api_auth_token=get("API_AUTH_TOKEN", ""),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.buffer_segment_seconds <= 0:
            raise ValueError("BUFFER_SEGMENT_SECONDS must be positive")
        if self.buffer_max_storage_mb < 64:
            raise ValueError("BUFFER_MAX_STORAGE_MB must be at least 64")
        if self.max_concurrent_ffmpeg < 1 or self.max_concurrent_ai_jobs < 1:
            raise ValueError("concurrency limits must be at least 1")
        if self.log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError(f"invalid LOG_LEVEL: {self.log_level}")
        if not (0 <= self.health_port <= 65535):
            raise ValueError("HEALTH_PORT out of range")

    def redacted(self) -> dict[str, object]:
        out: dict[str, object] = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if _is_secret(f.name):
                out[f.name] = "set" if value else "unset"
            else:
                out[f.name] = value
        return out
