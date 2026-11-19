"""NOVA Clip Engine entrypoint.

Phase 002 scope: configuration, structured logging, database migrations,
job-queue recovery, health endpoint and the supervised lifecycle. Media,
Twitch, Telegram and distribution services attach to the same supervisor in
later phases without restructuring.
"""
from __future__ import annotations

import asyncio

from app.core.config import AppConfig
from app.core.database import connect, migrate
from app.core.health import HealthServer, collect_metrics
from app.core.jobs import JobQueue
from app.core.lifecycle import HealthRegistry, Service, Supervisor
from app.core.logging import get_logger, setup_logging

log = get_logger("main")


async def amain() -> None:
    config = AppConfig.load(env_file=".env")
    setup_logging(config.log_dir, config.log_level)
    log.info("nova clip engine starting env=%s config=%s",
             config.environment, config.redacted())

    conn = connect(config.db_path)
    applied = migrate(conn)
    if applied:
        log.info("database migrations applied: %s", applied)

    queue = JobQueue(conn)
    recovered = queue.recover_interrupted()
    if recovered:
        log.warning("recovered %d interrupted jobs", recovered)

    registry = HealthRegistry()
    services = [
        Service("health",
                HealthServer(config.health_host, config.health_port,
                             registry,
                             metrics=lambda: collect_metrics(queue)).run),
    ]
    await Supervisor(services, registry).run()
    log.info("nova clip engine stopped")


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
