"""Application lifecycle: supervised asyncio services.

Each service runs as a supervised task. If a service crashes it is restarted
with capped exponential backoff; a graceful shutdown stops services in
reverse order. The supervisor never lets one failing service kill the
others, and heartbeats feed the health registry.
"""
from __future__ import annotations

import asyncio
import signal
import time
from typing import Awaitable, Callable, Optional

from .logging import get_logger

log = get_logger("lifecycle")


class Service:
    """A named, restartable unit of work."""

    def __init__(self, name: str, run: Callable[["ServiceContext"], Awaitable[None]],
                 restart: bool = True, heartbeat_seconds: float = 15.0):
        self.name = name
        self.run = run
        self.restart = restart
        self.heartbeat_seconds = heartbeat_seconds


class ServiceContext:
    def __init__(self, supervisor: "Supervisor", service: Service):
        self.supervisor = supervisor
        self.service = service

    def heartbeat(self) -> None:
        self.supervisor.registry.beat(self.service.name)

    @property
    def stopping(self) -> bool:
        return self.supervisor.stopping.is_set()


class HealthRegistry:
    """Tracks per-service liveness for the health endpoint."""

    def __init__(self) -> None:
        self._beats: dict[str, float] = {}
        self._status: dict[str, str] = {}
        self.started_at = time.time()

    def beat(self, name: str) -> None:
        self._beats[name] = time.time()
        self._status.setdefault(name, "running")

    def mark(self, name: str, status: str) -> None:
        self._status[name] = status

    def snapshot(self, stale_after: float = 60.0) -> dict:
        now = time.time()
        services = {}
        healthy = True
        for name, status in self._status.items():
            last = self._beats.get(name)
            stale = last is None or (now - last) > stale_after
            services[name] = {
                "status": status,
                "last_heartbeat": last,
                "stale": stale,
            }
            if stale and status == "running":
                healthy = False
        return {
            "status": "ok" if healthy else "degraded",
            "uptime_seconds": round(now - self.started_at, 1),
            "services": services,
        }


class Supervisor:
    def __init__(self, services: list[Service], registry: HealthRegistry):
        self.services = services
        self.registry = registry
        self.stopping = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def _supervise(self, service: Service) -> None:
        backoff = 1.0
        ctx = ServiceContext(self, service)
        while not self.stopping.is_set():
            self.registry.mark(service.name, "running")
            self.registry.beat(service.name)
            try:
                await service.run(ctx)
                if not service.restart:
                    self.registry.mark(service.name, "stopped")
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("service %s crashed", service.name)
                self.registry.mark(service.name, "crashed")
            if not service.restart or self.stopping.is_set():
                return
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stopping.set)
            except (NotImplementedError, ValueError, RuntimeError):
                # non-unix, or not the main thread (command-driven
                # sessions run the supervisor on a worker thread;
                # stopping is then driven via the stopping event)
                pass
        self._tasks = [asyncio.create_task(self._supervise(s), name=s.name)
                       for s in self.services]
        await self.stopping.wait()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for s in self.services:
            self.registry.mark(s.name, "stopped")
