"""Minimal dependency-free health/metrics HTTP endpoint.

Bound to localhost by default. Uses raw asyncio streams so the application
runs on AGT's Python with zero third-party packages.
"""
from __future__ import annotations

import asyncio
import json

from .lifecycle import HealthRegistry, ServiceContext
from .logging import get_logger

log = get_logger("health")


class HealthServer:
    def __init__(self, host: str, port: int, registry: HealthRegistry,
                 metrics=None):
        self.host = host
        self.port = port
        self.registry = registry
        self.metrics = metrics
        self._server: asyncio.AbstractServer | None = None

    async def _handle(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5)
            path = request_line.decode("latin-1").split(" ")[1] if request_line else "/"
            while await reader.readline() not in (b"\r\n", b"\n", b""):
                pass
            if path.startswith("/metrics") and self.metrics is not None:
                body = json.dumps(self.metrics()).encode()
            else:
                body = json.dumps(self.registry.snapshot()).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         b"Content-Length: %d\r\nConnection: close\r\n\r\n%s"
                         % (len(body), body))
            await writer.drain()
        except Exception:
            log.exception("health request failed")
        finally:
            writer.close()

    async def run(self, ctx: ServiceContext) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        ctx.heartbeat()
        async with self._server:
            while not ctx.stopping:
                ctx.heartbeat()
                await asyncio.sleep(ctx.service.heartbeat_seconds)


def collect_metrics(queue) -> dict:
    return {"jobs": queue.counts()}
