"""
Privacy-safe telemetry with out-of-band heartbeat.

Rules:
- Async, bounded, privacy-safe, droppable, non-blocking
- NEVER include: raw email, phone, name, company, fingerprint, reversible identity tokens
- Allowed: decision class, latency bucket, source type, error category, counts, versions

Out-of-Band Heartbeat (v4):
  Every 60s → StatsD/UDP → infrastructure monitoring (independent of application)
  heartbeat is INDEPENDENT of the telemetry queue
"""
from __future__ import annotations

import asyncio
import logging
import socket
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from lead_entry_guard.core.models import AuditMeta, DecisionClass, PolicyVersions, SourceType

logger = logging.getLogger(__name__)


class LatencyBucket(str, Enum):
    FAST = "fast"         # < 10ms
    NORMAL = "normal"     # 10–50ms
    SLOW = "slow"         # 50–200ms
    VERY_SLOW = "very_slow"  # > 200ms


def latency_bucket(ms: float) -> LatencyBucket:
    if ms < 10:
        return LatencyBucket.FAST
    if ms < 50:
        return LatencyBucket.NORMAL
    if ms < 200:
        return LatencyBucket.SLOW
    return LatencyBucket.VERY_SLOW


@dataclass
class TelemetryEvent:
    """Privacy-safe telemetry event. NO PII, NO fingerprints."""
    tenant_id: str  # opaque ID only
    decision: DecisionClass
    source_type: SourceType
    latency_bucket: LatencyBucket
    duplicate_check_skipped: bool
    degraded_mode: bool
    policy_version: str
    ruleset_version: str
    config_version: str
    error_category: str | None = None


class TelemetryQueue:
    """
    Bounded async telemetry queue.
    Queue full → drop event + increment dropped counter (via StatsD direct, not queue).
    """

    def __init__(
        self,
        max_size: int = 50_000,
        warn_threshold: float = 0.80,
        statsd_client: "StatsDClient | None" = None,
    ) -> None:
        self._queue: asyncio.Queue[TelemetryEvent] = asyncio.Queue(maxsize=max_size)
        self._max_size = max_size
        self._warn_threshold = warn_threshold
        self._statsd = statsd_client
        self._dropped = 0

    def enqueue(self, event: TelemetryEvent) -> None:
        """Non-blocking enqueue. Drops if full."""
        try:
            self._queue.put_nowait(event)
            fill = self._queue.qsize() / self._max_size
            if fill > self._warn_threshold:
                logger.warning(
                    "P2_ALERT: Telemetry queue near capacity",
                    extra={"fill_ratio": fill},
                )
        except asyncio.QueueFull:
            self._dropped += 1
            # Write dropped counter via StatsD directly — NOT through this queue
            if self._statsd:
                self._statsd.increment("telemetry.dropped")
            logger.debug("Telemetry event dropped (queue full)")

    async def dequeue(self) -> TelemetryEvent:
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()

    @property
    def dropped_count(self) -> int:
        return self._dropped

    @property
    def fill_ratio(self) -> float:
        return self._queue.qsize() / self._max_size


class StatsDClient:
    """Minimal StatsD UDP client."""

    def __init__(self, host: str = "localhost", port: int = 8125, prefix: str = "leg") -> None:
        self._host = host
        self._port = port
        self._prefix = prefix
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def _send(self, metric: str, value: str) -> None:
        try:
            data = f"{self._prefix}.{metric}:{value}".encode()
            self._sock.sendto(data, (self._host, self._port))
        except Exception:
            pass  # UDP — fire and forget, never block

    def increment(self, metric: str, value: int = 1) -> None:
        self._send(metric, f"{value}|c")

    def gauge(self, metric: str, value: float) -> None:
        self._send(metric, f"{value}|g")

    def timing(self, metric: str, ms: float) -> None:
        self._send(metric, f"{ms}|ms")


class TelemetryExporter:
    """Async exporter that drains the telemetry queue."""

    def __init__(
        self,
        queue: TelemetryQueue,
        statsd: StatsDClient,
    ) -> None:
        self._queue = queue
        self._statsd = statsd

    async def export_loop(self) -> None:
        while True:
            event = await self._queue.dequeue()
            try:
                self._export(event)
            except Exception:
                logger.exception("Telemetry export error")
            finally:
                self._queue.task_done()

    def _export(self, event: TelemetryEvent) -> None:
        self._statsd.increment(f"decision.{event.decision.value.lower()}")
        self._statsd.increment(f"latency.{event.latency_bucket.value}")
        self._statsd.increment(f"source.{event.source_type.value.lower()}")
        if event.duplicate_check_skipped:
            self._statsd.increment("duplicate_check_skipped")
        if event.degraded_mode:
            self._statsd.increment("degraded_mode")


class OOBHeartbeat:
    """
    Out-of-Band heartbeat via StatsD/UDP every 60s.
    INDEPENDENT of application telemetry queue.
    Infrastructure can alert on missing heartbeat.
    """

    def __init__(
        self,
        statsd: StatsDClient,
        telemetry_queue: TelemetryQueue,
        interval_seconds: int = 60,
    ) -> None:
        self._statsd = statsd
        self._queue = telemetry_queue
        self._interval = interval_seconds
        self._start_time = time.monotonic()

    async def heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            self._send_heartbeat()

    def _send_heartbeat(self) -> None:
        uptime = time.monotonic() - self._start_time
        self._statsd.gauge("system_alive", 1)
        self._statsd.gauge("process_uptime_seconds", uptime)
        self._statsd.gauge("telemetry_queue_fill_ratio", self._queue.fill_ratio)
        self._statsd.increment("telemetry_queue_full_events", self._queue.dropped_count)
        logger.debug("OOB heartbeat sent", extra={"uptime_seconds": uptime})
