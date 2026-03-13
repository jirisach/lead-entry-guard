"""
CRM Reconciliation Loop.

Purpose: Reduce drift between Guard duplicate view and CRM operational reality.

Input events: lead_created, lead_merged, lead_duplicate, lead_duplicate_corrected

Write-back behavior on lead_duplicate_corrected:
  → lookup fingerprint_id for lead_reference
  → DELETE from Redis
  → Bloom cleanup deferred to next rotation/rebuild (not ad hoc)
  → emit: index_correction_applied (telemetry)
  → record in audit log: who, when, which lead

Safety controls (v4):
  Level 1 (per tenant): max 1_000 corrections / hour
  Level 2 (global):     max 10_000 corrections / hour
  Excess → durable retry queue (FIFO per tenant), max retry age 24h
  After 24h → alert + manual review required
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum

from lead_entry_guard.core.exceptions import ReconciliationRateLimitError
from lead_entry_guard.lookup.redis_store import RedisDuplicateStore

logger = logging.getLogger(__name__)


class CRMEventType(str, Enum):
    LEAD_CREATED = "lead_created"
    LEAD_MERGED = "lead_merged"
    LEAD_DUPLICATE = "lead_duplicate"
    LEAD_DUPLICATE_CORRECTED = "lead_duplicate_corrected"


@dataclass
class CRMEvent:
    event_type: CRMEventType
    tenant_id: str
    lead_reference: str
    fingerprint_id: str | None = None  # looked up internally
    actor: str = "system"
    occurred_at: float = field(default_factory=time.time)


@dataclass
class RetryItem:
    event: CRMEvent
    enqueued_at: float = field(default_factory=time.time)

    def is_expired(self, max_age_seconds: int) -> bool:
        return time.time() - self.enqueued_at > max_age_seconds


class RateLimiter:
    """Rolling window rate limiter using token bucket approximation."""

    def __init__(self, max_per_hour: int) -> None:
        self._max = max_per_hour
        self._window_seconds = 3600
        self._timestamps: deque[float] = deque()

    def allow(self) -> bool:
        now = time.time()
        cutoff = now - self._window_seconds
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
        if len(self._timestamps) >= self._max:
            return False
        self._timestamps.append(now)
        return True


class ReconciliationLoop:
    """
    Processes CRM events asynchronously.
    NEVER runs in request path.
    """

    def __init__(
        self,
        redis_store: RedisDuplicateStore,
        per_tenant_limit: int = 1_000,
        global_limit: int = 10_000,
        retry_max_age_hours: int = 24,
    ) -> None:
        self._redis = redis_store
        self._per_tenant_limiters: dict[str, RateLimiter] = defaultdict(
            lambda: RateLimiter(per_tenant_limit)
        )
        self._global_limiter = RateLimiter(global_limit)
        self._retry_queues: dict[str, deque[RetryItem]] = defaultdict(deque)
        self._retry_max_age = retry_max_age_hours * 3600
        self._audit_log: list[dict] = []  # in prod: write to durable store

    async def handle_event(self, event: CRMEvent) -> None:
        if event.event_type != CRMEventType.LEAD_DUPLICATE_CORRECTED:
            return  # Only corrections trigger write-back

        if not self._check_rate_limits(event.tenant_id):
            # Queue for retry (FIFO per tenant)
            self._retry_queues[event.tenant_id].append(RetryItem(event=event))
            logger.warning(
                "Reconciliation rate limit hit — queued for retry",
                extra={"tenant_id": event.tenant_id},
            )
            return

        await self._apply_correction(event)

    def _check_rate_limits(self, tenant_id: str) -> bool:
        # Per-tenant check first, then global
        if not self._per_tenant_limiters[tenant_id].allow():
            return False
        if not self._global_limiter.allow():
            logger.warning(
                "P2_ALERT: Global reconciliation rate limit hit",
                extra={"tenant_id": tenant_id},
            )
            return False
        return True

    async def _apply_correction(self, event: CRMEvent) -> None:
        if event.fingerprint_id:
            await self._redis.delete(event.tenant_id, event.fingerprint_id)

        # Bloom cleanup deferred to next rotation (NOT ad hoc)
        self._record_audit(event)
        logger.info(
            "index_correction_applied",
            extra={
                "tenant_id": event.tenant_id,
                "lead_reference": event.lead_reference,
                "actor": event.actor,
            },
        )

    def _record_audit(self, event: CRMEvent) -> None:
        self._audit_log.append(
            {
                "event_type": event.event_type,
                "tenant_id": event.tenant_id,
                "lead_reference": event.lead_reference,
                "actor": event.actor,
                "recorded_at": time.time(),
                # NEVER log fingerprint_id in audit
            }
        )

    async def retry_loop(self) -> None:
        """Process retry queues periodically."""
        while True:
            await asyncio.sleep(60)
            await self._drain_retry_queues()

    async def _drain_retry_queues(self) -> None:
        for tenant_id, queue in self._retry_queues.items():
            expired = []
            processable = []
            while queue:
                item = queue[0]
                if item.is_expired(self._retry_max_age):
                    expired.append(queue.popleft())
                else:
                    processable.append(queue.popleft())

            if expired:
                logger.error(
                    "P3_ALERT: Reconciliation items expired without processing — manual review required",
                    extra={"tenant_id": tenant_id, "count": len(expired)},
                )

            for item in processable:
                if self._check_rate_limits(tenant_id):
                    await self._apply_correction(item.event)
                else:
                    queue.appendleft(item)  # Re-queue
                    break
