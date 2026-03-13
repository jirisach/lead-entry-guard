"""
Shadow Mode Governance.

Execution model (v4 — async):
  request → active policy → decision (synchronous, in request path)
          ↘ shadow queue → shadow policy (asynchronous, out of request path)

shadow_queue: max 10_000 events
Circuit breaker: if queue full → drop event (NEVER blocks main path)
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

from lead_entry_guard.core.models import DecisionClass, PolicyVersions
from lead_entry_guard.policies.engine import PolicyContext, PolicyEngine

logger = logging.getLogger(__name__)


class ShadowState(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    PROMOTE_CANDIDATE = "promote_candidate"
    ARCHIVED = "archived"


@dataclass
class ShadowEvent:
    ctx: PolicyContext
    active_decision: DecisionClass
    enqueued_at: float = field(default_factory=time.monotonic)


@dataclass
class ShadowComparisonResult:
    active_decision: DecisionClass
    shadow_decision: DecisionClass
    diverged: bool


@dataclass
class ShadowMetrics:
    total_evaluated: int = 0
    divergence_count: int = 0
    reject_delta: int = 0
    warn_delta: int = 0
    duplicate_hint_delta: int = 0
    dropped_events: int = 0

    @property
    def divergence_rate(self) -> float:
        if self.total_evaluated == 0:
            return 0.0
        return self.divergence_count / self.total_evaluated


class ShadowModeEngine:
    """
    Async shadow policy evaluator.

    - Never blocks ingestion path
    - Circuit breaker: drops when queue full
    - Sampling: 100% at < 1000 req/s, 10% at > 1000 req/s
    - min_sample_size accounts for sample_rate (v4 fix)
    """

    def __init__(
        self,
        shadow_policy: PolicyEngine,
        max_queue_size: int = 10_000,
        cpu_budget_pct: float = 0.20,
        sample_rate_low: float = 1.0,
        sample_rate_high: float = 0.10,
        traffic_threshold_rps: int = 1_000,
        min_sample_size: int = 1_000,
        max_duration_days: int = 30,
        escalate_after_days: int = 14,
    ) -> None:
        self._shadow_policy = shadow_policy
        self._queue: asyncio.Queue[ShadowEvent] = asyncio.Queue(maxsize=max_queue_size)
        self._sample_rate_low = sample_rate_low
        self._sample_rate_high = sample_rate_high
        self._traffic_threshold = traffic_threshold_rps
        self._min_sample_size = min_sample_size
        self._max_duration_days = max_duration_days
        self._escalate_after_days = escalate_after_days
        self._state = ShadowState.ACTIVE
        self._metrics = ShadowMetrics()
        self._current_rps: float = 0.0
        self._started_at = time.monotonic()

    def _sample_rate(self) -> float:
        return (
            self._sample_rate_low
            if self._current_rps < self._traffic_threshold
            else self._sample_rate_high
        )

    def _effective_min_sample(self) -> int:
        """v4 fix: min_sample_size must account for sample_rate."""
        rate = self._sample_rate()
        if rate >= 1.0:
            return self._min_sample_size
        return int(self._min_sample_size / rate)

    def enqueue(self, ctx: PolicyContext, active_decision: DecisionClass) -> None:
        """
        Try to enqueue shadow event. NEVER blocks — circuit breaker drops if full.
        """
        if self._state != ShadowState.ACTIVE:
            return
        if random.random() > self._sample_rate():
            return
        event = ShadowEvent(ctx=ctx, active_decision=active_decision)
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._metrics.dropped_events += 1
            logger.debug("Shadow queue full — dropping event (circuit breaker)")

    async def process_loop(self) -> None:
        """Background task — processes shadow queue."""
        while True:
            event = await self._queue.get()
            try:
                await self._process_event(event)
            except Exception:
                logger.exception("Shadow event processing failed")
            finally:
                self._queue.task_done()

    async def _process_event(self, event: ShadowEvent) -> None:
        shadow_decision, _ = self._shadow_policy.decide(event.ctx)
        self._metrics.total_evaluated += 1
        diverged = shadow_decision != event.active_decision
        if diverged:
            self._metrics.divergence_count += 1
            if event.active_decision == DecisionClass.REJECT and shadow_decision != DecisionClass.REJECT:
                self._metrics.reject_delta += 1
            if event.active_decision == DecisionClass.WARN and shadow_decision != DecisionClass.WARN:
                self._metrics.warn_delta += 1

        self._check_lifecycle()

    def _check_lifecycle(self) -> None:
        elapsed_days = (time.monotonic() - self._started_at) / 86400
        if elapsed_days >= self._max_duration_days and self._state == ShadowState.ACTIVE:
            self._state = ShadowState.ARCHIVED
            logger.warning(
                "P3_ALERT: Shadow policy expired without review — auto-archived",
                extra={"elapsed_days": elapsed_days},
            )
        elif elapsed_days >= self._escalate_after_days and self._state == ShadowState.ACTIVE:
            logger.warning(
                "Shadow policy auto-escalation: 14 days without promotion decision",
                extra={"elapsed_days": elapsed_days},
            )

    def promote(self) -> None:
        self._state = ShadowState.PROMOTE_CANDIDATE

    def pause(self) -> None:
        self._state = ShadowState.PAUSED

    def archive(self) -> None:
        self._state = ShadowState.ARCHIVED

    @property
    def metrics(self) -> ShadowMetrics:
        return self._metrics

    @property
    def state(self) -> ShadowState:
        return self._state

    @property
    def has_sufficient_samples(self) -> bool:
        return self._metrics.total_evaluated >= self._effective_min_sample()
