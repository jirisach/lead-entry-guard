"""
Core Ingestion Pipeline — orchestrates all layers.

Processing order:
  1. Idempotency check
  2. Normalization
  3. Validation
  4. Fingerprint Builder
  5. Duplicate Lookup Tier (Bloom → Redis)
  6. Policy / Scoring Engine
  7. Audit Meta (privacy-safe only)
  8. Async Telemetry Queue
  9. Store accepted fingerprint (async)

Degraded modes: ACCEPT_WITH_FLAG | REJECT | QUEUE
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import defaultdict, deque
from collections.abc import Coroutine
from dataclasses import dataclass, field
from typing import Any

from lead_entry_guard.config.settings import get_settings
from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
from lead_entry_guard.core.exceptions import (
    FingerprintError,
    KeyNotFoundError,
    RedisUnavailableError,
)
from lead_entry_guard.core.models import (
    DecisionClass,
    DecisionResult,
    DegradedModePolicy,
    DuplicateHint,
    LeadInput,
    PolicyVersions,
    ReasonCode,
)
from lead_entry_guard.fingerprint.builder import FingerprintBuilder
from lead_entry_guard.lookup.duplicate import DuplicateLookupTier
from lead_entry_guard.lookup.redis_store import RedisIdempotencyStore
from lead_entry_guard.normalization.normalizer import NormalizationLayer
from lead_entry_guard.policies.engine import PolicyContext, PolicyEngine
from lead_entry_guard.policies.shadow import ShadowModeEngine
from lead_entry_guard.telemetry.exporter import (
    TelemetryEvent,
    TelemetryQueue,
    latency_bucket,
)
from lead_entry_guard.validation.validator import ValidationLayer

logger = logging.getLogger(__name__)


@dataclass
class QueuedLead:
    lead: LeadInput
    enqueued_at: float = field(default_factory=time.monotonic)


class IngestionPipeline:
    """
    Main pipeline. Stateless per request; shared state only where necessary.
    """

    def __init__(
        self,
        normalizer: NormalizationLayer,
        validator: ValidationLayer,
        fingerprint_builder: FingerprintBuilder,
        duplicate_tier: DuplicateLookupTier,
        policy_engine: PolicyEngine,
        idempotency_store: RedisIdempotencyStore,
        telemetry_queue: TelemetryQueue,
        tenant_registry: TenantRegistry,
        shadow_engine: ShadowModeEngine | None = None,
    ) -> None:
        self._normalizer = normalizer
        self._validator = validator
        self._fp_builder = fingerprint_builder
        self._dup_tier = duplicate_tier
        self._policy = policy_engine
        self._idempotency = idempotency_store
        self._telemetry = telemetry_queue
        self._tenants = tenant_registry
        self._shadow = shadow_engine
        self._hold_queues: dict[str, deque[QueuedLead]] = defaultdict(deque)
        # Note: per-queue locking is not needed here because asyncio is single-threaded
        # and all queue mutations happen within a single coroutine without yield points
        # between the cap check and the append. If threading is introduced later,
        # re-add per-tenant asyncio.Lock here.
        #
        # Short-lived background tasks (fingerprint store, idempotency snapshot).
        # set keeps strong references so tasks aren't GC'd before completion.
        self._pending_tasks: set[asyncio.Task[Any]] = set()

    def _fire_and_forget(self, coro: Coroutine[Any, Any, Any], *, name: str) -> None:
        """
        Schedule a short-lived background task with guaranteed strong reference.

        Keeps the task in self._pending_tasks until it completes, so it cannot
        be garbage-collected mid-execution. The done-callback removes it from
        the set and logs any unexpected exception — failures are never silent.

        Matches the lifecycle pattern used by Container._spawn() in app.py.
        Unlike _spawn(), these tasks are not cancelled on shutdown — they are
        short-lived store operations that should be allowed to finish naturally.
        """
        task: asyncio.Task[Any] = asyncio.create_task(coro, name=name)
        self._pending_tasks.add(task)

        def _on_done(t: asyncio.Task[Any]) -> None:
            self._pending_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                logger.warning(
                    "Background task failed",
                    extra={"task": name, "error_type": type(exc).__name__, "error": str(exc)},
                )

        task.add_done_callback(_on_done)

    async def flush_pending(self) -> None:
        """Wait for all fire-and-forget background tasks to complete.

        Intended for tests and benchmarks that need deterministic state after
        pipeline.process() — avoids accessing the private _pending_tasks set directly.
        """
        if self._pending_tasks:
            await asyncio.gather(*list(self._pending_tasks), return_exceptions=True)

    async def process(self, lead: LeadInput) -> DecisionResult:
        start = time.monotonic()
        tenant_config = self._tenants.get(lead.tenant_id)

        # ── Idempotency check ─────────────────────────────────────────────
        # Only checked when source_id is present — transport-retry protection.
        # Uses a separate store with 24h TTL (distinct from the 30d duplicate store).
        request_hash = self._hash_request(lead)
        if lead.source_id:
            try:
                snapshot = await self._idempotency.get(
                    lead.tenant_id, lead.source_id, request_hash
                )
                if snapshot is not None:
                    logger.debug("Idempotency hit — returning original decision")
                    return self._result_from_snapshot(lead, snapshot)
            except RedisUnavailableError:
                # Idempotency store unavailable — non-fatal, proceed without replay protection.
                # Narrow catch: RedisUnavailableError only. Any other exception (e.g. deserialization
                # bug, contract violation) should propagate — masking bugs here is dangerous.
                logger.warning(
                    "Idempotency store unavailable — proceeding without replay protection",
                    extra={"error": "RedisUnavailableError"},
                )

        # ── Normalization ─────────────────────────────────────────────────
        normalized = self._normalizer.normalize(lead)

        # ── Validation ────────────────────────────────────────────────────
        validation = self._validator.validate(normalized)

        # Early reject on validation failure — before fingerprint/lookup.
        # Saves a Redis round-trip for clearly invalid inputs.
        if not validation.valid:
            codes = [e.reason_code for e in validation.errors]
            return self._finalize(
                lead, DecisionClass.REJECT, codes, None,
                dup_skipped=False, degraded=False, start=start, tenant_config=tenant_config,
            )

        # ── Fingerprint ───────────────────────────────────────────────────
        # KeyNotFoundError = key ring not loaded (startup issue or rotation gap).
        # ValueError = no identity fields present — treat as policy REJECT, not infra failure.
        try:
            fingerprint = self._fp_builder.build(normalized)
        except FingerprintError as exc:
            # No identity fields — no fingerprint possible → REJECT with explicit reason
            logger.warning("Fingerprint build skipped: no identity fields", extra={"error": str(exc)})
            return self._finalize(
                lead, DecisionClass.REJECT,
                [ReasonCode.REJECT_MISSING_REQUIRED], None,
                dup_skipped=False, degraded=False, start=start, tenant_config=tenant_config,
            )
        except KeyNotFoundError as exc:
            # HMAC key unavailable — infra failure, not a validation failure.
            # Must go through degraded_mode_policy (same as Redis outage), NOT queue_fallback_policy.
            # queue_fallback_policy is only for QUEUE cap/timeout — not for primary infra failures.
            logger.error("HMAC key unavailable — cannot build fingerprint", extra={"error": str(exc)})
            return await self._handle_redis_unavailable(lead, tenant_config, start)

        # ── Duplicate lookup ──────────────────────────────────────────────
        dup_hint: DuplicateHint | None = None
        dup_skipped = False
        try:
            dup_hint = await self._dup_tier.check(
                lead.tenant_id, fingerprint, tenant_config.bloom_capacity
            )
        except RedisUnavailableError:
            return await self._handle_redis_unavailable(lead, tenant_config, start)

        # ── Policy decision ───────────────────────────────────────────────
        ctx = PolicyContext(
            normalized_lead=normalized,
            validation_result=validation,
            duplicate_hint=dup_hint,
            duplicate_check_skipped=dup_skipped,
        )
        decision, codes = self._policy.decide(ctx)

        # Shadow mode enqueue (async, non-blocking — never in request path)
        if self._shadow:
            self._shadow.enqueue(ctx, decision)

        result = self._finalize(
            lead, decision, codes, dup_hint,
            dup_skipped=dup_skipped, degraded=False, start=start, tenant_config=tenant_config,
        )

        # Store fingerprint for accepted leads — fire-and-forget, never blocks.
        # Only store on outcomes where the lead is considered accepted.
        # Use result.request_id (finalized output ID), not lead.request_id (input ID).
        if decision in (DecisionClass.PASS, DecisionClass.WARN, DecisionClass.PASS_DEGRADED):
            self._fire_and_forget(
                self._dup_tier.store_accepted(lead.tenant_id, fingerprint, result.request_id),
                name="store_fingerprint",
            )

        # Store idempotency snapshot — fire-and-forget, never blocks.
        # Stored for ALL decisions (including REJECT, DUPLICATE_HINT) so that
        # replays always return the original outcome, not a synthetic PASS.
        if lead.source_id:
            self._fire_and_forget(
                self._store_idempotency_snapshot(lead, request_hash, result),
                name="store_idempotency_snapshot",
            )

        return result

    async def _handle_redis_unavailable(
        self, lead: LeadInput, tenant_config: TenantConfig, start: float
    ) -> DecisionResult:
        policy = tenant_config.degraded_mode_policy

        if policy == DegradedModePolicy.REJECT:
            return self._finalize(
                lead, DecisionClass.REJECT,
                [ReasonCode.DEGRADED_REDIS_UNAVAILABLE],
                None, dup_skipped=True, degraded=True, start=start, tenant_config=tenant_config,
            )

        if policy == DegradedModePolicy.QUEUE:
            return await self._handle_queue_mode(lead, tenant_config, start)

        # ACCEPT_WITH_FLAG (default for most tenants)
        return self._finalize(
            lead, DecisionClass.WARN,
            [ReasonCode.WARN_INDEX_UNAVAILABLE],
            None, dup_skipped=True, degraded=True, start=start, tenant_config=tenant_config,
        )

    async def _handle_queue_mode(
        self, lead: LeadInput, tenant_config: TenantConfig, start: float
    ) -> DecisionResult:
        settings = get_settings()
        tenant_id = lead.tenant_id
        queue = self._hold_queues[tenant_id]

        # Hard cap check — immediate fallback, never block
        if len(queue) >= settings.queue_hard_cap_per_tenant:
            logger.warning(
                "P2_ALERT: Queue cap hit — applying fallback policy",
                extra={"tenant_id": tenant_id, "queue_size": len(queue)},
            )
            return self._apply_degraded_policy(
                lead, tenant_config, start, reason=ReasonCode.WARN_INDEX_UNAVAILABLE
            )

        item = QueuedLead(lead=lead)
        queue.append(item)
        logger.info(
            "Lead queued (QUEUE mode)",
            extra={"tenant_id": tenant_id, "queue_size": len(queue)},
        )

        try:
            # Wait for Redis recovery with polling
            # Use a fresh start time so queue wait time is visible in latency
            timeout = settings.queue_hold_timeout_seconds
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                await asyncio.sleep(2)
                if await self._dup_tier.is_available():
                    # Redis recovered — process normally WITHOUT re-entering queue mode.
                    # Pass a sentinel that bypasses queue mode on retry to prevent
                    # re-entrancy: if Redis fails again mid-retry, it will ACCEPT_WITH_FLAG
                    # rather than re-queuing the same lead forever.
                    logger.info("Redis recovered — retrying queued lead", extra={"tenant_id": tenant_id})
                    return await self._process_post_recovery(lead, tenant_config, start)

            # Timeout expired
            logger.warning(
                "Queue hold timeout expired — applying fallback",
                extra={"tenant_id": tenant_id},
            )
            return self._apply_degraded_policy(
                lead, tenant_config, start, reason=ReasonCode.WARN_INDEX_UNAVAILABLE
            )
        finally:
            # Always remove from queue whether we succeeded, timed out, or hit an exception.
            # Use discard-style removal so a bug can never leave a ghost entry.
            try:
                queue.remove(item)
            except ValueError:
                pass  # Already removed (race with another path) — safe to ignore

    async def _process_post_recovery(
        self, lead: LeadInput, tenant_config: TenantConfig, original_start: float
    ) -> DecisionResult:
        """
        Process a lead after Redis recovery, bypassing queue mode to prevent re-entrancy.
        If Redis fails again here, we fall through to ACCEPT_WITH_FLAG directly.
        """
        normalized = self._normalizer.normalize(lead)
        validation = self._validator.validate(normalized)
        if not validation.valid:
            codes = [e.reason_code for e in validation.errors]
            return self._finalize(
                lead, DecisionClass.REJECT, codes, None,
                dup_skipped=False, degraded=False, start=original_start, tenant_config=tenant_config,
            )
        try:
            fingerprint = self._fp_builder.build(normalized)
            dup_hint = await self._dup_tier.check(
                lead.tenant_id, fingerprint, tenant_config.bloom_capacity
            )
        except (KeyNotFoundError, FingerprintError, RedisUnavailableError):
            # Second Redis failure or fingerprint issue during recovery — degrade gracefully,
            # do NOT re-enter queue mode
            return self._finalize(
                lead, DecisionClass.WARN,
                [ReasonCode.WARN_INDEX_UNAVAILABLE],
                None, dup_skipped=True, degraded=True,
                start=original_start, tenant_config=tenant_config,
            )

        ctx = PolicyContext(
            normalized_lead=normalized,
            validation_result=validation,
            duplicate_hint=dup_hint,
            duplicate_check_skipped=False,
        )
        decision, codes = self._policy.decide(ctx)
        result = self._finalize(
            lead, decision, codes, dup_hint,
            dup_skipped=False, degraded=False, start=original_start, tenant_config=tenant_config,
        )

        # Mirror main process() path — recovery must not create a divergent store contract.
        # A lead processed post-recovery must be indistinguishable from one on the main path:
        # same fingerprint stored, same idempotency snapshot persisted.
        if decision in (DecisionClass.PASS, DecisionClass.WARN, DecisionClass.PASS_DEGRADED):
            self._fire_and_forget(
                self._dup_tier.store_accepted(lead.tenant_id, fingerprint, result.request_id),
                name="store_fingerprint_post_recovery",
            )

        if lead.source_id:
            request_hash = self._hash_request(lead)
            self._fire_and_forget(
                self._store_idempotency_snapshot(lead, request_hash, result),
                name="store_idempotency_snapshot_post_recovery",
            )

        return result

    def _apply_degraded_policy(
        self,
        lead: LeadInput,
        tenant_config: TenantConfig,
        start: float,
        reason: ReasonCode,
    ) -> DecisionResult:
        # Use queue_fallback_policy — this method is called when:
        #   (a) queue cap is hit, (b) queue timeout fires, (c) key unavailable
        # It is NOT the same as degraded_mode_policy (which routes to QUEUE in the first place).
        policy = tenant_config.queue_fallback_policy
        decision = DecisionClass.REJECT if policy == DegradedModePolicy.REJECT else DecisionClass.WARN
        return self._finalize(
            lead, decision, [reason], None,
            dup_skipped=True, degraded=True, start=start, tenant_config=tenant_config,
        )

    def _finalize(
        self,
        lead: LeadInput,
        decision: DecisionClass,
        codes: list[ReasonCode],
        dup_hint: DuplicateHint | None,
        dup_skipped: bool,
        degraded: bool,
        start: float,
        tenant_config: TenantConfig,
    ) -> DecisionResult:
        latency_ms = (time.monotonic() - start) * 1000
        result = DecisionResult(
            request_id=lead.request_id,
            tenant_id=lead.tenant_id,
            decision=decision,
            reason_codes=codes,
            duplicate_hint=dup_hint,
            duplicate_check_skipped=dup_skipped,
            versions=self._policy.versions,
            latency_ms=latency_ms,
        )

        # Telemetry — privacy-safe, non-blocking, droppable.
        # degraded_mode is an independent flag from dup_skipped:
        #   dup_skipped = the check was bypassed for any reason
        #   degraded    = the bypass was due to an infra failure (not e.g. bloom negative)
        event = TelemetryEvent(
            tenant_id=lead.tenant_id,
            decision=decision,
            source_type=lead.source_type,
            latency_bucket=latency_bucket(latency_ms),
            duplicate_check_skipped=dup_skipped,
            degraded_mode=degraded,
            policy_version=self._policy.versions.policy_version,
            ruleset_version=self._policy.versions.ruleset_version,
            config_version=self._policy.versions.config_version,
        )
        self._telemetry.enqueue(event)

        return result

    def _hash_request(self, lead: LeadInput) -> str:
        """
        Compute a stable hash over all user-supplied input fields.

        Covers every field that could distinguish two logically different requests,
        excluding fields that are assigned by the system at receipt time
        (request_id, received_at) — those must not affect idempotency keying.

        Using all fields prevents false idempotency hits where two requests share
        the same email/phone but differ in name, company, source_type, or extra data.
        """
        payload = json.dumps(
            {
                "tenant_id": lead.tenant_id,
                "source_type": lead.source_type.value,
                "email": lead.email,
                "phone": lead.phone,
                "first_name": lead.first_name,
                "last_name": lead.last_name,
                "company": lead.company,
                "extra": lead.extra,
            },
            sort_keys=True,
            default=str,  # handles any non-JSON-serializable values in extra
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:24]

    def _result_from_snapshot(
        self, lead: LeadInput, snapshot: "IdempotencySnapshot"  # noqa: F821
    ) -> DecisionResult:
        """
        Reconstruct a DecisionResult from a stored idempotency snapshot.

        Returns the *original* decision — not a hardcoded PASS.
        This is the correct idempotency contract: same input → same output.
        """
        return DecisionResult(
            request_id=snapshot.request_id,
            tenant_id=lead.tenant_id,
            decision=DecisionClass(snapshot.decision),
            reason_codes=[ReasonCode(rc) for rc in snapshot.reason_codes],
            duplicate_check_skipped=snapshot.duplicate_check_skipped,
            versions=PolicyVersions(
                policy_version=snapshot.policy_version,
                ruleset_version=snapshot.ruleset_version,
                config_version=snapshot.config_version,
            ),
        )

    async def _store_idempotency_snapshot(
        self, lead: LeadInput, request_hash: str, result: DecisionResult
    ) -> None:
        """
        Persist decision snapshot for idempotency.
        Fire-and-forget — called via asyncio.create_task, never in request path.
        Failure is logged but never propagated.
        """
        if not lead.source_id:
            return
        from lead_entry_guard.lookup.redis_store import IdempotencySnapshot
        snapshot = IdempotencySnapshot(
            request_id=result.request_id,
            decision=result.decision.value,
            reason_codes=[rc.value for rc in result.reason_codes],
            duplicate_check_skipped=result.duplicate_check_skipped,
            policy_version=result.versions.policy_version,
            ruleset_version=result.versions.ruleset_version,
            config_version=result.versions.config_version,
        )
        try:
            await self._idempotency.store(
                lead.tenant_id, lead.source_id, request_hash, snapshot
            )
        except RedisUnavailableError:
            # Narrow catch — only swallow genuine Redis unavailability.
            # Other exceptions (serialization bugs, contract violations) must propagate
            # so they surface in _fire_and_forget's done-callback logger, not vanish silently.
            logger.warning(
                "Failed to store idempotency snapshot — replay protection may be incomplete",
                extra={"error": "RedisUnavailableError"},
            )
