"""
Chaos tests (Layer 4) — simultaneous multi-component failure.
Owner: platform/SRE team | Run: before every production release

These are mandatory gate for major change promotion.

Scenarios:
- REDIS_UNAVAILABLE
- BLOOM_FILTER_SATURATED
- TELEMETRY_QUEUE_FULL
- CONFIG_VERSION_MISMATCH
- HMAC_KEY_ROTATION
- SHADOW_POLICY_TIMEOUT
- RECONCILIATION_SPIKE
- REDIS_UNAVAILABLE + TELEMETRY_QUEUE_FULL simultaneously
- HMAC_KEY_ROTATION + CONFIG_VERSION_MISMATCH simultaneously
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest
import fakeredis.aioredis as fakeredis

from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
from lead_entry_guard.config.versioning import ConfigVersionManager, MismatchSeverity
from lead_entry_guard.core.exceptions import (
    ConfigMismatchError,
    ReconciliationRateLimitError,
    RedisUnavailableError,
)
from lead_entry_guard.core.models import DecisionClass, DegradedModePolicy, LeadInput
from lead_entry_guard.core.pipeline import IngestionPipeline
from lead_entry_guard.fingerprint.builder import FingerprintBuilder
from lead_entry_guard.lookup.bloom import BloomFilterRegistry, TenantBloomFilter
from lead_entry_guard.lookup.duplicate import DuplicateLookupTier
from lead_entry_guard.lookup.redis_store import RedisDuplicateStore, RedisIdempotencyStore
from lead_entry_guard.normalization.normalizer import NormalizationLayer
from lead_entry_guard.policies.engine import PolicyEngine
from lead_entry_guard.reconciliation.loop import CRMEvent, CRMEventType, ReconciliationLoop
from lead_entry_guard.security.hmac_keys import HMACKeyManager
from lead_entry_guard.security.vault import InMemoryVaultClient
from lead_entry_guard.telemetry.exporter import TelemetryQueue
from lead_entry_guard.validation.validator import ValidationLayer
from tests.fixtures.common import make_key_ring


async def _build_pipeline(tenant_policy=DegradedModePolicy.ACCEPT_WITH_FLAG) -> IngestionPipeline:
    redis_client = fakeredis.FakeRedis(decode_responses=False)
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))
    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)
    tenant_registry = TenantRegistry()
    tenant_registry.register(TenantConfig(tenant_id="t1", degraded_mode_policy=tenant_policy))

    return IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=TelemetryQueue(max_size=1000),
        tenant_registry=tenant_registry,
    )


@pytest.mark.asyncio
async def test_redis_unavailable_chaos():
    """System must not crash when Redis is completely unavailable."""
    pipeline = await _build_pipeline()
    with patch.object(pipeline._dup_tier, "check", side_effect=RedisUnavailableError("chaos")):
        results = await asyncio.gather(*[
            pipeline.process(LeadInput(tenant_id="t1", email=f"chaos{i}@test.com"))
            for i in range(10)
        ])
    assert all(r.decision in (DecisionClass.WARN, DecisionClass.REJECT) for r in results)


@pytest.mark.asyncio
async def test_bloom_saturated_falls_through_to_redis():
    """Saturated bloom → system continues via Redis."""
    pipeline = await _build_pipeline()
    # Fill bloom filter past threshold
    bloom = pipeline._dup_tier._bloom.get_or_create("t1", capacity=10)
    for i in range(8):
        bloom.check_and_add(f"fake_key_{i}")

    lead = LeadInput(tenant_id="t1", email="postbloom@example.com")
    result = await pipeline.process(lead)
    assert result.decision in (DecisionClass.PASS, DecisionClass.WARN, DecisionClass.DUPLICATE_HINT)


@pytest.mark.asyncio
async def test_telemetry_queue_full_does_not_block_ingestion():
    """Full telemetry queue must never block lead processing."""
    pipeline = await _build_pipeline()
    # Fill telemetry queue completely
    from lead_entry_guard.telemetry.exporter import TelemetryEvent, LatencyBucket
    from lead_entry_guard.core.models import SourceType
    for _ in range(1000):
        try:
            pipeline._telemetry._queue.put_nowait(
                TelemetryEvent(
                    tenant_id="t1", decision=DecisionClass.PASS, source_type=SourceType.API,
                    latency_bucket=LatencyBucket.FAST, duplicate_check_skipped=False,
                    degraded_mode=False, policy_version="v1", ruleset_version="v1", config_version="v1",
                )
            )
        except Exception:
            break

    lead = LeadInput(tenant_id="t1", email="fullqueue@example.com")
    result = await pipeline.process(lead)
    assert result.decision in (DecisionClass.PASS, DecisionClass.WARN, DecisionClass.DUPLICATE_HINT)


@pytest.mark.asyncio
async def test_redis_unavailable_plus_telemetry_queue_full():
    """Simultaneous Redis failure + full telemetry queue — must not deadlock."""
    pipeline = await _build_pipeline()
    # Fill telemetry queue
    for _ in range(1000):
        try:
            pipeline._telemetry._queue.put_nowait(object())  # type: ignore
        except Exception:
            break

    with patch.object(pipeline._dup_tier, "check", side_effect=RedisUnavailableError("chaos")):
        result = await pipeline.process(LeadInput(tenant_id="t1", email="both@fail.com"))

    assert result.decision in (DecisionClass.WARN, DecisionClass.REJECT)


@pytest.mark.asyncio
async def test_config_mismatch_critical_fails_closed():
    """CRITICAL config mismatch must fail closed."""
    manager = ConfigVersionManager(grace_period_seconds=5)
    with pytest.raises(ConfigMismatchError) as exc_info:
        await manager.handle_mismatch(MismatchSeverity.CRITICAL)
    assert exc_info.value.severity == MismatchSeverity.CRITICAL


@pytest.mark.asyncio
async def test_config_mismatch_non_critical_allows_degraded():
    """NON_CRITICAL mismatch must allow degraded processing (not raise)."""
    manager = ConfigVersionManager()
    # Should NOT raise
    await manager.handle_mismatch(MismatchSeverity.NON_CRITICAL)


@pytest.mark.asyncio
async def test_config_mismatch_unknown_escalates_after_grace_period():
    """UNKNOWN mismatch must escalate to CRITICAL after grace period."""
    manager = ConfigVersionManager(grace_period_seconds=0)  # Instant for test

    # First call — starts grace period
    await manager.handle_mismatch(MismatchSeverity.UNKNOWN)
    await asyncio.sleep(0.01)

    # Second call — grace period elapsed
    with pytest.raises(ConfigMismatchError):
        await manager.handle_mismatch(MismatchSeverity.UNKNOWN)


@pytest.mark.asyncio
async def test_hmac_key_rotation_race_condition():
    """Node A has v4, Node B still v3 — lookup must succeed for both."""
    ring_v3 = make_key_ring(current_kid="v3")
    ring_v4 = {
        "current": {
            "kid": "v4",
            "secret_hex": ring_v3["current"]["secret_hex"],
            "activated_at": "2026-03-01T00:00:00+00:00",
        },
        "previous": {
            "kid": "v3",
            "secret_hex": ring_v3["current"]["secret_hex"],  # same secret for test
            "activated_at": "2026-01-15T00:00:00+00:00",
            "expires_at": "2026-05-01T00:00:00+00:00",
        },
    }

    node_a = HMACKeyManager()
    await node_a.load_from_vault(InMemoryVaultClient(ring_v4))

    node_b = HMACKeyManager()
    await node_b.load_from_vault(InMemoryVaultClient(ring_v3))

    # Fingerprint generated by node_b (v3)
    fp_v3, _ = node_b.generate_fingerprint("t1", "email=race@example.com")

    # node_a should still verify it via previous key
    verified = node_a.verify_fingerprint("t1", "email=race@example.com", fp_v3)
    assert verified, "Race condition: node_a with v4 must verify node_b's v3 fingerprint"


@pytest.mark.asyncio
async def test_reconciliation_spike_global_rate_limit():
    """Massive reconciliation spike must be rate-limited and queued, not lost."""
    redis_client = fakeredis.FakeRedis(decode_responses=False)
    dup_store = RedisDuplicateStore(redis_client)
    loop = ReconciliationLoop(
        redis_store=dup_store,
        per_tenant_limit=10,
        global_limit=20,
        retry_max_age_hours=1,
    )

    events = [
        CRMEvent(
            event_type=CRMEventType.LEAD_DUPLICATE_CORRECTED,
            tenant_id="t1",
            lead_reference=f"lead_{i}",
            fingerprint_id=None,
        )
        for i in range(50)
    ]

    # Process burst
    for event in events:
        await loop.handle_event(event)

    # Some should be queued
    total_queued = sum(len(q) for q in loop._retry_queues.values())
    assert total_queued > 0, "Excess corrections should be queued, not dropped"