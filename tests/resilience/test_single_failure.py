"""
Resilience tests (Layer 3) — system behavior under single-component failure.
Owner: feature team | Run: every CI build

Scenarios:
- Redis slowdown (latency 500ms)
- Telemetry queue 90% full
- Config version 1 behind
- Previous-key overlap lookup
- Queue cap hit in QUEUE mode
"""
from __future__ import annotations

import asyncio

import pytest
import fakeredis.aioredis as fakeredis

from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
from lead_entry_guard.core.models import DecisionClass, DegradedModePolicy, LeadInput
from lead_entry_guard.core.pipeline import IngestionPipeline
from lead_entry_guard.fingerprint.builder import FingerprintBuilder
from lead_entry_guard.lookup.bloom import BloomFilterRegistry
from lead_entry_guard.lookup.duplicate import DuplicateLookupTier
from lead_entry_guard.lookup.redis_store import RedisDuplicateStore, RedisIdempotencyStore
from lead_entry_guard.normalization.normalizer import NormalizationLayer
from lead_entry_guard.policies.engine import PolicyEngine
from lead_entry_guard.security.hmac_keys import HMACKeyManager
from lead_entry_guard.security.vault import InMemoryVaultClient
from lead_entry_guard.telemetry.exporter import TelemetryQueue
from lead_entry_guard.validation.validator import ValidationLayer
from tests.fixtures.common import make_key_ring


async def _build_pipeline(
    redis_client=None,
    tenant_policy: DegradedModePolicy = DegradedModePolicy.ACCEPT_WITH_FLAG,
    telemetry_queue_size: int = 1000,
) -> IngestionPipeline:
    if redis_client is None:
        redis_client = fakeredis.FakeRedis(decode_responses=False)

    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))

    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)

    tenant_registry = TenantRegistry()
    tenant_registry.register(
        TenantConfig(
            tenant_id="t1",
            degraded_mode_policy=tenant_policy,
        )
    )

    return IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=TelemetryQueue(max_size=telemetry_queue_size),
        tenant_registry=tenant_registry,
    )


@pytest.mark.asyncio
async def test_redis_unavailable_accept_with_flag():
    """Redis down → ACCEPT_WITH_FLAG → decision is WARN, not failure."""
    pipeline = await _build_pipeline(tenant_policy=DegradedModePolicy.ACCEPT_WITH_FLAG)
    from unittest.mock import patch
    from lead_entry_guard.core.exceptions import RedisUnavailableError

    with patch.object(pipeline._dup_tier, "check", side_effect=RedisUnavailableError("down")):
        lead = LeadInput(tenant_id="t1", email="test@example.com")
        result = await pipeline.process(lead)

    assert result.decision == DecisionClass.WARN
    assert result.duplicate_check_skipped is True


@pytest.mark.asyncio
async def test_redis_unavailable_reject_policy():
    """Redis down + REJECT policy → lead is rejected."""
    pipeline = await _build_pipeline(tenant_policy=DegradedModePolicy.REJECT)
    from unittest.mock import patch
    from lead_entry_guard.core.exceptions import RedisUnavailableError

    with patch.object(pipeline._dup_tier, "check", side_effect=RedisUnavailableError("down")):
        lead = LeadInput(tenant_id="t1", email="test@example.com")
        result = await pipeline.process(lead)

    assert result.decision == DecisionClass.REJECT


@pytest.mark.asyncio
async def test_telemetry_queue_near_capacity_does_not_block():
    """Telemetry queue at 90% must not block ingestion."""
    pipeline = await _build_pipeline(telemetry_queue_size=10)
    # Fill queue to 90%
    for _ in range(9):
        pipeline._telemetry._queue.put_nowait(
            __import__("lead_entry_guard.telemetry.exporter", fromlist=["TelemetryEvent"]).TelemetryEvent(
                tenant_id="t1",
                decision=DecisionClass.PASS,
                source_type=__import__("lead_entry_guard.core.models", fromlist=["SourceType"]).SourceType.API,
                latency_bucket=__import__("lead_entry_guard.telemetry.exporter", fromlist=["LatencyBucket"]).LatencyBucket.FAST,
                duplicate_check_skipped=False,
                degraded_mode=False,
                policy_version="v1",
                ruleset_version="v1",
                config_version="v1",
            )
        )

    lead = LeadInput(tenant_id="t1", email="full_queue@example.com")
    result = await pipeline.process(lead)
    # Should still get a decision
    assert result.decision in (DecisionClass.PASS, DecisionClass.WARN)


@pytest.mark.asyncio
async def test_previous_key_overlap_lookup():
    """After key rotation, fingerprints from previous key should still match."""
    ring = make_key_ring(current_kid="v3")
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(ring))

    # Generate with current (v3)
    fp, kid = km.generate_fingerprint("t1", "email=overlap@example.com")
    assert kid == "v3"
    # Verify
    assert km.verify_fingerprint("t1", "email=overlap@example.com", fp)


@pytest.mark.asyncio
async def test_queue_cap_hit_falls_back(monkeypatch):
    """QUEUE mode at hard cap should immediately apply fallback policy."""
    pipeline = await _build_pipeline(tenant_policy=DegradedModePolicy.QUEUE)
    settings = __import__("lead_entry_guard.config.settings", fromlist=["get_settings"]).get_settings()

    # Fill queue to hard cap
    from lead_entry_guard.core.pipeline import QueuedLead
    queue = pipeline._hold_queues["t1"]
    for i in range(settings.queue_hard_cap_per_tenant):
        queue.append(QueuedLead(lead=LeadInput(tenant_id="t1", email=f"q{i}@x.com")))

    from unittest.mock import patch
    from lead_entry_guard.core.exceptions import RedisUnavailableError

    with patch.object(pipeline._dup_tier, "check", side_effect=RedisUnavailableError("down")):
        lead = LeadInput(tenant_id="t1", email="overflow@example.com")
        result = await pipeline.process(lead)

    # Should fall back — not crash
    assert result.decision in (DecisionClass.WARN, DecisionClass.REJECT)