"""Integration tests — API ingestion flow, Bloom + Redis lookup, idempotency."""
from __future__ import annotations

import asyncio

import pytest
import fakeredis.aioredis as fakeredis

from lead_entry_guard.config.tenant import TenantRegistry
from lead_entry_guard.core.models import DecisionClass, LeadInput, SourceType
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


@pytest.fixture
async def pipeline() -> IngestionPipeline:
    redis_client = fakeredis.FakeRedis(decode_responses=False)

    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))

    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client, idempotency_ttl=3600)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)

    return IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=TelemetryQueue(max_size=100),
        tenant_registry=TenantRegistry(),
    )


@pytest.mark.asyncio
async def test_valid_new_lead_passes(pipeline: IngestionPipeline):
    lead = LeadInput(tenant_id="t1", email="new@example.com")
    result = await pipeline.process(lead)
    assert result.decision == DecisionClass.PASS


@pytest.mark.asyncio
async def test_invalid_lead_rejected(pipeline: IngestionPipeline):
    lead = LeadInput(tenant_id="t1", email="not-an-email")
    result = await pipeline.process(lead)
    assert result.decision == DecisionClass.REJECT


@pytest.mark.asyncio
async def test_duplicate_lead_flagged(pipeline: IngestionPipeline):
    lead = LeadInput(tenant_id="t1", email="dupe@example.com", source_id="s1")

    result1 = await pipeline.process(lead)
    assert result1.decision == DecisionClass.PASS

    # Second submission — same email
    lead2 = LeadInput(tenant_id="t1", email="dupe@example.com", source_id="s2")
    result2 = await pipeline.process(lead2)
    assert result2.decision == DecisionClass.DUPLICATE_HINT


@pytest.mark.asyncio
async def test_idempotency_returns_same_decision(pipeline: IngestionPipeline):
    """
    A replayed request (same source_id + same payload) must return the
    exact original decision — not a synthetic PASS or any other value.

    Checks:
      - request_id is the same on both calls (same stored snapshot reference)
      - decision matches
      - reason_codes match
    """
    lead = LeadInput(
        tenant_id="t1",
        source_id="idempotent_src",
        email="idempotent@example.com",
    )
    result1 = await pipeline.process(lead)
    # Give the fire-and-forget snapshot store task time to complete
    await asyncio.sleep(0.05)

    result2 = await pipeline.process(lead)

    assert result2.request_id == result1.request_id, (
        f"Idempotency hit should return original request_id {result1.request_id!r}, "
        f"got {result2.request_id!r}"
    )
    assert result2.decision == result1.decision, (
        f"Idempotency hit should return original decision {result1.decision!r}, "
        f"got {result2.decision!r}"
    )
    assert result2.reason_codes == result1.reason_codes, (
        f"Idempotency hit should return original reason_codes {result1.reason_codes!r}, "
        f"got {result2.reason_codes!r}"
    )


@pytest.mark.asyncio
async def test_idempotency_preserves_reject_decision(pipeline: IngestionPipeline):
    """
    Idempotency must also preserve REJECT decisions — not silently upgrade
    a replayed rejected lead to PASS.
    """
    lead = LeadInput(
        tenant_id="t1",
        source_id="reject_src",
        email="not-an-email",  # will be REJECT
    )
    result1 = await pipeline.process(lead)
    assert result1.decision == DecisionClass.REJECT
    await asyncio.sleep(0.05)

    result2 = await pipeline.process(lead)
    assert result2.decision == DecisionClass.REJECT, (
        "Replayed REJECT lead must not be upgraded to PASS on second submission"
    )
    assert result2.request_id == result1.request_id


@pytest.mark.asyncio
async def test_tenant_isolation(pipeline: IngestionPipeline):
    """Same email for different tenants must be treated as separate identities."""
    lead_a = LeadInput(tenant_id="tenant_A", email="shared@example.com")
    lead_b = LeadInput(tenant_id="tenant_B", email="shared@example.com")

    result_a = await pipeline.process(lead_a)
    result_b = await pipeline.process(lead_b)

    # Both should pass — different tenants
    assert result_a.decision == DecisionClass.PASS
    assert result_b.decision == DecisionClass.PASS


@pytest.mark.asyncio
async def test_telemetry_enqueued_on_decision(pipeline: IngestionPipeline):
    initial = pipeline._telemetry._queue.qsize()
    lead = LeadInput(tenant_id="t1", email="telemetry@example.com")
    await pipeline.process(lead)
    assert pipeline._telemetry._queue.qsize() > initial


@pytest.mark.asyncio
async def test_decision_contains_version_metadata(pipeline: IngestionPipeline):
    lead = LeadInput(tenant_id="t1", email="version@example.com")
    result = await pipeline.process(lead)
    assert result.versions.policy_version
    assert result.versions.ruleset_version
    assert result.versions.config_version


@pytest.mark.asyncio
async def test_recovery_path_stores_fingerprint_and_idempotency_snapshot(monkeypatch):
    """
    Recovery path invariant: a lead processed via _process_post_recovery() must
    behave identically to a lead on the main path:

      1. fingerprint is stored → subsequent duplicate lead gets DUPLICATE_HINT
      2. idempotency snapshot is stored → replay of same source_id returns original decision

    This is the critical regression test for the _process_post_recovery() store fix.
    """
    import fakeredis.aioredis as fakeredis
    from unittest.mock import patch
    from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
    from lead_entry_guard.core.exceptions import RedisUnavailableError
    from lead_entry_guard.core.models import DegradedModePolicy
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
    import lead_entry_guard.core.pipeline as pipeline_module

    # queue_hold_timeout_seconds is a global setting, not per-tenant.
    # Patch it to 5s so the test doesn't wait 15 minutes for recovery.
    monkeypatch.setattr(
        pipeline_module.get_settings(),
        "queue_hold_timeout_seconds",
        5,
    )

    redis_client = fakeredis.FakeRedis(decode_responses=False)
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))

    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)

    tenant_registry = TenantRegistry()
    # queue_hold_timeout_seconds is NOT a TenantConfig field — it's a global setting.
    # Per-tenant fields are: degraded_mode_policy, queue_fallback_policy, bloom_capacity_override.
    tenant_registry.register(TenantConfig(
        tenant_id="t1",
        degraded_mode_policy=DegradedModePolicy.QUEUE,
    ))

    p = IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=TelemetryQueue(max_size=100),
        tenant_registry=tenant_registry,
    )

    lead = LeadInput(tenant_id="t1", email="recovery@example.com", source_id="rec-src-1")

    # First call: Redis fails on lookup → enters QUEUE mode.
    # Mock recovers quickly so _process_post_recovery() runs within the hold window.
    call_count = 0
    original_check = dup_tier.check

    async def flaky_check(tenant_id, fingerprint, bloom_capacity):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RedisUnavailableError("first call fails — triggers queue")
        return await original_check(tenant_id, fingerprint, bloom_capacity)

    with patch.object(dup_tier, "check", side_effect=flaky_check):
        result1 = await p.process(lead)

    # Recovery path should have produced a normal PASS decision
    assert result1.decision == DecisionClass.PASS, (
        f"Expected PASS from recovery path, got {result1.decision}"
    )

    # Allow fire-and-forget store tasks to complete
    await asyncio.sleep(0.1)

    # Invariant 1: fingerprint was stored → duplicate lead must be flagged
    duplicate_lead = LeadInput(tenant_id="t1", email="recovery@example.com", source_id="rec-src-2")
    result_dup = await p.process(duplicate_lead)
    assert result_dup.decision == DecisionClass.DUPLICATE_HINT, (
        "Recovery path must store fingerprint — subsequent same-email lead must get DUPLICATE_HINT, "
        f"got {result_dup.decision}"
    )

    # Invariant 2: idempotency snapshot was stored → replay returns original decision
    result_replay = await p.process(lead)  # same lead, same source_id
    assert result_replay.request_id == result1.request_id, (
        f"Idempotency replay must return original request_id {result1.request_id!r}, "
        f"got {result_replay.request_id!r}"
    )
    assert result_replay.decision == result1.decision, (
        f"Idempotency replay must return original decision {result1.decision!r}, "
        f"got {result_replay.decision!r}"
    )
