"""Resilience tests — Redis unavailability during duplicate check.

Verifies pipeline behavior when Redis is down at the duplicate lookup stage.
This is distinct from Redis down at startup — the pipeline is running normally
and Redis becomes unavailable mid-request.

Invariants:
  - pipeline must not crash
  - duplicate_check_skipped must be True
  - decision must reflect degraded mode policy (WARN / REJECT / QUEUE fallback)
  - no PII or fingerprint data in error logs

Implementation note:
  We patch _dup_tier.check (not _redis.lookup) because new leads go through
  Bloom negative path and never reach Redis lookup. Patching at the tier level
  correctly simulates Redis unavailability regardless of Bloom state.
"""
from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
import fakeredis.aioredis as fakeredis

from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
from lead_entry_guard.core.exceptions import RedisUnavailableError
from lead_entry_guard.core.models import (
    DecisionClass,
    DegradedModePolicy,
    LeadInput,
    ReasonCode,
    SalvagePolicy,
)
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
    degraded_policy: DegradedModePolicy = DegradedModePolicy.ACCEPT_WITH_FLAG,
    salvage_policy: SalvagePolicy = SalvagePolicy.STRICT,
) -> IngestionPipeline:
    redis_client = fakeredis.FakeRedis(decode_responses=False)
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))
    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)
    registry = TenantRegistry()
    registry.register(TenantConfig(
        tenant_id="t1",
        degraded_mode_policy=degraded_policy,
        salvage_policy=salvage_policy,
    ))
    return IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=TelemetryQueue(max_size=100),
        tenant_registry=registry,
    )


@pytest.mark.asyncio
async def test_redis_down_accept_with_flag():
    """Redis down + ACCEPT_WITH_FLAG policy → WARN with duplicate_check_skipped=True."""
    pipeline = await _build_pipeline(degraded_policy=DegradedModePolicy.ACCEPT_WITH_FLAG)

    with patch.object(
        pipeline._dup_tier, "check",
        side_effect=RedisUnavailableError("redis down"),
    ):
        lead = LeadInput(tenant_id="t1", email="redis-down@example.com")
        result = await pipeline.process(lead)

    assert result.decision == DecisionClass.WARN
    assert result.duplicate_check_skipped is True
    assert ReasonCode.WARN_INDEX_UNAVAILABLE in result.reason_codes


@pytest.mark.asyncio
async def test_redis_down_reject_policy():
    """Redis down + REJECT policy → REJECT, not crash."""
    pipeline = await _build_pipeline(degraded_policy=DegradedModePolicy.REJECT)

    with patch.object(
        pipeline._dup_tier, "check",
        side_effect=RedisUnavailableError("redis down"),
    ):
        lead = LeadInput(tenant_id="t1", email="redis-reject@example.com")
        result = await pipeline.process(lead)

    assert result.decision == DecisionClass.REJECT


@pytest.mark.asyncio
async def test_redis_down_multiple_concurrent_leads():
    """Redis down during concurrent burst — no crash, all leads get decisions."""
    pipeline = await _build_pipeline(degraded_policy=DegradedModePolicy.ACCEPT_WITH_FLAG)

    with patch.object(
        pipeline._dup_tier, "check",
        side_effect=RedisUnavailableError("redis down"),
    ):
        leads = [LeadInput(tenant_id="t1", email=f"redis-concurrent-{i}@example.com") for i in range(20)]
        results = await asyncio.gather(*[pipeline.process(l) for l in leads])

    assert len(results) == 20
    assert all(r.decision in (DecisionClass.WARN, DecisionClass.REJECT) for r in results)
    assert all(r.duplicate_check_skipped is True for r in results)


@pytest.mark.asyncio
async def test_redis_recovers_after_outage():
    """Redis recovers mid-session — subsequent leads get normal duplicate detection."""
    pipeline = await _build_pipeline()

    # Phase 1: Redis down — patch at tier level
    with patch.object(
        pipeline._dup_tier, "check",
        side_effect=RedisUnavailableError("redis down"),
    ):
        r_down = await pipeline.process(LeadInput(tenant_id="t1", email="recovery@example.com"))
    assert r_down.duplicate_check_skipped is True

    # Phase 2: Redis recovered — normal flow resumes
    r_first = await pipeline.process(LeadInput(tenant_id="t1", email="recovery@example.com"))
    assert r_first.decision == DecisionClass.PASS
    await pipeline.flush_pending()

    r_dup = await pipeline.process(LeadInput(tenant_id="t1", email="recovery@example.com"))
    assert r_dup.decision == DecisionClass.DUPLICATE_HINT
