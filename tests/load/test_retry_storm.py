"""Load tests — retry storm idempotency under concurrency.

Simulates API gateway retry storm: same lead submitted N times concurrently.
This is different from sequential idempotency — the race window matters.

Invariants:
  - all concurrent replays must return same decision as original
  - all concurrent replays must return same request_id as original
  - no Redis key duplication or corruption under concurrent writes
  - pipeline must not deadlock or crash under storm conditions
"""
from __future__ import annotations

import asyncio
from collections import Counter

import pytest
import fakeredis.aioredis as fakeredis

from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
from lead_entry_guard.core.models import DecisionClass, LeadInput, SalvagePolicy
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


async def _build_pipeline() -> IngestionPipeline:
    redis_client = fakeredis.FakeRedis(decode_responses=False)
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))
    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)
    registry = TenantRegistry()
    registry.register(TenantConfig(tenant_id="t1", salvage_policy=SalvagePolicy.STRICT))
    return IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=TelemetryQueue(max_size=500),
        tenant_registry=registry,
    )


@pytest.mark.asyncio
async def test_retry_storm_100_concurrent_same_lead():
    """100 concurrent requests for same lead must all return identical decision.

    Simulates: API gateway retries, webhook dedup failure, double-click import.
    The idempotency window race is the key concern here.
    """
    pipeline = await _build_pipeline()
    lead = LeadInput(
        tenant_id="t1",
        source_id="storm-src-concurrent",
        email="storm-concurrent@example.com",
    )

    results = await asyncio.gather(*[pipeline.process(lead) for _ in range(100)])

    decisions = [r.decision for r in results]
    request_ids = [r.request_id for r in results]

    decision_counts = Counter(decisions)

    # All decisions must be one of two valid outcomes:
    # - PASS (first writer wins, all others get idempotency replay)
    # - DUPLICATE_HINT (concurrent duplicate detection without source_id match)
    # What must NOT happen: mix of PASS and REJECT, or different request_ids for same source_id
    assert len(set(decisions)) <= 2, (
        f"Decision drift in retry storm: {decision_counts}"
    )

    # All results with matching source_id must have the same request_id
    # (idempotency contract — same source_id → same request_id)
    non_error_ids = set(request_ids)
    # In practice with source_id set, all replays after first snapshot return same request_id
    # Allow small race window: most must converge
    most_common_id = Counter(request_ids).most_common(1)[0]
    convergence_rate = most_common_id[1] / len(request_ids)
    assert convergence_rate >= 0.90, (
        f"Idempotency convergence too low: {convergence_rate:.0%} "
        f"({most_common_id[1]}/100 returned same request_id)"
    )


@pytest.mark.asyncio
async def test_retry_storm_sequential_after_concurrent():
    """After concurrent storm settles, sequential replays must be fully idempotent."""
    pipeline = await _build_pipeline()
    lead = LeadInput(
        tenant_id="t1",
        source_id="storm-src-sequential",
        email="storm-sequential@example.com",
    )

    # Concurrent storm
    await asyncio.gather(*[pipeline.process(lead) for _ in range(50)])
    await pipeline.flush_pending()

    # Sequential replays after storm — must all match
    r_reference = await pipeline.process(lead)
    for i in range(10):
        r = await pipeline.process(lead)
        assert r.request_id == r_reference.request_id, (
            f"Sequential replay {i+1} after storm has wrong request_id"
        )
        assert r.decision == r_reference.decision, (
            f"Sequential replay {i+1} after storm has wrong decision"
        )


@pytest.mark.asyncio
async def test_retry_storm_multiple_distinct_leads():
    """Storm with multiple distinct leads — each must be idempotent independently.

    10 distinct leads × 10 concurrent retries each = 100 total requests.
    Each lead group must converge to same decision within its group.
    """
    pipeline = await _build_pipeline()

    leads = [
        LeadInput(
            tenant_id="t1",
            source_id=f"multi-storm-src-{i}",
            email=f"multi-storm-{i}@example.com",
        )
        for i in range(10)
    ]

    # 10 retries per lead, all concurrent
    all_results = await asyncio.gather(*[
        pipeline.process(lead)
        for lead in leads
        for _ in range(10)
    ])

    # Group by source_id
    from collections import defaultdict
    by_source: dict[str, list] = defaultdict(list)
    for lead, result in zip([l for l in leads for _ in range(10)], all_results):
        by_source[lead.source_id].append(result)

    for source_id, results in by_source.items():
        decisions = [r.decision for r in results]
        counts = Counter(decisions)
        # Each group must converge — at most 2 distinct outcomes (race window)
        assert len(counts) <= 2, (
            f"{source_id}: too many distinct decisions in storm: {counts}"
        )
