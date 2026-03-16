"""Load tests — burst ingestion under realistic conditions.

Simulates real-world CRM pipeline bursts:
  - webhook retry storm
  - marketing batch upload
  - scraping dump
  - API retry loop

Not a throughput benchmark — correctness under concurrency is the goal.
For throughput numbers, use load_tests/hero_benchmark.py.
"""
from __future__ import annotations

import asyncio
import time
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
    registry.register(TenantConfig(
        tenant_id="t1",
        salvage_policy=SalvagePolicy.SALVAGE,
    ))
    return IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=TelemetryQueue(max_size=2000),
        tenant_registry=registry,
    )


def _make_burst(n: int = 1000) -> list[LeadInput]:
    """Generate burst dataset:
      60% clean unique leads
      30% duplicates (same email as earlier lead)
      10% invalid (bad email)
    """
    leads: list[LeadInput] = []

    n_unique = int(n * 0.60)
    n_dupes = int(n * 0.30)
    n_invalid = n - n_unique - n_dupes

    originals = []
    for i in range(n_unique):
        lead = LeadInput(tenant_id="t1", email=f"burst{i}@example.com")
        originals.append(lead)
        leads.append(lead)

    for i in range(n_dupes):
        original = originals[i % len(originals)]
        leads.append(LeadInput(
            tenant_id="t1",
            email=original.email,  # same email — duplicate
        ))

    for i in range(n_invalid):
        leads.append(LeadInput(tenant_id="t1", email=f"not-an-email-{i}"))

    return leads


@pytest.mark.asyncio
async def test_burst_1000_no_crash():
    """1000 concurrent leads must complete without crash or event loop blocking.

    Invariants:
    - no exception escapes
    - all 1000 leads get a decision
    - decision distribution is reasonable
    - duration reasonable (fakeredis — should complete well under 10s)
    """
    pipeline = await _build_pipeline()
    leads = _make_burst(1000)

    start = time.monotonic()
    semaphore = asyncio.Semaphore(50)  # concurrency = 50

    async def bounded(lead: LeadInput):
        async with semaphore:
            return await pipeline.process(lead)

    results = await asyncio.gather(*[bounded(l) for l in leads], return_exceptions=True)
    duration = time.monotonic() - start

    # No exceptions
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert not exceptions, f"{len(exceptions)} leads raised exceptions: {exceptions[:3]}"

    # All 1000 got decisions
    decisions = [r.decision for r in results if not isinstance(r, Exception)]
    assert len(decisions) == 1000

    counts = Counter(decisions)

    # Invalid emails → REJECT
    assert counts[DecisionClass.REJECT] >= 50, (
        f"Expected ~100 REJECTs for invalid emails, got {counts[DecisionClass.REJECT]}"
    )

    # Duplicates → DUPLICATE_HINT
    assert counts[DecisionClass.DUPLICATE_HINT] >= 100, (
        f"Expected ~300 DUPLICATE_HINTs, got {counts[DecisionClass.DUPLICATE_HINT]}"
    )

    # Unique clean leads → PASS
    assert counts[DecisionClass.PASS] >= 400, (
        f"Expected ~600 PASSes, got {counts[DecisionClass.PASS]}"
    )

    # Sanity: should be fast even with fakeredis
    assert duration < 15.0, f"Burst took {duration:.1f}s — possible event loop blocking"

    await pipeline.flush_pending()
