"""Integration tests — idempotency across all decision types.

Verifies that idempotency snapshots are stored and replayed correctly
for every decision class, not just PASS.

Invariants:
  - same source_id + same payload → same request_id on replay
  - same source_id + same payload → same decision on replay
  - same source_id + same payload → same reason_codes on replay
  - idempotency must hold for: PASS, WARN, REJECT, DUPLICATE_HINT
  - idempotency must not cause extra Redis writes on replay
"""
from __future__ import annotations

import asyncio

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


async def _build_pipeline(salvage_policy: SalvagePolicy = SalvagePolicy.SALVAGE) -> IngestionPipeline:
    redis_client = fakeredis.FakeRedis(decode_responses=False)
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))
    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)
    registry = TenantRegistry()
    registry.register(TenantConfig(tenant_id="t1", salvage_policy=salvage_policy))
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


async def _assert_idempotent(pipeline: IngestionPipeline, lead: LeadInput) -> DecisionClass:
    """Submit lead twice, assert idempotency invariants, return decision."""
    r1 = await pipeline.process(lead)
    await pipeline.flush_pending()
    r2 = await pipeline.process(lead)

    assert r2.request_id == r1.request_id, (
        f"Idempotency replay must return original request_id.\n"
        f"  first:  {r1.request_id}\n"
        f"  replay: {r2.request_id}"
    )
    assert r2.decision == r1.decision, (
        f"Idempotency replay must return original decision.\n"
        f"  first:  {r1.decision}\n"
        f"  replay: {r2.decision}"
    )
    assert r2.reason_codes == r1.reason_codes, (
        f"Idempotency replay must return original reason_codes.\n"
        f"  first:  {r1.reason_codes}\n"
        f"  replay: {r2.reason_codes}"
    )
    return r1.decision


@pytest.mark.asyncio
async def test_idempotency_pass_decision():
    """PASS decision is correctly replayed from idempotency snapshot."""
    pipeline = await _build_pipeline()
    lead = LeadInput(
        tenant_id="t1",
        source_id="idem-pass-src",
        email="idem-pass@example.com",
    )
    decision = await _assert_idempotent(pipeline, lead)
    assert decision == DecisionClass.PASS


@pytest.mark.asyncio
async def test_idempotency_reject_decision():
    """REJECT decision is correctly replayed — not silently upgraded to PASS."""
    pipeline = await _build_pipeline(salvage_policy=SalvagePolicy.STRICT)
    lead = LeadInput(
        tenant_id="t1",
        source_id="idem-reject-src",
        email="not-an-email",
    )
    decision = await _assert_idempotent(pipeline, lead)
    assert decision == DecisionClass.REJECT


@pytest.mark.asyncio
async def test_idempotency_warn_decision():
    """WARN decision (invalid phone under SALVAGE) is correctly replayed."""
    pipeline = await _build_pipeline(salvage_policy=SalvagePolicy.SALVAGE)
    lead = LeadInput(
        tenant_id="t1",
        source_id="idem-warn-src",
        email="idem-warn@example.com",
        phone="not-a-phone",
    )
    decision = await _assert_idempotent(pipeline, lead)
    assert decision == DecisionClass.WARN


@pytest.mark.asyncio
async def test_idempotency_duplicate_hint_decision():
    """DUPLICATE_HINT decision is correctly replayed.

    Setup: submit lead once (PASS + store fingerprint),
    submit different lead with same email (DUPLICATE_HINT),
    replay the duplicate lead (must get same DUPLICATE_HINT, not PASS).
    """
    pipeline = await _build_pipeline()

    # Establish duplicate
    first = LeadInput(tenant_id="t1", email="idem-dup@example.com")
    await pipeline.process(first)
    await pipeline.flush_pending()

    # Duplicate lead with source_id for idempotency
    duplicate = LeadInput(
        tenant_id="t1",
        source_id="idem-dup-src",
        email="idem-dup@example.com",
    )
    decision = await _assert_idempotent(pipeline, duplicate)
    assert decision == DecisionClass.DUPLICATE_HINT


@pytest.mark.asyncio
async def test_idempotency_retry_storm_100x():
    """Same lead submitted 100 times must return identical decision on all replays.

    Simulates API gateway retry storm or webhook deduplication failure.
    Verifies: no Redis corruption, no decision drift, deterministic behavior.
    """
    pipeline = await _build_pipeline()
    lead = LeadInput(
        tenant_id="t1",
        source_id="retry-storm-src",
        email="retry-storm@example.com",
    )

    # First submission
    r1 = await pipeline.process(lead)
    await pipeline.flush_pending()

    # 99 replays — all must return identical result
    replays = await asyncio.gather(*[pipeline.process(lead) for _ in range(99)])

    for i, r in enumerate(replays):
        assert r.request_id == r1.request_id, (
            f"Replay {i+1}: request_id mismatch — "
            f"expected {r1.request_id!r}, got {r.request_id!r}"
        )
        assert r.decision == r1.decision, (
            f"Replay {i+1}: decision drift — "
            f"expected {r1.decision!r}, got {r.decision!r}"
        )
