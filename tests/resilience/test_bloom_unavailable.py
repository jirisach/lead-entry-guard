"""Resilience tests — Bloom filter unavailability.

Tests the anti-corruption layer in bloom.py and duplicate.py:
  - BloomUnavailableError is translated to a graceful fallback
  - Pipeline falls back to Redis-only lookup when Bloom is unavailable
  - Duplicate detection still works correctly via Redis direct path
  - Pipeline decisions are correct regardless of Bloom state
"""
from __future__ import annotations

from unittest.mock import patch

import pytest
import fakeredis.aioredis as fakeredis

from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
from lead_entry_guard.core.exceptions import BloomUnavailableError
from lead_entry_guard.core.models import DecisionClass, LeadInput, SalvagePolicy
from lead_entry_guard.core.pipeline import IngestionPipeline
from lead_entry_guard.fingerprint.builder import FingerprintBuilder
from lead_entry_guard.lookup.bloom import BloomFilterRegistry, TenantBloomFilter
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
    registry.register(TenantConfig(tenant_id="t1"))
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
async def test_bloom_unavailable_falls_back_to_redis():
    """Bloom unavailable → pipeline falls back to Redis-only lookup, no crash."""
    pipeline = await _build_pipeline()

    with patch.object(
        pipeline._dup_tier._bloom, "get_or_create",
        side_effect=Exception("Bloom internal error"),
    ):
        lead = LeadInput(tenant_id="t1", email="bloom-down@example.com")
        result = await pipeline.process(lead)

    # Must get a valid decision — not crash
    assert result.decision in (
        DecisionClass.PASS,
        DecisionClass.WARN,
        DecisionClass.DUPLICATE_HINT,
    )


@pytest.mark.asyncio
async def test_bloom_unavailable_duplicate_still_detected():
    """Bloom unavailable → duplicate detection via Redis direct path still works.

    This verifies the anti-corruption layer: even when Bloom is completely down,
    Redis remains authoritative and duplicate detection is correct.
    """
    pipeline = await _build_pipeline()

    # First submission — Bloom works, fingerprint stored in Redis
    first = LeadInput(tenant_id="t1", email="bloom-dup@example.com")
    r1 = await pipeline.process(first)
    assert r1.decision == DecisionClass.PASS
    await pipeline.flush_pending()

    # Second submission — Bloom is down, must fall back to Redis
    with patch.object(
        pipeline._dup_tier._bloom, "get_or_create",
        side_effect=Exception("Bloom down"),
    ):
        second = LeadInput(tenant_id="t1", email="bloom-dup@example.com")
        r2 = await pipeline.process(second)

    assert r2.decision == DecisionClass.DUPLICATE_HINT, (
        f"Redis fallback must still detect duplicate, got {r2.decision}"
    )


@pytest.mark.asyncio
async def test_bloom_check_and_add_exception_falls_back():
    """check_and_add raising any exception → graceful Redis fallback, not pipeline crash."""
    pipeline = await _build_pipeline()

    bloom_filter = pipeline._dup_tier._bloom.get_or_create("t1", capacity=10000)
    with patch.object(bloom_filter, "check_and_add", side_effect=RuntimeError("corrupted")):
        lead = LeadInput(tenant_id="t1", email="corrupt-bloom@example.com")
        result = await pipeline.process(lead)

    assert result.decision in (DecisionClass.PASS, DecisionClass.WARN, DecisionClass.DUPLICATE_HINT)


@pytest.mark.asyncio
async def test_bloom_unavailable_new_lead_passes():
    """Bloom down + new lead (not in Redis) → PASS via Redis direct miss."""
    pipeline = await _build_pipeline()

    with patch.object(
        pipeline._dup_tier._bloom, "get_or_create",
        side_effect=Exception("Bloom down"),
    ):
        lead = LeadInput(tenant_id="t1", email="brand-new@example.com")
        result = await pipeline.process(lead)

    assert result.decision == DecisionClass.PASS
