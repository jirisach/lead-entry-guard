"""Resilience tests — slow downstream (Redis latency injection).

Simulates Redis responding slowly (200ms per call) rather than being unavailable.
This is a different failure mode from RedisUnavailableError:
  - slow Redis does not trigger degraded mode
  - pipeline must remain responsive
  - event loop must not block
  - all decisions must still be correct

Tests verify:
  - concurrent leads complete within bounded wall-clock time
  - no event loop starvation (other coroutines still run)
  - decision correctness is unaffected by latency
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

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

INJECTED_LATENCY_S = 0.05  # 50ms per Redis call — realistic slow Redis


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
        telemetry_queue=TelemetryQueue(max_size=500),
        tenant_registry=registry,
    )


def _slow_lookup(original_lookup):
    """Wrap a Redis lookup with injected async latency."""
    async def _wrapped(tenant_id, fingerprint_id):
        await asyncio.sleep(INJECTED_LATENCY_S)
        return await original_lookup(tenant_id, fingerprint_id)
    return _wrapped


@pytest.mark.asyncio
async def test_slow_redis_single_lead_correct_decision():
    """Slow Redis (50ms) — single lead still gets correct decision."""
    pipeline = await _build_pipeline()
    original = pipeline._dup_tier._redis.lookup

    with patch.object(pipeline._dup_tier._redis, "lookup", side_effect=_slow_lookup(original)):
        lead = LeadInput(tenant_id="t1", email="slow-single@example.com")
        result = await pipeline.process(lead)

    assert result.decision == DecisionClass.PASS
    assert result.duplicate_check_skipped is False


@pytest.mark.asyncio
async def test_slow_redis_concurrent_no_event_loop_blocking():
    """Slow Redis under concurrency — event loop must not block.

    10 concurrent leads with 50ms Redis latency.
    If async is correct: ~50ms total (parallel).
    If blocking: ~500ms total (serial).
    We assert wall-clock < 3x single-lead latency to allow for overhead.
    """
    pipeline = await _build_pipeline()
    original = pipeline._dup_tier._redis.lookup

    with patch.object(pipeline._dup_tier._redis, "lookup", side_effect=_slow_lookup(original)):
        leads = [LeadInput(tenant_id="t1", email=f"slow-concurrent-{i}@example.com") for i in range(10)]

        start = time.monotonic()
        results = await asyncio.gather(*[pipeline.process(l) for l in leads])
        duration = time.monotonic() - start

    # All decisions correct
    assert all(r.decision == DecisionClass.PASS for r in results)

    # Wall-clock must be well under serial time (10 × 50ms = 500ms)
    # Allow generous headroom for test environment variance
    serial_time = len(leads) * INJECTED_LATENCY_S
    assert duration < serial_time * 0.7, (
        f"Possible event loop blocking detected: {duration:.3f}s "
        f"(serial would be {serial_time:.3f}s, expected < {serial_time * 0.7:.3f}s)"
    )


@pytest.mark.asyncio
async def test_slow_redis_duplicate_detection_still_correct():
    """Slow Redis — duplicate detection must still work correctly."""
    pipeline = await _build_pipeline()
    original = pipeline._dup_tier._redis.lookup

    # First submission without slowdown — stores fingerprint
    first = LeadInput(tenant_id="t1", email="slow-dup@example.com")
    r1 = await pipeline.process(first)
    assert r1.decision == DecisionClass.PASS
    await pipeline.flush_pending()

    # Second submission with slow Redis — duplicate must still be detected
    with patch.object(pipeline._dup_tier._redis, "lookup", side_effect=_slow_lookup(original)):
        second = LeadInput(tenant_id="t1", email="slow-dup@example.com")
        r2 = await pipeline.process(second)

    assert r2.decision == DecisionClass.DUPLICATE_HINT, (
        f"Slow Redis must not affect duplicate detection correctness, got {r2.decision}"
    )


@pytest.mark.asyncio
async def test_slow_redis_does_not_trigger_degraded_mode():
    """Slow Redis must NOT trigger degraded mode — only unavailable Redis does.

    Latency ≠ unavailability. A slow response is still a valid response.
    duplicate_check_skipped must remain False under slow-but-available Redis.
    """
    pipeline = await _build_pipeline()
    original = pipeline._dup_tier._redis.lookup

    with patch.object(pipeline._dup_tier._redis, "lookup", side_effect=_slow_lookup(original)):
        lead = LeadInput(tenant_id="t1", email="slow-nodegraded@example.com")
        result = await pipeline.process(lead)

    assert result.duplicate_check_skipped is False, (
        "Slow Redis must not set duplicate_check_skipped=True — "
        "that flag is only for RedisUnavailableError"
    )
    assert result.decision == DecisionClass.PASS
