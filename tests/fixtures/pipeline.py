"""Shared pipeline builder for tests.

Provides a clean, isolated IngestionPipeline instance with:
  - fakeredis (in-process, no network)
  - InMemoryVaultClient (no Vault dependency)
  - configurable TenantConfig list
  - sensible defaults for all components

Usage:
    from tests.fixtures.pipeline import build_pipeline

    pipeline = await build_pipeline()
    pipeline = await build_pipeline(configs=[TenantConfig(tenant_id="t1", salvage_policy=SalvagePolicy.SALVAGE)])
"""
from __future__ import annotations

import fakeredis.aioredis as fakeredis

from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
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


async def build_pipeline(
    configs: list[TenantConfig] | None = None,
    telemetry_queue_size: int = 100,
) -> IngestionPipeline:
    """Build a fully isolated pipeline instance for testing.

    Each call returns a fresh pipeline with a new fakeredis instance —
    no shared state between calls. Safe to use in determinism tests.

    Args:
        configs: tenant configs to register. Defaults to a single STRICT t1 tenant.
        telemetry_queue_size: size of telemetry queue.
    """
    redis_client = fakeredis.FakeRedis(decode_responses=False)
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))

    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)

    registry = TenantRegistry()
    for config in (configs or [TenantConfig(tenant_id="t1")]):
        registry.register(config)

    return IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=TelemetryQueue(max_size=telemetry_queue_size),
        tenant_registry=registry,
    )
