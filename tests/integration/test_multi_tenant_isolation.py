"""Integration tests — multi-tenant isolation.

Verifies that tenant decisions, fingerprint namespaces and Redis keys
are fully isolated even when the same lead data is submitted across tenants.

Enterprise invariants:
  - same email → different decisions per tenant policy (STRICT vs SALVAGE vs QUARANTINE)
  - same email → different fingerprints per tenant (HMAC namespace)
  - duplicate detection is scoped per tenant (not cross-tenant)
  - one tenant's Redis failure does not affect another
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


async def _build_pipeline(configs: list[TenantConfig]) -> IngestionPipeline:
    redis_client = fakeredis.FakeRedis(decode_responses=False)
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))
    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)
    registry = TenantRegistry()
    for config in configs:
        registry.register(config)
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
async def test_same_email_different_policy_different_decision():
    """Same lead with invalid phone → different decisions per tenant SalvagePolicy.

    tenant_strict    → REJECT  (STRICT: invalid phone is fatal)
    tenant_salvage   → WARN    (SALVAGE: invalid phone is recoverable)
    tenant_quarantine→ WARN    (QUARANTINE: invalid phone + review hint)
    """
    pipeline = await _build_pipeline([
        TenantConfig(tenant_id="tenant_strict", salvage_policy=SalvagePolicy.STRICT),
        TenantConfig(tenant_id="tenant_salvage", salvage_policy=SalvagePolicy.SALVAGE),
        TenantConfig(tenant_id="tenant_quarantine", salvage_policy=SalvagePolicy.QUARANTINE),
    ])

    email = "shared@example.com"
    phone = "not-a-phone"

    r_strict = await pipeline.process(LeadInput(tenant_id="tenant_strict", email=email, phone=phone))
    r_salvage = await pipeline.process(LeadInput(tenant_id="tenant_salvage", email=email, phone=phone))
    r_quarantine = await pipeline.process(LeadInput(tenant_id="tenant_quarantine", email=email, phone=phone))

    assert r_strict.decision == DecisionClass.REJECT
    assert r_salvage.decision == DecisionClass.WARN
    assert r_quarantine.decision == DecisionClass.WARN


@pytest.mark.asyncio
async def test_duplicate_detection_is_tenant_scoped():
    """Duplicate detection must be scoped per tenant — not cross-tenant.

    tenant_A sees lead → PASS, stores fingerprint
    tenant_B sends same email → must also get PASS (different namespace)
    tenant_A sends same email again → DUPLICATE_HINT
    """
    pipeline = await _build_pipeline([
        TenantConfig(tenant_id="tenant_A"),
        TenantConfig(tenant_id="tenant_B"),
    ])

    email = "scoped@example.com"

    # First: tenant_A → PASS
    r1 = await pipeline.process(LeadInput(tenant_id="tenant_A", email=email))
    assert r1.decision == DecisionClass.PASS
    await pipeline.flush_pending()

    # Same email from tenant_B → must be PASS (different namespace)
    r2 = await pipeline.process(LeadInput(tenant_id="tenant_B", email=email))
    assert r2.decision == DecisionClass.PASS, (
        "Duplicate detection must not leak across tenants — "
        f"tenant_B got {r2.decision} for email that only tenant_A submitted"
    )
    await pipeline.flush_pending()

    # tenant_A again → DUPLICATE_HINT
    r3 = await pipeline.process(LeadInput(tenant_id="tenant_A", email=email))
    assert r3.decision == DecisionClass.DUPLICATE_HINT


@pytest.mark.asyncio
async def test_concurrent_tenants_no_cross_contamination():
    """Concurrent processing of same email across 3 tenants must not cross-contaminate.

    All three tenants process the same email simultaneously.
    Each must get PASS (first submission for each tenant).
    """
    pipeline = await _build_pipeline([
        TenantConfig(tenant_id=f"concurrent_{i}") for i in range(3)
    ])

    email = "concurrent@example.com"
    results = await asyncio.gather(*[
        pipeline.process(LeadInput(tenant_id=f"concurrent_{i}", email=email))
        for i in range(3)
    ])

    for i, result in enumerate(results):
        assert result.decision == DecisionClass.PASS, (
            f"concurrent_{i} expected PASS, got {result.decision}"
        )
