"""Integration tests — pipeline determinism.

Lead Entry Guard's core claim: same input + same config + same state → same decision.

These tests verify there is no:
  - random behaviour (hash salt, random scoring, unordered dict iteration)
  - hidden state bug (global cache, shared mutable registry, singleton pollution)
  - race condition (async ordering bug, concurrent write to shared state)

Each test runs against a fresh pipeline instance per iteration — this is intentional.
If a test fails, the bug is in the pipeline design, not in shared test state.

This test class is inspired by determinism requirements in:
  - payment gateways (Stripe authorize/reject must be repeatable)
  - fraud engines (same signal must always produce same flag)
  - CRM ingestion (same lead must always produce same decision)
"""
from __future__ import annotations

import asyncio

import pytest

from lead_entry_guard.config.tenant import TenantConfig
from lead_entry_guard.core.models import DecisionClass, LeadInput, SalvagePolicy
from tests.fixtures.pipeline import build_pipeline

_ITERATIONS = 10  # run each scenario N times with fresh pipeline


# ── Decision determinism ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_valid_lead_decision_is_deterministic():
    """Clean lead → same PASS decision across N fresh pipeline instances."""
    lead = LeadInput(
        tenant_id="t1",
        email="deterministic@example.com",
        phone="+420601123456",
    )
    decisions = []
    for _ in range(_ITERATIONS):
        pipeline = await build_pipeline()
        result = await pipeline.process(lead)
        decisions.append(result.decision)

    assert len(set(decisions)) == 1, (
        f"Non-deterministic decision for clean lead: {set(decisions)}"
    )
    assert decisions[0] == DecisionClass.PASS


@pytest.mark.asyncio
async def test_invalid_lead_decision_is_deterministic():
    """Invalid lead → same REJECT decision and same reason codes across N fresh pipelines."""
    lead = LeadInput(
        tenant_id="t1",
        email="not-an-email",
        phone="123",
    )
    outputs = []
    for _ in range(_ITERATIONS):
        pipeline = await build_pipeline()
        r = await pipeline.process(lead)
        outputs.append((r.decision, tuple(sorted(r.reason_codes))))

    assert len(set(outputs)) == 1, (
        f"Non-deterministic output for invalid lead: {set(outputs)}"
    )
    assert outputs[0][0] == DecisionClass.REJECT


@pytest.mark.asyncio
async def test_warn_decision_and_reason_codes_deterministic():
    """WARN lead (invalid phone, SALVAGE policy) → same decision + same reason codes."""
    lead = LeadInput(
        tenant_id="t1",
        email="dirty@example.com",
        phone="not-a-phone",
    )
    outputs = []
    for _ in range(_ITERATIONS):
        pipeline = await build_pipeline(
            configs=[TenantConfig(tenant_id="t1", salvage_policy=SalvagePolicy.SALVAGE)]
        )
        r = await pipeline.process(lead)
        outputs.append((r.decision, tuple(sorted(r.reason_codes))))

    assert len(set(outputs)) == 1, (
        f"Non-deterministic output for WARN lead: {set(outputs)}"
    )
    assert outputs[0][0] == DecisionClass.WARN


@pytest.mark.asyncio
async def test_reason_codes_order_is_deterministic():
    """Reason codes must be in consistent order — not dependent on dict/set iteration."""
    lead = LeadInput(
        tenant_id="t1",
        email="not-an-email",
        phone="also-invalid",
    )
    reason_code_lists = []
    for _ in range(_ITERATIONS):
        pipeline = await build_pipeline()
        r = await pipeline.process(lead)
        reason_code_lists.append(tuple(r.reason_codes))

    # All must be identical — including order
    assert len(set(reason_code_lists)) == 1, (
        f"Reason code order is non-deterministic: {set(reason_code_lists)}"
    )


# ── Version metadata determinism ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_version_metadata_is_deterministic():
    """Policy/ruleset/config versions must be identical across fresh pipeline instances."""
    lead = LeadInput(tenant_id="t1", email="version@example.com")
    version_tuples = []
    for _ in range(_ITERATIONS):
        pipeline = await build_pipeline()
        r = await pipeline.process(lead)
        version_tuples.append((
            r.versions.policy_version,
            r.versions.ruleset_version,
            r.versions.config_version,
        ))

    assert len(set(version_tuples)) == 1, (
        f"Non-deterministic version metadata: {set(version_tuples)}"
    )


# ── Concurrent determinism ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_concurrent_fresh_pipelines_same_decision():
    """N pipelines processing same lead concurrently must all return same decision.

    This catches async ordering bugs and race conditions in shared singletons
    (e.g. PolicyEngine shared DEFAULT_RULESET, settings cache).
    """
    lead = LeadInput(tenant_id="t1", email="concurrent-det@example.com")

    async def run_one():
        pipeline = await build_pipeline()
        return await pipeline.process(lead)

    results = await asyncio.gather(*[run_one() for _ in range(_ITERATIONS)])
    decisions = {r.decision for r in results}

    assert len(decisions) == 1, (
        f"Concurrent pipelines returned different decisions: {decisions}"
    )
    assert results[0].decision == DecisionClass.PASS, (
        f"Expected PASS for clean lead, got {results[0].decision}"
    )


@pytest.mark.asyncio
async def test_determinism_across_salvage_policies():
    """Same lead, same policy, repeated — each policy must be internally deterministic."""
    lead = LeadInput(
        tenant_id="t1",
        email="policy-det@example.com",
        phone="not-a-phone",
    )

    for policy in SalvagePolicy:
        outputs = []
        for _ in range(5):
            pipeline = await build_pipeline(
                configs=[TenantConfig(tenant_id="t1", salvage_policy=policy)]
            )
            r = await pipeline.process(lead)
            outputs.append((r.decision, tuple(sorted(r.reason_codes))))

        assert len(set(outputs)) == 1, (
            f"SalvagePolicy.{policy.value} is non-deterministic: {set(outputs)}"
        )
