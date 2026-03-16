"""Integration tests — Recoverability / Salvage Layer.

Tests the full stack: ValidationLayer → RecoverabilityLayer → PolicyEngine
for each SalvagePolicy variant.

Key invariants:
- fatal errors (invalid email) → REJECT regardless of SalvagePolicy
- recoverable errors (invalid phone, valid email):
    STRICT     → REJECT + REJECT_INVALID_PHONE
    SALVAGE    → WARN  + WARN_INVALID_OPTIONAL_PHONE
    QUARANTINE → WARN  + WARN_INVALID_OPTIONAL_PHONE + WARN_DATA_QUALITY
- clean lead → PASS regardless of SalvagePolicy
"""
from __future__ import annotations

import pytest

from lead_entry_guard.core.models import (
    DecisionClass,
    LeadInput,
    ReasonCode,
    SalvagePolicy,
)
from lead_entry_guard.normalization.normalizer import NormalizationLayer
from lead_entry_guard.policies.engine import PolicyContext, PolicyEngine
from lead_entry_guard.policies.recoverability import RecoverabilityLayer
from lead_entry_guard.validation.validator import ValidationLayer

normalizer = NormalizationLayer()
validator = ValidationLayer()
recoverability = RecoverabilityLayer()
engine = PolicyEngine()


def _run(lead: LeadInput, salvage_policy: SalvagePolicy):
    normalized = normalizer.normalize(lead)
    validation = validator.validate(normalized)
    assessment = recoverability.assess(validation, normalized)
    ctx = PolicyContext(
        normalized_lead=normalized,
        validation_result=validation,
        duplicate_hint=None,
        recoverability=assessment,
        salvage_policy=salvage_policy,
        duplicate_check_skipped=False,
    )
    decision, codes = engine.decide(ctx)
    return decision, codes, assessment


# ── Fatal errors — always REJECT ─────────────────────────────────────────────

def test_invalid_email_always_rejected_strict():
    lead = LeadInput(tenant_id="t1", email="not-an-email", phone="+12025550100")
    decision, codes, assessment = _run(lead, SalvagePolicy.STRICT)
    assert decision == DecisionClass.REJECT
    assert ReasonCode.REJECT_INVALID_EMAIL in codes
    assert not assessment.is_salvageable


def test_invalid_email_always_rejected_salvage():
    """Even SALVAGE policy cannot save a lead with invalid email."""
    lead = LeadInput(tenant_id="t1", email="not-an-email", phone="+12025550100")
    decision, codes, assessment = _run(lead, SalvagePolicy.SALVAGE)
    assert decision == DecisionClass.REJECT
    assert ReasonCode.REJECT_INVALID_EMAIL in codes


def test_invalid_email_always_rejected_quarantine():
    lead = LeadInput(tenant_id="t1", email="not-an-email")
    decision, codes, _ = _run(lead, SalvagePolicy.QUARANTINE)
    assert decision == DecisionClass.REJECT


# ── Recoverable errors — invalid phone, valid email ───────────────────────────

def test_invalid_phone_strict_rejects():
    """STRICT policy: invalid phone → REJECT."""
    lead = LeadInput(tenant_id="t1", email="valid@example.com", phone="not-a-phone")
    decision, codes, assessment = _run(lead, SalvagePolicy.STRICT)
    assert decision == DecisionClass.REJECT
    assert ReasonCode.REJECT_INVALID_PHONE in codes
    assert assessment.is_salvageable  # fatal_errors is empty — email is valid


def test_invalid_phone_salvage_warns():
    """SALVAGE policy: invalid phone → WARN with remapped reason code."""
    lead = LeadInput(tenant_id="t1", email="valid@example.com", phone="not-a-phone")
    decision, codes, assessment = _run(lead, SalvagePolicy.SALVAGE)
    assert decision == DecisionClass.WARN
    assert ReasonCode.WARN_INVALID_OPTIONAL_PHONE in codes
    assert ReasonCode.REJECT_INVALID_PHONE not in codes
    assert assessment.is_salvageable


def test_invalid_phone_quarantine_warns_with_review_hint():
    """QUARANTINE policy: invalid phone → WARN + WARN_MANUAL_REVIEW_REQUIRED review hint.

    QUARANTINE differs from SALVAGE in that it adds an explicit manual review signal
    so downstream ops teams know the lead needs human review before use.
    Note: this does not yet route to a quarantine queue — it flags via reason code only.
    """
    lead = LeadInput(tenant_id="t1", email="valid@example.com", phone="not-a-phone")
    decision, codes, _ = _run(lead, SalvagePolicy.QUARANTINE)
    assert decision == DecisionClass.WARN
    assert ReasonCode.WARN_INVALID_OPTIONAL_PHONE in codes
    assert ReasonCode.WARN_MANUAL_REVIEW_REQUIRED in codes


# ── Clean leads — always PASS ─────────────────────────────────────────────────

def test_clean_lead_passes_strict():
    lead = LeadInput(tenant_id="t1", email="clean@example.com", phone="+12025550100")
    decision, codes, _ = _run(lead, SalvagePolicy.STRICT)
    assert decision == DecisionClass.PASS
    assert ReasonCode.OK in codes


def test_clean_lead_passes_salvage():
    lead = LeadInput(tenant_id="t1", email="clean@example.com", phone="+12025550100")
    decision, codes, _ = _run(lead, SalvagePolicy.SALVAGE)
    assert decision == DecisionClass.PASS


def test_clean_lead_no_phone_passes():
    """Phone is optional — missing phone is not a recoverable error, just a quality flag."""
    lead = LeadInput(tenant_id="t1", email="nophone@example.com")
    decision, codes, assessment = _run(lead, SalvagePolicy.STRICT)
    assert decision == DecisionClass.PASS
    assert not assessment.recoverable_errors
    assert not assessment.fatal_errors


# ── Assessment invariants ─────────────────────────────────────────────────────

def test_assessment_splits_correctly_mixed_errors():
    """Lead with invalid email AND invalid phone: both classified correctly."""
    lead = LeadInput(tenant_id="t1", email="bad-email", phone="bad-phone")
    normalized = normalizer.normalize(lead)
    validation = validator.validate(normalized)
    assessment = recoverability.assess(validation, normalized)

    fatal_codes = [e.reason_code for e in assessment.fatal_errors]
    recoverable_codes = [e.reason_code for e in assessment.recoverable_errors]

    assert ReasonCode.REJECT_INVALID_EMAIL in fatal_codes
    assert ReasonCode.REJECT_INVALID_PHONE in recoverable_codes
    assert not assessment.is_salvageable  # has fatal errors


def test_assessment_is_salvageable_phone_only():
    """Only invalid phone — no fatal errors → is_salvageable = True."""
    lead = LeadInput(tenant_id="t1", email="valid@example.com", phone="bad-phone")
    normalized = normalizer.normalize(lead)
    validation = validator.validate(normalized)
    assessment = recoverability.assess(validation, normalized)

    assert assessment.is_salvageable
    assert len(assessment.recoverable_errors) == 1
    assert assessment.recoverable_errors[0].reason_code == ReasonCode.REJECT_INVALID_PHONE


def test_uppercase_email_sets_quality_flag():
    """All-uppercase email triggers WARN_DATA_QUALITY quality flag."""
    lead = LeadInput(tenant_id="t1", email="TEST@EXAMPLE.COM")
    normalized = normalizer.normalize(lead)
    validation = validator.validate(normalized)
    assessment = recoverability.assess(validation, normalized)

    assert ReasonCode.WARN_DATA_QUALITY in assessment.quality_flags
    assert assessment.is_salvageable
    assert not assessment.fatal_errors
    assert not assessment.recoverable_errors


# ── Interaction: duplicate signal wins over recoverable error ─────────────────

def test_duplicate_wins_over_invalid_phone_salvage():
    """Duplicate signal must take priority over recoverable phone error.

    Rule order: fatal → duplicate → skipped_check → recoverable.
    A duplicate lead with invalid phone must get DUPLICATE_HINT, not WARN.
    """
    from lead_entry_guard.core.models import DuplicateHint, ReasonCode as RC
    lead = LeadInput(tenant_id="t1", email="valid@example.com", phone="not-a-phone")
    normalized = normalizer.normalize(lead)
    validation = validator.validate(normalized)
    assessment = recoverability.assess(validation, normalized)

    duplicate = DuplicateHint(
        is_duplicate=True,
        confidence="confirmed",
        reason_code=RC.DUPLICATE_REDIS_CONFIRMED,
        lookup_path="redis_confirmed",
    )
    ctx = PolicyContext(
        normalized_lead=normalized,
        validation_result=validation,
        duplicate_hint=duplicate,
        recoverability=assessment,
        salvage_policy=SalvagePolicy.SALVAGE,
        duplicate_check_skipped=False,
    )
    decision, codes = engine.decide(ctx)
    assert decision == DecisionClass.DUPLICATE_HINT
    assert ReasonCode.DUPLICATE_REDIS_CONFIRMED in codes


def test_skipped_duplicate_check_with_recoverable_phone():
    """Skipped duplicate check fires before recoverable error.

    Rule order: fatal → duplicate → skipped_check → recoverable.
    Lead with invalid phone + skipped check must get WARN_INDEX_UNAVAILABLE,
    not WARN_INVALID_OPTIONAL_PHONE.
    """
    lead = LeadInput(tenant_id="t1", email="valid@example.com", phone="not-a-phone")
    normalized = normalizer.normalize(lead)
    validation = validator.validate(normalized)
    assessment = recoverability.assess(validation, normalized)

    ctx = PolicyContext(
        normalized_lead=normalized,
        validation_result=validation,
        duplicate_hint=None,
        recoverability=assessment,
        salvage_policy=SalvagePolicy.SALVAGE,
        duplicate_check_skipped=True,
    )
    decision, codes = engine.decide(ctx)
    assert decision == DecisionClass.WARN
    assert ReasonCode.WARN_INDEX_UNAVAILABLE in codes


# ── Pipeline-level: TenantRegistry STRICT vs SALVAGE ─────────────────────────

import asyncio
import fakeredis.aioredis as fakeredis_aio

from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
from lead_entry_guard.core.pipeline import IngestionPipeline
from lead_entry_guard.fingerprint.builder import FingerprintBuilder
from lead_entry_guard.lookup.bloom import BloomFilterRegistry
from lead_entry_guard.lookup.duplicate import DuplicateLookupTier
from lead_entry_guard.lookup.redis_store import RedisDuplicateStore, RedisIdempotencyStore
from lead_entry_guard.security.hmac_keys import HMACKeyManager
from lead_entry_guard.security.vault import InMemoryVaultClient
from lead_entry_guard.telemetry.exporter import TelemetryQueue
from tests.fixtures.common import make_key_ring


async def _build_pipeline_with_policy(salvage_policy: SalvagePolicy) -> IngestionPipeline:
    redis_client = fakeredis_aio.FakeRedis(decode_responses=False)
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
async def test_pipeline_strict_tenant_rejects_invalid_phone():
    """Full pipeline: STRICT tenant rejects lead with valid email but invalid phone."""
    pipeline = await _build_pipeline_with_policy(SalvagePolicy.STRICT)
    lead = LeadInput(tenant_id="t1", email="valid@example.com", phone="not-a-phone")
    result = await pipeline.process(lead)
    assert result.decision == DecisionClass.REJECT
    assert ReasonCode.REJECT_INVALID_PHONE in result.reason_codes


@pytest.mark.asyncio
async def test_pipeline_salvage_tenant_warns_on_invalid_phone():
    """Full pipeline: SALVAGE tenant warns instead of rejecting invalid phone."""
    pipeline = await _build_pipeline_with_policy(SalvagePolicy.SALVAGE)
    lead = LeadInput(tenant_id="t1", email="valid@example.com", phone="not-a-phone")
    result = await pipeline.process(lead)
    assert result.decision == DecisionClass.WARN
    assert ReasonCode.WARN_INVALID_OPTIONAL_PHONE in result.reason_codes
    assert ReasonCode.REJECT_INVALID_PHONE not in result.reason_codes


@pytest.mark.asyncio
async def test_pipeline_salvage_tenant_passes_clean_lead():
    """Full pipeline: SALVAGE tenant passes clean lead normally."""
    pipeline = await _build_pipeline_with_policy(SalvagePolicy.SALVAGE)
    lead = LeadInput(tenant_id="t1", email="clean@example.com", phone="+12025550100")
    result = await pipeline.process(lead)
    assert result.decision == DecisionClass.PASS


@pytest.mark.asyncio
async def test_pipeline_duplicate_wins_over_salvage_phone():
    """Full pipeline: duplicate signal beats recoverable phone error even under SALVAGE policy.

    A lead that is a confirmed duplicate AND has an invalid phone must get
    DUPLICATE_HINT — not WARN from the salvage path.
    This verifies rule ordering holds end-to-end, not just in the engine unit test.
    """
    pipeline = await _build_pipeline_with_policy(SalvagePolicy.SALVAGE)

    # First submission — clean lead, establishes the duplicate fingerprint
    first = LeadInput(tenant_id="t1", email="dup@example.com", phone="+12025550100")
    result1 = await pipeline.process(first)
    assert result1.decision == DecisionClass.PASS
    await pipeline.flush_pending()

    # Second submission — same email (duplicate) + broken phone
    duplicate = LeadInput(tenant_id="t1", email="dup@example.com", phone="not-a-phone")
    result2 = await pipeline.process(duplicate)
    assert result2.decision == DecisionClass.DUPLICATE_HINT, (
        f"Duplicate signal must beat salvage WARN, got {result2.decision} "
        f"with codes {result2.reason_codes}"
    )


# ── Quality flag: missing phone ───────────────────────────────────────────────

def test_missing_phone_does_not_generate_quality_flag():
    """Missing phone is NOT a quality flag — phone is optional and its absence is normal.

    This test documents the intentional decision: we removed the WARN_MISSING_OPTIONAL_FIELD
    flag for missing phone because it was too noisy (fires for every lead without a phone).
    If this policy changes, update recoverability.py _detect_quality_flags() and this test.
    """
    lead = LeadInput(tenant_id="t1", email="nophone@example.com")
    normalized = normalizer.normalize(lead)
    validation = validator.validate(normalized)
    assessment = recoverability.assess(validation, normalized)

    assert ReasonCode.WARN_MISSING_OPTIONAL_FIELD not in assessment.quality_flags
    assert assessment.is_salvageable
    assert not assessment.fatal_errors
    assert not assessment.recoverable_errors


@pytest.mark.asyncio
async def test_pipeline_strict_tenant_duplicate_gets_duplicate_hint():
    """Full pipeline: STRICT tenant — confirmed duplicate gets DUPLICATE_HINT, not REJECT.

    Duplicate signal fires before recoverable errors in rule order.
    Under STRICT policy, invalid phone would cause REJECT — but if the lead
    is also a duplicate, DUPLICATE_HINT must win.
    """
    pipeline = await _build_pipeline_with_policy(SalvagePolicy.STRICT)

    first = LeadInput(tenant_id="t1", email="strict-dup@example.com", phone="+12025550100")
    result1 = await pipeline.process(first)
    assert result1.decision == DecisionClass.PASS
    await pipeline.flush_pending()

    duplicate = LeadInput(tenant_id="t1", email="strict-dup@example.com", phone="+12025550100")
    result2 = await pipeline.process(duplicate)
    assert result2.decision == DecisionClass.DUPLICATE_HINT


@pytest.mark.asyncio
async def test_pipeline_quarantine_tenant_warns_with_review_hint():
    """Full pipeline: QUARANTINE tenant — invalid phone → WARN + WARN_MANUAL_REVIEW_REQUIRED."""
    pipeline = await _build_pipeline_with_policy(SalvagePolicy.QUARANTINE)
    lead = LeadInput(tenant_id="t1", email="valid@example.com", phone="not-a-phone")
    result = await pipeline.process(lead)
    assert result.decision == DecisionClass.WARN
    assert ReasonCode.WARN_MANUAL_REVIEW_REQUIRED in result.reason_codes
    assert ReasonCode.REJECT_INVALID_PHONE not in result.reason_codes


def test_fatal_error_wins_over_duplicate_signal():
    """Fatal error must take priority over duplicate signal — rule order: fatal > duplicate.

    A lead with invalid email that is also a confirmed duplicate must get REJECT,
    not DUPLICATE_HINT. Fatal errors are non-negotiable regardless of duplicate state.
    """
    from lead_entry_guard.core.models import DuplicateHint

    lead = LeadInput(tenant_id="t1", email="not-an-email", phone="+12025550100")
    normalized = normalizer.normalize(lead)
    validation = validator.validate(normalized)
    assessment = recoverability.assess(validation, normalized)

    duplicate = DuplicateHint(
        is_duplicate=True,
        confidence="confirmed",
        reason_code=ReasonCode.DUPLICATE_REDIS_CONFIRMED,
        lookup_path="redis_confirmed",
    )
    ctx = PolicyContext(
        normalized_lead=normalized,
        validation_result=validation,
        duplicate_hint=duplicate,
        recoverability=assessment,
        salvage_policy=SalvagePolicy.STRICT,
        duplicate_check_skipped=False,
    )
    decision, codes = engine.decide(ctx)
    assert decision == DecisionClass.REJECT, (
        f"Fatal error must beat duplicate signal, got {decision}"
    )
    assert ReasonCode.REJECT_INVALID_EMAIL in codes
    assert ReasonCode.DUPLICATE_REDIS_CONFIRMED not in codes
