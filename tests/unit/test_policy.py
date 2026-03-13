"""Unit tests — policy engine rule evaluation and reason codes."""
import pytest

from lead_entry_guard.core.models import (
    DecisionClass,
    DuplicateHint,
    LeadInput,
    ReasonCode,
    ValidationError,
    ValidationResult,
)
from lead_entry_guard.normalization.normalizer import NormalizationLayer
from lead_entry_guard.policies.engine import PolicyContext, PolicyEngine
from lead_entry_guard.validation.validator import ValidationLayer


def _make_ctx(
    email: str = "test@example.com",
    valid: bool = True,
    is_duplicate: bool = False,
    dup_skipped: bool = False,
) -> PolicyContext:
    normalizer = NormalizationLayer()
    lead = LeadInput(tenant_id="t1", email=email)
    normalized = normalizer.normalize(lead)

    if valid:
        validation = ValidationResult(valid=True)
    else:
        validation = ValidationResult(
            valid=False,
            errors=[
                ValidationError(
                    field="email",
                    reason_code=ReasonCode.REJECT_INVALID_EMAIL,
                    message="Invalid",
                )
            ],
        )

    dup_hint = None
    if is_duplicate:
        dup_hint = DuplicateHint(
            is_duplicate=True,
            confidence="confirmed",
            reason_code=ReasonCode.DUPLICATE_REDIS_CONFIRMED,
            lookup_path="redis_confirmed",
        )

    return PolicyContext(
        normalized_lead=normalized,
        validation_result=validation,
        duplicate_hint=dup_hint,
        duplicate_check_skipped=dup_skipped,
    )


def test_pass_on_valid_new_lead():
    engine = PolicyEngine()
    ctx = _make_ctx(valid=True, is_duplicate=False)
    decision, codes = engine.decide(ctx)
    assert decision == DecisionClass.PASS
    assert ReasonCode.OK in codes


def test_reject_on_invalid_email():
    engine = PolicyEngine()
    ctx = _make_ctx(valid=False)
    decision, codes = engine.decide(ctx)
    assert decision == DecisionClass.REJECT
    assert ReasonCode.REJECT_INVALID_EMAIL in codes


def test_duplicate_hint_on_confirmed_duplicate():
    engine = PolicyEngine()
    ctx = _make_ctx(is_duplicate=True)
    decision, codes = engine.decide(ctx)
    assert decision == DecisionClass.DUPLICATE_HINT
    assert ReasonCode.DUPLICATE_REDIS_CONFIRMED in codes


def test_warn_when_duplicate_check_skipped():
    engine = PolicyEngine()
    ctx = _make_ctx(dup_skipped=True)
    decision, codes = engine.decide(ctx)
    assert decision == DecisionClass.WARN
    assert ReasonCode.WARN_INDEX_UNAVAILABLE in codes


def test_every_decision_has_version_metadata():
    engine = PolicyEngine()
    ctx = _make_ctx()
    engine.decide(ctx)
    versions = engine.versions
    assert versions.policy_version
    assert versions.ruleset_version
    assert versions.config_version
