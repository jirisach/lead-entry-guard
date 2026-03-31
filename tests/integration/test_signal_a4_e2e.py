"""
Phase 3B — End-to-end enforcement test for A4 signal.

Tests that source_conflict_manual_vs_enrichment signal flows correctly from
detection → SignalResult → DecisionResultV2 with correct visibility.

ADR-008 invariants verified:
  - Signal has action, visibility, fallback (via contract test — not repeated here)
  - Visibility fields contain no PII
  - SignalResult is a deep copy (mutation test)
  - DecisionResultV2.signals is always a list
  - Downstream-visible fields are set (crm_status, routing_tags, api_flags)
"""
from __future__ import annotations

import pytest

from lead_entry_guard.core.signal_models import (
    DecisionResultV2,
    FallbackMode,
    FieldSourceRecord,
    LeadSignalContext,
    SignalAction,
    SignalClass,
)
from lead_entry_guard.policies.signal_a4 import A4_SIGNAL, A4SignalRule
from lead_entry_guard.policies.signal_evaluator import SignalEvaluator


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_conflict_context(
    field_name: str = "phone",
    manual_value: str = "+420777111000",
    enrichment_value: str = "+420777999999",
) -> LeadSignalContext:
    return LeadSignalContext(
        tenant_id="tenant_A",
        fields=[
            FieldSourceRecord(field_name=field_name, source_type="manual", value=manual_value),
            FieldSourceRecord(field_name=field_name, source_type="enrichment", value=enrichment_value),
        ],
    )


def make_no_conflict_context() -> LeadSignalContext:
    """Same value from both sources — no conflict."""
    return LeadSignalContext(
        tenant_id="tenant_A",
        fields=[
            FieldSourceRecord(field_name="phone", source_type="manual", value="+420777111000"),
            FieldSourceRecord(field_name="phone", source_type="enrichment", value="+420777111000"),
        ],
    )


def make_manual_only_context() -> LeadSignalContext:
    """Only manual source — no enrichment to conflict with."""
    return LeadSignalContext(
        tenant_id="tenant_A",
        fields=[
            FieldSourceRecord(field_name="phone", source_type="manual", value="+420777111000"),
        ],
    )


# ── A4 rule unit tests ────────────────────────────────────────────────────────

class TestA4SignalRule:

    def test_conflict_emits_signal(self):
        """Manual + enrichment with different values → signal emitted."""
        rule = A4SignalRule()
        signals = rule.evaluate(make_conflict_context())
        assert len(signals) == 1
        assert signals[0].code == "source_conflict_manual_vs_enrichment"

    def test_no_conflict_emits_no_signal(self):
        """Same value from both sources → no signal."""
        rule = A4SignalRule()
        signals = rule.evaluate(make_no_conflict_context())
        assert signals == []

    def test_manual_only_emits_no_signal(self):
        """Only manual source → no conflict possible → no signal."""
        rule = A4SignalRule()
        signals = rule.evaluate(make_manual_only_context())
        assert signals == []

    def test_signal_action_is_preserve_manual(self):
        """A4 signal must prescribe PRESERVE_MANUAL_VALUE."""
        rule = A4SignalRule()
        signals = rule.evaluate(make_conflict_context())
        assert signals[0].action == SignalAction.PRESERVE_MANUAL_VALUE

    def test_signal_fallback_is_manual_authoritative(self):
        """A4 fallback must declare manual value as authoritative."""
        rule = A4SignalRule()
        signals = rule.evaluate(make_conflict_context())
        assert signals[0].fallback is not None
        assert signals[0].fallback.mode == FallbackMode.MANUAL_VALUE_AUTHORITATIVE

    def test_multiple_conflicting_fields_emit_one_signal_each(self):
        """Two fields with conflicts → two signals (one per field)."""
        context = LeadSignalContext(
            tenant_id="tenant_A",
            fields=[
                FieldSourceRecord("phone", "manual", "+420777111000"),
                FieldSourceRecord("phone", "enrichment", "+420777999999"),
                FieldSourceRecord("company", "manual", "Acme Ltd"),
                FieldSourceRecord("company", "enrichment", "Acme Incorporated"),
            ],
        )
        rule = A4SignalRule()
        signals = rule.evaluate(context)
        assert len(signals) == 2
        assert all(s.code == "source_conflict_manual_vs_enrichment" for s in signals)


# ── Visibility invariants ─────────────────────────────────────────────────────

class TestA4Visibility:

    def test_crm_status_is_set(self):
        """Downstream CRM must see conflict_flagged status."""
        rule = A4SignalRule()
        signal = rule.evaluate(make_conflict_context())[0]
        assert signal.visibility.crm_status == "conflict_flagged"

    def test_routing_tag_is_set(self):
        """Routing layer must see source_conflict tag."""
        rule = A4SignalRule()
        signal = rule.evaluate(make_conflict_context())[0]
        assert "source_conflict" in signal.visibility.routing_tags

    def test_api_flag_conflict_detected_is_true(self):
        """API consumers must see conflict_detected=True without reading tags."""
        rule = A4SignalRule()
        signal = rule.evaluate(make_conflict_context())[0]
        assert signal.visibility.api_flags.get("conflict_detected") is True

    def test_visibility_contains_no_pii(self):
        """Visibility projection must not contain raw field values (PII)."""
        rule = A4SignalRule()
        signal = rule.evaluate(make_conflict_context())[0]
        v = signal.visibility

        # crm_status must be a code, not a value
        assert v.crm_status == "conflict_flagged"
        assert "@" not in v.crm_status
        assert "420" not in v.crm_status  # no phone fragments

        # routing_tags must be labels
        for tag in v.routing_tags:
            assert "@" not in tag
            assert len(tag) < 64

        # api_flags values must be bool
        for key, val in v.api_flags.items():
            assert isinstance(val, bool), (
                f"api_flags['{key}'] must be bool, got {type(val).__name__} — "
                "free-form values may contain PII (ADR-008)"
            )

    def test_visibility_is_deep_copy(self):
        """
        SignalResult.visibility must be a deep copy of the definition.
        Mutating the definition after emit must not affect the emitted result.
        """
        from lead_entry_guard.core.signal_models import SignalResult
        original_status = A4_SIGNAL.visibility.crm_status

        result = SignalResult.from_definition(A4_SIGNAL)

        # Mutate the definition's visibility (simulates accidental mutation)
        A4_SIGNAL.visibility.crm_status = "mutated_status"

        # Result must be unaffected
        assert result.visibility.crm_status == original_status

        # Restore definition to original state
        A4_SIGNAL.visibility.crm_status = original_status


# ── DecisionResultV2 integration ──────────────────────────────────────────────

class TestDecisionResultV2:

    def test_signals_field_is_always_list(self):
        """DecisionResultV2.signals must never be None."""
        result = DecisionResultV2(
            request_id="req-001",
            tenant_id="tenant_A",
            decision="PASS",
            reason_codes=[],
        )
        assert result.signals is not None
        assert isinstance(result.signals, list)

    def test_signals_empty_by_default(self):
        """No signals by default — clean lead produces no enforcement signals."""
        result = DecisionResultV2(
            request_id="req-001",
            tenant_id="tenant_A",
            decision="PASS",
            reason_codes=[],
        )
        assert result.signals == []

    def test_a4_signal_attached_to_result(self):
        """A4 signal is correctly attached to DecisionResultV2."""
        rule = A4SignalRule()
        signals = rule.evaluate(make_conflict_context())

        result = DecisionResultV2(
            request_id="req-002",
            tenant_id="tenant_A",
            decision="PASS",
            reason_codes=[],
            signals=signals,
        )

        assert result.has_signal("source_conflict_manual_vs_enrichment")
        assert result.conflict_detected() is True

    def test_no_conflict_result_has_no_signal(self):
        """Clean lead — no conflict — produces result with empty signals."""
        rule = A4SignalRule()
        signals = rule.evaluate(make_no_conflict_context())

        result = DecisionResultV2(
            request_id="req-003",
            tenant_id="tenant_A",
            decision="PASS",
            reason_codes=[],
            signals=signals,
        )

        assert not result.conflict_detected()
        assert result.signals == []


# ── SignalEvaluator end-to-end ────────────────────────────────────────────────

class TestSignalEvaluatorEndToEnd:
    """
    Full path: LeadSignalContext → SignalEvaluator → DecisionResultV2

    This is the enforcement test (3B.7):
    action + visibility + fallback all propagate correctly end-to-end.
    """

    def test_conflict_lead_gets_enforcement_signal_in_result(self):
        """
        End-to-end: enrichment conflict on phone field →
        signal emitted → DecisionResultV2 carries visibility downstream.
        """
        context = make_conflict_context(
            field_name="phone",
            manual_value="+420777111000",
            enrichment_value="+420777999999",
        )

        evaluator = SignalEvaluator()
        signals = evaluator.evaluate(context)

        result = DecisionResultV2(
            request_id="req-e2e-001",
            tenant_id="tenant_A",
            decision="PASS",
            reason_codes=[],
            signals=signals,
        )

        # Primary decision is still PASS — signal annotates, not changes
        assert result.decision == "PASS"

        # Signal is present
        assert result.conflict_detected()

        # Downstream visibility is unavoidable
        signal = result.signals[0]
        assert signal.visibility.crm_status == "conflict_flagged"
        assert signal.visibility.api_flags.get("conflict_detected") is True

        # Fallback is defined
        assert signal.fallback is not None
        assert signal.fallback.mode == FallbackMode.MANUAL_VALUE_AUTHORITATIVE

    def test_clean_lead_gets_no_signals(self):
        """Clean lead with no source conflicts → empty signals → no enforcement overhead."""
        context = make_manual_only_context()
        evaluator = SignalEvaluator()
        signals = evaluator.evaluate(context)

        result = DecisionResultV2(
            request_id="req-e2e-002",
            tenant_id="tenant_A",
            decision="PASS",
            reason_codes=[],
            signals=signals,
        )

        assert result.signals == []
        assert not result.conflict_detected()

    def test_signal_without_consequence_cannot_exist_in_result(self):
        """
        A signal in DecisionResultV2 must always have visibility set.
        This test verifies the enforcement chain:
          SignalDefinition (model validator) → SignalResult (deep copy) →
          DecisionResultV2 (carries result)

        If this test passes, it is impossible for a consequence-free signal
        to reach downstream through the normal evaluation path.
        """
        from lead_entry_guard.core.signal_models import SignalDefinition, SignalClass, VisibilityProjection
        from pydantic import ValidationError

        # Cannot create a signal without visibility — model validator blocks it
        with pytest.raises(ValidationError):
            SignalDefinition(
                code="ghost_signal",
                signal_class=SignalClass.INFORMATIONAL,
                action="accept_low_quality",
                visibility=VisibilityProjection(),  # empty — invalid
                fallback=None,
            )
