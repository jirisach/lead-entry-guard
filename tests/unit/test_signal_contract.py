"""
Phase 3B — Contract test for signal enforcement.

This is the Phase Gate for all of Phase 3B.

ADR-008 invariant:
  A signal without operational consequence is observability, not policy.

Every SignalDefinition emitted by the pipeline must define:
  - action      — immediate system behavior
  - visibility  — downstream-facing projection (no PII, at least one field set)
  - fallback    — consequence if no reaction occurs (required for CRITICAL signals)

If any test in this file fails, the feature must not be merged.

Test structure:
  Negative invariants — invalid definitions must be rejected at construction time.
  Positive invariants — valid definitions must be accepted and project into output.
  PII invariant       — visibility must never contain raw PII.
  Informational class — fallback-exempt signals must still define visibility.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from lead_entry_guard.core.signal_models import (
    FallbackMode,
    FallbackPolicy,
    SignalAction,
    SignalClass,
    SignalDefinition,
    SignalResult,
    VisibilityProjection,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def minimal_visibility() -> VisibilityProjection:
    """Smallest valid VisibilityProjection."""
    return VisibilityProjection(crm_status="needs_review")


def minimal_fallback() -> FallbackPolicy:
    """Smallest valid FallbackPolicy."""
    return FallbackPolicy(
        mode=FallbackMode.MANUAL_VALUE_AUTHORITATIVE,
        then="manual_value_remains_authoritative",
    )


def valid_critical_signal(code: str = "test_signal") -> SignalDefinition:
    return SignalDefinition(
        code=code,
        signal_class=SignalClass.CRITICAL,
        action=SignalAction.ACCEPT_WITH_FLAG,
        visibility=minimal_visibility(),
        fallback=minimal_fallback(),
    )


def valid_informational_signal(code: str = "test_info_signal") -> SignalDefinition:
    return SignalDefinition(
        code=code,
        signal_class=SignalClass.INFORMATIONAL,
        action=SignalAction.ACCEPT_LOW_QUALITY,
        visibility=VisibilityProjection(routing_tags=["low_quality"]),
        fallback=None,  # explicitly exempt
    )


# ── Negative invariants ───────────────────────────────────────────────────────

class TestNegativeInvariants:
    """
    Invalid signal definitions must be rejected at construction time.
    They must not be emitted by the pipeline.
    """

    def test_critical_signal_without_fallback_is_invalid(self):
        """
        CRITICAL signal without fallback violates ADR-008.
        'requires_review: true' without a fallback is not a valid signal definition.
        """
        with pytest.raises(ValidationError, match="must define fallback"):
            SignalDefinition(
                code="no_fallback",
                signal_class=SignalClass.CRITICAL,
                action=SignalAction.ACCEPT_WITH_FLAG,
                visibility=minimal_visibility(),
                fallback=None,  # ← invalid for CRITICAL
            )

    def test_signal_with_empty_visibility_is_invalid(self):
        """
        A signal with no visibility fields violates ADR-008.
        Visibility is the minimum consequence — no signal may be consequence-free.
        """
        with pytest.raises(ValidationError, match="at least one field"):
            VisibilityProjection(
                crm_status=None,
                routing_tags=[],
                api_flags={},
            )

    def test_signal_visibility_with_no_fields_blocks_construction(self):
        """
        SignalDefinition cannot be constructed with an empty VisibilityProjection.
        The validator fires at VisibilityProjection level, before SignalDefinition.
        """
        with pytest.raises(ValidationError):
            SignalDefinition(
                code="no_visibility",
                signal_class=SignalClass.CRITICAL,
                action=SignalAction.ACCEPT_WITH_FLAG,
                visibility=VisibilityProjection(),  # all fields default to None/empty
                fallback=minimal_fallback(),
            )


# ── Positive invariants ───────────────────────────────────────────────────────

class TestPositiveInvariants:
    """
    Valid signal definitions must be accepted and must be projectable into output.
    """

    def test_complete_critical_signal_is_valid(self):
        """A CRITICAL signal with action + visibility + fallback is valid."""
        signal = valid_critical_signal()
        assert signal.code == "test_signal"
        assert signal.action == SignalAction.ACCEPT_WITH_FLAG
        assert signal.visibility.crm_status == "needs_review"
        assert signal.fallback is not None

    def test_complete_informational_signal_is_valid(self):
        """An INFORMATIONAL signal with action + visibility (no fallback) is valid."""
        signal = valid_informational_signal()
        assert signal.signal_class == SignalClass.INFORMATIONAL
        assert signal.fallback is None
        assert signal.visibility.routing_tags == ["low_quality"]

    def test_complete_signal_projects_into_signal_result(self):
        """
        A valid SignalDefinition must produce a SignalResult via from_definition().
        This is the downstream contract — SignalResult is what DecisionResult carries.
        """
        definition = valid_critical_signal("source_conflict")
        result = SignalResult.from_definition(definition)

        assert result.code == definition.code
        assert result.action == definition.action
        assert result.visibility == definition.visibility
        assert result.fallback == definition.fallback
        assert result.signal_class == definition.signal_class

    def test_signal_result_has_all_required_fields(self):
        """SignalResult carries action, visibility, and fallback — never just code."""
        result = SignalResult.from_definition(valid_critical_signal())
        assert result.action is not None
        assert result.visibility is not None
        assert result.fallback is not None

    def test_visibility_with_only_crm_status_is_valid(self):
        v = VisibilityProjection(crm_status="conflict_flagged")
        assert v.crm_status == "conflict_flagged"

    def test_visibility_with_only_routing_tags_is_valid(self):
        v = VisibilityProjection(routing_tags=["low_trust"])
        assert v.routing_tags == ["low_trust"]

    def test_visibility_with_only_api_flags_is_valid(self):
        v = VisibilityProjection(api_flags={"requires_review": True})
        assert v.api_flags["requires_review"] is True


# ── PII invariant ─────────────────────────────────────────────────────────────

class TestPIIInvariant:
    """
    ADR-008 — Data Exposure Invariant:
    Visibility fields must never contain raw PII.

    These tests document the invariant contractually.
    They cannot enforce it automatically at the model level (strings are strings),
    but they establish the contract and catch obvious violations in code review.

    A future linting or model-level enforcement step may be added in Phase 3B.
    """

    def test_visibility_crm_status_must_be_code_not_pii(self):
        """
        crm_status must be a status code, not a PII-containing string.
        Valid: "needs_review", "conflict_flagged", "low_trust"
        Invalid: "email: user@example.com", "suspicious: john@weird.com"
        """
        # Valid — status code only
        v = VisibilityProjection(crm_status="needs_review")
        assert "@" not in v.crm_status, (
            "crm_status must not contain raw PII (e.g. email addresses). "
            "ADR-008 Data Exposure Invariant."
        )

    def test_visibility_routing_tags_must_not_contain_pii(self):
        """routing_tags must be short classification labels, not PII values."""
        v = VisibilityProjection(routing_tags=["low_trust", "source_conflict"])
        for tag in v.routing_tags:
            assert "@" not in tag, f"routing_tag '{tag}' contains PII — ADR-008 violation"
            assert len(tag) < 64, f"routing_tag '{tag}' is suspiciously long — may contain PII"

    def test_visibility_api_flags_keys_must_be_descriptive_not_pii(self):
        """api_flags keys must be flag names, values must be booleans."""
        v = VisibilityProjection(api_flags={"requires_review": True, "conflict_detected": False})
        for key, value in v.api_flags.items():
            assert isinstance(value, bool), (
                f"api_flags value for '{key}' must be bool, got {type(value).__name__}. "
                "Free-form text in api_flags may contain PII — ADR-008 violation."
            )


# ── A4 signal definition (source_conflict_manual_vs_enrichment) ───────────────

class TestA4SignalDefinition:
    """
    Validates the concrete signal definition for A4:
    enrichment overwrites a manually collected field.

    Expected behavior (from field feedback, Prianka March 2026):
      - manual value takes priority
      - accept the lead but flag the conflict
      - downstream must see the conflict without reading internal metadata
    """

    def build_a4_signal(self) -> SignalDefinition:
        return SignalDefinition(
            code="source_conflict_manual_vs_enrichment",
            signal_class=SignalClass.CRITICAL,
            action=SignalAction.PRESERVE_MANUAL_VALUE,
            visibility=VisibilityProjection(
                crm_status="conflict_flagged",
                routing_tags=["source_conflict"],
                api_flags={"conflict_detected": True},
            ),
            fallback=FallbackPolicy(
                mode=FallbackMode.MANUAL_VALUE_AUTHORITATIVE,
                then="manual_value_remains_authoritative",
            ),
        )

    def test_a4_signal_is_valid(self):
        """A4 signal definition must pass the full contract."""
        signal = self.build_a4_signal()
        assert signal.code == "source_conflict_manual_vs_enrichment"
        assert signal.action == SignalAction.PRESERVE_MANUAL_VALUE

    def test_a4_signal_has_unavoidable_visibility(self):
        """A4 visibility must set crm_status AND api_flags — not just tags."""
        signal = self.build_a4_signal()
        assert signal.visibility.crm_status is not None
        assert signal.visibility.api_flags.get("conflict_detected") is True

    def test_a4_fallback_preserves_manual_value(self):
        """A4 fallback must specify that manual value remains authoritative."""
        signal = self.build_a4_signal()
        assert signal.fallback is not None
        assert signal.fallback.mode == FallbackMode.MANUAL_VALUE_AUTHORITATIVE
        assert "manual" in signal.fallback.then

    def test_a4_projects_into_signal_result(self):
        """A4 definition must project cleanly into SignalResult."""
        definition = self.build_a4_signal()
        result = SignalResult.from_definition(definition)
        assert result.code == definition.code
        assert result.fallback.mode == FallbackMode.MANUAL_VALUE_AUTHORITATIVE

    def test_a4_visibility_contains_no_pii(self):
        """A4 visibility must use codes and flags only — no raw field values."""
        signal = self.build_a4_signal()
        v = signal.visibility
        assert v.crm_status == "conflict_flagged"  # code, not raw value
        for tag in v.routing_tags:
            assert "@" not in tag
        for key, val in v.api_flags.items():
            assert isinstance(val, bool)
