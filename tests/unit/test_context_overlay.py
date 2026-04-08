"""
Context overlay tests — apply_context_overlay()

Tests the pure function that applies context-signal consequences
to a finalized DecisionResultV2.

Coverage:
  - REJECT is never overridden
  - C1 sets review_required
  - C2 sets review_required
  - C3 sets decision_confidence = "low"
  - C3 does not set review_required
  - C1 + C2 simultaneously sets review_required
  - C1 + C3 sets both review_required and low confidence
  - A-series signals alone change nothing
  - Clean result is returned unchanged
  - Idempotency: applying overlay twice is same as once
  - Input is never mutated (pure function)
  - Return value is new instance when changes applied

Placement: tests/unit/test_context_overlay.py
"""
from __future__ import annotations

from dataclasses import replace

import pytest

from lead_entry_guard.core.signal_models import (
    DecisionResultV2,
    FallbackMode,
    FallbackPolicy,
    SignalAction,
    SignalClass,
    SignalDefinition,
    SignalResult,
    VisibilityProjection,
)
from lead_entry_guard.policies.context_overlay import apply_context_overlay


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_result(
    decision: str = "PASS",
    signals: list[SignalResult] | None = None,
    review_required: bool = False,
    decision_confidence: str = "normal",
) -> DecisionResultV2:
    return DecisionResultV2(
        request_id="req_test",
        tenant_id="t1",
        decision=decision,
        reason_codes=[],
        signals=signals or [],
        review_required=review_required,
        decision_confidence=decision_confidence,
    )


def make_signal(code: str, family: str = "context") -> SignalResult:
    """Minimal SignalResult for overlay testing."""
    defn = SignalDefinition(
        code=code,
        signal_family=family,
        signal_class=SignalClass.INFORMATIONAL,
        action=SignalAction.ACCEPT_WITH_FLAG,
        visibility=VisibilityProjection(crm_status="test"),
        fallback=FallbackPolicy(
            mode=FallbackMode.KEEP_ACCEPTED_LOW_TRUST,
            then="test_fallback",
        ),
    )
    return SignalResult.from_definition(defn)


# ── REJECT is terminal ────────────────────────────────────────────────────────

class TestRejectIsTerminal:

    def test_reject_with_c1_is_not_overridden(self):
        result = make_result(
            decision="REJECT",
            signals=[make_signal("missing_context")],
        )
        out = apply_context_overlay(result)
        assert out.decision == "REJECT"
        assert out.review_required is False

    def test_reject_with_c2_is_not_overridden(self):
        result = make_result(
            decision="REJECT",
            signals=[make_signal("conflicting_context")],
        )
        out = apply_context_overlay(result)
        assert out.review_required is False

    def test_reject_with_c3_is_not_overridden(self):
        result = make_result(
            decision="REJECT",
            signals=[make_signal("false_clarity")],
        )
        out = apply_context_overlay(result)
        assert out.decision_confidence == "normal"

    def test_reject_returns_same_instance(self):
        """REJECT returns the input object unchanged — no allocation."""
        result = make_result(decision="REJECT")
        out = apply_context_overlay(result)
        assert out is result


# ── C1: missing_context ───────────────────────────────────────────────────────

class TestC1Overlay:

    def test_c1_sets_review_required(self):
        result = make_result(signals=[make_signal("missing_context")])
        out = apply_context_overlay(result)
        assert out.review_required is True

    def test_c1_does_not_change_decision(self):
        result = make_result(decision="PASS", signals=[make_signal("missing_context")])
        out = apply_context_overlay(result)
        assert out.decision == "PASS"

    def test_c1_does_not_set_low_confidence(self):
        """C1 alone must not lower confidence — only C3 does that."""
        result = make_result(signals=[make_signal("missing_context")])
        out = apply_context_overlay(result)
        assert out.decision_confidence == "normal"


# ── C2: conflicting_context ───────────────────────────────────────────────────

class TestC2Overlay:

    def test_c2_sets_review_required(self):
        result = make_result(signals=[make_signal("conflicting_context")])
        out = apply_context_overlay(result)
        assert out.review_required is True

    def test_c2_does_not_change_decision(self):
        result = make_result(decision="PASS", signals=[make_signal("conflicting_context")])
        out = apply_context_overlay(result)
        assert out.decision == "PASS"

    def test_c2_does_not_set_low_confidence(self):
        result = make_result(signals=[make_signal("conflicting_context")])
        out = apply_context_overlay(result)
        assert out.decision_confidence == "normal"


# ── C3: false_clarity ─────────────────────────────────────────────────────────

class TestC3Overlay:

    def test_c3_sets_low_confidence(self):
        result = make_result(signals=[make_signal("false_clarity")])
        out = apply_context_overlay(result)
        assert out.decision_confidence == "low"

    def test_c3_does_not_set_review_required(self):
        """C3 flags, does not force review."""
        result = make_result(signals=[make_signal("false_clarity")])
        out = apply_context_overlay(result)
        assert out.review_required is False

    def test_c3_does_not_change_decision(self):
        result = make_result(decision="PASS", signals=[make_signal("false_clarity")])
        out = apply_context_overlay(result)
        assert out.decision == "PASS"


# ── Combinations ──────────────────────────────────────────────────────────────

class TestCombinations:

    def test_c1_and_c2_both_set_review_required(self):
        result = make_result(signals=[
            make_signal("missing_context"),
            make_signal("conflicting_context"),
        ])
        out = apply_context_overlay(result)
        assert out.review_required is True
        assert out.decision_confidence == "normal"

    def test_c1_and_c3_set_both_flags(self):
        """C1 sets review_required, C3 sets low confidence — both apply."""
        result = make_result(signals=[
            make_signal("missing_context"),
            make_signal("false_clarity"),
        ])
        out = apply_context_overlay(result)
        assert out.review_required is True
        assert out.decision_confidence == "low"

    def test_c2_and_c3_set_both_flags(self):
        result = make_result(signals=[
            make_signal("conflicting_context"),
            make_signal("false_clarity"),
        ])
        out = apply_context_overlay(result)
        assert out.review_required is True
        assert out.decision_confidence == "low"

    def test_a_series_signal_alone_changes_nothing(self):
        """Data signals do not affect overlay output."""
        result = make_result(signals=[make_signal("suspicious_domain", family="data")])
        out = apply_context_overlay(result)
        assert out.review_required is False
        assert out.decision_confidence == "normal"

    def test_a_series_with_c1_still_sets_review_required(self):
        result = make_result(signals=[
            make_signal("suspicious_domain", family="data"),
            make_signal("missing_context"),
        ])
        out = apply_context_overlay(result)
        assert out.review_required is True


# ── Clean result ──────────────────────────────────────────────────────────────

class TestCleanResult:

    def test_clean_result_unchanged(self):
        result = make_result()
        out = apply_context_overlay(result)
        assert out.review_required is False
        assert out.decision_confidence == "normal"

    def test_clean_result_returns_same_instance(self):
        """No allocation when nothing changes."""
        result = make_result()
        out = apply_context_overlay(result)
        assert out is result


# ── Idempotency ───────────────────────────────────────────────────────────────

class TestIdempotency:

    def test_applying_twice_same_as_once_c1(self):
        result = make_result(signals=[make_signal("missing_context")])
        once = apply_context_overlay(result)
        twice = apply_context_overlay(once)
        assert twice.review_required == once.review_required
        assert twice.decision_confidence == once.decision_confidence

    def test_applying_twice_same_as_once_c3(self):
        result = make_result(signals=[make_signal("false_clarity")])
        once = apply_context_overlay(result)
        twice = apply_context_overlay(once)
        assert twice.decision_confidence == once.decision_confidence

    def test_applying_twice_same_as_once_clean(self):
        result = make_result()
        once = apply_context_overlay(result)
        twice = apply_context_overlay(once)
        assert twice is once  # both return same instance — no change


# ── Pure function — no mutation ───────────────────────────────────────────────

class TestPureFunction:

    def test_input_not_mutated_by_c1(self):
        result = make_result(signals=[make_signal("missing_context")])
        original_review_required = result.review_required
        _ = apply_context_overlay(result)
        assert result.review_required == original_review_required

    def test_input_not_mutated_by_c3(self):
        result = make_result(signals=[make_signal("false_clarity")])
        original_confidence = result.decision_confidence
        _ = apply_context_overlay(result)
        assert result.decision_confidence == original_confidence

    def test_output_is_new_instance_when_changed(self):
        result = make_result(signals=[make_signal("missing_context")])
        out = apply_context_overlay(result)
        assert out is not result
