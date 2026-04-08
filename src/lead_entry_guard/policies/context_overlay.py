"""
Context Overlay — apply_context_overlay()

Applies context-signal consequences to a finalized DecisionResultV2.

This is a pure function. It:
  - Takes a DecisionResultV2 with signals already attached.
  - Returns a new DecisionResultV2 with review_required and decision_confidence set.
  - Never mutates the input.
  - Never changes the primary decision field.
  - Never overrides REJECT.

Rules:
  REJECT is terminal — overlay returns the result unchanged.
  C1 (missing_context) or C2 (conflicting_context) → review_required = True
  C3 (false_clarity) → decision_confidence = "low"
  Clean result (no context signals) → returned unchanged.

Idempotent: calling apply_context_overlay() twice on the same result
produces the same output. Fields are set to explicit values, not toggled.

Call site:
  # TODO: apply_context_overlay(result) after signals are attached to DecisionResultV2.
  # Correct place: wherever DecisionResultV2 is assembled with signals list populated.

Design constraints:
  - Does not validate data. Does not re-run signals.
  - Does not set numeric confidence scores.
  - Does not introduce priority ordering between context signals.
  - Does not touch reason_codes, decision, or any other field.

Related:
  - signal_c1.py — C1, C2, C3 detection logic
  - signal_models.py — DecisionResultV2, SignalResult
  - context-signals-v1.md — design rationale
"""
from __future__ import annotations

from dataclasses import replace

from lead_entry_guard.core.signal_models import DecisionResultV2

# Context signal codes that require human review.
# C1: no usable identity anchor.
# C2: routing-relevant cross-source conflict.
_REVIEW_REQUIRED_CODES: frozenset[str] = frozenset({
    "missing_context",
    "conflicting_context",
})

# Context signal codes that lower decision confidence without forcing review.
# C3: data present but decision context insufficient (false clarity).
_LOW_CONFIDENCE_CODES: frozenset[str] = frozenset({
    "false_clarity",
})


def apply_context_overlay(result: DecisionResultV2) -> DecisionResultV2:
    """
    Apply context-signal consequences to a DecisionResultV2.

    Returns a new DecisionResultV2 instance. Input is never mutated.

    Args:
        result: A DecisionResultV2 with signals already populated.

    Returns:
        A new DecisionResultV2 with review_required and/or decision_confidence
        adjusted based on context signals. All other fields are identical to input.

    Guarantees:
        - REJECT decision is never modified. Returns input unchanged.
        - review_required is only set True, never forced to False.
        - decision_confidence is only set to "low", never forced back to "normal".
        - Idempotent: applying twice produces the same result as applying once.
    """
    # REJECT is terminal. No context signal changes this.
    if result.decision == "REJECT":
        return result

    signal_codes = {s.code for s in result.signals}

    review_required = result.review_required
    decision_confidence = result.decision_confidence

    if signal_codes & _REVIEW_REQUIRED_CODES:
        review_required = True

    if signal_codes & _LOW_CONFIDENCE_CODES:
        decision_confidence = "low"

    # Return unchanged instance if nothing changed — avoids unnecessary allocation.
    if (
        review_required == result.review_required
        and decision_confidence == result.decision_confidence
    ):
        return result

    return replace(
        result,
        review_required=review_required,
        decision_confidence=decision_confidence,
    )
