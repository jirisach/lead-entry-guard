"""
Context signal tests — C1, C2, C3 + evaluator-level overlap.

Two layers:
  1. Rule-level: C1C2C3SignalRule in isolation.
  2. Evaluator-level: SignalEvaluator — overlap with A4 and A6.

Coverage:
  C1 — missing identity anchor
  C2 — routing-relevant cross-source conflict
  C3 — false clarity (shared inbox + no company + no enrichment)
  Suppression: C3 suppressed by C1, C3 suppressed by C2
  Simultaneous: C1 + C2, A6 + C3, A4 + C2
  None vs "none" semantic boundary for phone

Placement: tests/unit/test_context_signals.py
"""
from __future__ import annotations

import pytest

from lead_entry_guard.core.signal_models import FieldSourceRecord, LeadSignalContext
from lead_entry_guard.policies.signal_c1 import (
    C1C2C3SignalRule,
    has_missing_context,
    has_conflicting_context,
    has_false_clarity,
)
from lead_entry_guard.policies.signal_evaluator import SignalEvaluator


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_context(
    email: str | None = None,
    fields: list[FieldSourceRecord] | None = None,
    tenant_id: str = "t1",
) -> LeadSignalContext:
    return LeadSignalContext(
        tenant_id=tenant_id,
        email=email,
        fields=fields or [],
    )


def field(name: str, value, source_type: str = "manual") -> FieldSourceRecord:
    return FieldSourceRecord(field_name=name, source_type=source_type, value=value)


def signal_codes(context: LeadSignalContext) -> list[str]:
    """Run C1C2C3SignalRule and return emitted codes."""
    return [s.code for s in C1C2C3SignalRule().evaluate(context)]


def evaluator_codes(context: LeadSignalContext) -> list[str]:
    """Run full SignalEvaluator and return emitted codes."""
    return [s.code for s in SignalEvaluator().evaluate(context)]


# ── C1: missing identity anchor ───────────────────────────────────────────────

class TestC1MissingContext:

    def test_fires_when_no_email_and_no_phone(self):
        ctx = make_context(email=None)
        assert "missing_context" in signal_codes(ctx)

    def test_fires_when_email_empty_string(self):
        ctx = make_context(email="")
        assert "missing_context" in signal_codes(ctx)

    def test_fires_when_email_is_placeholder(self):
        ctx = make_context(email="unknown")
        assert "missing_context" in signal_codes(ctx)

    def test_fires_when_email_placeholder_and_no_phone(self):
        ctx = make_context(email="n/a")
        assert "missing_context" in signal_codes(ctx)

    def test_does_not_fire_when_usable_email_present(self):
        ctx = make_context(email="alice@example.com")
        assert "missing_context" not in signal_codes(ctx)

    def test_does_not_fire_when_usable_phone_present_no_email(self):
        ctx = make_context(
            email=None,
            fields=[field("phone", "+420777123456")],
        )
        assert "missing_context" not in signal_codes(ctx)

    def test_fires_when_phone_is_none_explicit(self):
        """None phone must not be treated as placeholder — it is missing."""
        ctx = make_context(
            email=None,
            fields=[field("phone", None)],
        )
        assert "missing_context" in signal_codes(ctx)

    def test_fires_when_phone_string_none_is_placeholder(self):
        """
        String "none" is a placeholder — not a usable phone.
        C1 must fire because no usable anchor exists.

        This verifies the semantic boundary:
          None  → missing (no data provided)
          "none" → placeholder (data provided but semantically empty)
        """
        ctx = make_context(
            email=None,
            fields=[field("phone", "none")],
        )
        assert "missing_context" in signal_codes(ctx)

    def test_none_phone_and_string_none_phone_both_unusable(self):
        """Both None and 'none' must result in C1 — different reasons, same outcome."""
        ctx_none = make_context(email=None, fields=[field("phone", None)])
        ctx_str = make_context(email=None, fields=[field("phone", "none")])
        assert "missing_context" in signal_codes(ctx_none)
        assert "missing_context" in signal_codes(ctx_str)

    def test_company_absence_does_not_trigger_c1(self):
        """Company is business context, not identity anchor. C1 must not fire."""
        ctx = make_context(
            email="alice@example.com",
            fields=[],  # no company
        )
        assert "missing_context" not in signal_codes(ctx)

    def test_signal_family_is_context(self):
        ctx = make_context(email=None)
        results = C1C2C3SignalRule().evaluate(ctx)
        c1 = next((s for s in results if s.code == "missing_context"), None)
        assert c1 is not None
        assert c1.signal_family == "context"


# ── C2: routing-relevant cross-source conflict ────────────────────────────────

class TestC2ConflictingContext:

    def test_fires_on_company_conflict_across_sources(self):
        ctx = make_context(
            email="alice@example.com",
            fields=[
                field("company", "Acme Corp", source_type="manual"),
                field("company", "Acme Inc", source_type="enrichment"),
            ],
        )
        assert "conflicting_context" in signal_codes(ctx)

    def test_fires_on_phone_conflict_across_sources(self):
        ctx = make_context(
            email="alice@example.com",
            fields=[
                field("phone", "+420777111222", source_type="manual"),
                field("phone", "+420777999888", source_type="enrichment"),
            ],
        )
        assert "conflicting_context" in signal_codes(ctx)

    def test_does_not_fire_when_same_source_type(self):
        """Two manual records with different values — not a cross-source conflict."""
        ctx = make_context(
            email="alice@example.com",
            fields=[
                field("company", "Acme Corp", source_type="manual"),
                field("company", "Acme Inc", source_type="manual"),
            ],
        )
        assert "conflicting_context" not in signal_codes(ctx)

    def test_does_not_fire_when_values_identical_after_normalization(self):
        ctx = make_context(
            email="alice@example.com",
            fields=[
                field("company", "Acme Corp", source_type="manual"),
                field("company", "  acme corp  ", source_type="enrichment"),
            ],
        )
        assert "conflicting_context" not in signal_codes(ctx)

    def test_does_not_fire_on_email_conflict(self):
        """Email is excluded from C2 v1 — too noisy (aliases, case, secondary)."""
        ctx = make_context(
            email="alice@example.com",
            fields=[
                field("email", "alice@example.com", source_type="manual"),
                field("email", "alice.smith@example.com", source_type="enrichment"),
            ],
        )
        assert "conflicting_context" not in signal_codes(ctx)

    def test_does_not_fire_on_irrelevant_field(self):
        """Fields outside C2 scope (e.g. country) must not trigger C2."""
        ctx = make_context(
            email="alice@example.com",
            fields=[
                field("country", "CZ", source_type="manual"),
                field("country", "SK", source_type="enrichment"),
            ],
        )
        assert "conflicting_context" not in signal_codes(ctx)

    def test_does_not_fire_with_single_record(self):
        ctx = make_context(
            email="alice@example.com",
            fields=[field("company", "Acme", source_type="manual")],
        )
        assert "conflicting_context" not in signal_codes(ctx)

    def test_does_not_fire_when_none_values_excluded(self):
        """None values must be excluded from comparison — not treated as empty string."""
        ctx = make_context(
            email="alice@example.com",
            fields=[
                field("company", None, source_type="manual"),
                field("company", "Acme", source_type="enrichment"),
            ],
        )
        assert "conflicting_context" not in signal_codes(ctx)

    def test_signal_family_is_context(self):
        ctx = make_context(
            email="alice@example.com",
            fields=[
                field("company", "Acme Corp", source_type="manual"),
                field("company", "Acme Inc", source_type="enrichment"),
            ],
        )
        results = C1C2C3SignalRule().evaluate(ctx)
        c2 = next((s for s in results if s.code == "conflicting_context"), None)
        assert c2 is not None
        assert c2.signal_family == "context"


# ── C3: false clarity ─────────────────────────────────────────────────────────

class TestC3FalseClarity:

    def test_fires_on_shared_inbox_no_company_no_enrichment(self):
        ctx = make_context(email="info@example.com")
        assert "false_clarity" in signal_codes(ctx)

    def test_fires_for_all_shared_inbox_prefixes(self):
        prefixes = ["info", "support", "sales", "contact", "hello"]
        for prefix in prefixes:
            ctx = make_context(email=f"{prefix}@example.com")
            assert "false_clarity" in signal_codes(ctx), (
                f"C3 must fire for shared inbox prefix '{prefix}'"
            )

    def test_does_not_fire_when_company_present(self):
        ctx = make_context(
            email="info@example.com",
            fields=[field("company", "Acme Corp")],
        )
        assert "false_clarity" not in signal_codes(ctx)

    def test_does_not_fire_when_enrichment_present(self):
        ctx = make_context(
            email="info@example.com",
            fields=[field("company", "Acme", source_type="enrichment")],
        )
        assert "false_clarity" not in signal_codes(ctx)

    def test_does_not_fire_without_shared_inbox_prefix(self):
        ctx = make_context(email="alice@example.com")
        assert "false_clarity" not in signal_codes(ctx)

    def test_fires_when_company_is_placeholder(self):
        """Placeholder company must not satisfy the company condition."""
        ctx = make_context(
            email="info@example.com",
            fields=[field("company", "unknown")],
        )
        assert "false_clarity" in signal_codes(ctx)

    def test_signal_family_is_context(self):
        ctx = make_context(email="info@example.com")
        results = C1C2C3SignalRule().evaluate(ctx)
        c3 = next((s for s in results if s.code == "false_clarity"), None)
        assert c3 is not None
        assert c3.signal_family == "context"


# ── Suppression rules ─────────────────────────────────────────────────────────

class TestSuppressionRules:

    def test_c3_suppressed_by_c1(self):
        """
        When C1 fires (no identity anchor), C3 must not fire.
        No usable email → no shared inbox pattern to evaluate.
        """
        ctx = make_context(email=None)
        codes = signal_codes(ctx)
        assert "missing_context" in codes
        assert "false_clarity" not in codes

    def test_c3_suppressed_by_c2(self):
        """
        When C2 fires (routing conflict), C3 must not fire.
        """
        ctx = make_context(
            email="info@example.com",
            fields=[
                field("company", "Acme Corp", source_type="manual"),
                field("company", "Acme Inc", source_type="enrichment"),
            ],
        )
        codes = signal_codes(ctx)
        assert "conflicting_context" in codes
        assert "false_clarity" not in codes

    def test_c3_fires_when_c1_and_c2_clean(self):
        """C3 fires only when C1 and C2 both return clean."""
        ctx = make_context(email="info@example.com")
        codes = signal_codes(ctx)
        assert "false_clarity" in codes
        assert "missing_context" not in codes
        assert "conflicting_context" not in codes


# ── Simultaneous signals ──────────────────────────────────────────────────────

class TestSimultaneousSignals:

    def test_c1_and_c2_can_fire_simultaneously(self):
        """
        C1: no usable email or phone.
        C2: company conflict across sources.
        Both are independent — both may fire.

        This is a valid multi-layer diagnosis, not a bug.
        """
        ctx = make_context(
            email=None,
            fields=[
                field("company", "Acme Corp", source_type="manual"),
                field("company", "Acme Inc", source_type="enrichment"),
                # no phone
            ],
        )
        codes = signal_codes(ctx)
        assert "missing_context" in codes
        assert "conflicting_context" in codes
        assert "false_clarity" not in codes

    def test_a6_and_c3_can_fire_simultaneously(self):
        """
        A6: shared inbox (data observation).
        C3: false clarity (context conclusion — shared inbox + no company + no enrichment).

        Both fire on the same lead. That is correct:
          A6 = what we observed about the email
          C3 = what that means for decision readiness
        """
        ctx = make_context(email="info@example.com")
        codes = evaluator_codes(ctx)
        assert "shared_inbox" in codes, "A6 must fire on shared inbox prefix"
        assert "false_clarity" in codes, "C3 must fire — no company, no enrichment"

    def test_a4_and_c2_can_fire_simultaneously(self):
        """
        A4: manual vs enrichment conflict on company (data observation).
        C2: routing-relevant cross-source conflict on company (context conclusion).

        Both fire. That is correct:
          A4 = concrete conflict between manual and enrichment
          C2 = decision readiness degraded by routing-relevant conflict
        """
        ctx = make_context(
            email="alice@example.com",
            fields=[
                field("company", "Acme Corp", source_type="manual"),
                field("company", "Acme Inc", source_type="enrichment"),
            ],
        )
        codes = evaluator_codes(ctx)
        assert "source_conflict_manual_vs_enrichment" in codes, "A4 must fire"
        assert "conflicting_context" in codes, "C2 must fire"

    def test_clean_lead_emits_no_context_signals(self):
        """Healthy lead with usable email and no conflicts — no context signals."""
        ctx = make_context(
            email="alice@acme.com",
            fields=[
                field("company", "Acme Corp", source_type="manual"),
                field("phone", "+420777123456", source_type="manual"),
            ],
        )
        codes = signal_codes(ctx)
        assert "missing_context" not in codes
        assert "conflicting_context" not in codes
        assert "false_clarity" not in codes


# ── signal_family invariant across all C signals ──────────────────────────────

class TestSignalFamilyInvariant:

    def test_all_c_signals_have_context_family(self):
        """
        Every signal emitted by C1C2C3SignalRule must have signal_family == 'context'.
        Regression guard: adding a new C signal without family would break this.
        """
        # C1
        c1_ctx = make_context(email=None)
        # C2
        c2_ctx = make_context(
            email="alice@example.com",
            fields=[
                field("company", "Acme", source_type="manual"),
                field("company", "Acme Inc", source_type="enrichment"),
            ],
        )
        # C3
        c3_ctx = make_context(email="info@example.com")

        rule = C1C2C3SignalRule()
        for ctx in [c1_ctx, c2_ctx, c3_ctx]:
            for signal in rule.evaluate(ctx):
                assert signal.signal_family == "context", (
                    f"Signal '{signal.code}' has family '{signal.signal_family}', "
                    "expected 'context'. All C-series signals must be context family."
                )

    def test_a_series_signals_have_data_family(self):
        """
        Regression guard: A-series signals must have signal_family == 'data'.
        Verifies that adding signal_family to existing definitions was correct.
        """
        # Triggers A3 (suspicious TLD) and A6 (shared inbox)
        ctx = make_context(email="info@example.xyz")
        results = SignalEvaluator().evaluate(ctx)
        a_series = [s for s in results if s.signal_family == "data"]
        assert len(a_series) > 0, (
            "At least one data-family signal must fire on suspicious + shared inbox email."
        )
        for s in a_series:
            assert s.signal_family == "data"
