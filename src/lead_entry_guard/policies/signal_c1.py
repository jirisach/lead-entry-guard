"""
Phase 3B — Context quality signals: C1, C2, C3

Three signals that evaluate decision readiness, not data quality.

  C1 — missing_context
    No usable identity anchor: no usable email AND no usable phone.
    "Usable" = present and not a placeholder value.
    Action: ROUTE_FOR_REVIEW
    CRM: incomplete_lead

    Note: C1 is a context signal, not a validation replacement.
    The validation layer rejects fatal cases (missing required email) before
    signals run. C1 fires when identity is present but unusable.

  C2 — conflicting_context
    Routing-relevant fields (company, phone) have contradicting values from
    different trusted sources after normalization.
    Action: ROUTE_FOR_REVIEW
    CRM: conflicting_data

    Distinction from A4:
      A4 = data observation: manual value conflicts with enrichment on any field.
      C2 = context conclusion: cross-source conflict on routing-relevant fields
           degrades the system's ability to safely make a routing decision.
    Both may fire on the same lead. That is correct and intentional.

  C3 — false_clarity
    Data appears complete but decision context is insufficient.
    Shared inbox prefix + no company + no enrichment records.
    Action: ACCEPT_WITH_FLAG
    CRM: low_confidence_decision

Evaluation rules:
  - C1 and C2 are evaluated independently.
  - C2 evaluates only when there is enough field data to make a conflict
    meaningful (i.e. not a pure C1 missing-identity case).
  - C3 only fires if both C1 and C2 return clean.
  - C1 + C2 simultaneous is theoretically possible but rare by design:
    C2 requires field records to compare, which C1 cases typically lack.

signal_family: "context" for all three.
"""
from __future__ import annotations

from lead_entry_guard.core.signal_models import (
    FallbackMode,
    FallbackPolicy,
    LeadSignalContext,
    SignalAction,
    SignalClass,
    SignalDefinition,
    SignalResult,
    VisibilityProjection,
)


# ── Constants ─────────────────────────────────────────────────────────────────

_PLACEHOLDER_VALUES: frozenset[str] = frozenset({
    "test", "n/a", "na", "unknown", "none", "null",
    "placeholder", "example", "sample",
})

_SHARED_INBOX_PREFIXES: frozenset[str] = frozenset({
    "info", "support", "sales", "contact", "hello",
})

# V1: company and phone only.
# Email excluded — aliasing, case variance, and secondary emails create too
# much noise to be reliably treated as a routing-relevant conflict.
_C2_RELEVANT_FIELDS: frozenset[str] = frozenset({"company", "phone"})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_missing(value: str | None) -> bool:
    """True if value is absent or empty string."""
    return value is None or not value.strip()


def _is_placeholder(value: str) -> bool:
    """
    True if value is present but semantically empty.
    Caller must guarantee value is not None.
    """
    return value.strip().lower() in _PLACEHOLDER_VALUES


def _is_usable(value: str | None) -> bool:
    """
    True if value is present and not a placeholder.

    Separation of _is_missing and _is_placeholder is intentional:
      None            → missing (no data provided)
      "none" / "n/a"  → placeholder (data provided but semantically empty)
    These are different situations and must not be conflated.
    """
    if _is_missing(value):
        return False
    assert value is not None  # narrowing for type checker
    return not _is_placeholder(value)


def _get_email_prefix(email: str) -> str | None:
    """Extract local part before @. Returns None if malformed."""
    at_index = email.find("@")
    if at_index <= 0:
        return None
    return email[:at_index].lower()


# ── Signal definitions ────────────────────────────────────────────────────────

C1_SIGNAL: SignalDefinition = SignalDefinition(
    code="missing_context",
    signal_family="context",
    signal_class=SignalClass.CRITICAL,
    action=SignalAction.ROUTE_FOR_REVIEW,
    visibility=VisibilityProjection(
        crm_status="incomplete_lead",
        routing_tags=["missing_context"],
        api_flags={"context_missing": True},
    ),
    fallback=FallbackPolicy(
        mode=FallbackMode.AUTO_EXPIRE_REVIEW,
        after_hours=24,
        then="lead_expired_no_identity_anchor",
    ),
)

C2_SIGNAL: SignalDefinition = SignalDefinition(
    code="conflicting_context",
    signal_family="context",
    signal_class=SignalClass.CRITICAL,
    action=SignalAction.ROUTE_FOR_REVIEW,
    visibility=VisibilityProjection(
        crm_status="conflicting_data",
        routing_tags=["conflicting_context"],
        api_flags={"context_conflict": True},
    ),
    fallback=FallbackPolicy(
        mode=FallbackMode.AUTO_EXPIRE_REVIEW,
        after_hours=24,
        then="lead_expired_routing_conflict_unresolved",
    ),
)

C3_SIGNAL: SignalDefinition = SignalDefinition(
    code="false_clarity",
    signal_family="context",
    signal_class=SignalClass.INFORMATIONAL,
    action=SignalAction.ACCEPT_WITH_FLAG,
    visibility=VisibilityProjection(
        crm_status="low_confidence_decision",
        routing_tags=["false_clarity"],
        api_flags={"decision_confidence_low": True},
    ),
    fallback=FallbackPolicy(
        mode=FallbackMode.KEEP_ACCEPTED_LOW_TRUST,
        then="lead_accepted_with_low_decision_confidence",
    ),
)


# ── Detection functions ───────────────────────────────────────────────────────

def has_missing_context(context: LeadSignalContext) -> bool:
    """
    True if lead has no usable identity anchor.

    Identity anchor = usable email OR usable phone.
    Company is not part of this check — company is business context, not identity.

    Usable = present (not None, not empty) and not a placeholder value.
    Phone None check is explicit — str(None) == "None" would accidentally
    match the "none" placeholder, masking a missing value as a placeholder.
    """
    has_usable_email = _is_usable(context.email)

    has_usable_phone = False
    for f in context.fields:
        if f.field_name != "phone":
            continue
        if f.value is None:
            continue  # explicit None check — not treated as placeholder
        if _is_usable(str(f.value)):
            has_usable_phone = True
            break

    return not has_usable_email and not has_usable_phone


def has_conflicting_context(context: LeadSignalContext) -> bool:
    """
    True if routing-relevant fields have cross-source value conflicts.

    V1 scope: company and phone only.

    Conflict = same field has records from 2+ different source_types
    with semantically different values after normalization (strip + lowercase).

    Not all mismatches qualify:
      - Records from the same source_type do not constitute a conflict.
      - Empty / None values are excluded from comparison.
      - Normalization reduces formatting noise (whitespace, case).

    Designed to be rare in C1 cases: C1 fires when identity fields are
    missing/unusable, which typically means there are few field records
    to compare. C2 requires populated field records to detect a conflict.
    """
    for field_name in _C2_RELEVANT_FIELDS:
        records = context.get_fields(field_name)
        if len(records) < 2:
            continue

        # Group normalized values by source_type
        by_source: dict[str, str] = {}
        for r in records:
            if r.value is None:
                continue
            normalized = str(r.value).strip().lower()
            if not normalized:
                continue
            # First record per source_type wins — representative value
            if r.source_type not in by_source:
                by_source[r.source_type] = normalized

        if len(by_source) < 2:
            continue  # all records from same source — not a cross-source conflict

        unique_values = set(by_source.values())
        if len(unique_values) > 1:
            return True

    return False


def has_false_clarity(context: LeadSignalContext) -> bool:
    """
    True if data appears complete but decision context is insufficient.

    All conditions must hold:
      1. Email is present and usable (if not, C1 fires instead)
      2. Email prefix is a known shared inbox pattern
      3. No company field with a usable value
      4. No enrichment source records present

    Rationale: shared inbox + no company + no enrichment means the system
    cannot determine who this lead represents, even though data is present.
    The data looks complete; the decision context is not.
    """
    if not _is_usable(context.email):
        return False  # C1 territory, not C3

    prefix = _get_email_prefix(context.email)  # type: ignore[arg-type]
    if not prefix or prefix not in _SHARED_INBOX_PREFIXES:
        return False

    has_usable_company = any(
        f.field_name == "company" and f.value is not None and _is_usable(str(f.value))
        for f in context.fields
    )
    if has_usable_company:
        return False

    has_enrichment = any(f.source_type == "enrichment" for f in context.fields)
    return not has_enrichment


# ── Rule class ────────────────────────────────────────────────────────────────

class C1C2C3SignalRule:
    """
    Context quality signal evaluation — C1, C2, C3.

    Evaluation order:
      1. C1 (missing identity anchor) — evaluated unconditionally.
      2. C2 (routing-relevant conflict) — evaluated unconditionally.
         Rare to fire simultaneously with C1 by design: C2 requires populated
         field records, which C1 cases typically lack.
      3. C3 (false clarity) — only fires if C1 and C2 both return clean.
         C3 presupposes usable identity; C1/C2 supersede it.

    Returns empty list if no context quality issues detected.
    Never returns None.
    """

    def evaluate(self, context: LeadSignalContext) -> list[SignalResult]:
        signals: list[SignalResult] = []

        c1_fired = has_missing_context(context)
        c2_fired = has_conflicting_context(context)

        if c1_fired:
            signals.append(SignalResult.from_definition(C1_SIGNAL))
        if c2_fired:
            signals.append(SignalResult.from_definition(C2_SIGNAL))

        # C3 only when C1 and C2 are both clean
        if not c1_fired and not c2_fired:
            if has_false_clarity(context):
                signals.append(SignalResult.from_definition(C3_SIGNAL))

        return signals
