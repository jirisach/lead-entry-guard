"""
Phase 3B — A4 signal: source_conflict_manual_vs_enrichment

Detects enrichment conflict against manually collected field values.

Signal: source_conflict_manual_vs_enrichment
Class: CRITICAL — manual value must be preserved, fallback required.
Action: PRESERVE_MANUAL_VALUE
Consequence: conflict flagged in CRM, manual value remains authoritative.

Detection rule:
  - For each field in LeadSignalContext.fields:
    - if the same field has both a manual and an enrichment record
    - and the values differ
    → emit signal

Distinction from C2 (conflicting_context):
  A4 = data observation: any manual-vs-enrichment mismatch on any field.
  C2 = context conclusion: cross-source conflict on routing-relevant fields
       (company, phone) degrades decision readiness.
  Both may fire on the same lead. That is correct and intentional.

Field values are never copied into SignalResult or VisibilityProjection.
Only tags, status codes, and boolean flags are emitted (ADR-008 PII invariant).
"""
from __future__ import annotations

from lead_entry_guard.core.signal_models import (
    FallbackMode,
    FallbackPolicy,
    FieldSourceRecord,
    LeadSignalContext,
    SignalAction,
    SignalClass,
    SignalDefinition,
    SignalResult,
    VisibilityProjection,
)


# ── A4 signal definition ──────────────────────────────────────────────────────

A4_SIGNAL: SignalDefinition = SignalDefinition(
    code="source_conflict_manual_vs_enrichment",
    signal_family="data",
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


# ── A4 signal rule ────────────────────────────────────────────────────────────

class A4SignalRule:
    """
    Detects enrichment-vs-manual conflict on a lead field.

    Fires when:
      - the same field has both a manual record and an enrichment record
      - the values differ

    Input: LeadSignalContext — uses context.fields only.
    Output: one SignalResult per conflicting field.
    """

    def evaluate(self, context: LeadSignalContext) -> list[SignalResult]:
        signals: list[SignalResult] = []

        field_names = {r.field_name for r in context.fields}
        for field_name in sorted(field_names):
            records = context.get_fields(field_name)
            manual_records = [r for r in records if r.source_type == "manual"]
            enrichment_records = [r for r in records if r.source_type == "enrichment"]

            if not manual_records or not enrichment_records:
                continue

            manual_value = manual_records[0].value
            enrichment_value = enrichment_records[0].value
            if manual_value == enrichment_value:
                continue

            # Conflict detected — emit signal without PII
            signals.append(SignalResult.from_definition(A4_SIGNAL))

        return signals
