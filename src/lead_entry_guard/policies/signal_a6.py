"""
Phase 3B — A6 signal: shared_inbox

Detects shared / role-based email addresses submitted through any source.

Signal: shared_inbox
Class: INFORMATIONAL — lead is accepted, quality is degraded, no hard block.
Action: ACCEPT_LOW_QUALITY

Detection rule:
  - Extract local part (before @) from context.email
  - Case insensitive exact match against SHARED_INBOX_PREFIXES
  - Empty or missing email → rule does not fire
  - Substring match is not used

Odsouhlasená prefix sada (March 2026):
  info, support, sales, contact, hello

Relationship to C3 (false_clarity):
  A6 fires on shared inbox prefix alone — it is a data observation.
  C3 fires when shared inbox prefix combines with no company + no enrichment —
  it is a context conclusion about decision readiness.
  Both may fire on the same lead. That is correct and intentional.

ADR-008 invariants:
  - Visibility is the minimum consequence.
  - Visibility fields contain no PII.
  - fallback-exempt does not mean consequence-free.

# Future note:
#   low_quality_reason="shared_inbox" as separate field may be added later.
#   Keep minimal scope for now.
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


# ── Odsouhlasená prefix sada ──────────────────────────────────────────────────

SHARED_INBOX_PREFIXES: frozenset[str] = frozenset({
    "info",
    "support",
    "sales",
    "contact",
    "hello",
})

# Intentionally excluded:
#   noreply / no-reply  → different signal type (non_contact_email)
#   admin               → may be internal technical mailbox
#   team / office       → too broad, high false positive risk


# ── A6 signal definition ──────────────────────────────────────────────────────

A6_SIGNAL: SignalDefinition = SignalDefinition(
    code="shared_inbox",
    signal_family="data",
    signal_class=SignalClass.INFORMATIONAL,
    action=SignalAction.ACCEPT_LOW_QUALITY,
    visibility=VisibilityProjection(
        crm_status="low_quality_lead",
        routing_tags=["shared_inbox"],
        api_flags={"low_quality": True},
    ),
    fallback=FallbackPolicy(
        mode=FallbackMode.KEEP_ACCEPTED_LOW_TRUST,
        then="lead_accepted_with_reduced_trust_score",
    ),
)


# ── Detection logic ───────────────────────────────────────────────────────────

def _extract_local_part(email: str) -> str | None:
    if not email:
        return None
    at_index = email.find("@")
    if at_index <= 0:
        return None
    return email[:at_index].lower()


def is_shared_inbox(email: str | None) -> bool:
    if not email:
        return False
    local = _extract_local_part(email)
    if local is None:
        return False
    return local in SHARED_INBOX_PREFIXES


# ── A6 signal rule ────────────────────────────────────────────────────────────

class A6SignalRule:
    """
    Emits shared_inbox signal when email local part matches a known shared inbox prefix.

    Input: LeadSignalContext — uses context.email only.
    Output: list[SignalResult] — empty if no signal fires, one item if it does.

    Email value is never copied into SignalResult or VisibilityProjection.
    """

    def evaluate(self, context: LeadSignalContext) -> list[SignalResult]:
        if not is_shared_inbox(context.email):
            return []
        return [SignalResult.from_definition(A6_SIGNAL)]
