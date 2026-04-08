"""
Phase 3B — A3 signal: suspicious_domain + low_trust_domain

Two-layer domain trust detection:

  A3 hard — suspicious_domain
    Fires on known high-abuse TLDs (.xyz, .ml, .cf etc.)
    Action: ACCEPT_WITH_FLAG
    CRM: needs_review

  A3 soft — low_trust_domain
    Fires on borderline .com and common TLDs with structural risk patterns
    Action: ACCEPT_LOW_QUALITY
    CRM: low_trust_lead

Hard A3 takes priority — if hard fires, soft does not evaluate.

Soft detection heuristics (Phase 3B — two rules only):
  1. More than one hyphen in label before TLD
     newco-mail-online.com → True
  2. Label longer than 20 characters
     verylongsyntheticdomain.com → True

Intentionally excluded from soft detection:
  - Numeric patterns (too common in B2B — high false positive)
  - DNS/MX lookup
  - Reputation APIs
  - Short domain heuristics (future layer)

ADR-008 invariants:
  - Visibility is the minimum consequence.
  - Visibility fields contain no PII.
  - fallback-exempt does not mean consequence-free.

# Future note (Phase 3C):
#   Numeric + structural combination patterns may be added later:
#   numbers + multiple hyphens, numbers + uncommon TLD.
#   Keep as separate rule — independently testable.
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


# ── Odsouhlasená TLD sada (hard A3) ──────────────────────────────────────────

SUSPICIOUS_TLDS: frozenset[str] = frozenset({
    ".xyz",
    ".top",
    ".click",
    ".loan",
    ".gq",
    ".ml",
    ".cf",
    ".tk",
})

# Soft detection constants
_MAX_LABEL_LENGTH = 20
_MAX_HYPHENS = 1  # more than this → soft signal


# ── Signal definitions ────────────────────────────────────────────────────────

A3_SIGNAL: SignalDefinition = SignalDefinition(
    code="suspicious_domain",
    signal_family="data",
    signal_class=SignalClass.INFORMATIONAL,
    action=SignalAction.ACCEPT_WITH_FLAG,
    visibility=VisibilityProjection(
        crm_status="needs_review",
        routing_tags=["suspicious_domain"],
        api_flags={"requires_review": True},
    ),
    fallback=FallbackPolicy(
        mode=FallbackMode.KEEP_ACCEPTED_LOW_TRUST,
        then="lead_accepted_with_review_flag_and_reduced_trust",
    ),
)

A3_SOFT_SIGNAL: SignalDefinition = SignalDefinition(
    code="low_trust_domain",
    signal_family="data",
    signal_class=SignalClass.INFORMATIONAL,
    action=SignalAction.ACCEPT_LOW_QUALITY,
    visibility=VisibilityProjection(
        crm_status="low_trust_lead",
        routing_tags=["low_trust_domain"],
        api_flags={"low_trust": True},
    ),
    fallback=FallbackPolicy(
        mode=FallbackMode.KEEP_ACCEPTED_LOW_TRUST,
        then="lead_accepted_with_reduced_trust_score",
    ),
)


# ── Detection logic ───────────────────────────────────────────────────────────

def _extract_domain(email: str) -> str | None:
    if not email:
        return None
    at_index = email.find("@")
    if at_index < 0 or at_index == len(email) - 1:
        return None
    return email[at_index + 1:].lower()


def _extract_label(domain: str) -> str | None:
    """Extract the label immediately before the TLD (last dot-separated segment before TLD)."""
    parts = domain.rsplit(".", 1)
    if len(parts) < 2:
        return None
    # For multi-part TLDs like .co.uk, take the part before the last two segments
    label_part = parts[0]
    # Take the rightmost label (closest to TLD)
    return label_part.rsplit(".", 1)[-1] if "." in label_part else label_part


def has_suspicious_tld(email: str | None) -> bool:
    """Returns True if email domain ends with a known high-abuse TLD."""
    if not email:
        return False
    domain = _extract_domain(email)
    if domain is None:
        return False
    return any(domain.endswith(tld) for tld in SUSPICIOUS_TLDS)


def has_soft_domain_risk(email: str | None) -> bool:
    """
    Returns True if email domain has structural patterns suggesting low trust.

    Heuristics (Phase 3B — two rules only):
      1. More than one hyphen in the label before TLD
      2. Label longer than 20 characters

    Only fires for common TLDs (.com, .net, .org etc.) —
    hard suspicious TLDs are handled by has_suspicious_tld().

    Never fires if has_suspicious_tld() would also fire — hard takes priority.
    """
    if not email:
        return False
    domain = _extract_domain(email)
    if domain is None:
        return False

    # Do not evaluate soft if hard already fires
    if has_suspicious_tld(email):
        return False

    label = _extract_label(domain)
    if label is None:
        return False

    hyphen_count = label.count("-")
    label_length = len(label)

    return hyphen_count > _MAX_HYPHENS or label_length > _MAX_LABEL_LENGTH


# ── A3 signal rule ────────────────────────────────────────────────────────────

class A3SignalRule:
    """
    Two-layer domain trust detection.

    Hard A3: fires on known suspicious TLDs → needs_review
    Soft A3: fires on structural domain risk patterns → low_trust_lead

    Hard takes priority — mutual exclusion is enforced in has_soft_domain_risk().

    Input: LeadSignalContext — uses context.email only.
    Output: list[SignalResult] — at most one signal, never both.

    Email value is never copied into SignalResult or VisibilityProjection.
    """

    def evaluate(self, context: LeadSignalContext) -> list[SignalResult]:
        if has_suspicious_tld(context.email):
            return [SignalResult.from_definition(A3_SIGNAL)]
        if has_soft_domain_risk(context.email):
            return [SignalResult.from_definition(A3_SOFT_SIGNAL)]
        return []
