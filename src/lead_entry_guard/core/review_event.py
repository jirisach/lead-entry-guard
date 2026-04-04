"""
ReviewEvent v1 — Feedback loop capture model.

Scope:
  Captures what happens after a compound signal routes a lead for review.
  Closes the feedback loop: signal → routing → outcome → pattern learning.

What this is NOT:
  - Not a workflow engine
  - Not a UI model
  - Not a CRM integration
  - Not a scoring input
  - Not in the request path

Design decisions:
  - fingerprint_id as join key (deterministic, same as ingest pipeline)
  - expired_no_review is system-only — never submitted by human callers
  - actor is optional in API but always resolved server-side (never unknown)
  - reason is optional but absence marked as low_insight_event in analytics
  - append-only store — no updates, no deletes

Validated by:
  Priyanka (RevOps/BI, Dynamics 365 + Power Platform), April 2026:
  "reviews piling up breaks trust — expiry is critical"
  "lightweight outcome + optional reason is sufficient"
  "feedback shows up in behavior first, not structured input"

Related:
  - signal-combination-hypothesis.md
  - review-event-v1-design-note.md
  - ADR-011 (deferred: ReviewEvent / feedback loop integration)
  - ADR-012 (to be written after implementation is validated)
"""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Iterator

logger = logging.getLogger(__name__)

# Default expiry window — configurable per tenant in future versions
_DEFAULT_EXPIRY_HOURS: int = 24
_MAX_REASON_LENGTH: int = 200


# ── Enums ────────────────────────────────────────────────────────────────────

class ReviewAction(str, Enum):
    """
    Outcome of a human review decision.

    ACCEPT:              Reviewer confirmed lead is valid — proceed normally.
    REJECT:              Reviewer determined lead should not enter pipeline.
    REASSIGN:            Lead valid but routed to wrong owner — redirect.
    EXPIRED_NO_REVIEW:   System-set only. Never submitted by human callers.
                         Fires when expires_at is reached with no human action.
                         Must not be mixed with human decisions in analytics.
    """
    ACCEPT = "accept"
    REJECT = "reject"
    REASSIGN = "reassign"
    EXPIRED_NO_REVIEW = "expired_no_review"  # system-only

    @property
    def is_human_action(self) -> bool:
        return self != ReviewAction.EXPIRED_NO_REVIEW

    @property
    def is_system_action(self) -> bool:
        return self == ReviewAction.EXPIRED_NO_REVIEW


# ── Core model ────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ReviewEvent:
    """
    Immutable record of a review outcome for a compound-routed lead.

    Frozen — ReviewEvent is an append-only fact. No field may be mutated
    after creation. Updates are new events, not modifications.

    Fields:
      review_id:      Unique identifier for this event. UUID, system-generated.
      fingerprint_id: Join key back to the original ingest decision.
                      Deterministic HMAC — same as used in the ingest pipeline.
                      Never a lead_id or raw PII.
      action:         What happened. Human actions: accept/reject/reassign.
                      System action: expired_no_review (set by expiry job only).
      reason:         Optional short text from reviewer. Max 200 chars.
                      Not parsed, not structured, not PII.
                      Absence → event marked as low_insight_event in analytics.
      actor:          Who or what set this outcome. Optional in API, always
                      resolved server-side — never stored as None internally.
                      Human: role or system identifier (no raw user PII).
                      System: "system" (for expired_no_review).
      compound_code:  Which compound signal triggered the routing.
                      Preserved for analytics join without re-querying ingest.
      triggered_by:   Frozenset of base signal codes that caused the compound.
                      Preserved for pattern analysis.
      tenant_id:      Tenant scope. Never cross-tenant reads.
      timestamp:      When this review action was taken (or system-resolved).
      expires_at:     When the review opportunity expired.
                      Set at routing time: routing_time + DEFAULT_EXPIRY_HOURS.
      low_insight:    True if reason is absent. Set at construction time.
                      Used in analytics to track signal quality of feedback.
    """
    review_id: str
    fingerprint_id: str
    action: ReviewAction
    actor: str                          # always resolved, never None internally
    compound_code: str
    triggered_by: frozenset[str]
    tenant_id: str
    timestamp: datetime
    expires_at: datetime
    reason: str | None = None
    low_insight: bool = False           # computed at construction
    pending_id: str | None = None       # join key back to PendingReview — aids debug

    def __post_init__(self) -> None:
        # Enforce max reason length
        if self.reason is not None and len(self.reason) > _MAX_REASON_LENGTH:
            raise ValueError(
                f"ReviewEvent.reason exceeds max length of {_MAX_REASON_LENGTH} chars. "
                f"Got {len(self.reason)}."
            )
        # low_insight is derived — must match reason absence
        expected_low_insight = self.reason is None
        if self.low_insight != expected_low_insight:
            raise ValueError(
                "ReviewEvent.low_insight must be True iff reason is None. "
                f"reason={self.reason!r}, low_insight={self.low_insight}"
            )
        # expires_at must be after timestamp — caller cannot set expiry in the past
        if self.expires_at <= self.timestamp:
            raise ValueError(
                f"ReviewEvent.expires_at must be after timestamp. "
                f"expires_at={self.expires_at.isoformat()}, "
                f"timestamp={self.timestamp.isoformat()}"
            )

    def to_log_dict(self) -> dict:
        """
        Structured log payload. Safe to emit to any log sink.
        No PII. All fields are codes, booleans, or ISO timestamps.
        """
        return {
            "review_id": self.review_id,
            "fingerprint_id": self.fingerprint_id,
            "action": self.action.value,
            "actor": self.actor,
            "compound_code": self.compound_code,
            "triggered_by": sorted(self.triggered_by),
            "tenant_id": self.tenant_id,
            "timestamp": self.timestamp.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "low_insight": self.low_insight,
            "is_human_action": self.action.is_human_action,
            "pending_id": self.pending_id,
        }


# ── Factory ───────────────────────────────────────────────────────────────────

def create_human_review_event(
    *,
    fingerprint_id: str,
    action: ReviewAction,
    actor: str,
    compound_code: str,
    triggered_by: frozenset[str],
    tenant_id: str,
    expires_at: datetime,
    reason: str | None = None,
    pending_id: str | None = None,
) -> ReviewEvent:
    """
    Factory for human-submitted review events.

    Validates that action is a human action — rejects expired_no_review.
    Sets review_id and timestamp automatically.

    Args:
        fingerprint_id: From ingest pipeline. Join key.
        action:         Must be accept, reject, or reassign.
        actor:          Resolved from API key context. Never None here.
        compound_code:  Which compound signal triggered routing.
        triggered_by:   Base signal codes that caused the compound.
        tenant_id:      Tenant scope.
        expires_at:     Set at routing time — passed through here.
        reason:         Optional short text. Max 200 chars.
        pending_id:     Optional join key back to PendingReview. Aids debug.

    Raises:
        ValueError: If action is expired_no_review (system-only).
        ValueError: If reason exceeds max length.
        ValueError: If expires_at is not after now (set at routing time).
    """
    if not action.is_human_action:
        raise ValueError(
            f"ReviewAction.{action.value} is system-only and cannot be submitted "
            "by human callers. Use create_expired_review_event() for system resolution."
        )

    now = datetime.now(timezone.utc)
    low_insight = reason is None

    event = ReviewEvent(
        review_id=str(uuid.uuid4()),
        fingerprint_id=fingerprint_id,
        action=action,
        reason=reason,
        actor=actor,
        compound_code=compound_code,
        triggered_by=triggered_by,
        tenant_id=tenant_id,
        timestamp=now,
        expires_at=expires_at,
        low_insight=low_insight,
        pending_id=pending_id,
    )

    logger.info("review_event_created", extra=event.to_log_dict())
    return event


def create_expired_review_event(
    *,
    fingerprint_id: str,
    compound_code: str,
    triggered_by: frozenset[str],
    tenant_id: str,
    expires_at: datetime,
    pending_id: str | None = None,
) -> ReviewEvent:
    """
    Factory for system-generated expiry events.

    Called by the expiry resolution job when expires_at is reached
    with no human ReviewEvent present.

    Sets action=expired_no_review and actor="system" automatically.
    Always low_insight=True (no human reason possible).
    pending_id links back to the PendingReview record for traceability.
    """
    now = datetime.now(timezone.utc)

    event = ReviewEvent(
        review_id=str(uuid.uuid4()),
        fingerprint_id=fingerprint_id,
        action=ReviewAction.EXPIRED_NO_REVIEW,
        reason=None,
        actor="system",
        compound_code=compound_code,
        triggered_by=triggered_by,
        tenant_id=tenant_id,
        timestamp=now,
        expires_at=expires_at,
        low_insight=True,
        pending_id=pending_id,
    )

    logger.warning("review_event_expired", extra=event.to_log_dict())
    return event


# ── Pending review record ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class PendingReview:
    """
    Record of a lead routed for review, awaiting outcome.

    Created when compound signal fires and routes a lead.
    Resolved when either:
      - A human submits a ReviewEvent (accept/reject/reassign)
      - expires_at is reached → expiry job creates expired_no_review event

    This is the state that the expiry job scans.
    Not the same as ReviewEvent — PendingReview is input, ReviewEvent is output.
    """
    pending_id: str
    fingerprint_id: str
    compound_code: str
    triggered_by: frozenset[str]
    tenant_id: str
    routed_at: datetime
    expires_at: datetime
    resolved: bool = False


def create_pending_review(
    *,
    fingerprint_id: str,
    compound_code: str,
    triggered_by: frozenset[str],
    tenant_id: str,
    expiry_hours: int = _DEFAULT_EXPIRY_HOURS,
) -> PendingReview:
    """
    Factory for PendingReview — called when compound signal fires.

    expiry_hours defaults to _DEFAULT_EXPIRY_HOURS (24h).
    In future: per-tenant config.
    """
    now = datetime.now(timezone.utc)
    expires_at = datetime.fromtimestamp(
        now.timestamp() + expiry_hours * 3600,
        tz=timezone.utc,
    )
    return PendingReview(
        pending_id=str(uuid.uuid4()),
        fingerprint_id=fingerprint_id,
        compound_code=compound_code,
        triggered_by=triggered_by,
        tenant_id=tenant_id,
        routed_at=now,
        expires_at=expires_at,
    )


# ── In-memory append-only store ───────────────────────────────────────────────

class ReviewEventStore:
    """
    Append-only store for ReviewEvent records.

    v1: in-memory. Production: replace with durable append-only store
    (PostgreSQL append-only table, event log, or similar).

    Invariants:
      - No updates. No deletes. Append only.
      - 1 fingerprint → max 1 human ReviewEvent per tenant.
      - expired_no_review is skipped if human event already exists.
      - All reads are by fingerprint_id or tenant_id.
      - Cross-tenant reads are not possible — tenant_id is always required.

    Thread safety: not guaranteed in v1. Add locking if used concurrently.
    """

    def __init__(self) -> None:
        self._events: list[ReviewEvent] = []
        # Dict for O(1) lookup and mark_resolved — key: pending_id
        self._pending: dict[str, PendingReview] = {}
        # Set for O(1) human_event_exists — key: (fingerprint_id, tenant_id)
        self._human_index: set[tuple[str, str]] = set()

    # ── Write ────────────────────────────────────────────────────────────────

    def append(self, event: ReviewEvent) -> None:
        """
        Append a ReviewEvent. Never updates existing events.

        Duplicate guard:
          - Human events: raises if a human event already exists for this fingerprint.
            1 fingerprint → max 1 human outcome per tenant.
          - expired_no_review: silently skipped if human event already exists.
            Prevents race condition between expiry job and late human submission.
        """
        if event.action.is_human_action:
            if self.human_event_exists(event.fingerprint_id, event.tenant_id):
                raise ValueError(
                    f"A human ReviewEvent already exists for fingerprint "
                    f"{event.fingerprint_id!r} in tenant {event.tenant_id!r}. "
                    "1 fingerprint → max 1 human outcome. Cannot overwrite."
                )
        else:
            # expired_no_review — skip silently if human already acted
            if self.human_event_exists(event.fingerprint_id, event.tenant_id):
                logger.info(
                    "review_expiry_skipped_human_exists",
                    extra={
                        "fingerprint_id": event.fingerprint_id,
                        "tenant_id": event.tenant_id,
                    },
                )
                return

        self._events.append(event)
        if event.action.is_human_action:
            self._human_index.add((event.fingerprint_id, event.tenant_id))
        # Auto-resolve linked PendingReview — closes lifecycle automatically
        if event.pending_id:
            pending = self._pending.get(event.pending_id)
            if pending and pending.tenant_id != event.tenant_id:
                raise ValueError(
                    f"Tenant mismatch: ReviewEvent.tenant_id={event.tenant_id!r} "
                    f"does not match PendingReview.tenant_id={pending.tenant_id!r} "
                    f"for pending_id={event.pending_id!r}. "
                    "Cross-tenant resolution is not allowed."
                )
            self.mark_resolved(event.pending_id)
        logger.info(
            "review_event_stored",
            extra={
                "review_id": event.review_id,
                "fingerprint_id": event.fingerprint_id,
                "action": event.action.value,
                "tenant_id": event.tenant_id,
                "pending_id": event.pending_id,
            },
        )

    def add_pending(self, pending: PendingReview) -> None:
        """Register a lead as pending review."""
        self._pending[pending.pending_id] = pending

    def mark_resolved(self, pending_id: str) -> None:
        """
        Mark a PendingReview as resolved. O(1). Does not delete it.
        No-op if pending_id not found (idempotent).
        """
        if pending_id not in self._pending:
            logger.warning(
                "mark_resolved_pending_not_found",
                extra={"pending_id": pending_id},
            )
            return
        existing = self._pending[pending_id]
        # Frozen dataclass — replace with resolved=True copy
        self._pending[pending_id] = PendingReview(
            pending_id=existing.pending_id,
            fingerprint_id=existing.fingerprint_id,
            compound_code=existing.compound_code,
            triggered_by=existing.triggered_by,
            tenant_id=existing.tenant_id,
            routed_at=existing.routed_at,
            expires_at=existing.expires_at,
            resolved=True,
        )

    # ── Read ─────────────────────────────────────────────────────────────────

    def get_by_fingerprint(
        self, fingerprint_id: str, tenant_id: str
    ) -> list[ReviewEvent]:
        """All events for a fingerprint within a tenant. Never cross-tenant."""
        return [
            e for e in self._events
            if e.fingerprint_id == fingerprint_id and e.tenant_id == tenant_id
        ]

    def get_expired_pending(self) -> Iterator[PendingReview]:
        """
        Yields unresolved PendingReview records past their expires_at.
        Called by the expiry resolution job.
        """
        now = datetime.now(timezone.utc)
        for p in self._pending.values():
            if not p.resolved and p.expires_at <= now:
                yield p

    def human_event_exists(self, fingerprint_id: str, tenant_id: str) -> bool:
        """True if a human review action exists for this fingerprint. O(1)."""
        return (fingerprint_id, tenant_id) in self._human_index

    # ── Analytics helpers ─────────────────────────────────────────────────────

    def low_insight_ratio(self, tenant_id: str) -> float:
        """
        Fraction of human events with no reason (low_insight=True).
        Used to track feedback signal quality over time.
        """
        human_events = [
            e for e in self._events
            if e.tenant_id == tenant_id and e.action.is_human_action
        ]
        if not human_events:
            return 0.0
        low_insight_count = sum(1 for e in human_events if e.low_insight)
        return low_insight_count / len(human_events)

    def expired_ratio(self, tenant_id: str) -> float:
        """
        Fraction of all events that are expired_no_review.
        Alert threshold: > 0.20 signals broken routing or queue overload.
        """
        tenant_events = [e for e in self._events if e.tenant_id == tenant_id]
        if not tenant_events:
            return 0.0
        expired_count = sum(
            1 for e in tenant_events
            if e.action == ReviewAction.EXPIRED_NO_REVIEW
        )
        return expired_count / len(tenant_events)

    def action_breakdown(self, tenant_id: str, compound_code: str) -> dict[str, int]:
        """
        Count of each action for a given compound_code within a tenant.
        Used for pattern validation: if reject/reassign dominate → rule candidate.
        """
        relevant = [
            e for e in self._events
            if e.tenant_id == tenant_id and e.compound_code == compound_code
        ]
        breakdown: dict[str, int] = {action.value: 0 for action in ReviewAction}
        for e in relevant:
            breakdown[e.action.value] += 1
        return breakdown
