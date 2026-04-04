"""
Review Events Router — POST /v1/review-events

Lightweight capture endpoint for compound-signal review outcomes.

What this does:
  Accepts a human review decision (accept/reject/reassign) for a lead
  that was routed for review by the compound signal layer.
  Stores a ReviewEvent. Closes the feedback loop.

What this does NOT do:
  - Does not change the original ingest decision
  - Does not re-evaluate signals
  - Does not write to CRM
  - Does not expose expired_no_review as valid input

Auth:
  Same X-API-Key mechanism as /v1/leads/ingest.
  tenant_id is resolved from the API key — never accepted from request body.
  actor is resolved from API key context — never accepted from request body.

Scope guard — do not add:
  - actor or tenant_id as request body fields
  - decision override or re-routing logic
  - enrichment or signal re-evaluation
  - UI-facing response fields beyond review_id and recorded_at

Related:
  - review_event.py — ReviewEvent model and store
  - review-event-v1-design-note.md — design decisions
  - ADR-011 deferred item 4 — ReviewEvent / feedback loop integration
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator

from lead_entry_guard.core.review_event import (
    ReviewAction,
    ReviewEventStore,
    create_human_review_event,
)
from lead_entry_guard.api.auth import require_tenant

logger = logging.getLogger(__name__)

router = APIRouter()

# Module-level store — in production: inject via dependency
# v1: shared in-process store, same instance as expiry job
_store = ReviewEventStore()

# Default expiry window passed from routing context
# In v1: fixed. Future: per-tenant config via TenantConfig.
_DEFAULT_EXPIRY_HOURS: int = 24


# ── Request / Response models ─────────────────────────────────────────────────

class ReviewEventRequest(BaseModel):
    """
    Human review outcome submission.

    fingerprint_id: Join key from the compound signal routing event.
                    Caller must supply the fingerprint_id from the
                    ingest response or routing notification.
    action:         One of: accept, reject, reassign.
                    expired_no_review is rejected — system-only.
    reason:         Optional short text. Max 200 chars. No PII.
    expires_at:     When the review window expires. Set at routing time.
                    Caller passes it back — server validates it is not past.

    tenant_id and actor are NOT accepted from the request body.
    Both are resolved server-side from the API key context.
    """
    fingerprint_id: str = Field(..., min_length=1)
    action: ReviewAction
    reason: str | None = Field(default=None, max_length=200)
    expires_at: datetime
    pending_id: str | None = Field(default=None)

    @field_validator("action")
    @classmethod
    def action_must_be_human(cls, v: ReviewAction) -> ReviewAction:
        if not v.is_human_action:
            raise ValueError(
                f"action '{v.value}' is system-only and cannot be submitted "
                "via this endpoint."
            )
        return v


class ReviewEventResponse(BaseModel):
    """
    Minimal confirmation response.

    Does not echo back reason or actor — those are internal.
    review_id is the stable identifier for this review outcome.
    """
    review_id: str
    fingerprint_id: str
    action: str
    recorded_at: str       # ISO8601
    low_insight: bool


# ── Handler ───────────────────────────────────────────────────────────────────

@router.post(
    "/review-events",
    response_model=ReviewEventResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a review outcome for a compound-routed lead",
    description=(
        "Capture the outcome of a human review decision. "
        "Accepts accept, reject, or reassign. "
        "expired_no_review is system-only and will be rejected. "
        "tenant_id and actor are resolved from the API key — not accepted from body. "
        "One human outcome per fingerprint per tenant."
    ),
)
def submit_review_event(
    body: ReviewEventRequest,
    tenant: dict = Depends(require_tenant),
) -> ReviewEventResponse:
    """
    Submit a human review outcome for a compound-routed lead.

    Flow:
      1. Resolve tenant_id and actor from API key (auth layer)
      2. Validate action is human (Pydantic validator)
      3. Validate expires_at is not in the past
      4. Create ReviewEvent via factory
      5. Append to store (duplicate guard enforced by store)
      6. Return confirmation

    Errors:
      400: action is expired_no_review
      400: expires_at is in the past
      409: human ReviewEvent already exists for this fingerprint
      422: request body validation failure
    """
    t_start = time.monotonic()
    tenant_id: str = tenant["tenant_id"]
    actor: str = tenant.get("actor_role", "api_caller")

    # Validate expires_at is not in the past
    now = datetime.now(timezone.utc)
    if body.expires_at <= now:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "EXPIRES_AT_IN_PAST",
                "message": (
                    f"expires_at {body.expires_at.isoformat()} is in the past. "
                    "The review window has already closed."
                ),
            },
        )

    try:
        event = create_human_review_event(
            fingerprint_id=body.fingerprint_id,
            action=body.action,
            actor=actor,
            compound_code="compound_signal_alignment",  # v1: single compound code
            triggered_by=frozenset(),   # v1: not tracked at endpoint level
            tenant_id=tenant_id,
            expires_at=body.expires_at,
            reason=body.reason,
            pending_id=body.pending_id,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "INVALID_REVIEW_EVENT", "message": str(exc)},
        ) from exc

    try:
        _store.append(event)
    except ValueError as exc:
        # Duplicate human event for this fingerprint
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "REVIEW_EVENT_ALREADY_EXISTS",
                "message": str(exc),
            },
        ) from exc

    latency_ms = (time.monotonic() - t_start) * 1000
    logger.info(
        "review_event_submitted",
        extra={
            "review_id": event.review_id,
            "fingerprint_id": event.fingerprint_id,
            "action": event.action.value,
            "tenant_id": tenant_id,
            "latency_ms": round(latency_ms, 2),
            "low_insight": event.low_insight,
        },
    )

    return ReviewEventResponse(
        review_id=event.review_id,
        fingerprint_id=event.fingerprint_id,
        action=event.action.value,
        recorded_at=event.timestamp.isoformat(),
        low_insight=event.low_insight,
    )


# ── Expiry resolution job ─────────────────────────────────────────────────────

class ReviewExpiryJob:
    """
    Background job — resolves pending reviews that have passed expires_at
    with no human action.

    Runs out of request path. Never blocks ingest.

    Invariants:
      - Only fires expired_no_review if no human event exists (store enforces this)
      - Marks pending as resolved after creating expired event
      - Logs every expiry for visibility
      - Alert if expired_ratio > ALERT_THRESHOLD

    Usage:
        job = ReviewExpiryJob(store=_store)
        # In production: schedule via APScheduler, Celery beat, or cron
        job.run_once()
    """

    ALERT_THRESHOLD: float = 0.20

    def __init__(self, store: ReviewEventStore) -> None:
        self._store = store

    def run_once(self) -> dict:
        """
        Process all currently expired pending reviews.

        Returns summary dict for observability:
          processed: total expired pending found
          expired_events_created: events written
          skipped_human_exists: skipped because human acted already
        """
        processed = 0
        created = 0
        skipped = 0

        for pending in list(self._store.get_expired_pending()):
            processed += 1

            if self._store.human_event_exists(pending.fingerprint_id, pending.tenant_id):
                # Human acted after expires_at — skip, mark resolved
                self._store.mark_resolved(pending.pending_id)
                skipped += 1
                continue

            from lead_entry_guard.core.review_event import create_expired_review_event
            event = create_expired_review_event(
                fingerprint_id=pending.fingerprint_id,
                compound_code=pending.compound_code,
                triggered_by=pending.triggered_by,
                tenant_id=pending.tenant_id,
                expires_at=pending.expires_at,
                pending_id=pending.pending_id,
            )
            self._store.append(event)
            created += 1

        # Alert check per tenant
        self._check_alert_thresholds()

        summary = {
            "processed": processed,
            "expired_events_created": created,
            "skipped_human_exists": skipped,
        }
        logger.info("expiry_job_run_complete", extra=summary)
        return summary

    def _check_alert_thresholds(self) -> None:
        """
        Check expired_ratio per tenant. Log warning if above threshold.
        In production: emit metric / PagerDuty alert.
        """
        # Collect unique tenant_ids from all events
        tenant_ids = {e.tenant_id for e in self._store._events}
        for tenant_id in tenant_ids:
            ratio = self._store.expired_ratio(tenant_id)
            if ratio > self.ALERT_THRESHOLD:
                logger.warning(
                    "ALERT: expired_ratio_above_threshold",
                    extra={
                        "tenant_id": tenant_id,
                        "expired_ratio": round(ratio, 3),
                        "threshold": self.ALERT_THRESHOLD,
                        "action": "check routing ownership and queue size",
                    },
                )
