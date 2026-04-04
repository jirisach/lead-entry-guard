"""
ReviewEvent Endpoint Contract Test — POST /v1/review-events

PURPOSE
-------
This test enforces the ReviewEvent endpoint boundary as a production slice.

Six invariant groups:

  1. VALID ACTIONS — accept / reject / reassign → 201
  2. SYSTEM ACTION BLOCKED — expired_no_review from human caller → 422 (Pydantic validator)
  3. EXPIRES_AT IN PAST — review window closed → 400
  4. DUPLICATE HUMAN EVENT — second human event same fingerprint → 409
  5. TENANT + ACTOR FROM AUTH — never from request body
  6. TENANT MISMATCH VIA PENDING_ID — cross-tenant resolution → 400

Trust model:
  - tenant_id resolved from X-API-Key (require_tenant dependency)
  - actor resolved server-side (fixed "api_caller" in v1)
  - Neither tenant_id nor actor accepted from request body

Scope guard:
  - expired_no_review is system-only — blocked at Pydantic validator level
  - expires_at validation happens before store write
  - Duplicate guard is enforced by ReviewEventStore.append()

Placement: tests/contract/test_review_events_contract.py
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Annotated
from unittest.mock import AsyncMock

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from review_events_router import router, _store, ReviewEventStore
from review_event import ReviewAction, create_pending_review


# ── Test app setup ────────────────────────────────────────────────────────────
#
# We build a minimal FastAPI app with a mocked require_tenant dependency.
# This isolates the router contract from the DB/auth infrastructure —
# same pattern as test_signal_check_api.py.
#
# Tenant identity is fixed as "test_tenant" / actor "api_caller".
# Tests that verify body injection guard inject a different tenant_id
# in the body and assert the response tenant comes from auth, not body.

TENANT_ID = "test_tenant"
ACTOR = "api_caller"

# Fresh store per test — prevents state leakage between tests
_test_store: ReviewEventStore


class _FakeTenant:
    """
    Minimal TenantRow stand-in for dependency override.

    Uses a plain object with explicit attributes — NOT MagicMock.
    MagicMock would silently swallow attribute access errors and make
    the test pass even if the router reads a field that doesn't exist.
    A plain object raises AttributeError immediately on wrong access,
    which is the correct behavior for a contract test.

    Shape matches what the router actually reads (review_events_router.py):
        tenant_id: str = tenant.tenant_id
    """
    tenant_id: str = TENANT_ID


def _mock_tenant() -> _FakeTenant:
    return _FakeTenant()


def _build_app(store: ReviewEventStore | None = None) -> tuple[FastAPI, TestClient]:
    """
    Build a test app with:
      - review_events_router mounted at /v1
      - require_tenant overridden with mock returning TENANT_ID
      - optional custom store injected (for isolation between duplicate tests)
    """
    import review_events_router as rev_module

    app = FastAPI()

    # Override store if provided
    if store is not None:
        rev_module._store = store

    app.include_router(router, prefix="/v1")

    # FastAPI resolves dependencies by object identity.
    # Override must target the require_tenant that the router imported into its
    # own namespace — not the object from the source module (different reference,
    # override would miss entirely and tests would call real auth).
    app.dependency_overrides[rev_module.require_tenant] = _mock_tenant

    return app, TestClient(app)


URL = "/v1/review-events"


def future_expires_at(hours: int = 24) -> str:
    """ISO8601 string for a future expires_at."""
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def past_expires_at(seconds: int = 1) -> str:
    """ISO8601 string for a past expires_at."""
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def valid_payload(
    action: str = "accept",
    fingerprint_id: str = "fp_contract_test",
    reason: str | None = None,
    expires_at: str | None = None,
    pending_id: str | None = None,
) -> dict:
    return {
        "fingerprint_id": fingerprint_id,
        "action": action,
        "reason": reason,
        "expires_at": expires_at or future_expires_at(),
        **({"pending_id": pending_id} if pending_id is not None else {}),
    }


# ── Group 1: Valid human actions → 201 ───────────────────────────────────────

class TestValidActions:
    """
    accept / reject / reassign must return HTTP 201 with stable response shape.

    This is the happy path — verifies the endpoint is wired correctly and
    the response contract matches ReviewEventResponse fields.
    """

    def setup_method(self):
        """Fresh store per test — no state leakage."""
        import review_events_router as m
        m._store = ReviewEventStore()

    def _client(self) -> TestClient:
        _, client = _build_app()
        return client

    @pytest.mark.parametrize("action", ["accept", "reject", "reassign"])
    def test_valid_action_returns_201(self, action: str) -> None:
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()
        r = client.post(URL, json=valid_payload(
            action=action,
            fingerprint_id=f"fp_{action}",
        ))
        assert r.status_code == 201, (
            f"action='{action}' must return 201. Got {r.status_code}: {r.text}"
        )

    def test_response_has_review_id(self) -> None:
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()
        r = client.post(URL, json=valid_payload())
        assert "review_id" in r.json(), "response must contain review_id"
        assert r.json()["review_id"], "review_id must not be empty"

    def test_response_has_fingerprint_id(self) -> None:
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()
        r = client.post(URL, json=valid_payload(fingerprint_id="fp_shape_check"))
        assert r.json()["fingerprint_id"] == "fp_shape_check"

    def test_response_has_action(self) -> None:
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()
        r = client.post(URL, json=valid_payload(action="reassign", fingerprint_id="fp_action"))
        assert r.json()["action"] == "reassign"

    def test_response_has_recorded_at(self) -> None:
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()
        r = client.post(URL, json=valid_payload())
        assert "recorded_at" in r.json(), "response must contain recorded_at"

    def test_response_has_low_insight(self) -> None:
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()
        r = client.post(URL, json=valid_payload(reason=None))
        data = r.json()
        assert "low_insight" in data
        assert data["low_insight"] is True  # no reason → low_insight

    def test_reason_present_sets_low_insight_false(self) -> None:
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()
        r = client.post(URL, json=valid_payload(reason="valid SMB lead"))
        assert r.json()["low_insight"] is False


# ── Group 2: expired_no_review blocked ───────────────────────────────────────

class TestSystemActionBlocked:
    """
    expired_no_review is system-only — must never be accepted from a human caller.

    Blocked at Pydantic validator level (ReviewEventRequest.action_must_be_human).
    This fires before the handler runs — correct status is 422, not 400.
    400 is reserved for business validation the handler checks explicitly
    (e.g. expires_at in past). Pydantic input rejection is always 422.
    """

    def test_expired_no_review_returns_422(self) -> None:
        """
        expired_no_review submitted by caller → 422 (Pydantic validator rejects it).

        This is the correct status: the action field validator fires before
        the handler runs, so it is an input validation failure (422),
        not a business logic failure (400).
        """
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()

        r = client.post(URL, json=valid_payload(action="expired_no_review"))
        assert r.status_code == 422, (
            f"expired_no_review must be rejected at validation (422). Got {r.status_code}: {r.text}"
        )

    def test_expired_no_review_error_message(self) -> None:
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()

        r = client.post(URL, json=valid_payload(action="expired_no_review"))
        body = r.text
        assert "system-only" in body or "system" in body or "expired" in body.lower(), (
            f"Error message should reference system-only constraint. Got: {body}"
        )


# ── Group 3: expires_at in the past → 400 ────────────────────────────────────

class TestExpiresAtInPast:
    """
    expires_at in the past means the review window has closed.
    The endpoint must reject this with 400 EXPIRES_AT_IN_PAST.

    This is a business validation — different from Pydantic input validation.
    The error comes from the handler, not the schema.
    """

    def test_past_expires_at_returns_400(self) -> None:
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()

        r = client.post(URL, json=valid_payload(expires_at=past_expires_at()))
        assert r.status_code == 400, (
            f"Past expires_at must return 400. Got {r.status_code}: {r.text}"
        )

    def test_past_expires_at_error_code(self) -> None:
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()

        r = client.post(URL, json=valid_payload(expires_at=past_expires_at()))
        assert r.json()["detail"]["error"] == "EXPIRES_AT_IN_PAST", (
            f"Error code must be EXPIRES_AT_IN_PAST. Got: {r.json()}"
        )

    def test_future_expires_at_passes(self) -> None:
        """Regression guard — future expires_at must not be rejected."""
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()

        r = client.post(URL, json=valid_payload(expires_at=future_expires_at(hours=1)))
        assert r.status_code == 201


# ── Group 4: Duplicate human event → 409 ─────────────────────────────────────

class TestDuplicateHumanEvent:
    """
    1 fingerprint → max 1 human ReviewEvent per tenant.

    Second human event for the same fingerprint must return 409 REVIEW_EVENT_ALREADY_EXISTS.
    This enforces the append-only store contract at the API boundary.
    """

    def test_duplicate_human_event_returns_409(self) -> None:
        store = ReviewEventStore()
        _, client = _build_app(store=store)

        # First submission
        r1 = client.post(URL, json=valid_payload(fingerprint_id="fp_dup"))
        assert r1.status_code == 201, f"First submission must succeed. Got: {r1.text}"

        # Second submission — same fingerprint, same tenant
        r2 = client.post(URL, json=valid_payload(
            fingerprint_id="fp_dup",
            action="reject",  # different action, same fingerprint
        ))
        assert r2.status_code == 409, (
            f"Second human event must return 409. Got {r2.status_code}: {r2.text}"
        )

    def test_duplicate_error_code(self) -> None:
        store = ReviewEventStore()
        _, client = _build_app(store=store)

        client.post(URL, json=valid_payload(fingerprint_id="fp_dup_code"))
        r = client.post(URL, json=valid_payload(fingerprint_id="fp_dup_code"))

        assert r.json()["detail"]["error"] == "REVIEW_EVENT_ALREADY_EXISTS"

    def test_different_fingerprints_both_succeed(self) -> None:
        """Different fingerprints are independent — both must return 201."""
        store = ReviewEventStore()
        _, client = _build_app(store=store)

        r1 = client.post(URL, json=valid_payload(fingerprint_id="fp_A"))
        r2 = client.post(URL, json=valid_payload(fingerprint_id="fp_B"))

        assert r1.status_code == 201
        assert r2.status_code == 201


# ── Group 5: tenant_id and actor NOT from body ────────────────────────────────

class TestTrustModel:
    """
    tenant_id and actor are resolved server-side from the API key.
    They must never be accepted from the request body.

    This is the same trust model as /v1/leads/ingest (ADR-007):
    any field not in the schema is silently dropped.

    The review_id in the response is the only server-generated identity.
    tenant_id does not appear in ReviewEventResponse — it is internal.
    """

    def test_tenant_id_in_body_is_ignored(self) -> None:
        """
        Injecting tenant_id into the body must not affect which tenant
        stores the event. The store always uses the auth-resolved tenant_id.
        """
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()

        payload = valid_payload(fingerprint_id="fp_trust")
        payload["tenant_id"] = "evil_tenant"  # attempt injection

        r = client.post(URL, json=payload)
        # Must succeed (extra field dropped) and store under TENANT_ID
        assert r.status_code == 201, (
            f"Body injection of tenant_id must not cause 422. Got {r.status_code}: {r.text}"
        )

        # Verify the event was stored under the auth tenant, not evil_tenant
        events = m._store.get_by_fingerprint("fp_trust", TENANT_ID)
        assert len(events) == 1, "Event must be stored under auth tenant"

        evil_events = m._store.get_by_fingerprint("fp_trust", "evil_tenant")
        assert len(evil_events) == 0, "No event must exist under injected tenant_id"

    def test_actor_in_body_is_ignored(self) -> None:
        """
        actor in body must not override the server-resolved actor.
        ReviewEventResponse does not echo actor — it is internal.
        """
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()

        payload = valid_payload(fingerprint_id="fp_actor_inject")
        payload["actor"] = "injected_actor"  # attempt injection

        r = client.post(URL, json=payload)
        assert r.status_code == 201, (
            f"Body injection of actor must not cause 422. Got: {r.text}"
        )

        # Actor in stored event must be server-resolved, not injected
        events = m._store.get_by_fingerprint("fp_actor_inject", TENANT_ID)
        assert len(events) == 1
        assert events[0].actor == ACTOR, (
            f"Stored actor must be server-resolved '{ACTOR}', not injected. "
            f"Got: {events[0].actor!r}"
        )

    def test_response_does_not_echo_tenant_id(self) -> None:
        """
        ReviewEventResponse must not include tenant_id.
        It is an internal field — not part of the public response contract.
        """
        import review_events_router as m
        m._store = ReviewEventStore()
        _, client = _build_app()

        r = client.post(URL, json=valid_payload())
        assert "tenant_id" not in r.json(), (
            "tenant_id must not appear in ReviewEventResponse. "
            "It is resolved from auth and stored internally — not echoed back."
        )


# ── Group 6: tenant mismatch via pending_id → 400 ────────────────────────────

class TestTenantMismatchViaPendingId:
    """
    When a ReviewEvent references a pending_id that belongs to a different tenant,
    the store must reject it with a ValueError, which the handler maps to 400.

    This enforces cross-tenant isolation at the review layer:
    a tenant cannot resolve another tenant's pending review.
    """

    def test_tenant_mismatch_pending_id_returns_400(self) -> None:
        """
        pending_id registered under tenant_A.
        ReviewEvent submitted under tenant_B (via auth mock).
        Must return 400 — not silently succeed.
        """
        store = ReviewEventStore()

        # Register pending under a DIFFERENT tenant
        pending = create_pending_review(
            fingerprint_id="fp_mismatch",
            compound_code="compound_signal_alignment",
            triggered_by=frozenset({"low_trust_domain"}),
            tenant_id="tenant_OTHER",  # not TENANT_ID
        )
        store.add_pending(pending)

        _, client = _build_app(store=store)

        # Submit event under TENANT_ID (auth mock) referencing OTHER's pending_id
        r = client.post(URL, json=valid_payload(
            fingerprint_id="fp_mismatch",
            pending_id=pending.pending_id,
        ))

        assert r.status_code == 400, (
            f"Tenant mismatch via pending_id must return 400. "
            f"Got {r.status_code}: {r.text}"
        )

    def test_tenant_mismatch_error_references_mismatch(self) -> None:
        """Error message must mention mismatch — not expose the other tenant's ID."""
        store = ReviewEventStore()
        pending = create_pending_review(
            fingerprint_id="fp_mismatch_msg",
            compound_code="compound_signal_alignment",
            triggered_by=frozenset({"low_trust_domain"}),
            tenant_id="tenant_OTHER",
        )
        store.add_pending(pending)
        _, client = _build_app(store=store)

        r = client.post(URL, json=valid_payload(
            fingerprint_id="fp_mismatch_msg",
            pending_id=pending.pending_id,
        ))

        body = r.text.lower()
        assert "mismatch" in body or "cross-tenant" in body or "invalid" in body, (
            f"Error must reference mismatch. Got: {r.text}"
        )

    def test_matching_tenant_pending_id_succeeds(self) -> None:
        """
        Regression guard: pending_id from the same tenant must work normally.
        """
        store = ReviewEventStore()
        pending = create_pending_review(
            fingerprint_id="fp_match",
            compound_code="compound_signal_alignment",
            triggered_by=frozenset({"low_trust_domain"}),
            tenant_id=TENANT_ID,  # same tenant as auth mock
        )
        store.add_pending(pending)
        _, client = _build_app(store=store)

        r = client.post(URL, json=valid_payload(
            fingerprint_id="fp_match",
            pending_id=pending.pending_id,
        ))
        assert r.status_code == 201, (
            f"Same-tenant pending_id must succeed. Got {r.status_code}: {r.text}"
        )
