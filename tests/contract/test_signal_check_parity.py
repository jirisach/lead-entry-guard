"""
Parity contract — signal-check endpoint vs SignalEvaluator direct call.

This test exists to prevent sandbox/production divergence.

The risk:
  Someone adds a feature flag, mock evaluator, or simplified path inside
  the signal_check handler "just for demo speed". The handler then returns
  different signals than the real evaluator would — silently.

What this test does:
  For a set of representative inputs, it calls the endpoint AND calls
  SignalEvaluator directly with an identical context. It then asserts
  that the signal codes match exactly.

  If the handler ever substitutes or wraps the evaluator, this test breaks.

Placement: tests/contract/test_signal_check_parity.py

This is a contract test — it must pass before every merge (same as test_signal_contract.py).
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from lead_entry_guard.api.routers.signal_check import router
from lead_entry_guard.core.signal_models import FieldSourceRecord, LeadSignalContext
from lead_entry_guard.policies.signal_evaluator import SignalEvaluator

URL = "/v1/leads/signal-check"


@pytest.fixture(scope="module")
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


# ── Parity scenarios ──────────────────────────────────────────────────────────
#
# Each scenario is: (description, request_body, LeadSignalContext)
# The context must be semantically identical to the request body.

PARITY_SCENARIOS = [
    (
        "clean email — no signals expected",
        {"scenario_id": "parity_tenant", "email": "jan.novak@example.com"},
        LeadSignalContext(tenant_id="parity_tenant", email="jan.novak@example.com"),
    ),
    (
        "shared inbox — A6 expected",
        {"scenario_id": "parity_tenant", "email": "info@acme.com"},
        LeadSignalContext(tenant_id="parity_tenant", email="info@acme.com"),
    ),
    (
        "suspicious domain — A3 hard expected",
        {"scenario_id": "parity_tenant", "email": "user@spammy.xyz"},
        LeadSignalContext(tenant_id="parity_tenant", email="user@spammy.xyz"),
    ),
    (
        "low trust domain — A3 soft expected",
        {"scenario_id": "parity_tenant", "email": "user@some-very-odd-domain.com"},
        LeadSignalContext(tenant_id="parity_tenant", email="user@some-very-odd-domain.com"),
    ),
    (
        "source conflict — A4 expected",
        {
            "scenario_id": "parity_tenant",
            "email": "jan@example.com",
            "fields": [
                {"field_name": "phone", "source_type": "manual", "value": "A"},
                {"field_name": "phone", "source_type": "enrichment", "value": "B"},
            ],
        },
        LeadSignalContext(
            tenant_id="parity_tenant",
            email="jan@example.com",
            fields=[
                FieldSourceRecord("phone", "manual", "A"),
                FieldSourceRecord("phone", "enrichment", "B"),
            ],
        ),
    ),
    (
        "tri-signal — A3 hard + A4 + A6 all expected",
        {
            "scenario_id": "parity_tenant",
            "email": "info@spammy.xyz",
            "fields": [
                {"field_name": "phone", "source_type": "manual", "value": "A"},
                {"field_name": "phone", "source_type": "enrichment", "value": "B"},
            ],
        },
        LeadSignalContext(
            tenant_id="parity_tenant",
            email="info@spammy.xyz",
            fields=[
                FieldSourceRecord("phone", "manual", "A"),
                FieldSourceRecord("phone", "enrichment", "B"),
            ],
        ),
    ),
]


@pytest.mark.contract
@pytest.mark.parametrize("description,body,context", PARITY_SCENARIOS, ids=[s[0] for s in PARITY_SCENARIOS])
def test_endpoint_matches_direct_evaluator(
    client: TestClient,
    description: str,
    body: dict,
    context: LeadSignalContext,
) -> None:
    """
    The endpoint must return exactly the same signal codes as a direct
    SignalEvaluator call with an identical context.

    Sorted by code on both sides — same sort as the handler applies,
    so ordering differences don't produce false failures.
    """
    # Direct evaluator call
    direct_signals = sorted(
        SignalEvaluator().evaluate(context),
        key=lambda s: s.code,
    )
    direct_codes = [s.code for s in direct_signals]

    # Endpoint call
    r = client.post(URL, json=body)
    assert r.status_code == 200, f"[{description}] endpoint returned {r.status_code}"
    endpoint_codes = [s["code"] for s in r.json()["signals"]]

    assert endpoint_codes == direct_codes, (
        f"[{description}]\n"
        f"  endpoint  : {endpoint_codes}\n"
        f"  evaluator : {direct_codes}\n"
        "Signal-check endpoint has diverged from SignalEvaluator. "
        "Check for mock evaluators, feature flags, or simplified paths in the handler."
    )
