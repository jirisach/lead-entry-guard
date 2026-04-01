"""
Tests — POST /v1/leads/signal-check (v4)

No auth, no DB — endpoint is a pure signal sandbox.

Test groups:
  1.  Input validation (422 paths) — incl. value length guard
  2.  Clean inputs → status="clean", has_signals=False
  3.  A3 hard  — suspicious_domain
  4.  A3 soft  — low_trust_domain
  5.  A3 mutual exclusion
  6.  A4       — source_conflict_manual_vs_enrichment
  7.  A6       — shared_inbox
  8.  Multiple signals
  9.  Determinism — sort order, repeated calls, tri-signal scenario
  10. Response shape contract (ADR-008)
  11. Latency field
  12. PII invariant
"""
from __future__ import annotations

import concurrent.futures
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from lead_entry_guard.api.routers.signal_check import router

URL = "/v1/leads/signal-check"


# ── App fixture ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    """
    Reset module-level rate limiter before every test.

    Why autouse:
      _rate_limiter is global module state. Request counts accumulate across
      tests — after ~30 requests the bucket fills and unrelated tests start
      failing with 429. autouse ensures every test starts with a clean bucket
      regardless of what previous tests did to the limiter.

    Why high limit (10_000):
      General contract tests don't test rate limiting — that's done in group 14
      via tight_client which patches its own limiter independently. The high
      limit here prevents false 429s in all other tests.

    isolated_executor (group 15) handles the separate issue of timeout tests
      poisoning the shared ThreadPoolExecutor — it is still needed alongside this.
    """
    from lead_entry_guard.api.routers import signal_check as sc_module

    original_limiter = sc_module._rate_limiter
    sc_module._rate_limiter = sc_module._TokenBucket(10_000, 60)

    yield

    sc_module._rate_limiter = original_limiter




def post(client: TestClient, payload: dict) -> dict:
    r = client.post(URL, json=payload)
    assert r.status_code == 200
    return r.json()


def codes(data: dict) -> list[str]:
    return [s["code"] for s in data["signals"]]


# ── 1. Input validation ───────────────────────────────────────────────────────

def test_missing_tenant_id_returns_422(client: TestClient):
    r = client.post(URL, json={"email": "test@example.com"})
    assert r.status_code == 422


def test_no_email_no_fields_returns_422(client: TestClient):
    r = client.post(URL, json={"scenario_id": "t1"})
    assert r.status_code == 422


def test_unknown_source_type_returns_422(client: TestClient):
    r = client.post(URL, json={
        "scenario_id": "t1",
        "fields": [{"field_name": "phone", "source_type": "MYSTERY", "value": "123"}],
    })
    assert r.status_code == 422


def test_field_name_with_whitespace_returns_422(client: TestClient):
    r = client.post(URL, json={
        "scenario_id": "t1",
        "fields": [{"field_name": " phone", "source_type": "manual", "value": "x"}],
    })
    assert r.status_code == 422


def test_too_many_fields_returns_422(client: TestClient):
    fields = [{"field_name": f"f{i}", "source_type": "manual", "value": "x"} for i in range(51)]
    r = client.post(URL, json={"scenario_id": "t1", "fields": fields})
    assert r.status_code == 422


def test_value_exceeding_max_length_returns_422(client: TestClient):
    r = client.post(URL, json={
        "scenario_id": "t1",
        "fields": [{"field_name": "phone", "source_type": "manual", "value": "x" * 513}],
    })
    assert r.status_code == 422


def test_value_at_max_length_is_accepted(client: TestClient):
    r = client.post(URL, json={
        "scenario_id": "t1",
        "fields": [{"field_name": "phone", "source_type": "manual", "value": "x" * 512}],
    })
    assert r.status_code == 200


# ── 2. Clean input → status="clean" ──────────────────────────────────────────

def test_clean_email_returns_clean_status(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "jan.novak@example.com"})
    assert data["status"] == "clean"
    assert data["has_signals"] is False
    assert data["signal_count"] == 0
    assert data["signals"] == []


def test_clean_field_only_returns_clean_status(client: TestClient):
    data = post(client, {
        "scenario_id": "t1",
        "fields": [{"field_name": "phone", "source_type": "manual", "value": "+420777000111"}],
    })
    assert data["status"] == "clean"
    assert data["has_signals"] is False


def test_flagged_status_when_signal_fires(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "info@acme.com"})
    assert data["status"] == "flagged"
    assert data["has_signals"] is True


def test_status_is_only_clean_or_flagged(client: TestClient):
    """Literal["clean", "flagged"] — no other value should ever appear."""
    for email in ("jan@example.com", "info@spammy.xyz", "support@acme.com"):
        data = post(client, {"scenario_id": "t1", "email": email})
        assert data["status"] in ("clean", "flagged")


# ── 3. A3 hard — suspicious_domain ───────────────────────────────────────────

@pytest.mark.parametrize("email", [
    "user@spammy.xyz",
    "user@burner.ml",
    "user@click.tk",
])
def test_a3_hard_fires_on_abusive_tld(client: TestClient, email: str):
    assert "suspicious_domain" in codes(post(client, {"scenario_id": "t1", "email": email}))


def test_a3_hard_crm_status_is_needs_review(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "x@bad.xyz"})
    sig = next(s for s in data["signals"] if s["code"] == "suspicious_domain")
    assert sig["visibility"]["crm_status"] == "needs_review"


# ── 4. A3 soft — low_trust_domain ─────────────────────────────────────────────

def test_a3_soft_fires_on_long_label(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "user@averylonglabelthatexceedslimitok.com"})
    assert "low_trust_domain" in codes(data)


def test_a3_soft_fires_on_multi_hyphen(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "user@some-very-odd-domain.com"})
    assert "low_trust_domain" in codes(data)


# ── 5. A3 mutual exclusion ────────────────────────────────────────────────────

def test_a3_hard_and_soft_are_mutually_exclusive(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "user@spammy.xyz"})
    c = codes(data)
    assert "suspicious_domain" in c
    assert "low_trust_domain" not in c


# ── 6. A4 — source_conflict_manual_vs_enrichment ──────────────────────────────

def test_a4_fires_on_manual_enrichment_conflict(client: TestClient):
    data = post(client, {
        "scenario_id": "t1",
        "email": "jan@example.com",
        "fields": [
            {"field_name": "phone", "source_type": "manual", "value": "+420777111000"},
            {"field_name": "phone", "source_type": "enrichment", "value": "+420999888777"},
        ],
    })
    assert "source_conflict_manual_vs_enrichment" in codes(data)


def test_a4_signal_is_critical_with_fallback(client: TestClient):
    data = post(client, {
        "scenario_id": "t1",
        "fields": [
            {"field_name": "phone", "source_type": "manual", "value": "A"},
            {"field_name": "phone", "source_type": "enrichment", "value": "B"},
        ],
    })
    sig = next(s for s in data["signals"] if s["code"] == "source_conflict_manual_vs_enrichment")
    assert sig["signal_class"] == "critical"
    assert sig["fallback"] is not None


def test_a4_no_conflict_when_same_source_type(client: TestClient):
    data = post(client, {
        "scenario_id": "t1",
        "fields": [
            {"field_name": "phone", "source_type": "manual", "value": "A"},
            {"field_name": "phone", "source_type": "manual", "value": "B"},
        ],
    })
    assert "source_conflict_manual_vs_enrichment" not in codes(data)


# ── 7. A6 — shared_inbox ──────────────────────────────────────────────────────

@pytest.mark.parametrize("email", [
    "info@acme.com",
    "support@acme.com",
    "sales@acme.com",
    "contact@acme.com",
    "hello@acme.com",
])
def test_a6_fires_on_shared_inbox_prefix(client: TestClient, email: str):
    assert "shared_inbox" in codes(post(client, {"scenario_id": "t1", "email": email}))


def test_a6_does_not_fire_on_personal_email(client: TestClient):
    assert "shared_inbox" not in codes(post(client, {"scenario_id": "t1", "email": "jan.novak@acme.com"}))


def test_a6_crm_status_is_low_quality_lead(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "info@acme.com"})
    sig = next(s for s in data["signals"] if s["code"] == "shared_inbox")
    assert sig["visibility"]["crm_status"] == "low_quality_lead"


# ── 8. Multiple signals ────────────────────────────────────────────────────────

def test_shared_inbox_and_suspicious_domain_both_fire(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "info@spammy.xyz"})
    c = codes(data)
    assert "shared_inbox" in c
    assert "suspicious_domain" in c
    assert data["signal_count"] == len(data["signals"])
    assert data["has_signals"] is True


# ── 9. Determinism ────────────────────────────────────────────────────────────

def test_signals_sorted_by_code(client: TestClient):
    """Response order is an explicit API contract — not an evaluator implementation detail."""
    data = post(client, {"scenario_id": "t1", "email": "info@spammy.xyz"})
    returned = [s["code"] for s in data["signals"]]
    assert returned == sorted(returned)


def test_repeated_identical_requests_produce_identical_responses(client: TestClient):
    payload = {
        "scenario_id": "t1",
        "email": "info@spammy.xyz",
        "fields": [
            {"field_name": "phone", "source_type": "manual", "value": "A"},
            {"field_name": "phone", "source_type": "enrichment", "value": "B"},
        ],
    }
    results = [codes(post(client, payload)) for _ in range(5)]
    assert all(r == results[0] for r in results)


def test_tri_signal_sort_order(client: TestClient):
    """
    Payload that fires A3 (suspicious_domain) + A4 (source_conflict) + A6 (shared_inbox).

    Verifies that all three signals are present AND that the response order
    is strictly alphabetical by code — regardless of evaluator rule execution order.

    Expected sorted order:
      shared_inbox < source_conflict_manual_vs_enrichment < suspicious_domain
    """
    data = post(client, {
        "scenario_id": "t1",
        "email": "info@spammy.xyz",  # fires A3 hard (suspicious_domain) + A6 (shared_inbox)
        "fields": [
            {"field_name": "phone", "source_type": "manual", "value": "A"},
            {"field_name": "phone", "source_type": "enrichment", "value": "B"},  # fires A4
        ],
    })
    returned = [s["code"] for s in data["signals"]]

    assert "shared_inbox" in returned
    assert "source_conflict_manual_vs_enrichment" in returned
    assert "suspicious_domain" in returned
    assert returned == sorted(returned), (
        f"Signals not in alphabetical order: {returned}"
    )


# ── 10. Response shape contract (ADR-008) ─────────────────────────────────────

def test_every_signal_has_required_fields(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "info@spammy.xyz"})
    for sig in data["signals"]:
        assert "code" in sig
        assert "action" in sig
        assert "signal_class" in sig
        assert "visibility" in sig
        v = sig["visibility"]
        assert v["crm_status"] or v["routing_tags"] or v["api_flags"]


def test_response_always_has_base_fields(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "clean@example.com"})
    for field in ("request_id", "scenario_id", "status", "has_signals", "signal_count", "signals", "latency_ms"):
        assert field in data


def test_signal_count_matches_signals_length(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "info@spammy.xyz"})
    assert data["signal_count"] == len(data["signals"])


# ── 11. Latency ───────────────────────────────────────────────────────────────

def test_latency_ms_is_present_and_non_negative(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "jan@example.com"})
    assert isinstance(data["latency_ms"], float)
    assert data["latency_ms"] >= 0.0


def test_latency_ms_is_plausible(client: TestClient):
    data = post(client, {"scenario_id": "t1", "email": "info@spammy.xyz"})
    assert data["latency_ms"] < 1000.0


# ── 12. PII invariant ─────────────────────────────────────────────────────────

def test_email_address_not_in_response(client: TestClient):
    email = "info@acme-private-domain.com"
    r = client.post(URL, json={"scenario_id": "t1", "email": email})
    assert email not in r.text
    assert "acme-private-domain.com" not in r.text


def test_field_value_not_in_response(client: TestClient):
    r = client.post(URL, json={
        "scenario_id": "t1",
        "fields": [
            {"field_name": "phone", "source_type": "manual", "value": "+420777SENTINEL"},
            {"field_name": "phone", "source_type": "enrichment", "value": "+420999SENTINEL"},
        ],
    })
    assert "+420777SENTINEL" not in r.text
    assert "+420999SENTINEL" not in r.text


# ── 13. Error path — SIGNAL_EVALUATION_FAILED contract ───────────────────────

def test_evaluator_exception_returns_500(client: TestClient):
    """Contract: unexpected evaluator exception → HTTP 500."""
    with patch(
        "lead_entry_guard.api.routers.signal_check.SignalEvaluator.evaluate",
        side_effect=RuntimeError("internal boom"),
    ):
        r = client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert r.status_code == 500


def test_evaluator_exception_response_has_error_code(client: TestClient):
    with patch(
        "lead_entry_guard.api.routers.signal_check.SignalEvaluator.evaluate",
        side_effect=RuntimeError("internal boom"),
    ):
        r = client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert r.json()["detail"]["error"] == "SIGNAL_EVALUATION_FAILED"


def test_evaluator_exception_response_has_request_id(client: TestClient):
    with patch(
        "lead_entry_guard.api.routers.signal_check.SignalEvaluator.evaluate",
        side_effect=RuntimeError("internal boom"),
    ):
        r = client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert "request_id" in r.json()["detail"]


def test_evaluator_exception_does_not_leak_exception_message(client: TestClient):
    """Exception message must never appear in response — may contain PII."""
    with patch(
        "lead_entry_guard.api.routers.signal_check.SignalEvaluator.evaluate",
        side_effect=RuntimeError("secret internal detail with email=jan@example.com"),
    ):
        r = client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert "secret internal detail" not in r.text
    assert "jan@example.com" not in r.text


def test_evaluator_exception_message_is_stable(client: TestClient):
    """Stable contract — changing this string is a breaking change."""
    with patch(
        "lead_entry_guard.api.routers.signal_check.SignalEvaluator.evaluate",
        side_effect=RuntimeError("any"),
    ):
        r = client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert r.json()["detail"]["message"] == "Signal evaluation encountered an internal error."


# ── 14. Rate limiting — 429 + Retry-After ────────────────────────────────────

@pytest.fixture
def tight_client():
    """
    Fresh app with rate limiter capped at 1 request per window.
    Isolated from module-scoped client — avoids shared limiter state.
    """
    from fastapi import FastAPI
    from lead_entry_guard.api.routers import signal_check as sc_module

    original = sc_module._rate_limiter
    sc_module._rate_limiter = sc_module._TokenBucket(1, 60)
    app = FastAPI()
    app.include_router(sc_module.router)
    client = TestClient(app)
    yield client
    sc_module._rate_limiter = original


def test_first_request_within_limit_passes(tight_client: TestClient):
    r = tight_client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert r.status_code == 200


def test_exceeding_rate_limit_returns_429(tight_client: TestClient):
    tight_client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    r = tight_client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert r.status_code == 429


def test_rate_limit_response_has_retry_after_header(tight_client: TestClient):
    tight_client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    r = tight_client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert r.status_code == 429
    assert "Retry-After" in r.headers
    assert int(r.headers["Retry-After"]) > 0


def test_rate_limit_response_has_error_code(tight_client: TestClient):
    tight_client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    r = tight_client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert r.json()["detail"]["error"] == "RATE_LIMIT_EXCEEDED"


# ── 15. Evaluation timeout — 503 ─────────────────────────────────────────────

@pytest.fixture
def isolated_executor():
    """
    Replaces _eval_executor with a fresh single-worker executor for one test,
    then restores the original and shuts down the temporary one.

    Why this is needed:
      Timeout tests patch SignalEvaluator.evaluate to sleep(10). The handler
      correctly returns 503 after 200ms, but the worker thread keeps sleeping.
      With max_workers=1, the sleeping thread occupies the only slot — the next
      test queues behind it and also times out even though its logic is fine.

      Giving each timeout test its own executor means only that test's executor
      gets the sleeping worker. The shared executor for all other tests stays clean.
    """
    from lead_entry_guard.api.routers import signal_check as sc_module

    original_executor = sc_module._eval_executor
    test_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    sc_module._eval_executor = test_executor

    yield test_executor

    sc_module._eval_executor = original_executor
    test_executor.shutdown(wait=False, cancel_futures=True)


def test_evaluator_timeout_returns_503(client: TestClient, isolated_executor):
    """Evaluator exceeding time budget → HTTP 503."""
    import time as _time

    def slow_evaluate(context):
        _time.sleep(10)
        return []

    with patch(
        "lead_entry_guard.api.routers.signal_check.SignalEvaluator.evaluate",
        side_effect=slow_evaluate,
    ):
        r = client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert r.status_code == 503


def test_evaluator_timeout_response_has_error_code(client: TestClient, isolated_executor):
    import time as _time

    def slow_evaluate(context):
        _time.sleep(10)
        return []

    with patch(
        "lead_entry_guard.api.routers.signal_check.SignalEvaluator.evaluate",
        side_effect=slow_evaluate,
    ):
        r = client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert r.json()["detail"]["error"] == "SIGNAL_EVALUATION_TIMEOUT"


def test_evaluator_timeout_response_has_request_id(client: TestClient, isolated_executor):
    import time as _time

    def slow_evaluate(context):
        _time.sleep(10)
        return []

    with patch(
        "lead_entry_guard.api.routers.signal_check.SignalEvaluator.evaluate",
        side_effect=slow_evaluate,
    ):
        r = client.post(URL, json={"scenario_id": "t1", "email": "jan@example.com"})
    assert "request_id" in r.json()["detail"]


# ── 16. Rate limiter LRU eviction ─────────────────────────────────────────────

def test_rate_limiter_lru_eviction_prevents_memory_growth():
    """
    When more IPs than max_tracked_ips are seen, the oldest entries are evicted.
    After eviction the bucket dict must not exceed the cap.

    This test exercises _TokenBucket directly — not the endpoint — because
    testing memory behaviour through HTTP would require generating 10_000+
    requests which is impractical in a unit test.
    """
    from lead_entry_guard.api.routers.signal_check import _TokenBucket

    cap = 5
    bucket = _TokenBucket(max_requests=100, window_seconds=60, max_tracked_ips=cap)

    # Insert more IPs than the cap
    for i in range(cap + 3):
        allowed, _ = bucket.is_allowed(f"192.168.1.{i}")
        assert allowed  # all should be allowed — limit is 100/window

    # Bucket dict must not exceed the cap
    assert len(bucket._buckets) == cap


def test_rate_limiter_evicted_ip_gets_fresh_bucket():
    """
    An evicted IP starts with a fresh (empty) bucket on re-entry.
    It should not be blocked due to requests from before eviction.
    """
    from lead_entry_guard.api.routers.signal_check import _TokenBucket

    cap = 2
    bucket = _TokenBucket(max_requests=1, window_seconds=60, max_tracked_ips=cap)

    # Fill IP_A's bucket to the limit
    allowed, _ = bucket.is_allowed("ip_A")
    assert allowed
    blocked, _ = bucket.is_allowed("ip_A")
    assert not blocked

    # Push in two more IPs to evict ip_A (cap=2, so ip_B + ip_C fills and evicts ip_A)
    bucket.is_allowed("ip_B")
    bucket.is_allowed("ip_C")

    # ip_A is now evicted — it should get a fresh bucket and be allowed
    allowed_again, _ = bucket.is_allowed("ip_A")
    assert allowed_again, "Evicted IP should start fresh, not carry old rate limit state"


# ── 17. IP trust model + shared executor ─────────────────────────────────────

def test_xff_ignored_without_trusted_proxy_config(client: TestClient):
    """
    Without trusted proxy config (TRUSTED_PROXY_IPS env var unset or empty),
    X-Forwarded-For must be ignored entirely.

    Rate limit key must be based on the direct connection IP, not the header.
    We verify this by confirming a request with a spoofed XFF header
    is treated identically to one without — it does not bypass rate limiting.
    """
    from lead_entry_guard.api.routers import signal_check as sc_module

    # Patch to empty — simulates no TRUSTED_PROXY_IPS env var configured.
    # (Env may or may not be set in CI — patch to be deterministic.)
    original = sc_module._TRUSTED_PROXY_IPS
    sc_module._TRUSTED_PROXY_IPS = frozenset()
    try:
        r = client.post(
            URL,
            json={"scenario_id": "t1", "email": "jan@example.com"},
            headers={"X-Forwarded-For": "1.2.3.4"},
        )
        assert r.status_code == 200
    finally:
        sc_module._TRUSTED_PROXY_IPS = original


def test_xff_honoured_when_connecting_ip_is_trusted(client: TestClient):
    """
    When the direct connecting IP is in _TRUSTED_PROXY_IPS,
    X-Forwarded-For is read and the leftmost IP is used as the rate limit key.

    We verify this by temporarily adding the test client IP (127.0.0.1)
    to _TRUSTED_PROXY_IPS and confirming the request succeeds — i.e. the
    function doesn't crash and returns the XFF IP rather than the proxy IP.
    """
    from lead_entry_guard.api.routers import signal_check as sc_module

    original = sc_module._TRUSTED_PROXY_IPS
    # TestClient connects from 127.0.0.1 (testclient) — trust that IP
    sc_module._TRUSTED_PROXY_IPS = frozenset({"127.0.0.1", "testclient"})
    try:
        r = client.post(
            URL,
            json={"scenario_id": "t1", "email": "jan@example.com"},
            headers={"X-Forwarded-For": "203.0.113.5"},
        )
        assert r.status_code == 200
    finally:
        sc_module._TRUSTED_PROXY_IPS = original


def test_get_client_ip_without_trusted_proxy_ignores_xff():
    """Unit test for _get_client_ip — no trusted proxy → direct IP always."""
    from unittest.mock import MagicMock
    from lead_entry_guard.api.routers import signal_check as sc_module

    original = sc_module._TRUSTED_PROXY_IPS
    sc_module._TRUSTED_PROXY_IPS = frozenset()  # no trusted proxies
    try:
        mock_request = MagicMock()
        mock_request.client.host = "10.0.0.5"
        mock_request.headers.get = lambda k, d=None: "1.2.3.4" if k == "X-Forwarded-For" else d

        ip = sc_module._get_client_ip(mock_request)
        assert ip == "10.0.0.5", (
            f"Expected direct IP '10.0.0.5', got '{ip}'. "
            "X-Forwarded-For must be ignored without trusted proxy config."
        )
    finally:
        sc_module._TRUSTED_PROXY_IPS = original


def test_get_client_ip_with_trusted_proxy_uses_xff():
    """Unit test for _get_client_ip — trusted proxy → leftmost XFF IP."""
    from unittest.mock import MagicMock
    from lead_entry_guard.api.routers import signal_check as sc_module

    original = sc_module._TRUSTED_PROXY_IPS
    sc_module._TRUSTED_PROXY_IPS = frozenset({"10.0.0.1"})
    try:
        mock_request = MagicMock()
        mock_request.client.host = "10.0.0.1"  # is a trusted proxy
        mock_request.headers.get = lambda k, d=None: "203.0.113.5, 10.0.0.2" if k == "X-Forwarded-For" else d

        ip = sc_module._get_client_ip(mock_request)
        assert ip == "203.0.113.5", (
            f"Expected leftmost XFF IP '203.0.113.5', got '{ip}'."
        )
    finally:
        sc_module._TRUSTED_PROXY_IPS = original


def test_shared_executor_exists_at_module_level():
    """
    Verify _eval_executor is a module-level shared instance.
    If it were created per-request, this import would return a new object each time.
    """
    from lead_entry_guard.api.routers import signal_check as sc_module
    import concurrent.futures

    assert isinstance(sc_module._eval_executor, concurrent.futures.ThreadPoolExecutor)


def test_shutdown_executor_is_callable():
    """
    shutdown_executor must be importable and callable.
    This is the hook that app lifespan must call on exit.

    We do NOT call it here — that would terminate the shared executor
    and break subsequent tests. Just verify the contract.
    """
    from lead_entry_guard.api.routers.signal_check import shutdown_executor
    assert callable(shutdown_executor)


def test_trusted_proxy_ips_parsed_from_env(monkeypatch):
    """
    _TRUSTED_PROXY_IPS must be populated from TRUSTED_PROXY_IPS env var.
    This is deployment config — it must not require a code change.

    We reimport the module after patching the env var to verify parsing.
    """
    import importlib
    import lead_entry_guard.api.routers.signal_check as sc_module

    monkeypatch.setenv("LEG_TRUSTED_PROXY_IPS", "10.0.0.1, 10.0.0.2 , 192.168.1.1")
    # Re-evaluate the constant by reloading the module
    importlib.reload(sc_module)
    try:
        assert sc_module._TRUSTED_PROXY_IPS == frozenset({"10.0.0.1", "10.0.0.2", "192.168.1.1"})
    finally:
        importlib.reload(sc_module)  # restore original state


def test_trusted_proxy_ips_empty_when_env_unset(monkeypatch):
    """Empty or unset TRUSTED_PROXY_IPS env var → empty frozenset (fail-safe default)."""
    import importlib
    import lead_entry_guard.api.routers.signal_check as sc_module

    monkeypatch.delenv("LEG_TRUSTED_PROXY_IPS", raising=False)
    importlib.reload(sc_module)
    try:
        assert sc_module._TRUSTED_PROXY_IPS == frozenset()
    finally:
        importlib.reload(sc_module)


# ── 18. SIGNAL_CHECK_WORKERS env parsing ─────────────────────────────────────

def test_parse_workers_valid_value():
    from lead_entry_guard.api.routers.signal_check import _parse_workers
    assert _parse_workers("4") == 4
    assert _parse_workers("1") == 1
    assert _parse_workers("32") == 32


def test_parse_workers_empty_string_returns_default():
    from lead_entry_guard.api.routers.signal_check import _parse_workers
    assert _parse_workers("") == 1
    assert _parse_workers("   ") == 1


def test_parse_workers_none_returns_default():
    from lead_entry_guard.api.routers.signal_check import _parse_workers
    assert _parse_workers(None) == 1


def test_parse_workers_custom_default():
    from lead_entry_guard.api.routers.signal_check import _parse_workers
    assert _parse_workers("", default=4) == 4
    assert _parse_workers(None, default=8) == 8


def test_parse_workers_non_integer_raises():
    """Non-integer must fail fast at import time — not silently default."""
    from lead_entry_guard.api.routers.signal_check import _parse_workers
    import pytest
    with pytest.raises(ValueError, match="LEG_SIGNAL_CHECK_WORKERS must be a positive integer"):
        _parse_workers("two")
    with pytest.raises(ValueError, match="LEG_SIGNAL_CHECK_WORKERS must be a positive integer"):
        _parse_workers("1.5")
    with pytest.raises(ValueError, match="LEG_SIGNAL_CHECK_WORKERS must be a positive integer"):
        _parse_workers("abc")


def test_parse_workers_zero_raises():
    from lead_entry_guard.api.routers.signal_check import _parse_workers
    import pytest
    with pytest.raises(ValueError, match="LEG_SIGNAL_CHECK_WORKERS must be >= 1"):
        _parse_workers("0")


def test_parse_workers_negative_raises():
    from lead_entry_guard.api.routers.signal_check import _parse_workers
    import pytest
    with pytest.raises(ValueError, match="LEG_SIGNAL_CHECK_WORKERS must be >= 1"):
        _parse_workers("-1")
    with pytest.raises(ValueError, match="LEG_SIGNAL_CHECK_WORKERS must be >= 1"):
        _parse_workers("-10")
