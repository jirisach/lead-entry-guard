# ADR-011: Production Ingest Boundary v1

**Status:** Accepted  
**Date:** 2026-04  
**Deciders:** jirisach  
**Replaces:** —  
**Related:** ADR-007 (tenant identity from auth), ADR-004 (idempotency snapshot contract),  
ADR-001 (recovery path store contract), ADR-002 (degraded mode policy separation),  
ADR-010 (signal check as non-production validation surface)

---

## Context

The `/v1/leads/ingest` endpoint exists and is operational. Auth, tenant identity,
idempotency, duplicate detection, pipeline degraded modes, and signal evaluation
are all implemented.

What does not yet exist is a single ADR-level contract that:

- states what is authoritative and contractual for v1
- explicitly separates stable contracts from implementation details
- records deferred decisions so they are not accidentally finalized

This ADR formalizes the production ingest boundary as it exists in v1.
It does not redesign the pipeline. It codifies the contract.

---

## What is already true in implementation

The following are implemented and stable. This ADR elevates them to contract level.

### Trust model

- `tenant_id` in the response is always derived from the authenticated API key
- `tenant_id` is never accepted from the request body — the field is absent from `IngestRequest`
- Attempting to inject `tenant_id` via the body is silently ignored (Pydantic `extra="ignore"`)
- An invalid or missing `X-API-Key` returns HTTP 401
- A valid key for an inactive tenant returns HTTP 403

Source: ADR-007, `app.py` `IngestRequest`, `tests/integration/test_auth_ingest.py`

### Request contract (v1)

```
POST /v1/leads/ingest
X-API-Key: <tenant api key>

{
  "source_id":   string | null,   // caller-assigned idempotency key
  "source_type": enum,            // API | MANUAL | IMPORT | FORM | ENRICHMENT
  "email":       string | null,
  "phone":       string | null,
  "first_name":  string | null,
  "last_name":   string | null,
  "company":     string | null,
  "extra":       object           // arbitrary additional fields
}
```

`tenant_id` is intentionally absent from the request body.

### Response contract (v1)

```json
{
  "request_id":              "uuid",
  "tenant_id":               "string",
  "decision":                "PASS | WARN | REJECT | DUPLICATE_HINT",
  "reason_codes":            ["string"],
  "duplicate_check_skipped": false,
  "policy_version":          "string",
  "ruleset_version":         "string",
  "config_version":          "string",
  "latency_ms":              0.0
}
```

### Decisions

| Decision | Meaning |
|---|---|
| `PASS` | Lead accepted, no quality issues |
| `WARN` | Lead accepted with warnings (recoverable issues) |
| `REJECT` | Lead rejected (fatal validation failure) |
| `DUPLICATE_HINT` | Lead fingerprint matches an existing accepted lead |

### Idempotency

Same `source_id` + same payload hash → same `request_id` and same `decision` on replay.
Payload hash is computed over normalized request fields — not raw JSON.
TTL: 24 hours.
Idempotency is opt-in: requests without `source_id` have no idempotency protection.
Source: ADR-004.

### Write semantics

On `PASS` or `WARN`:
- Lead fingerprint written to duplicate store (Redis, TTL 30 days)
- Idempotency snapshot written (Redis, TTL 24 hours)
- Telemetry event emitted (fire-and-forget, never blocks response)

On `REJECT`:
- Idempotency snapshot written (so replay returns same REJECT, not PASS)
- No fingerprint written to duplicate store

On `DUPLICATE_HINT`:
- No new fingerprint written
- Idempotency snapshot written

All store writes are non-blocking and best-effort. The decision outcome is fully
determined before any write attempt — writes are a controlled side effect of the
decision, not a source of truth for it. Write failures are logged but do not
cause the response to fail.
Source: ADR-001, ADR-004.

### Degraded mode

When Redis is unavailable, per-tenant policy applies:

| Policy | Behaviour |
|---|---|
| `ACCEPT_WITH_FLAG` | Lead continues, `duplicate_check_skipped=true` in response |
| `REJECT` | Request rejected with 503 |
| `QUEUE` | Request held in memory up to 15 minutes, then fallback policy |

Source: ADR-002.

### Signal evaluation

Signal layer (A3, A4, A6) runs after the primary decision is made.
Signals annotate the decision in v1 — they do not influence the primary decision outcome.
Signal exposure in the response and compound evaluation are explicitly deferred (see below).
Signals are internal to the pipeline. They are not exposed in the v1 response.

Source: ADR-008, ADR-009.

### PII handling

- No raw PII in logs (enforced by `PIIRedactingFilter`)
- No raw PII in error responses
- `request_id` is the safe tracing key across all log lines
- Fingerprint stored as HMAC-SHA256, never raw email or phone

---

## What is contractual for v1

The following are stable external contracts. Breaking changes require a new ADR
and a version bump.

1. `POST /v1/leads/ingest` path and method
2. `X-API-Key` header as the auth mechanism
3. `tenant_id` absent from request body, present in response
4. Decision values: `PASS`, `WARN`, `REJECT`, `DUPLICATE_HINT`
5. `request_id` always present in response
6. `duplicate_check_skipped` field in response
7. Idempotency behaviour for requests with `source_id`
8. HTTP status codes: 200 (success), 401 (auth), 403 (inactive tenant), 422 (validation), 503 (degraded)

---

## What is implementation detail (not contractual)

The following may change without a new ADR, as long as external behaviour is preserved.

- Internal pipeline stage order (normalization → validation → fingerprint → duplicate → policy)
- Signal rule implementations (A3, A4, A6) and their internal thresholds
- Redis key structure and TTL values (within bounds of idempotency and duplicate TTL contracts)
- Telemetry event schema and export mechanism
- Vault/KMS backend for HMAC keys

---

## Deferred decisions

The following are explicitly deferred. They must not be finalized until the
conditions described are met.

### 1. Signal exposure in ingest response

**Current state:** Signals are evaluated internally but not included in the ingest response.

**Why deferred:** Signal semantics for compound evaluation are under active
validation (see `docs/planning/signal-combination-hypothesis.md`). Exposing
signals in the response before compound semantics are stable would create a
breaking contract change when compound is eventually included or modified.

**Condition to resolve:** Signal combination hypothesis validated by 2+ design
partners + pilot data. Write ADR before exposing signals in response.

### 2. Compound signal evaluation in ingest pipeline

**Current state:** `SignalEvaluator` runs A3, A4, A6 independently. No compound
evaluation in the production pipeline.

**Why deferred:** Compound signal logic is experimental. Action thresholds,
signal strength tiers, and the confidence/retire model are not yet validated.
See `docs/planning/signal-combination-hypothesis.md`.

**Condition to resolve:** Same as above — 2+ design partners + data.

### 3. Multi-signal public API contract

**Current state:** No compound or multi-signal fields in request or response.

**Why deferred:** Exposing compound semantics before the signal combination
hypothesis is resolved would lock in an API contract that may need to change.

**Condition to resolve:** Compound model finalized and ADR written.

### 4. ReviewEvent / feedback loop integration

**Current state:** `ReviewEvent` schema designed but not implemented.
See `docs/planning/signal-combination-hypothesis.md`.

**Why deferred:** Feedback loop requires ownership model and outcome tracking
infrastructure. Premature integration would couple ingest boundary to
an unvalidated review workflow.

**Condition to resolve:** Ownership model validated in practice + ReviewEvent
schema finalized.

---

## Scope guard

The following must not be added to `/v1/leads/ingest` without a new ADR:

- Signal codes or compound evaluation results in the response body
- Any field that implies signal-level explainability or reasoning chains
- Tenant-configurable signal rules via request payload
- Write operations beyond fingerprint store, idempotency store, and telemetry
- Synchronous enrichment calls in the request path
- Any field that exposes internal pipeline state not listed in the v1 response contract

---

## Test coverage

| Test | Type | What it guards |
|---|---|---|
| `tests/integration/test_auth_ingest.py` | Integration | Auth contract: 401/403, tenant from auth, body injection ignored |
| `tests/integration/test_pipeline.py` | Integration | Idempotency, duplicate detection, decision correctness |
| `tests/integration/test_idempotency_across_decisions.py` | Integration | Idempotency across PASS/REJECT/WARN |
| `tests/resilience/` | Resilience | Degraded mode, Redis unavailable, Bloom unavailable |

---

## References

- `src/lead_entry_guard/api/app.py` — IngestRequest, IngestResponse, endpoint handler
- `src/lead_entry_guard/core/pipeline.py` — pipeline implementation
- ADR-001 — recovery path store contract
- ADR-002 — degraded mode policy separation
- ADR-004 — idempotency snapshot contract
- ADR-007 — tenant identity from auth only
- ADR-008 — signals must be actionable
- ADR-010 — signal check as non-production validation surface
- `docs/planning/signal-combination-hypothesis.md` — compound signal experiment
