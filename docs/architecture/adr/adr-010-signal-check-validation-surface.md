# ADR-010: Enforce Separation of Signal Validation Surface from Production Ingest Boundary

**Status:** Accepted  
**Date:** 2026-04  
**Deciders:** jirisach  
**Replaces:** —  
**Related:** ADR-007 (tenant identity from auth), ADR-008 (signals must be actionable)

---

## Context

Phase 3B delivered a fully functional signal layer (A3, A4, A6) with a unified
`SignalEvaluator` and `LeadSignalContext`. The next natural step was exposing
signal evaluation via an API endpoint for:

- demo and sales walkthroughs
- design partner feedback (e.g. Priyanka — RevOps/BI)
- scenario testing and regression validation
- explainability: "why was this lead flagged?"

The naive approach would be to add a `/v1/leads/signal-check` endpoint to the
existing ingest surface — with auth, tenant lookup, and the full request
lifecycle. That approach was considered and rejected.

### Why the naive approach is wrong

The existing `/v1/leads/ingest` is a production trust boundary:

- tenant identity comes from auth only (ADR-007)
- writes to Redis, idempotency store, and audit trail
- duplicate detection runs
- fallback policies apply

Mixing a demo/validation surface into this boundary creates four failure patterns:

1. **False trust signal** — callers start treating `signal-check` as a
   production endpoint because it looks like one. `tenant_id` from body
   gets treated as authoritative identity.

2. **Scope creep magnet** — once auth exists, the next PR adds enrichment
   lookup "just for demo". Then duplicate check. Then DB writes. The
   boundary dissolves gradually and silently.

3. **Signal semantic drift** — if the endpoint uses a simplified or mocked
   evaluator path for speed, sandbox output diverges from production output.
   Design partner feedback becomes invalid because it no longer reflects
   real system behaviour.

4. **Demo friction** — requiring a valid API key and a running DB makes
   quick scenario testing unnecessarily difficult, reducing the value of
   the validation surface.

---

## Decision

We introduce `/v1/leads/signal-check` as a **non-production validation surface**,
architecturally separate from the ingest boundary.

### What it is

- A stateless, read-only wrapper around `SignalEvaluator`
- No auth, no DB, no Redis, no persistence, no side effects
- `scenario_id` in the request body is a demo-scoped label, not trusted identity
- Response is deterministic: signals sorted by code, same input → same output always
- Rate limited per-IP (in-process token bucket, sandbox grade)
- Shared `ThreadPoolExecutor` with timeout guard for bounded execution

### What it explicitly is not

- Not a production ingest endpoint
- Not an auth boundary — `scenario_id` is never authoritative identity
- Not a tenant isolation boundary
- Not a compliance surface for write operations

### Parity guarantee

The endpoint calls `SignalEvaluator().evaluate(context)` directly — the same
evaluator used by the production pipeline. It does not wrap, mock, or simplify
the evaluator. This is enforced by `tests/contract/test_signal_check_parity.py`,
which runs the endpoint and the evaluator side-by-side and asserts identical
output. If this test breaks, the endpoint has diverged from production semantics.

The parity test is a **phase gate** — it must pass before every merge, same as
`test_signal_contract.py`. Divergence is a blocking condition, not a warning.

### Scope guard (do not add)

The following must never be added to this endpoint:

- tenant config lookup or DB access
- real auth / `require_tenant`
- enrichment calls
- duplicate detection
- explainability text generation
- any write side effect

This guard is documented in the module docstring and enforced by the parity test.

---

## Non-goals

This endpoint will never:

- be used for production ingestion or as a write-capable surface
- provide tenant isolation guarantees
- act as a compliance or audit boundary
- serve as the basis for access control decisions
- be "upgraded" into a production endpoint by adding auth

Any production-grade signal evaluation must be implemented as part of the ingest
pipeline, not by extending this endpoint. If production signal evaluation is
needed with auth and persistence, a new endpoint must be designed from scratch
with the full ingest contract in mind.

---

## Consequences

### Positive

- Safe demo surface — no production data at risk, no write side effects
- Signal semantics identical to production — design partner feedback is valid
- Simple, auditable contract — easy to reason about and test
- Short feedback loop — no infra required to run signal scenarios
- Parity test acts as architectural safety net against future drift

### Negative / known trade-offs

- `scenario_id` is not authoritative — cannot be used for access control or
  data isolation. Named explicitly to prevent misuse (`scenario_id` not `tenant_id`).
- Rate limiting is in-process only — does not survive restarts, does not
  coordinate across replicas. Acceptable for sandbox traffic; must be replaced
  with Redis-backed or gateway-level limiting before production exposure.
- `ThreadPoolExecutor(max_workers=1)` — concurrent requests queue behind each
  other. Timeout budget covers queue wait + execution. Acceptable for sandbox;
  increase `LEG_SIGNAL_CHECK_WORKERS` and review `_EVAL_TIMEOUT_SECONDS` together
  before higher-traffic exposure.
- Logging is not integrity-protected — production deployment requires
  append-only log sink (see `docs/compliance/security-incident-response.md`).

### Failure modes

**If parity breaks:**
The endpoint must be considered invalid for design partner feedback and scenario
testing. Deployment must be blocked until parity is restored. Parity failure
indicates that signal semantics have diverged between sandbox and production —
any feedback collected during the divergence window may be invalid.

**If rate limiting is bypassed at scale:**
This endpoint is not protected against abuse at scale. It must not be exposed
publicly without upstream protection (API gateway or WAF). This applies to
production deployments and public demo links equally — a publicly shared demo
URL without upstream protection is the same attack surface as a production
endpoint. Exposing it directly to the internet without a trusted proxy layer
allows rate limit spoofing via `X-Forwarded-For` and brute-force signal probing.

**If scope guard is violated:**
Adding DB access, auth, or write side effects converts this endpoint into an
implicit ingest surface without the safety contracts of the real ingest pipeline.
This creates a compliance gap and undermines tenant isolation guarantees.

---

## Evolution path

This endpoint is intentionally sandbox-grade. The correct evolution path is:

- **More signal rules** → add to `SignalEvaluator`, covered automatically by parity test
- **Higher traffic** → increase `LEG_SIGNAL_CHECK_WORKERS`, add upstream rate limiting
- **Production signal evaluation with auth** → new endpoint, new ADR, full ingest contract

What is not a valid evolution path:

- Adding auth to this endpoint and calling it production-ready
- Adding DB lookups "just for one feature"
- Relaxing the parity test to allow simplified evaluation paths

---

## Env configuration

| Variable | Default | Purpose |
|---|---|---|
| `LEG_TRUSTED_PROXY_IPS` | `""` | Comma-separated IPs of trusted reverse proxies. Empty = ignore X-Forwarded-For (fail-safe). |
| `LEG_SIGNAL_CHECK_WORKERS` | `1` | ThreadPoolExecutor worker count. Increase before higher-traffic exposure alongside `_EVAL_TIMEOUT_SECONDS`. |

---

## Test coverage

| Test file | Type | What it guards |
|---|---|---|
| `tests/api/test_signal_check_api.py` | Unit | Contract, error paths, rate limiting, timeout, PII invariant, IP trust model, LRU eviction, env parsing |
| `tests/contract/test_signal_check_parity.py` | Contract (phase gate) | Endpoint output identical to direct `SignalEvaluator` call — blocks divergence |
| `tests/integration/test_app_lifespan_shutdown.py` | Integration | `shutdown_executor` called in app lifespan — no thread leak on restart |

---

## References

- `src/lead_entry_guard/api/routers/signal_check.py` — implementation
- `tests/contract/test_signal_check_parity.py` — parity phase gate
- ADR-007 — tenant identity from auth only (applies to write paths; this endpoint has no write path)
- ADR-008 — signals must be actionable (signal contract enforced by `SignalEvaluator`)
