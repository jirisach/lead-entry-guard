# Phase 3B — Signal Enforcement Layer — Backlog

**Status:** Planning  
**Date:** 2026-03-27  
**Invariant source:** ADR-008

---

## Goal

Transform the binary decision pipeline into an enforceable signal pipeline.

```
Phase 3B = binary decision pipeline → enforceable signal pipeline
```

Not a governance platform. Not a CRM write-back system.  
A signal enforcement layer on top of the existing decision engine.

---

## Phase Gate

> **If a signal has no operational consequence, it is not part of Phase 3B.**

Every 3B deliverable must pass this gate before it is considered complete.  
See ADR-008 — Phase Gate section for the normative definition.

---

## 3B.0 — Contract test (entry condition for all 3B features)

**This must be implemented first. Nothing else in 3B is valid without it.**

Write `test_signal_contract.py` with two invariants:

**Negative invariant**  
A signal definition missing `action`, `visibility`, or `fallback` must be rejected
as invalid. The system must not emit it.

```python
def test_signal_without_action_is_invalid(): ...
def test_signal_without_visibility_is_invalid(): ...
def test_signal_without_fallback_is_invalid(): ...
```

**Positive invariant**  
A signal definition with all three properties is valid and can be projected
into `DecisionResult`.

```python
def test_complete_signal_is_valid(): ...
def test_complete_signal_projects_into_decision_result(): ...
```

This test is the executable form of ADR-008.  
It enforces the Phase Gate defined in ADR-008.  
**If it fails, the feature must not be merged.**

---

## 3B.1 — Extend DecisionResult schema

`DecisionResult` must carry visibility fields for downstream consumption.

Current output:
```python
decision: DecisionClass
reason_codes: list[ReasonCode]
duplicate_check_skipped: bool
versions: PolicyVersions
```

Extended output (3B):
```python
decision: DecisionClass
reason_codes: list[ReasonCode]
duplicate_check_skipped: bool
versions: PolicyVersions
signals: list[SignalResult]        # new — actionable signals
enforcement: EnforcementProjection # new — downstream-facing visibility
```

`signals` must be part of the response contract, not internal logs.

---

## 3B.2 — Signal definition model

Define the `SignalDefinition` model with required fields:

```python
@dataclass
class SignalDefinition:
    code: str
    action: SignalAction
    visibility: VisibilityProjection
    fallback: FallbackPolicy
```

All three fields are required. A `SignalDefinition` without any of them
must fail `test_signal_contract.py`.

---

## 3B.3 — Visibility projection layer

Implement `VisibilityProjection` — translates internal signals into
fields downstream systems already consume.

Requirements:
- Must affect fields downstream systems rely on (status, tags, routing)
- Must not rely on consumers reading optional metadata
- Must be part of `DecisionResult`, not only logs

---

## 3B.4 — Fallback semantics

Implement `FallbackPolicy` per signal.

Two classes of signals (see ADR-008, Open Question 3):

| Class | Fallback required? |
|---|---|
| Critical (`requires_review`, `conflict_detected`) | Yes — mandatory |
| Informational (`low_quality`) | Recommended — may be exempt |

Fallback must be tenant-configurable. Hardcoded fallback is not allowed.

---

## 3B.5 — Per-tenant signal policy

Extend `TenantConfig` with signal-level policy:

```python
@dataclass
class TenantSignalPolicy:
    domain_policy: SignalPolicyLevel          # allow | warn | block
    shared_inbox_policy: SignalPolicyLevel    # allow | warn | block
    source_priority: list[SourceType]         # manual > enrichment > api
    incomplete_duplicate_policy: SignalPolicyLevel
```

This is a thin first layer — not a full config explosion.  
Only signals validated by field feedback (March 2026) are included.

---

## 3B.6 — First signal implementations

Implement the four signals confirmed by field validation (Prianka, March 2026):

| Signal | Source | Feedback |
|---|---|---|
| `suspicious_domain` | A3 | Allow but flag — WARN preferred over binary block |
| `shared_inbox` | A6 | Allow but lower quality — scoring, not validation |
| `source_conflict_manual_vs_enrichment` | A4 | Manual value takes priority |
| `duplicate_on_incomplete_payload` | A2 | Accept with caution — do not overwrite better data |

Each signal must pass `test_signal_contract.py` before merge.

---

## 3B.7 — End-to-end enforcement test

End-to-end proof that the full signal contract holds in a real pipeline run.
This is not a contract test — it is an integration proof that action, visibility,
and fallback all propagate correctly from signal definition to `DecisionResult`.

```
lead with suspicious domain →
  action: accept_with_flag →
  visibility: crm_status=needs_review →
  fallback: auto_expire after 24h →
  DecisionResult contains all three →
  test passes
```

A `DecisionResult` that contains signals without visibility or fallback
must fail this test.

---

## Explicitly out of scope for 3B

The following are deferred to a later phase:

- Background review queue orchestration
- Full CRM write-back loop
- Generalized conflict engine across multiple systems
- Multi-signal prioritization framework
- Enterprise governance UI
- Time-based fallback background jobs (design in 3B, implement later)

If any of these appears in a 3B PR, it must be challenged against the phase gate.

---

## Related

- ADR-008 — Signals must be actionable (normative source)
- ADR-002 — Degraded mode policy separation
- ADR-007 — Tenant identity from auth only

---

## Post-3B — Public demo exposure hardening

**Epic:** Signal Check public exposure hardening  
**Trigger:** Before any public URL for `/v1/leads/signal-check` is shared externally  
**Status:** Blocked — preconditions not met  
**Reference:** ADR-010, `docs/security/SECURITY-NOTES.md`

### Why this is a hard gate, not a nice-to-have

The in-process rate limiter is sandbox-grade. Direct public exposure without
upstream protection enables enumeration, scripted probing, and resource exhaustion.
A publicly shared demo link without gateway protection is the same attack surface
as a production endpoint.

### Tasks (implement in order)

1. **Gateway / reverse proxy** — nginx, Cloudflare, or cloud API gateway in front of signal-check
2. **Gateway-level rate limiting** — outside process, per-IP and per-ASN
3. **WAF rules** — bot filtering, request size limits, header validation
4. **IP reputation filtering** — block known abuse sources at edge
5. **Edge request logging** — structured logs at proxy layer with retention
6. **Distributed rate limiter** — replace `_TokenBucket` with Redis-backed limiter
7. **Firewall rule** — ensure app port is not directly reachable from internet

**Do not share a public demo link until items 1–3 are complete.**

See `docs/security/SECURITY-NOTES.md` for the full checklist.
