# ADR-008 — Signals Must Be Actionable

**Status:** Proposed  
**Date:** 2026-03-27  
**Owners:** core team  

---

## Context

Phase 3B introduces a signal enforcement layer on top of the existing decision engine.

The current pipeline emits a primary decision (`PASS`, `REJECT`, `WARN`, `DUPLICATE_HINT`)
and attaches auxiliary metadata such as `reason_codes` and `duplicate_check_skipped`.
This is sufficient for binary accept/reject enforcement, but it is not sufficient for
the signal-based policy model introduced in Phase 3B.

Field validation from an external BI specialist (March 2026) confirmed a real-world pattern:

> In practice, downstream systems — CRMs, automation tools, webhook consumers —
> often read only the primary decision and ignore auxiliary metadata fields.

This creates a class of failure that is silent and invisible:

```
LEG emits signal → downstream reads only `decision: ACCEPT` → signal ignored → 
automation proceeds → no flag, no review, no fallback
```

In this state, the system is **observational, not enforceable**.
It knows something is wrong but cannot guarantee that this knowledge has any effect.

This ADR defines the invariant that prevents this failure mode.

---

## Decision

Every signal emitted by the pipeline must define three properties:

### 1. Action
What the system does immediately — in the request path or directly after decision evaluation.

Not logging. Not metadata attachment. A concrete behavioral change:
- modify the decision class
- set a status field consumed by downstream
- route to a different processing path

### 2. Visibility
How the signal is exposed downstream in a way that does not require consumers
to explicitly read internal metadata.

The signal must affect fields that downstream systems **already rely on** — not only
optional or auxiliary fields they may ignore.

```yaml
# Weak visibility — downstream can ignore this
signals: ["suspicious_domain"]
requires_review: true

# Strong visibility — downstream cannot ignore this without breaking their own flow
crm_status: needs_review
routing_tag: low_trust
```

### 3. Fallback
What happens if no human or downstream system reacts to the signal within
the expected window.

A signal without a fallback is a passive annotation. It does not change system state
if ignored. This is not policy — it is observability.

```yaml
# Invalid — no fallback defined
signal: suspicious_domain
action: accept_with_flag
fallback: null   ← not allowed

# Valid
signal: suspicious_domain
action: accept_with_flag
fallback:
  mode: auto_expire_review
  after_hours: 24
  then: keep_accepted_with_low_trust
```

---

## Invariant

> **A signal without operational consequence is observability, not policy.**

This invariant applies to every signal emitted by the pipeline.
A signal that exists only in logs or internal structures and has no downstream
effect on status, routing, or state is not considered a policy signal.

---

## Data Exposure Invariant

> **Visibility fields must never contain raw PII.**

Signals must be expressed using status codes, tags, and boolean flags only.

Raw values such as email addresses, phone numbers, names, or any free-form text
derived from input data must not appear in visibility projections.

```yaml
# Invalid — raw PII in visibility
visibility:
  reason: "email suspicious: user@weird-domain.com"   # not allowed
  debug_context: "phone matched: +420777123456"       # not allowed

# Valid — no PII
visibility:
  crm_status: needs_review
  routing_tag: low_trust
  requires_review: true
```

**Rationale:** Visibility fields cross the internal system boundary — they propagate
to downstream CRM integrations, telemetry, and logs. Including raw PII here would
create unintended data exposure and constitute a compliance incident, not a bug.
This cannot be fixed retroactively once downstream systems have consumed the data.

---

## Worked examples

### suspicious_domain

```yaml
signal:
  code: suspicious_domain
  action: accept_with_flag
  visibility:
    crm_status: needs_review
    export_tag: low_trust
    api_field: requires_review=true
  fallback:
    mode: auto_expire_review
    after_hours: 24
    then: keep_accepted_with_low_trust
```

### source_conflict — manual vs enrichment

```yaml
signal:
  code: source_conflict_manual_vs_enrichment
  action: preserve_manual_value
  visibility:
    crm_status: conflict_flagged
    export_tag: source_conflict
    api_field: conflict_detected=true
  fallback:
    mode: no_overwrite
    then: manual_value_remains_authoritative
```

---

## Consequences

**DecisionResult must include visibility fields.**  
Signals cannot exist only in logs or internal structures.
Their effects must be present in the output consumed by downstream systems.

**Visibility must be unavoidable.**  
The signal must affect fields that downstream systems already rely on —
status, tags, routing — not only optional metadata.

**Fallback must be explicitly defined.**  
Every signal must define what happens if no reaction occurs.
`requires_review: true` without a fallback is not a valid signal definition.

**Visibility fields must never carry raw PII.**  
See Data Exposure Invariant above. This applies to every visibility projection
without exception. Violations are compliance incidents, not implementation bugs.

**Fallback must be configurable per tenant.**  
Fallback behavior is part of tenant policy configuration.
Hardcoded fallback behavior is not allowed.

**Separation of concerns.**

| Layer | Responsibility |
|---|---|
| Decision layer | Determines what is true about the input |
| Enforcement layer | Ensures that truth has operational consequences |

Without the enforcement layer, LEG is a smart filter.  
With it, LEG is an enforcement system.

---

## Alternatives considered

**Emit signals as metadata only, let downstream decide.**  
Rejected. Field validation confirmed that downstream systems ignore auxiliary metadata
in practice. This is the exact failure mode this ADR is designed to prevent.

**Enforce at CRM level only.**  
Rejected. Enforcement inside CRM depends on CRM configuration and is outside LEG's
control. LEG must guarantee consequences before data enters CRM.

**Enforce before CRM only (hard block).**  
Rejected. Field validation (March 2026) confirmed that binary blocking is too aggressive
for ambiguous signals such as suspicious domains and shared inboxes. The correct model
is: accept → classify → route / flag / degrade.

---

## Open Questions

1. **Minimal visibility surface** — What is the minimum set of fields that guarantees
   downstream awareness across different CRM integrations?
   (e.g. tags vs status vs routing changes)

2. **Time-based fallbacks** — How should time-based fallbacks be implemented and tested?
   (e.g. auto-resolve after 24 hours — requires background job or TTL mechanism)

3. **Signal scope** — Should all signals require a fallback, or only a subset?  
   Proposal: informational signals (low_quality) may be fallback-exempt;
   critical signals (requires_review, conflict_detected) must define fallback.  
   **Important:** fallback-exempt does not mean no consequence. An informational
   signal must still define its visibility projection. Exempt means no time-based
   or state-transition fallback is required — not that the signal may be silent.
   **Visibility is the minimum consequence. No signal may be consequence-free.**

4. **Signal conflicts** — How do we prevent two signals with conflicting visibility
   or fallback definitions from producing inconsistent downstream state?
   (e.g. `low_trust` + `conflict_flagged` on the same lead)

---

## Phase Gate — Signal Enforcement (Phase 3B)

Any signal that does not define all three of the following:

- `action` — immediate system behavior
- `visibility` — downstream-facing projection that cannot be ignored
- `fallback` — defined consequence if no reaction occurs

is considered **invalid within Phase 3B** and must not be emitted.

This is a normative gate, not a guideline. It applies to every signal introduced
in Phase 3B without exception.

> **If a signal has no operational consequence, it is not part of Phase 3B.**

This gate is enforced by a contract test (`test_signal_contract.py`) that must
pass before any 3B signal definition is considered complete. The test is the
executable form of this invariant — the ADR is the source of truth,
the test ensures it cannot be violated in code.

---

## Related

- ADR-002 — Degraded mode policy separation
- ADR-003 — Bloom anti-corruption layer  
- ADR-005 — Dual fatal reject enforcement
- ADR-006 — Per-tenant concurrency isolation
- ADR-007 — Tenant identity from auth only
