# ADR-009 — Signal Layer Evolution and Context Unification

**Status:** Accepted  
**Date:** 2026-03-31  
**Owners:** core team

---

## Context

Phase 3B introduced the first signal definitions (A3, A4, A6) as isolated rules
with inconsistent input contracts:

- `A3SignalRule` and `A6SignalRule` took a raw `email: str | None`
- `A4SignalRule` took a custom `LeadSourceContext` dataclass defined inside `signal_a4.py`
- `SignalEvaluator` bridged the difference by extracting `context.email` before calling A3 and A6

This created a fragmented rule interface that would not scale as more signals were added.

Additionally, field feedback (RevOps / BI specialist, March 2026) identified a gap
in the A3 domain trust signal:

> "Even for borderline .com domains, I'd still expect a lightweight signal to surface
> softer trust issues without triggering a full review."

The existing A3 signal had only one output level — `suspicious_domain` → `needs_review`.
There was no intermediate layer between "fully clean" and "needs review".

---

## Decision

### 1. Unified input contract — `LeadSignalContext`

Introduced `LeadSignalContext` in `core/signal_models.py` as the single input type
for all signal rules:

```python
@dataclass
class LeadSignalContext:
    tenant_id: str
    email: str | None = None
    fields: list[FieldSourceRecord] = field(default_factory=list)
```

Every rule now has the same signature:

```python
def evaluate(self, context: LeadSignalContext) -> list[SignalResult]: ...
```

Each rule extracts what it needs:
- A3, A6: `context.email`
- A4: `context.fields`

`FieldSourceRecord` moved from `signal_a4.py` to `signal_models.py` —
it is now part of the shared signal contract, not an A4 implementation detail.

### 2. Separated `SignalEvaluator`

`SignalEvaluator` extracted from `signal_a4.py` into its own file
`policies/signal_evaluator.py`. It now calls all rules with the same contract:

```python
signals.extend(self._a3.evaluate(context))
signals.extend(self._a4.evaluate(context))
signals.extend(self._a6.evaluate(context))
```

### 3. Two-layer A3 domain trust detection

A3 extended with a soft signal layer based on field feedback (March 2026).

**Hard A3 — `suspicious_domain` (unchanged)**
- Fires on known high-abuse TLDs (.xyz, .ml, .cf, .tk etc.)
- Action: `ACCEPT_WITH_FLAG`
- CRM status: `needs_review`

**Soft A3 — `low_trust_domain` (new)**
- Fires on structural domain risk patterns for common TLDs (.com, .net etc.)
- Action: `ACCEPT_LOW_QUALITY`
- CRM status: `low_trust_lead`

Soft detection heuristics (Phase 3B — two rules only, intentionally minimal):

```python
# 1. More than one hyphen in label before TLD
#    newco-mail-online.com → True
# 2. Label longer than 20 characters
#    verylongsyntheticdomain.com → True
```

Mutual exclusion enforced in `has_soft_domain_risk()` — if hard fires, soft does not evaluate.

**Intentionally excluded from soft detection:**
- Numeric patterns (too common in B2B — high false positive rate: b2b.com, api2crm.com)
- DNS/MX lookup
- Reputation APIs
- Short domain heuristics (deferred to Phase 3C)

---

## Invariants

**Hard A3 takes priority.**
If `has_suspicious_tld()` returns True, `has_soft_domain_risk()` never evaluates.
Exactly one domain trust signal may be emitted per lead — never both.

**Soft signal is not noise. It must carry a consequence.**
A soft signal must still define visibility (ADR-008 minimum consequence invariant).
`low_trust_domain` carries `crm_status`, `routing_tags`, and `api_flags` — it is
not a silent annotation.

**Heuristics over ML.**
Soft detection uses deterministic rules, not scoring or ML. This is a conscious
decision: the system must remain explainable and auditable. If a signal fires,
there must be a clear, statable reason.

---

## Consequences

**Positive:**
- Signal layer can evolve (new signals, new heuristics) without touching decision layer
- Unified `LeadSignalContext` makes adding new rules trivial — one interface, no bridging
- Two-level trust signal matches real-world operator expectation (confirmed by field feedback)
- `DecisionResultV2` carries signals as first-class output — downstream systems see consequences

**Trade-offs:**
- More signals require governance — each new signal must pass Phase Gate (ADR-008 contract test)
- Heuristic-based detection requires periodic review as domain patterns evolve
- Soft signal threshold (20 chars, >1 hyphen) is conservative by design — some borderline
  domains will not be flagged. This is intentional: false negatives are preferable to
  false positives for a soft trust signal.

---

## Validation

Field feedback (RevOps / BI specialist, March 2026) confirmed:
- A4 source conflict behavior: correct
- A6 shared inbox behavior: correct  
- A3 gap identified: soft trust layer needed between PASS and needs_review

Post-implementation walkthrough via `run_scenario.py` confirmed all scenarios
match expected output including the new soft A3 layer.

Test baseline: 247 passed after full refactor and A3 extension.
No regressions introduced during refactor.

---

## Related

- ADR-008 — Signals must be actionable (Phase Gate, PII invariant)
- ADR-002 — Degraded mode policy separation
- ADR-007 — Tenant identity from auth only
- `phase-3b-backlog.md` — implementation sequence and scope guards
