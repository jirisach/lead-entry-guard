# ADR-005 — Dual Fatal Reject Enforcement

**Status:** Accepted  
**Date:** 2026-03-14  
**Owners:** core team

---

## Context

The pipeline has two places that can reject a lead on fatal validation errors:

1. **`IngestionPipeline.process()`** — early reject after `RecoverabilityLayer.assess()`, before fingerprint build and Redis lookup.
2. **`PolicyEngine` / `RejectOnFatalValidationError` rule** — fires when `PolicyContext` contains fatal errors.

At first glance this looks like duplication. It is intentional.

---

## Decision

Both enforcement points are kept. They serve different responsibilities:

### Pipeline early reject (Layer 1)

```python
if assessment.fatal_errors:
    return self._finalize(lead, DecisionClass.REJECT, ...)
```

**Purpose:** performance optimization — saves a Redis round-trip for clearly invalid inputs (invalid email, missing required fields). These leads will never pass regardless of duplicate lookup result, so there is no reason to hit Redis.

**Scope:** only fires inside `IngestionPipeline.process()`.

### Engine `RejectOnFatalValidationError` (Layer 2)

```python
class RejectOnFatalValidationError:
    def evaluate(self, ctx: PolicyContext) -> ...:
        if ctx.recoverability and ctx.recoverability.fatal_errors:
            ...
```

**Purpose:** correctness guarantee — ensures fatal errors are always rejected even if `PolicyContext` is assembled outside the standard pipeline path (tests, shadow mode, future tooling).

**Scope:** fires for any caller of `PolicyEngine.decide()`.

---

## Consequences

- Fatal leads are rejected in O(1) without Redis I/O — no performance regression.
- `PolicyEngine` remains correct as a standalone component regardless of how `PolicyContext` is assembled.
- If the early reject is ever removed, the engine fallback still holds. If the engine rule is ever removed, the pipeline still rejects early.
- Maintainers must keep both paths in sync when adding new fatal error types. Adding a new `ReasonCode` to `_FATAL_REASON_CODES` in `recoverability.py` is sufficient — both paths derive from `assessment.fatal_errors`.

---

## Invariants

These invariants must hold at all times and are enforced by both layers:

| Condition | Expected decision |
|---|---|
| `assessment.fatal_errors` non-empty | `REJECT` — regardless of `SalvagePolicy` |
| `assessment.fatal_errors` empty, `recoverable_errors` non-empty, `STRICT` | `REJECT` |
| `assessment.fatal_errors` empty, `recoverable_errors` non-empty, `SALVAGE` | `WARN` |
| `assessment.fatal_errors` empty, `recoverable_errors` empty | `PASS` (or `DUPLICATE_HINT` / `WARN` from other rules) |

The key invariant in one line:

```
fatal_errors != [] → decision MUST be REJECT, regardless of SalvagePolicy or duplicate signal
```

Verified by: `test_salvage_layer.py::test_invalid_email_always_rejected_salvage`


**Single enforcement in engine only** — rejected. Would add a Redis round-trip for every invalid email, which is the most common rejection reason in practice (messy data imports).

**Single enforcement in pipeline only** — rejected. Would make `PolicyEngine` incorrect when used standalone, breaking test isolation and shadow mode.
