# Context Signals v1 — Planning Note

**Status:** Implemented, validating  
**Date:** April 2026  
**Related:** ADR-008, ADR-009, `signal_c1.py`, `test_context_signals.py`

---

## Why this exists

The signal layer (A3, A4, A6) detects specific data observations — suspicious domains, source conflicts, shared inbox prefixes. These are useful but incomplete.

What was missing: a way to express whether the system has enough context to make a reliable decision at entry — regardless of whether the individual fields look valid.

This insight came from two independent design partner conversations:

**Design Partner A** (RevOps, enterprise CRM): described how teams initially treat repeated patterns as operational noise, but the moment those patterns affect forecast accuracy or pipeline reporting, the conversation shifts from data quality to a revenue problem. They also articulated that early signals — stage jumps, missing next steps, stalled deals — show up before reporting and indicate that the decision framework wasn't clear at entry.

**Design Partner B** (HubSpot Solutions Architect): named the three categories explicitly — missing fields, conflicting inputs, and false clarity where data looks complete but still does not provide enough decision context. They also confirmed: "the real issue is not just bad data — it is the absence of a structured decision layer around uncertain data."

That last sentence is the product insight behind C-series signals.

---

## Data signals vs context signals

**A-series (data signals):** concrete observations about field values and sources.  
What did the system see? What was present, missing, or conflicting at the field level?

**C-series (context signals):** conclusions about decision readiness.  
Does the system have enough context to make a reliable decision at entry?

Both families may fire on the same lead. That is correct and intentional.

Examples of valid co-occurrence:
- A6 (shared_inbox) + C3 (false_clarity): A6 observes the email prefix; C3 concludes that combined with no company and no enrichment, the system cannot determine who this lead represents.
- A4 (source_conflict_manual_vs_enrichment) + C2 (conflicting_context): A4 observes a field-level conflict; C2 concludes that the conflict degrades routing decision readiness.

The `signal_family` field (`"data"` | `"context"`) makes this distinction explicit in the model. It is not a documentation convention — it is architectural metadata that downstream systems can use to filter, group, and render signals differently.

> Context signals do not only describe the input. They describe whether the system has enough context to make a reliable decision at entry.

---

## Current v1 scope

### C1 — missing_context
**Fires when:** no usable identity anchor — no usable email AND no usable phone.  
**Usable:** present and not a placeholder value (`"none"`, `"n/a"`, `"unknown"`, etc.).  
**Action:** ROUTE_FOR_REVIEW  
**CRM status:** `incomplete_lead`  
**Fallback:** AUTO_EXPIRE_REVIEW after 24h → `lead_expired_no_identity_anchor`

Note: company is NOT part of the identity anchor definition. Company is business context, not identity. C1 fires only when the system cannot determine who the lead is, not when business detail is thin.

Note: `None` phone and `"none"` phone are handled differently by design. `None` = missing (no data provided). `"none"` = placeholder (data provided but semantically empty). Both result in C1 firing, but through different code paths. This semantic boundary is explicit and tested.

### C2 — conflicting_context
**Fires when:** routing-relevant fields (company, phone) have contradicting values from different trusted sources after normalization.  
**V1 scope:** company and phone only. Email excluded — aliases, case variance, and secondary emails create too much noise to be reliably treated as a routing decision conflict.  
**Action:** ROUTE_FOR_REVIEW  
**CRM status:** `conflicting_data`  
**Fallback:** AUTO_EXPIRE_REVIEW after 24h → `lead_expired_routing_conflict_unresolved`

Distinction from A4: A4 fires on any manual-vs-enrichment mismatch on any field. C2 fires when a cross-source conflict on routing-relevant fields degrades the system's ability to make a safe routing decision. Both may fire simultaneously — they are independent layers.

### C3 — false_clarity
**Fires when:** email is present and usable, email prefix is a shared inbox pattern, no usable company field, and no enrichment source records present.  
**Action:** ACCEPT_WITH_FLAG (not blocked — flagged)  
**CRM status:** `low_confidence_decision`  
**Fallback:** KEEP_ACCEPTED_LOW_TRUST → `lead_accepted_with_low_decision_confidence`

Rationale: the data looks complete. The decision context is not. This is not a data quality issue. It is a representation ambiguity. The system cannot determine who this lead represents, even though fields are technically present. Routing it for hard review is too aggressive — flagging it as low confidence is the right call.

### signal_family
Added to `SignalDefinition` and `SignalResult` as `Literal["data", "context"]`. No default — omitting `signal_family` is a construction error. This was intentional: the guardrail must fail loudly on incomplete definitions.

Downstream implication: data signals can be used for data hygiene and enrichment workflows, while context signals should influence routing confidence and review decisions.

Convenience methods added to `DecisionResultV2`: `has_context_signal()` and `has_data_signal()`.

### Suppression rules
- C1 and C2 are evaluated independently. Both may fire simultaneously (rare by design — C2 requires populated field records, which C1 cases typically lack). Downstream systems must not assume a single context signal per lead.
- C3 only fires if both C1 and C2 return clean. C3 presupposes usable identity; C1/C2 supersede it.

---

## Explicit non-goals — deferred

The following were considered and explicitly deferred:

- **Standalone context classifier** — C-series lives inside the signal layer for now. Promotion to a separate pre-pipeline context evaluation step requires validation that context signals drive meaningfully different decisions than data signals alone.
- **Signal scoring or confidence weighting** — deterministic rules only in v1. No ML, no numeric confidence, no threshold tuning.
- **Auto-promotion of patterns to rules** — C-series detects and flags. It does not decide when a pattern becomes a rule. That remains a human decision.
- **Context summary in API response** — `signal_family` is available in `SignalResult` but no context-specific summary field is exposed in v1. Deferred until at least one design partner validates the signal output format.
- **New ADR** — C-series is an additive change within the existing signal layer contract (ADR-008, ADR-009). A new ADR is appropriate only if C-series is promoted to a separate architectural layer or if the API response contract changes to expose context signals explicitly.

---

## Promotion criteria

The following must be true before C-series warrants a standalone architectural layer or new ADR:

1. At least one design partner confirms that C-series signals surfaced something actionable that A-series signals missed.
2. `expired_ratio` for C1/C2-routed leads is tracked and shows a meaningful pattern (not just noise).
3. At least one concrete case where C3 (false_clarity) flagged a lead that would have otherwise passed cleanly through the pipeline and caused downstream friction.

Until then: C-series stays in the signal layer, feature-flagged by the evaluator, additive to existing decisions.

---

## Open questions

- **API response exposure:** should context signals appear in the ingest response body, or remain internal? Currently internal. Requires design partner validation before any exposure decision.
- **Context summary field:** would a single `context_quality` field (e.g. `"ok"` | `"incomplete"` | `"conflicting"` | `"low_confidence"`) be more useful downstream than individual signal codes? Open until validated.
- **C2 field scope expansion:** email is excluded from C2 v1. If a reliable normalization strategy for email aliases emerges, email conflict detection could be added. Needs a concrete use case first.
- **C3 heuristic generalization:** C3 currently fires on shared inbox prefix + no company + no enrichment. False clarity is a broader concept — there may be other patterns that qualify. Keeping the definition narrow until a second pattern is validated in practice.
- **C1 + C2 simultaneous handling in downstream systems:** both may fire on the same lead. CRM and routing layers should be tested to confirm they handle multiple context signals gracefully, not just the first one.
