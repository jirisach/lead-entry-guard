# ReviewEvent v1 — Design Note

**Status:** Proposed  
**Date:** 2026-04  
**Author:** jirisach  
**Related:** ADR-008, ADR-011, signal-combination-hypothesis.md  
**Prerequisite for:** ADR-012 (ReviewEvent endpoint contract)

---

## Context

The compound signal layer (`compound_signal_alignment`) routes leads for review
when multiple signals co-occur. The routing fires — but currently nothing captures
what happens next.

This is the confirmed failure mode from design partner validation (Design Partner A, April 2026):

> "When reviews piled up and decisions weren't visible, reps stopped trusting
> the routing and started working around it."

Without outcome capture, compound signals stay in a permanently "noticed but
not acted on" state. The feedback loop that would validate or retire compound
patterns never closes.

This design note specifies the minimal ReviewEvent contract that closes that loop.

---

## What this is NOT

- Not a workflow engine
- Not a UI specification
- Not a CRM integration
- Not a scoring input
- Not a change to the core decision pipeline

ReviewEvent is a lightweight capture layer. It records what happened after routing.
It does not influence what happens during routing.

---

## Operational model (validated)

Design Partner A (April 2026) confirmed the operational pattern:

> "It usually started informal but needed some lightweight structure pretty quickly.
> The reviewer would accept, reassign, or reject — the important part was just
> capturing a simple reason."

> "The setups that worked best kept it lightweight — just outcome and optional
> reason — and over time that started showing patterns."

Ownership model (confirmed same conversation):

- **RevOps** owns the structure and which patterns become rules
- **SDR leadership** acts on signals in real time
- Handoff between the two is where it breaks — not the signals themselves

---

## ReviewEvent v1 schema

```python
@dataclass
class ReviewEvent:
    """
    Lightweight outcome capture for compound-routed leads.

    fingerprint_id: join key back to the original decision event.
                    Deterministic — same as used in ingest pipeline.
                    Never a lead_id or PII.

    action:         What the reviewer actually did.
                    Exactly three values — no extensions in v1.

    reason:         Optional short text from reviewer.
                    Free-form, not parsed, not structured.
                    Max 200 chars. Never PII.

    actor:          Role or identifier of reviewer.
                    Optional — may be system-assigned from auth context.
                    Never raw user PII.

    timestamp:      When the review action was taken.

    expires_at:     When this review opportunity expires.
                    Set at routing time: routing_time + 24h (configurable per tenant).
                    After expiry → auto-resolve to ReviewOutcome.EXPIRED_NO_REVIEW.
    """
    fingerprint_id: str
    action: ReviewAction
    reason: str | None                # max 200 chars, no PII
    actor: str | None                 # role or system id, no raw PII
    timestamp: datetime
    expires_at: datetime


class ReviewAction(str, Enum):
    ACCEPT = "accept"
    REJECT = "reject"
    REASSIGN = "reassign"
    EXPIRED_NO_REVIEW = "expired_no_review"   # system-set, not human-set
```

---

## Invariants

**1. `expired_no_review` is system-only**

A human cannot submit `action=expired_no_review` via the API.
It is set automatically by the expiry resolution job.
It must never be mixed with human decisions in analytics.

**2. `fingerprint_id` is the only join key**

No `lead_id`, no `request_id` as primary join.
`fingerprint_id` is deterministic and available independently on both sides
of the join (ingest decision + review outcome).

**3. No PII in any field**

`reason` is free text but must not contain email, phone, name, or company.
This is a policy constraint, not a technical one — enforcement is operational.
`actor` is a role or system identifier, never a raw user record.

**4. Reviews must be visible**

A routed lead with no visible review status is a system failure, not a
missing feature. Visibility is a core invariant, not a nice-to-have.

**5. Reviews must not pile up indefinitely**

`expires_at` is set at routing time. After expiry, the system resolves
the event automatically. A stale review queue breaks rep trust.
(Confirmed failure mode — Design Partner A, April 2026.)

---

## Lifecycle

```
compound_signal fires
    │
    ▼
lead routed for review
    │
    ├── expires_at = now + 24h (configurable)
    │
    ▼
reviewer acts (within expires_at)
    │
    ├── action = accept | reject | reassign
    ├── reason = optional short text
    │
    ▼
ReviewEvent stored
    │
    ▼
pattern metrics updated
    │
    ├── if compound consistently → reject/reassign:
    │       candidate for formal rule
    │
    └── if compound consistently → accept:
            candidate for retirement (false positive pattern)

OR

reviewer does NOT act (expires_at reached)
    │
    ▼
auto-resolve: action = expired_no_review
    │
    ▼
ReviewEvent stored (system actor)
    │
    ▼
alert if expired_no_review rate exceeds threshold
    (signals routing is broken or queue is too large)
```

---

## Auto-resolve behavior

`expired_no_review` is an explicit outcome, not a default accept or reject.

Rationale: mixing expired events with human decisions would corrupt pattern
analytics. A lead that expired without review tells you something different
than a lead that was explicitly accepted.

Alert threshold for expired_no_review rate is tenant-configurable.
Default: alert if > 20% of routed leads expire without review in a 24h window.

---

## Capture mechanism v1

**`POST /v1/review-events`**

Not a UI. Not a webhook sink (in v1).
A clean API endpoint — easiest to contract-test and integrate.

```
POST /v1/review-events
X-API-Key: <tenant api key>

{
  "fingerprint_id": "string",
  "action":         "accept | reject | reassign",
  "reason":         "string | null",
  "actor":          "string | null"
}
```

Response:

```json
{
  "review_id":      "uuid",
  "fingerprint_id": "string",
  "action":         "accept | reject | reassign",
  "recorded_at":    "ISO8601"
}
```

`expires_at` is not in the request — it is set server-side at routing time.
The caller cannot extend or override expiry in v1.

---

## Visibility v1

Minimum viable visibility — no UI required:

```
structured log per ReviewEvent:
{
  "fingerprint_id":   "...",
  "action":           "accept | reject | reassign | expired_no_review",
  "compound_code":    "compound_signal_alignment",
  "triggered_by":     ["low_trust_domain", "source_conflict_..."],
  "actor":            "sdr_manager | system",
  "timestamp":        "...",
  "expired":          false
}
```

Export: append-only log, readable by RevOps tooling.
No real-time dashboard in v1.

---

## Pattern validation path

```
ReviewEvent outcomes over time
    │
    ▼
Aggregate per compound_code:
    - accept rate
    - reject rate
    - reassign rate
    - expired_no_review rate
    - conversion rate vs baseline (external join)
    │
    ▼
Thresholds (intentionally open — depend on volume and ICP):
    - min_samples: N fires before evaluation
    - min_time_window: Y days of observation
    - metric_delta: Z% conversion delta vs baseline
    │
    ├── pattern consistently underperforms → rule candidate
    ├── pattern = noise (baseline conversion) → retire compound combination
    └── expired rate too high → routing is broken → fix ownership first
```

---

## What is explicitly deferred

- UI for review queue
- Webhook-based capture (post v1)
- Tenant-configurable expiry UI
- Multi-reviewer consensus model
- Automated rule promotion (human decision required in v1)
- Integration with CRM write-back

---

## Implementation sequence

1. This design note reviewed and approved
2. `ReviewAction` enum + `ReviewEvent` dataclass in `core/models.py`
3. Storage model (append-only, indexed by `fingerprint_id`)
4. Expiry resolution job (background, not in request path)
5. `POST /v1/review-events` endpoint contract + auth
6. Tests:
   - contract: valid actions accepted, `expired_no_review` rejected from human callers
   - expiry: auto-resolve fires correctly after TTL
   - join: ReviewEvent links back to ingest decision via fingerprint
   - visibility: log output matches schema
7. Structured log visibility
8. ADR-012 written after implementation is stable

---

## Conditions to write ADR-012

- ReviewEvent endpoint implemented and tested
- Expiry logic confirmed working in at least one controlled scenario
- At least one compound pattern observed through full lifecycle
  (routed → reviewed → outcome recorded)
- Visibility confirmed readable by RevOps (not just engineers)

ADR-012 documents the decision after the implementation is validated.
Not before.

---

## Design partner validation summary

| Source | Key insight |
|---|---|
| Design Partner A (April 2026) | Reviews piling up breaks rep trust — expiry is critical |
| Design Partner A (April 2026) | Lightweight outcome + optional reason is sufficient |
| Design Partner A (April 2026) | Feedback shows up in behavior first, not structured input |
| Design Partner A (April 2026) | RevOps owns structure, SDR acts — handoff is the failure point |
| Design Partner A (April 2026) | expired_ratio is a health signal for the process, not just lead quality |
| Design Partner A (April 2026) | When expired_ratio rises, the workflow exists but isn't being acted on |
| Design Partner A (April 2026) | Escalation on threshold crossing + fallback ownership = v2 layer |

---

## Framing correction (April 2026)

Original framing of `expired_ratio`:
> "Alert indicator — signals broken routing or queue overload."

Corrected framing (Design Partner A, April 2026):
> "expired_ratio is a health signal for the process itself, not just lead quality.
> When it starts rising, it's often a sign the workflow exists but isn't being acted on."

This is a more precise framing. `expired_ratio` is not primarily a failure alert —
it is an operational metric that tells you whether the review process is functioning.
A rising ratio means ownership is unclear or the queue is too large to act on.

Implementation note: the 20% alert threshold in `ReviewEventStore.expired_ratio()`
and `ReviewExpiryJob._check_alert_thresholds()` is correct, but the log message
should be updated to reflect this framing:

```python
# Current (too narrow):
"action": "check routing ownership and queue size"

# Better:
"action": "expired_ratio is a process health signal — check ownership assignment and queue volume"
```

---

## Deferred to v2 (from design partner feedback)

Design Partner A identified a natural next layer that was not in v1 scope:

- **Threshold-based escalation**: when `expired_ratio` crosses a threshold,
  automatically escalate to a fallback owner rather than just alerting.
- **Fallback ownership**: define a default owner at tenant level so reviews
  never sit in a queue with no assigned owner.

These are not in v1. They should be considered for v2 after the v1 lifecycle
is validated in at least one real scenario. Do not implement before then.
