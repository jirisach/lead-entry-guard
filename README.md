# Lead Entry Guard

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![CI](https://github.com/jirisach/lead-entry-guard/actions/workflows/ci.yml/badge.svg)
![Benchmark](https://img.shields.io/badge/benchmark-100k%20leads-blue)

Decide what becomes data before it enters your CRM.

Lead Entry Guard is a signal-aware ingestion layer placed in front of CRM systems.
It detects patterns at entry, evaluates their frequency and downstream impact, and turns only the ones that consistently create friction into enforceable decisions.

---

## Why Lead Entry Guard

Most CRM problems don't start in the CRM.

They start at ingestion.

Duplicate submissions, conflicting sources, shared inboxes, and malformed inputs
slowly poison downstream systems. By the time the problem shows up in routing or
reporting, it's already hard to fix cleanly.

Most systems react after data enters. Lead Entry Guard shifts this earlier.

Instead of defining rules upfront or relying on manual review, it observes what
happens at entry: which patterns repeat, which ones create friction, and which ones
impact routing or reporting. Only then are those patterns turned into enforceable
decisions.

```
Problem                         Protection
──────────────────────────────────────────────────────────────
Retry storms                    →  Idempotency layer
  (webhook retries,                  same source_id = same result
   API gateway floods,               no duplicate downstream writes
   double-click imports)

Duplicate leads                 →  Bloom + Redis detection
  (re-uploads, CRM sync,             HMAC fingerprint per tenant
   data broker imports)              deterministic identity signal

Data quality issues             →  Validation + SalvagePolicy
  (invalid phones,                   fatal errors → REJECT
   malformed emails,                 recoverable errors → WARN or REJECT
   partial payloads)                 per-tenant policy (STRICT / SALVAGE)

Ambiguous signals               →  Signal layer (A3, A4, A6)
  (low trust domains,                each signal has action + visibility
   shared inboxes,                   + fallback — no silent annotations
   source conflicts)

Co-occurring signals            →  Compound signal evaluation
  (individually OK,                  pattern fires when signals align
   together suspicious)              route_for_review, not hard block

Review outcomes                 →  ReviewEvent capture
  (who acted, what happened,         expired_ratio tracks process health
   what expired without action)      not just data quality
──────────────────────────────────────────────────────────────
```

---

## How LEG evolves decisions

LEG does not enforce everything immediately. It follows a progression:

**Signals**
Individual events at entry — duplicate patterns, source conflicts, domain trust
issues, shared inboxes. Each signal defines an action, a visibility projection,
and a fallback. No signal is a silent annotation.

**Compound signals**
When signals co-occur, the system evaluates combinations. A low trust domain
alone stays informational. Combined with a source conflict, it crosses into
something that requires a decision.

**Review outcomes**
When a compound signal fires, the lead is routed for review. The reviewer
decides: accept, reject, or reassign. The outcome is captured — including
whether the review expired without action.

**Pattern learning**
Expired reviews, repeated overrides, and recurring rejects surface as process
signals. If a pattern appears consistently and impacts routing or reporting,
it becomes a candidate for enforcement at entry.

**Prioritization**
Not all patterns are enforced. Patterns are evaluated based on frequency,
downstream impact, and whether they create measurable friction for routing,
reporting, or rep workflows.

Only patterns that consistently cause problems are promoted to candidate rules.

**Decision at entry**
Patterns that prove themselves become deterministic actions applied before
data enters the CRM. The CRM workflow layer shifts from primary defense to
exception handler.

---

## Quickstart

```bash
docker compose up
```

Send a lead:

```bash
curl -X POST http://localhost:8000/v1/leads/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "t1",
    "source_id": "demo-1",
    "email": "demo@example.com",
    "phone": "+12025550123"
  }'
```

Response:

```json
{
  "decision": "PASS",
  "reason_codes": [],
  "duplicate_check_skipped": false,
  "versions": {
    "policy_version": "v1",
    "ruleset_version": "v1",
    "config_version": "v1"
  }
}
```

Check health:

```bash
curl http://localhost:8000/ready
curl http://localhost:8000/health
```

---

## Signal Check API

Deterministic signal sandbox — evaluates domain trust, source conflict, and shared
inbox rules without auth, persistence, or side effects. Designed for demo, design
partner validation, and scenario testing.

```bash
curl -X POST http://localhost:8000/v1/leads/signal-check \
  -H "Content-Type: application/json" \
  -d '{
    "scenario_id": "demo-1",
    "email": "info@spammy.xyz",
    "fields": [
      {"field_name": "phone", "source_type": "manual", "value": "+420777111000"},
      {"field_name": "phone", "source_type": "enrichment", "value": "+420999888777"}
    ]
  }'
```

Response:

```json
{
  "request_id": "...",
  "scenario_id": "demo-1",
  "status": "flagged",
  "has_signals": true,
  "signal_count": 3,
  "signals": [
    {"code": "shared_inbox", "action": "accept_low_quality", "signal_class": "informational", ...},
    {"code": "source_conflict_manual_vs_enrichment", "action": "preserve_manual_value", "signal_class": "critical", ...},
    {"code": "suspicious_domain", "action": "accept_with_flag", "signal_class": "informational", ...}
  ],
  "latency_ms": 0.8
}
```

`status: "clean"` means no signals fired — not an error. Signals are sorted
alphabetically by code — same input always produces same response.

Rate limited: 30 requests / 60 seconds per IP.

---

## Architecture

```
Lead Input
     │
     ▼
Ingestion API
     │
     ▼
Normalization Layer
     │
     ▼
Validation Layer
     │
     ▼
Fingerprint Builder (HMAC identity signal)
     │
     ▼
Duplicate Lookup Tier
(Bloom → Redis → Decision)
     │
     ▼
Policy / Scoring Engine
(active + async shadow)
     │
     ▼
Signal Layer (A3, A4, A6)
(domain trust, source conflict, shared inbox)
     │
     ▼
Compound Signal Evaluation
(co-occurring signals → route_for_review)
     │
     ├─ Audit Metadata (safe only)
     │
     └─ Async Telemetry Queue
            ↓
        Telemetry Exporter
        + OOB Heartbeat (UDP)
```

---

## Installation

```bash
git clone https://github.com/jirisach/lead-entry-guard
cd lead-entry-guard

python -m venv .venv
source .venv/bin/activate        # Linux/macOS
.venv\Scripts\activate           # Windows

pip install -e ".[dev,benchmark]"
```

---

## Running

```bash
# local
uvicorn lead_entry_guard.api.app:app --reload

# docker
docker compose up
```

---

## Tests

```bash
# Fast — unit + contract tests (~seconds)
pytest -q -m "not integration"

# Integration — full app lifecycle
pytest -q -m "integration"

# Full — everything, use before release
pytest -q
```

| Profile | What runs | When to use |
|---|---|---|
| `fast` | unit + contract | every commit |
| `integration` | integration only | before merge if app layer changed |
| `full` | everything | before release |

Phase gate — must pass before every merge:

```bash
pytest -q tests/contract/
```

---

## Load tests & benchmarks

```bash
python load_tests/hero_benchmark.py
python load_tests/scale_scenarios.py --scenario all --output
python load_tests/generate_report.py
```

### Benchmark baseline — 100,000 messy leads

| Metric | Value |
|---|---|
| Throughput | ~1,310–1,387 records/s |
| Latency p50 | 0.71 ms |
| Latency p95 | 0.97 ms |
| Latency p99 | 1.19 ms |
| Strict accuracy | **100%** |
| Strict false positives | **0** |

Lead Entry Guard prioritizes false-positive safety. In ambiguous cases the
system prefers PASS over REJECT to ensure valid leads are not blocked.

---

## Reliability

| Layer | Tests | What it covers |
|---|---|---|
| Unit | 39 | Normalization, fingerprint determinism, policy rules, salvage layer |
| Integration | 32 | End-to-end pipeline flow, idempotency, tenant isolation, replay suite |
| Resilience | 13 | Redis failures, Bloom failures, slow downstream, degraded modes |
| Chaos | 9 | Multi-component failure, HMAC race conditions, reconciliation spikes |
| Load | 6 | Retry storms, ingestion burst, jitter storm |
| **Total** | **~99** | |

Key properties validated:

- **Determinism** — same input always produces same decision
- **Idempotency** — same `source_id` always returns same decision on replay
- **Tenant isolation** — fingerprint namespaces fully scoped per tenant
- **Graceful degradation** — Redis down, Bloom down, slow downstream all handled
- **Retry storm safety** — 300 concurrent retries produce identical outcome

---

## Example decision

```json
{
  "decision": "WARN",
  "reason_codes": ["WARN_INVALID_OPTIONAL_PHONE"],
  "duplicate_hint": null,
  "duplicate_check_skipped": false,
  "versions": {
    "policy_version": "v1",
    "ruleset_version": "v1",
    "config_version": "v1"
  }
}
```

Possible decisions: `PASS` · `WARN` · `REJECT` · `DUPLICATE_HINT`

---

## Core design principles

| # | Principle |
|---|---|
| 1 | Stateless-first request processing |
| 2 | No raw PII in logs |
| 3 | No fingerprint artifacts in telemetry |
| 4 | Deterministic decision engine with explicit versioning |
| 5 | Graceful degraded modes |
| 6 | Tenant isolation by design |
| 7 | Async side-effects must never block ingestion |
| 8 | Privacy-safe observability |
| 9 | Signals annotate decisions — they do not change them |
| 10 | Patterns prove themselves before becoming rules |

---

## Degraded modes

| Policy | Description |
|---|---|
| `ACCEPT_WITH_FLAG` | Lead continues with `duplicate_check_skipped=true` |
| `REJECT` | Request rejected for high-risk tenants |
| `QUEUE` | Wait up to 15 minutes for Redis recovery, then fallback policy |

---

## Configuration

```env
LEG_REDIS_URL=redis://localhost:6379/0
LEG_VAULT_URL=http://vault:8200
LEG_VAULT_TOKEN=<token>
LEG_DUPLICATE_TTL_SECONDS=2592000
LEG_IDEMPOTENCY_TTL_SECONDS=86400
```

---

## HMAC key security

- Keys stored only in Vault / KMS
- Never committed to git
- Dual-key rotation model with overlap window ≥ Redis TTL (30 days)
- Fingerprints never appear in logs or telemetry

---

## Architecture decisions

Major design decisions are documented in `docs/architecture/adr/`.

---

## Project structure

```
src/lead_entry_guard/
 ├─ api/            # FastAPI ingestion layer
 ├─ core/           # domain models, signal models, review events
 ├─ normalization/  # email / phone normalization
 ├─ validation/     # input validation rules
 ├─ fingerprint/    # HMAC identity builder
 ├─ lookup/         # Bloom + Redis duplicate detection
 ├─ policies/       # decision engine, signal rules, compound evaluator
 ├─ telemetry/      # async metrics exporter
 ├─ reconciliation/ # recovery / consistency loops
 └─ security/       # key handling / vault integration

tests/
 ├─ unit/
 ├─ integration/
 ├─ contract/       # boundary tests, signal parity, review event contract
 ├─ resilience/
 └─ chaos/

docs/
 ├─ architecture/adr/
 └─ planning/       # signal hypothesis, design notes
```

---

## Non-goals

Lead Entry Guard intentionally does not:

- store or process raw PII beyond the ingestion boundary
- replace CRM systems or marketing automation platforms
- perform heavy enrichment or external data lookups during ingestion
- guarantee cross-system deduplication outside the configured identity signals
- score leads or build probabilistic models
- learn automatically — pattern evolution requires explicit human confirmation

The system focuses on **deterministic ingestion protection** and
**signal-aware enforcement at the pipeline boundary**.
