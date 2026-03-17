# Lead Entry Guard

![Python](https://img.shields.io/badge/python-3.12-blue)
![Tests](https://img.shields.io/badge/tests-passing-brightgreen)
![Benchmark](https://img.shields.io/badge/benchmark-100k%20leads-blue)

Deterministic, privacy-safe, tenant-aware ingestion gateway for CRM and marketing pipelines.

Lead Entry Guard is designed as a protective ingestion gateway placed in front
of CRM systems to prevent bad data, duplicate storms and pipeline instability.

**Protects against:**
- duplicate storms and webhook retry floods
- malformed phone numbers and inconsistent lead formats
- partial payloads and missing required fields
- CRM ingestion instability under high concurrency

---

## Why Lead Entry Guard

Most CRM problems don't start in the CRM.

They start at ingestion.

Duplicate submissions, malformed phones, retries and partial payloads
slowly poison downstream systems.

Lead Entry Guard acts as a deterministic ingestion gateway that protects
CRM pipelines before bad data can enter the system.

```
Problem                      Protection
─────────────────────────────────────────────────────
Retry storms                 →  Idempotency layer
  (webhook retries,               same source_id = same result
   API gateway floods,            no duplicate downstream writes
   double-click imports)

Duplicate leads              →  Bloom + Redis detection
  (re-uploads, CRM sync,          HMAC fingerprint per tenant
   data broker imports)           deterministic identity signal

Data quality issues          →  Validation + SalvagePolicy
  (invalid phones,                fatal errors → REJECT
   malformed emails,              recoverable errors → WARN or REJECT
   partial payloads)              per-tenant policy (STRICT / SALVAGE)
─────────────────────────────────────────────────────
```

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
# Clone
git clone https://github.com/jirisach/lead-entry-guard
cd lead-entry-guard

# Create virtual environment
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
.venv\Scripts\activate           # Windows

# Install — development + tests + benchmarks
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
# Unit + Integration + Resilience
pytest tests/unit tests/integration tests/resilience -v

# Chaos tests
pytest tests/chaos -v
```

---

## Load tests & benchmarks

```bash
# Hero benchmark — 100k messy leads
python load_tests/hero_benchmark.py

# Failure scenarios
python load_tests/scale_scenarios.py --scenario all --output

# Generate report
python load_tests/generate_report.py
```

See `docs/testing/` for full benchmark documentation.

### Benchmark baseline — 100,000 messy leads

| Metric | Value |
|---|---|
| Throughput | ~1,310–1,387 records/s |
| Latency p50 | 0.71 ms |
| Latency p95 | 0.97 ms |
| Latency p99 | 1.19 ms |
| Strict accuracy (clean / broken / exact-duplicate) | **100%** |
| Strict false positives | **0** |

Lead Entry Guard prioritizes false-positive safety. In ambiguous cases the
system prefers PASS over REJECT to ensure valid leads are not blocked.

See `docs/testing/benchmark_100k_baseline.md` for full results and methodology.

---

## Reliability testing

Lead Entry Guard includes a comprehensive reliability test suite covering correctness, resilience, and long-running stability:

| Layer | Tests | What it covers |
|---|---|---|
| Unit | 39 | Normalization, fingerprint determinism, policy rules, salvage layer |
| Integration | 32 | End-to-end pipeline flow, idempotency, tenant isolation, replay suite |
| Resilience | 13 | Redis failures, Bloom failures, slow downstream, degraded modes |
| Chaos | 9 | Multi-component failure, HMAC race conditions, reconciliation spikes |
| Load | 6 | Retry storms (300 concurrent), ingestion burst (1,000 leads), jitter storm |
| **Total** | **~99** | |

Key reliability properties validated:

- **Determinism** — same input always produces same decision, regardless of concurrency
- **Idempotency** — same `source_id` always returns same decision on replay
- **Tenant isolation** — fingerprint namespaces and decisions are fully scoped per tenant
- **Graceful degradation** — Redis down, Bloom down, slow downstream all handled without crash
- **Retry storm safety** — 300 concurrent retries of same lead produce identical outcome

Soak tests validate stability over time: memory growth, throughput drift, and telemetry backlog are monitored across multi-minute runs.

See `docs/testing/` for full test inventory and benchmark methodology.

---

## Example decision

A lead with a valid email but an invalid phone number, under a SALVAGE tenant policy:

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

---

## Degraded modes

| Policy | Description |
|---|---|
| `ACCEPT_WITH_FLAG` | Lead continues with `duplicate_check_skipped=true` |
| `REJECT` | Request rejected for high-risk tenants |
| `QUEUE` | Wait up to 15 minutes for Redis recovery, then fallback policy |

---

## Configuration

All runtime parameters are configured via environment variables:

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
 ├─ normalization/  # email / phone normalization
 ├─ validation/     # input validation rules
 ├─ fingerprint/    # HMAC identity builder
 ├─ lookup/         # Bloom + Redis duplicate detection
 ├─ policies/       # decision engine
 ├─ telemetry/      # async metrics exporter
 ├─ reconciliation/ # recovery / consistency loops
 └─ security/       # key handling / vault integration

tests/
 ├─ unit/
 ├─ integration/
 ├─ resilience/
 └─ chaos/

load_tests/
 ├─ hero_benchmark.py
 ├─ scale_scenarios.py
 └─ generate_report.py

synthetic_data/
 ├─ generator/
 └─ analyze_benchmark_accuracy.py
```

---

## Non-goals

Lead Entry Guard intentionally does **not** attempt to:

- store or process raw PII beyond the ingestion boundary
- replace CRM systems or marketing automation platforms
- perform heavy enrichment or external data lookups during ingestion
- guarantee cross-system deduplication outside the configured identity signals

The system focuses strictly on **deterministic ingestion protection** and
**data quality enforcement at the pipeline boundary**.
