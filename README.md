# Lead Entry Guard

![Python](https://img.shields.io/badge/python-3.12-blue)
![Tests](https://img.shields.io/badge/tests-passing-brightgreen)
![Benchmark](https://img.shields.io/badge/benchmark-100k%20leads-blue)

Deterministic, privacy-safe, tenant-aware ingestion gateway for CRM and marketing pipelines.

Lead Entry Guard is designed as a protective ingestion gateway placed in front
of CRM systems to prevent bad data, duplicate storms and pipeline instability.

---

## Why Lead Entry Guard

Most CRM problems don't start in the CRM.

They start at ingestion.

Duplicate submissions, malformed phones, retries and partial payloads
slowly poison downstream systems.

Lead Entry Guard acts as a deterministic ingestion gateway that protects
CRM pipelines before bad data can enter the system.

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
# development + tests
pip install -e ".[dev]"

# load tests
pip install -e ".[dev]" && pip install aiohttp
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

## Performance snapshot

Lead Entry Guard was tested against **100,000 messy leads** simulating real CRM ingestion conditions:

- duplicate storms
- malformed phone numbers
- retries and partial payloads
- inconsistent formatting

**Results**

- Throughput: **~1,300 leads/sec**
- Latency p50: **0.71 ms**
- Latency p95: **0.97 ms**
- Latency p99: **1.19 ms**
- Strict accuracy (clean / broken / exact-duplicate): **100%**

The benchmark intentionally prioritizes **false-positive safety** —
valid leads should not be blocked during ingestion.

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
