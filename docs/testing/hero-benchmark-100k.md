# Hero benchmark — 100,000 messy leads

Testing a CRM ingestion pipeline against 100,000 messy leads.

Most lead ingestion demos look clean. Real pipelines look like this:

```
JOHN@example.com
john@example.com
john+ads@example.com

+420777777777
777 777 777
0042077777777

invalid_phone
```

The goal of this benchmark is not throughput. The goal is **deterministic behavior under:**

- duplicate storms (same lead arriving 30× from a misfiring webhook)
- retry loops (API gateway retrying a timed-out request 4×)
- malformed payloads (invalid emails, broken phones)
- missing fields (partial CSV imports)
- near-duplicate evaluation (same person, different email variant)

---

## Dataset composition

| Bucket | Count | Share | Guarantee |
|---|---|---|---|
| valid unique | 45,000 | 45% | `PASS` or `WARN` |
| duplicate storm | 25,000 | 25% | 1 `PASS` + N `DUPLICATE_HINT` per group |
| retry / idempotency | 10,000 | 10% | same `request_id` on every replay |
| invalid email | 5,000 | 5% | `REJECT` (validation) |
| broken phone | 5,000 | 5% | `PASS` or `WARN` (phone is optional) |
| missing fields | 5,000 | 5% | `PASS`, `WARN`, or `REJECT` by policy |
| near-duplicate eval | 5,000 | 5% | **exploratory — no hard assertion** |
| **total** | **100,000** | **100%** | |

---

## What is guaranteed

### Duplicate storm correctness

Duplicate storms are groups of 30 leads sharing the same email (same fingerprint).

Expected behavior:
```
lead group (30 leads, same email)
  → lead 1:    PASS          (first seen, stored in duplicate store)
  → lead 2–30: DUPLICATE_HINT (Bloom → Redis lookup → match found)
```

This works because the fingerprint is computed from `email_normalized` + `phone_normalized`
and stored in the Redis duplicate store after the first `PASS`.

Invariant:
```
all subsequent leads in each storm group == DUPLICATE_HINT
```

### Retry idempotency

API clients retry on timeout. The same `source_id` may arrive 4× in quick succession.

Expected behavior:
```
POST /lead  source_id=abc  →  PASS,  request_id=req-001
POST /lead  source_id=abc  →  PASS,  request_id=req-001  ← same
POST /lead  source_id=abc  →  PASS,  request_id=req-001  ← same
POST /lead  source_id=abc  →  PASS,  request_id=req-001  ← same
```

This is enforced by the idempotency snapshot store (ADR-004):
`key = tenant_id + source_id + request_hash`.

Invariant:
```
result.request_id == first_request_id for all replays of same source_id
```

### Validation rejection

Invalid emails are caught at the validation layer — before fingerprinting,
before Redis, before duplicate detection.

Expected behavior:
```
email = "not-an-email"  →  REJECT
email = ""              →  REJECT
email = "spaces @x.com" →  REJECT
```

Invariant: implicit — captured in decision breakdown, not hard-asserted
(policy may override to WARN depending on tenant config).

---

## What is exploratory

### Near-duplicate evaluation bucket

The near-duplicate bucket contains groups of 4 email variants for the same identity:

```
john@gmail.com
john.doe@gmail.com
j.doe@gmail.com
john_doe@gmail.com
```

These all share the same phone number, which may trigger duplicate detection
via the phone fingerprint field. The email fingerprint will NOT match because
the normalized values are different.

**This bucket is evaluated but not asserted.** Results are reported separately.

Why this matters for the future:
Exact/normalized duplicate detection is the current guaranteed capability.
Near-duplicate / fuzzy identity resolution requires a different model
(edit distance, ML-based entity resolution, or probabilistic matching).
That is a distinct product capability — not a v1 guarantee.

What to watch in results:
- `DUPLICATE_HINT` rate in this bucket via phone match (current capability)
- `PASS` rate (not detected — identity fragmentation that reaches CRM)
- Use as input for sizing the near-duplicate problem

---

## Running the benchmark

```bash
# Full run: generate → process → save results
python load_tests/hero_benchmark.py

# Generate dataset only (for inspection before running)
python load_tests/hero_benchmark.py --generate-only

# Run against existing dataset
python load_tests/hero_benchmark.py --run-only
```

Output files:
```
load_tests/datasets/
├── hero_100k.jsonl               ← dataset (one API payload per line)
└── hero_benchmark_results.json   ← full results with invariant outcomes
```

Then generate the report:
```bash
python load_tests/generate_report.py
```

---

## Expected outcomes (reference)

| Bucket | Expected dominant decision | Hard invariant |
|---|---|---|
| valid unique | `PASS` | — |
| duplicate storm | `DUPLICATE_HINT` (subsequent) | ✓ all subsequent = `DUPLICATE_HINT` |
| retry / idempotency | `PASS` | ✓ `request_id` identical on replay |
| invalid email | `REJECT` | — |
| broken phone | `PASS` or `WARN` | — |
| missing fields | mixed | — |
| near-duplicate eval | `PASS` (email not matched) | none — exploratory |

---

## Connection to architecture

This benchmark exercises the full pipeline stack:

```
Input
 ↓
NormalizationLayer      ← handles JOHN@EXAMPLE.COM → john@example.com
 ↓
ValidationLayer         ← catches invalid_email, rejects early
 ↓
FingerprintBuilder      ← email_normalized + phone_normalized → HMAC fingerprint
 ↓
DuplicateLookupTier
  BloomFilter           ← fast negative check (NO → skip Redis)
  RedisDuplicateStore   ← authoritative duplicate check (MAYBE → Redis lookup)
 ↓
PolicyEngine            ← applies tenant degraded_mode_policy
 ↓
RedisIdempotencyStore   ← snapshot: tenant_id + source_id + request_hash
 ↓
TelemetryQueue          ← fire-and-forget, never blocks pipeline
```

For architecture decisions behind each layer, see `docs/architecture/adr/`.

For the recovery path correctness test (Redis outage → QUEUE → drain),
see `docs/testing/hero-test-recovery.md` and
`tests/integration/test_pipeline.py::test_recovery_path_stores_fingerprint_and_idempotency_snapshot`.

---

_Lead Entry Guard — hero benchmark documentation._
_Scale failure mode tests: `load_tests/scale_scenarios.py`_
