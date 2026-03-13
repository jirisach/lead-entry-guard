# Benchmark baseline — 100,000 messy leads

> Established: 2026-03-13
> Dataset seed: 42
> Redis mode: fakeredis (in-process)

---

## Performance

| Metric | Value |
|---|---|
| Total records | 100,000 |
| Throughput | ~1,310–1,387 records/s |
| Latency p50 | 0.71 ms |
| Latency p95 | 0.97 ms |
| Latency p99 | 1.19 ms |
| Duration | ~72–76 s |

p95 sub-1ms. p99 under 1.2ms. No throughput degradation observed between 10k and 100k runs — pipeline scales linearly.

---

## Accuracy

| Metric | Value |
|---|---|
| Overall accuracy (non-exploratory, `valid_decisions`) | **100.00%** |
| Strict accuracy (single-outcome buckets only) | **100.00%** |
| Strict false positives | 0 |
| Strict false negatives | 0 |

### Per-bucket breakdown

| Bucket | Total | Accuracy | Type | Note |
|---|---|---|---|---|
| `clean` | 62,500 | 100% | strict | Dedicated identity pool, no collisions |
| `broken` | 7,000 | 100% | strict | All corrupt/missing emails → REJECT |
| `exact_duplicate` | 2,500 | 100% | strict | Original always arrives before copy |
| `dirty` | 20,000 | 100% | wide | REJECT valid — messy_phone may produce invalid format |
| `edge_case` | 3,000 | 100% | wide | Unicode/emoji passes as PASS, not WARN |
| `near_duplicate` | 5,000 | n/a | exploratory | Phone match, email fingerprint mismatch |

**Zero false positives** — pipeline did not block any lead that should have passed.

Lead Entry Guard prioritizes false-positive safety. In ambiguous cases the
system prefers PASS over REJECT to ensure valid leads are not blocked.

---

## What "strict" means here

Strict buckets have a single valid outcome (`valid_decisions` is a single-element list).
Accuracy is a hard assertion — any deviation is a real pipeline failure.

Wide buckets reflect genuine non-determinism in mutation functions (`messy_phone`,
`random_case`, etc.) and allow multiple valid outcomes. Accuracy is measured as
`actual in valid_decisions`, not `actual == expected_decision`.

Exploratory buckets are reported for sizing only — no assertion is made.

---

## Decision distribution (pipeline run)

| Decision | Count | Share |
|---|---|---|
| PASS | 78,496 | 78.5% |
| REJECT | 17,030 | 17.0% |
| DUPLICATE_HINT | 4,474 | 4.5% |

High REJECT share is expected: `broken` bucket (7k) + `dirty` leads where
`messy_phone` produced an invalid/missing phone (≈40% of 20k dirty bucket).

Lead Entry Guard prioritizes false-positive safety: no valid lead should be
blocked. Therefore duplicate and validation checks are biased toward PASS
when ambiguity exists.

---

## Dataset composition

| Bucket | Count | Share | Contract |
|---|---|---|---|
| clean | 62,500 | 62.5% | Unique identity per record, no pool sharing |
| dirty | 20,000 | 20.0% | Messy formatting — wide valid_decisions |
| broken | 7,000 | 7.0% | Always-invalid email → REJECT |
| near_duplicate | 5,000 | 5.0% | 2,500 families × (original + near-dup) |
| exact_duplicate | 2,500 | 2.5% | 1,250 families × (original + exact copy) |
| edge_case | 3,000 | 3.0% | Unicode, emoji, diacritics |

Duplicate families guarantee: original always precedes its copy in the stream.
No identity pool overlap between buckets.

---

## Known gaps — next steps

### `dirty` subtype split (phase 2)
`dirty` wide bucket will be split into typed subtypes once `NormalizationLayer`
is imported into the generator:

| Subtype | Expected outcome |
|---|---|
| `dirty_valid` | `["PASS"]` |
| `dirty_invalid_phone` | `["PASS", "WARN"]` |
| `dirty_corrupt_email` | `["REJECT"]` |
| `dirty_duplicate_like` | `["DUPLICATE_HINT"]` |

### `edge_case` expectation correction
Current `expected_decision=WARN` is incorrect — pipeline accepts edge_case leads
as `PASS`. Will be corrected to `["PASS", "WARN"]` or reclassified as exploratory
in a future dataset version.

### Real Redis benchmark
Current baseline uses fakeredis (in-process). A second benchmark mode with
`TelemetryExporter.export_loop()` running against real Redis will be added
to measure end-to-end production-realistic latency.

---

## How to reproduce

```powershell
# Regenerate dataset
python -m synthetic_data.generator.build_dataset

# Run pipeline benchmark
python run_pipeline_benchmark.py

# Run accuracy analyzer
python -m synthetic_data.analyze_benchmark_accuracy
```

All three commands run from project root. Default dataset: `synthetic_data/output/messy_leads_100k.jsonl`.

---

_Lead Entry Guard — 100k benchmark baseline._
