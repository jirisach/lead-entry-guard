# Lead Entry Guard — Test Coverage & Benchmark Report

> Stav: March 2026 | Pipeline v4

---

## Architecture summary

```
Input (LeadInput)
        ↓
Idempotency check          ← source_id + request_hash → snapshot hit/miss
        ↓
Normalization layer        ← email lowercase, phone E.164, whitespace trim
        ↓
Validation layer           ← required fields, email format, phone format
        ↓
Recoverability / Salvage   ← fatal vs recoverable errors, SalvagePolicy per tenant
        ↓
Fingerprint builder        ← HMAC-SHA256, email = primary anchor, phone = fallback
        ↓
Duplicate detection        ← Bloom (fast negative) → Redis (authoritative)
        ↓
Decision engine            ← PolicyEngine, deterministic rule chain
        ↓
Audit meta + Telemetry     ← privacy-safe, fire-and-forget, never blocks pipeline
        ↓
DecisionResult             ← PASS / WARN / REJECT / DUPLICATE_HINT
```

**Degraded modes** (při Redis outage):

```
ACCEPT_WITH_FLAG  →  WARN + duplicate_check_skipped=True
REJECT            →  REJECT
QUEUE             →  čeká na obnovu Redis, pak fallback policy
```

**SalvagePolicy** (per tenant, pro recoverable errors):

```
STRICT      →  invalid phone = REJECT
SALVAGE     →  invalid phone = WARN
QUARANTINE  →  invalid phone = WARN + WARN_MANUAL_REVIEW_REQUIRED
```

---

## Test suite

### Přehled

| Vrstva | Soubory | Testů | Účel |
|---|---|---|---|
| unit | 4 | 39 | Izolované komponenty — normalizace, fingerprint, policy, salvage layer |
| integration | 5 | 32 | End-to-end pipeline flow, idempotency, tenant isolation, determinism, replay suite |
| resilience | 3 | 13 | Single-component failure — Bloom down, Redis down, slow downstream |
| chaos | 1 | 9 | Multi-component failure — souběžné selhání Redis + telemetry, HMAC race |
| load | 2 | 6 | Burst ingestion (1000 leads), retry storm, jitter storm (300 concurrent) |
| **celkem** | **15** | **~99** | |

### Pokryté invarianty

**Correctness**
- Stejný lead → stejné rozhodnutí (determinism)
- Stejný `source_id` → stejný `request_id` na replay (idempotency)
- Tenant A fingerprint ≠ Tenant B fingerprint pro stejný email (namespace isolation)
- Fatal error (invalid email) vždy REJECT bez ohledu na SalvagePolicy
- Duplicate signal má přednost před recoverable phone error (rule precedence)

**Resilience**
- Bloom down → fallback na Redis direct lookup, žádný crash
- Redis down → degraded mode policy (WARN / REJECT dle konfigurace tenanta)
- Slow Redis (50ms latence) → event loop se neblokuje
- 1000 concurrent leads → žádná výjimka, žádný deadlock
- 300 concurrent retries se jitterem → stabilní outcome

**Salvage Layer**
- STRICT: invalid phone → REJECT
- SALVAGE: invalid phone → WARN
- QUARANTINE: invalid phone → WARN + `WARN_MANUAL_REVIEW_REQUIRED`

---

## Synthetic benchmark (100k messy leads)

> Prostředí: fakeredis, sekvenční zpracování, telemetry export vypnutý.
> Jedná se o baseline pipeline performance benchmark, ne produkční end-to-end čísla.

### Throughput

```
~966 – 1036 leads/s
```

### Latence

| Percentil | Hodnota |
|---|---|
| p50 | ~0.87 ms |
| p95 | ~1.41 ms |
| p99 | ~1.80 ms |

### Kompozice datasetu (100 000 leadů)

| Bucket | Počet | Typ |
|---|---|---|
| clean | 62 500 | strict |
| dirty | 20 000 | wide |
| broken | 7 000 | strict |
| near_duplicate | 5 000 | exploratory |
| exact_duplicate | 2 500 | strict |
| edge_case | 3 000 | wide |

---

## Accuracy report (synthetic benchmark)

### Celková přesnost

| Metrika | Hodnota |
|---|---|
| Overall accuracy (non-exploratory) | **100.00 %** (95 000 / 95 000) |
| Strict accuracy (single-outcome buckets) | **100.00 %** (72 000 / 72 000) |
| False positives (should stop, didn't) | **0** |
| False negatives (valid lead blocked) | **0** (strict buckets) |

### Výsledky per bucket

| Bucket | Výsledek | Poznámka |
|---|---|---|
| clean | 100 % PASS | ✓ |
| broken | 100 % REJECT | ✓ |
| exact_duplicate | 100 % DUPLICATE_HINT | ✓ |
| edge_case | 100 % PASS (valid: PASS nebo WARN) | ✓ wide bucket |
| dirty | 11 959 PASS / 8 041 REJECT | ✓ wide bucket — obojí je validní outcome |
| near_duplicate | 5 000 WARN | exploratory — bez hard assertu |

### Poznámka k dirty bucketu

8 041 leadů dostalo REJECT místo WARN. Bucket je `wide` — obojí je validním výsledkem podle kontraktu. Signál ukazuje, že pipeline je přísná na dirty data s nevalidním telefonem. Pokud je cílem salvage mode, lze nakonfigurovat `SalvagePolicy.SALVAGE` per tenant.

### Poznámka k near_duplicate bucketu

Near-duplicate leady dostaly WARN, nikoli DUPLICATE_HINT. Bucket je záměrně `EXPLORATORY` — near-duplicate detection (fuzzy matching, entity resolution) není v aktuální verzi garantovanou schopností. Výsledek je reportován pro sizing, ne jako correctness metrika.

---

## Známá omezení

**Benchmark**
- Sekvenční zpracování — nepokrývá concurrent throughput
- fakeredis — nezahrnuje network latenci real Redis
- Telemetry export loop vypnutý — nezahrnuje overhead async side effects
- Produkční end-to-end throughput claim vyžaduje benchmark s real Redis + concurrency + zapnutou telemetrií

**Near-duplicate detection**
- Aktuální schopnost: exact duplicate detection přes HMAC fingerprint (email jako primární anchor)
- Není zahrnuto: fuzzy matching, edit distance, ML-based entity resolution
- Near-duplicate bucket je `EXPLORATORY` — výsledky jsou reportovány, ale nejsou součástí correctness garance

**Fingerprint identity strategy**
- Email je primární identity anchor
- Phone je sekundární fallback (pouze pokud email chybí)
- Stejný email + různý/invalid/chybějící phone → stejný fingerprint (záměrné)

---

## Phase 2 — Production benchmark výsledky

> Prostředí: real Redis (Docker), Windows localhost, psutil memory tracking, aktivní telemetry drain.

### Concurrency sweep (real Redis)

| Concurrency | Throughput | p50 | p99 | Memory growth |
|---|---|---|---|---|
| 10 | **1,056 req/s** | 6.98 ms | 25.8 ms | 15.6 MB |
| 50 | 912 req/s | 36.0 ms | 230 ms | 15.7 MB |
| 100 | 756 req/s | 74.7 ms | 553 ms | 22.2 MB |
| 200 | 532 req/s | 150.9 ms | 1271 ms | 25.0 MB |

**Sweet spot: concurrency 10–25** — nejlepší poměr throughput / latence pro localhost Docker Redis.

### Soak test (30 minut, real Redis, concurrency 25)

| Metrika | Start | Střed | Konec | Drift |
|---|---|---|---|---|
| Throughput | 1,299 req/s | 1,401 req/s | 1,460 req/s | ↑ +12.4% ✓ |
| p50 latence | 12.9 ms | 12.6 ms | 12.2 ms | ↓ -5.8% ✓ |
| p99 latence | 23.9 ms | 18.6 ms | 15.6 ms | ↓ -34.9% ✓ |
| Memory | 65.9 MB | 67.4 MB | 62.6 MB | ↓ -5.0% ✓ |
| Telemetry backlog | 0 | 0 | 0 | ✓ |

**Celkem:** 2,398,150 leadů · 0 errors · avg 1,332 req/s · žádný memory leak signál.

Throughput se v čase zlepšoval (warm-up efekt connection poolu + Bloom cache) — žádná degradace.

### Noisy neighbor test (po ADR-006 per-tenant semaphore)

| Scénář | Noisy tenant | Normal tenant p99 | Quiet tenant p99 | Verdict |
|---|---|---|---|---|
| Two-tenant storm | 500 concurrent | 517 ms | — | PASS |
| Three-tenant storm | 500 concurrent | 33 ms | 62 ms | PASS |

Per-tenant concurrency cap (ADR-006) úspěšně izoluje noisy tenanta od ostatních.  
Bez capu: normal tenant p99 = 2125 ms (FAIL). Po capu: 517 ms (PASS).

---

## Doporučené další kroky

1. **Zpřesnit dirty bucket v generátoru** — rozdělit na typed subtypes (`dirty_valid_email`, `dirty_invalid_phone`, ...) pro přesnější accuracy metriku
2. **Near-duplicate bucket truth kontrakt** — ujasnit co je validní outcome pro exploratory bucket v accuracy reportu
3. **Phase 3** — API surface (FastAPI auth + rate limiting), observability (health endpoints, Prometheus metrics), deployment example

---

*Lead Entry Guard v4 — dokumentace generována ze syntetického benchmarku a test suite výsledků.*
