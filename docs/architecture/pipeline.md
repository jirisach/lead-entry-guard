# Pipeline Architecture

## Processing Flow

```
                    ┌─────────────────────────────┐
                    │        IngestRequest         │
                    │  tenant_id, source_id,       │
                    │  email, phone, name, ...      │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │      Idempotency Check       │
                    │  source_id + request_hash    │
                    │  → hit: return snapshot      │
                    │  → miss: continue            │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │        Normalization         │
                    │  email lowercase, phone E164 │
                    │  whitespace trim, ...        │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │          Validation          │
                    │  format, required fields     │
                    │  → invalid: REJECT (early)   │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │     Fingerprint Builder      │
                    │  HMAC over identity fields   │
                    │  tenant-scoped, key-versioned│
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │     Duplicate Lookup Tier    │
                    │                             │
                    │  ┌──────────────────────┐   │
                    │  │    Bloom Filter       │   │
                    │  │  fast negative check  │   │
                    │  │  DEFINITELY NOT →     │   │
                    │  │  skip Redis           │   │
                    │  └──────────┬───────────┘   │
                    │             │ MAYBE          │
                    │  ┌──────────▼───────────┐   │
                    │  │    Redis Store        │   │
                    │  │  authoritative lookup │   │
                    │  └──────────────────────┘   │
                    └──────────────┬──────────────┘
                                   │ DuplicateHint
                    ┌──────────────▼──────────────┐
                    │        Policy Engine         │
                    │  PASS / REJECT /             │
                    │  DUPLICATE_HINT / WARN       │
                    └──────────────┬──────────────┘
                                   │ Decision
                    ┌──────────────▼──────────────┐
                    │      Telemetry (async)       │
                    │  privacy-safe, non-blocking  │
                    │  dropped if queue full       │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │     Store Side Effects       │
                    │     (fire-and-forget)        │
                    │                             │
                    │  if PASS/WARN:               │
                    │    duplicate store ← fp      │
                    │  if source_id present:       │
                    │    idempotency snapshot ← ✓  │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │         DecisionResult       │
                    │  request_id, decision,       │
                    │  reason_codes, versions,     │
                    │  latency_ms                  │
                    └─────────────────────────────┘
```

## Degraded Mode Flow

Když je Redis nedostupný při duplicate lookup:

```
RedisUnavailableError
        │
        ▼
_handle_redis_unavailable()
        │
        ├── degraded_mode_policy = ACCEPT_WITH_FLAG
        │       → WARN + WARN_INDEX_UNAVAILABLE
        │
        ├── degraded_mode_policy = REJECT
        │       → REJECT + DEGRADED_REDIS_UNAVAILABLE
        │
        └── degraded_mode_policy = QUEUE
                │
                ├── queue not full + within timeout
                │       → hold in memory
                │       → poll for Redis recovery
                │       → on recovery: _process_post_recovery()
                │         (mirrors main path — ADR-001)
                │
                ├── queue cap hit
                │       → _apply_degraded_policy()
                │         → queue_fallback_policy (ADR-002)
                │
                └── timeout expired
                        → _apply_degraded_policy()
                          → queue_fallback_policy (ADR-002)
```

## Component Responsibilities

| Komponenta | Odpovědnost | Source of Truth |
|---|---|---|
| `NormalizationLayer` | Kanonický tvar vstupních polí | — |
| `ValidationLayer` | Formátová správnost | — |
| `FingerprintBuilder` | HMAC identity hash, key-versioned | `HMACKeyManager` |
| `BloomFilterRegistry` | Fast negative pre-check, per-tenant | in-memory |
| `RedisDuplicateStore` | Autoritativní duplicate store, 30d TTL | Redis |
| `RedisIdempotencyStore` | Replay ochrana, 24h TTL | Redis |
| `PolicyEngine` | Business rozhodnutí | tenant config + ruleset |
| `TelemetryQueue` | Async metrics drain | StatsD |

## Key Architectural Decisions

- **ADR-001** — Recovery path mirroruje main path (store operace)
- **ADR-002** — `degraded_mode_policy` ≠ `queue_fallback_policy`
- **ADR-003** — Bloom je anti-corruption layer nad `pybloom-live`
- **ADR-004** — Idempotency: `source_id` + request hash, snapshot pro všechna rozhodnutí
