# ADR-001: Recovery Path Store Contract

**Datum:** 2026-03-13
**Autoři:** core team
**Status:** accepted

---

## Kontext a důvod

`IngestionPipeline` podporuje QUEUE degraded mode: když je Redis při příchodu requestu nedostupný
a tenant má `degraded_mode_policy = QUEUE`, lead se drží v paměti a čeká na zotavení Redis.
Po zotavení se zpracuje přes `_process_post_recovery()`.

Při první implementaci `_process_post_recovery()` volalo `_finalize()` a rovnou vracelo výsledek —
bez uložení fingerprinte do duplicate store a bez uložení idempotency snapshotu.

Tím vznikl **semantický drift** mezi dvěma cestami stejnou pipeline:

| Krok | `process()` (main path) | `_process_post_recovery()` (před opravou) |
|---|---|---|
| Fingerprint store | ✅ fire-and-forget | ❌ vynecháno |
| Idempotency snapshot | ✅ fire-and-forget | ❌ vynecháno |
| Decision returned | ✅ | ✅ |

Důsledky driftu:
- Lead zpracovaný přes recovery path nebyl registrován jako duplicate → následný identický lead prošel jako PASS
- Replay stejného `source_id` po recovery neměl uložený snapshot → idempotency kontrakt selhal
- Chování systému záviselo na tom, kudy request prošel — to je skrytá nestabilita

## Alternativy

### Alternativa A: Nechat recovery path bez store operací

- Výhody: jednodušší kód, žádná závislost na recovery path store
- Nevýhody: porušuje idempotency kontrakt, vyrábí phantom leads mimo duplicate store,
  nedeterministické chování závislé na infra stavu v momentě příchodu requestu

### Alternativa B: Recovery path mirroruje main path po decision fázi (zvoleno)

- Výhody: jediný kontrakt pro všechny cesty, determismus, idempotency funguje across paths
- Nevýhody: mírně složitější `_process_post_recovery()`, nutnost udržovat paritu při budoucích změnách

### Alternativa C: Sloučit recovery path s main path refaktorem

- Výhody: eliminuje duplicitu kódu
- Nevýhody: výrazně složitější refaktor, vyšší riziko regrese, odložení opravy

## Rozhodnutí

**`_process_post_recovery()` musí po úspěšném rozhodnutí provést identické store operace jako `process()`.**

Konkrétně — po `_finalize()` a před `return result`:

1. Pokud `decision in (PASS, WARN, PASS_DEGRADED)` → `store_accepted()` fire-and-forget
2. Pokud `lead.source_id` je přítomno → `_store_idempotency_snapshot()` fire-and-forget

Obě operace jsou fire-and-forget přes `_fire_and_forget()` — recovery path neblokuje request path,
stejně jako main path.

Invariant formulovaný jako pravidlo:

> **Lead zpracovaný přes recovery path musí být z pohledu duplicate store a idempotency store
> nerozeznatelný od leadu zpracovaného přes main path.**

## Důsledky

**Pozitivní:**
- Idempotency kontrakt platí across all paths: same `source_id` + same payload → same decision, vždy
- Duplicate detection funguje i pro leads zpracované přes recovery: následný identický lead dostane `DUPLICATE_HINT`
- Systém je deterministický nezávisle na infra stavu při příchodu requestu

**Rizika a údržba:**
- Jakákoliv budoucí změna store operací v `process()` musí být zrcadlena i v `_process_post_recovery()`
- Toto pravidlo není automaticky vynutitelné typovým systémem — chrání ho integrační test

**Chráněno testem:**
`tests/integration/test_pipeline.py::test_recovery_path_stores_fingerprint_and_idempotency_snapshot`

Tento test explicitně ověřuje oba invarianty:
1. Fingerprint uložen → druhý identický lead dostane `DUPLICATE_HINT`
2. Snapshot uložen → replay stejného `source_id` vrátí původní `request_id` a `decision`

Pokud někdo v budoucnu zjednodušuje `_process_post_recovery()`, tento test selže
a tím signalizuje porušení kontraktu z tohoto ADR.
