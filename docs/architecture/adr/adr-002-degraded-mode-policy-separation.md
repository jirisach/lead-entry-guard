# ADR-002: Degraded Mode Policy Separation

**Datum:** 2026-03-13
**Autoři:** core team
**Status:** accepted

---

## Kontext a důvod

`IngestionPipeline` podporuje tři reakce na nedostupnost duplicate subsystemu (Redis/Bloom):

- `ACCEPT_WITH_FLAG` — lead projde s `WARN` decision a `WARN_INDEX_UNAVAILABLE` reason code
- `REJECT` — lead je odmítnut
- `QUEUE` — lead se drží v paměti a čeká na zotavení (s timeoutem a hard capem)

Při první implementaci existovala jediná policy pro všechny degraded scénáře: `degraded_mode_policy`.
Když byla QUEUE mode zavedena, přibyla i situace „queue je plná nebo timeout vypršel" — a tato situace
potřebuje vlastní fallback, protože re-vstup do QUEUE by vedl k nekonečné rekurzi nebo zahltění.

Problém: v jedné iteraci kódu byl `KeyNotFoundError` (HMAC klíč nedostupný) zpracován přes
`_apply_degraded_policy()`, která čte `queue_fallback_policy` — **sekundární** pravidlo pro
queue cap/timeout — místo `degraded_mode_policy`, které je **primárním** pravidlem pro infra outage.

Tím tenant s `degraded_mode_policy = QUEUE` dostal při výpadku HMAC klíče jiné chování
než při výpadku Redis, ačkoliv oba jsou provozní selhání stejné úrovně.

## Alternativy

### Alternativa A: Jedna policy pro všechny degraded scénáře

- Výhody: jednodušší model, jeden konfigurační parametr
- Nevýhody: není možné oddělit „co dělat při infra outage" od „co dělat když QUEUE přeteče";
  tenant nemůže říct „při výpadku Redis čekej, ale při přetečení fronty přijmi s vlajkou"

### Alternativa B: Dvě policy s explicitně oddělenou sémantikou (zvoleno)

- Výhody: čistý model, každá policy má jednoznačnou odpovědnost, bezpečné rozšíření
- Nevýhody: dva konfigurační parametry, nutnost dokumentace aby nedošlo k záměně

### Alternativa C: Hierarchický policy strom

- Výhody: maximální flexibilita
- Nevýhody: over-engineering pro aktuální use case, zbytečná komplexita

## Rozhodnutí

**`TenantConfig` obsahuje dvě explicitně oddělené policy s různou sémantikou:**

### `degraded_mode_policy`

**Kdy se aplikuje:** při nedostupnosti duplicate subsystemu (Redis down, HMAC klíč nedostupný)
— tedy při primárním provozním selhání infrastruktury.

**Řídí:** co se stane s leadem, když pipeline nemůže provést duplicate check.

**Možné hodnoty:** `ACCEPT_WITH_FLAG` | `REJECT` | `QUEUE`

**Volá ji:** `_handle_redis_unavailable()` — primární degraded handler

### `queue_fallback_policy`

**Kdy se aplikuje:** pouze pokud je `degraded_mode_policy = QUEUE` a zároveň:
- queue dosáhla hard capu (`queue_hard_cap_per_tenant`), nebo
- hold timeout vypršel (`queue_hold_timeout_seconds`)

**Řídí:** co se stane s leadem, který nelze dále držet v queue.

**Možné hodnoty:** `ACCEPT_WITH_FLAG` | `REJECT` (QUEUE zde nedává smysl — vedlo by k rekurzi)

**Volá ji:** `_apply_degraded_policy()` — sekundární fallback handler

### Správné flow

```
Redis down
  → _handle_redis_unavailable()
      → degraded_mode_policy = QUEUE
          → čekat
          → timeout/cap hit
              → _apply_degraded_policy()
                  → queue_fallback_policy = ACCEPT_WITH_FLAG → WARN
```

### Chybné flow (před opravou)

```
KeyNotFoundError (HMAC klíč nedostupný)
  → _apply_degraded_policy()        ← CHYBA: šlo přímo na sekundární handler
      → queue_fallback_policy        ← CHYBA: četl nesprávnou policy
```

### Opravené flow

```
KeyNotFoundError (HMAC klíč nedostupný)
  → _handle_redis_unavailable()     ← správně: primární infra failure handler
      → degraded_mode_policy         ← správně: čte primární policy
```

## Důsledky

**Pozitivní:**
- Tenant s `degraded_mode_policy = QUEUE` a `queue_fallback_policy = ACCEPT_WITH_FLAG` dostane
  konzistentní chování: při jakémkoliv primárním infra selhání čeká, při přetečení fronty přijme s vlajkou
- Chování systému je předvídatelné a nezávisí na typu infra selhání (Redis vs. key unavailable)
- Policy model je bezpečně rozšiřitelný: přidání dalšího degraded scénáře nevyžaduje změnu
  konfigurační logiky, stačí ho routovat přes `_handle_redis_unavailable()`

**Pravidlo pro budoucí úpravy:**

> **Jakýkoliv primární infra failure (nedostupný Redis, nedostupný HMAC klíč, nedostupný Bloom
> bez Redis fallbacku) musí být routován přes `_handle_redis_unavailable()` a řízen
> `degraded_mode_policy`. `_apply_degraded_policy()` je výhradně pro QUEUE cap/timeout situace.**

**Pojmenování:** `_handle_redis_unavailable()` je historický název — funkce ve skutečnosti řeší
obecné primary infra failure, nejen Redis. Přejmenování na `_handle_infra_failure()` by bylo
přesnější, ale není blocker pro aktuální verzi.
