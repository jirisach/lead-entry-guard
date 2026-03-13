# ADR-004: Idempotency Snapshot Contract

**Datum:** 2026-03-13
**Autoři:** core team
**Status:** accepted

---

## Kontext a důvod

Ingestion API je vystaveno transportním retry mechanismům: load balancery, API gateway, klientské
knihovny — všechny mohou za určitých podmínek poslat stejný request vícekrát. Bez ochrany by každý
retry mohl vytvořit nový lead s jiným `request_id`, jiným `latency_ms` a potenciálně i jiným
rozhodnutím pokud se mezitím změnil stav duplicate store.

Naivní řešení — „deduplikuj podle `email` nebo `phone`" — není správné, protože:
- duplicate check je tenant-specific a TTL-bounded (30 dní)
- dva různé requesty se stejným emailem ale různým `source_type` nebo `extra` jsou legitimně různé
- caller potřebuje dostat zpět **původní** rozhodnutí, ne přepočítané

Potřebujeme transport-level idempotency: stejný logický request → vždy stejná odpověď.

## Alternativy

### Alternativa A: Deduplikace pouze podle `source_id`

- Výhody: jednoduchý klíč, snadná implementace
- Nevýhody: `source_id` je caller-assigned — dva různé requesty mohou sdílet `source_id`
  pokud caller nesprávně implementuje retry logiku; vedlo by k falešným idempotency hitům
  pro legitimně různé requesty

### Alternativa B: Deduplikace podle `source_id` + request hash (zvoleno)

- Výhody: dvojitá záruka — `source_id` identifikuje logický request, hash ověřuje
  že payload je skutečně stejný; falešné hity jsou eliminovány
- Nevýhody: mírně složitější implementace, nutnost hashovat celý relevantní payload

### Alternativa C: Deduplikace pouze podle request hash bez `source_id`

- Výhody: nevyžaduje spolupráci callera
- Nevýhody: hash musí pokrývat celý payload; nedává callerovi kontrolu nad tím,
  které requesty jsou „stejné"; hash kolize (byť nepravděpodobné) by způsobily tiché chyby

### Alternativa D: Ukládat jen `PASS` rozhodnutí

- Výhody: jednodušší snapshot model
- Nevýhody: **porušuje kontrakt** — replay `REJECT`-ovaného leadu by vrátil `PASS`;
  idempotency musí vracet původní rozhodnutí bez ohledu na jeho hodnotu

## Rozhodnutí

**Idempotency je implementována jako dvouklíčový snapshot store s TTL 24 hodin.**

### Klíč

Snapshot je uložen pod kombinací:
```
tenant_id + source_id + request_hash
```

kde `request_hash` je SHA-256 (zkrácený na 24 znaků) nad všemi user-supplied poli:

```python
payload = {
    "tenant_id": ...,
    "source_type": ...,
    "email": ...,
    "phone": ...,
    "first_name": ...,
    "last_name": ...,
    "company": ...,
    "extra": ...,
}
```

**Záměrně vynecháno z hashe:** `request_id` a `received_at` — tyto hodnoty jsou přiřazeny
systémem při příjmu, ne callerem, a nesmí ovlivnit idempotency keying.

### Snapshot obsah

```python
@dataclass
class IdempotencySnapshot:
    request_id: str        # původní request_id — vrátí se na replay
    decision: str          # původní decision.value
    reason_codes: list[str]
    duplicate_check_skipped: bool
    policy_version: str
    ruleset_version: str
    config_version: str
```

### Klíčový invariant

> **Idempotency replay vrací původní snapshot — nikoliv přepočítané rozhodnutí.**
>
> Stejný `source_id` + stejný payload → stejný `request_id`, stejné `decision`,
> stejné `reason_codes`. Vždy. Nezávisle na aktuálním stavu duplicate store,
> policy verzi nebo dostupnosti Redis.

### Kdy se snapshot ukládá

- Pro **všechna** rozhodnutí: `PASS`, `REJECT`, `DUPLICATE_HINT`, `WARN`, `PASS_DEGRADED`
- Pouze pokud je `source_id` přítomno v requestu — bez `source_id` idempotency neplatí
- Fire-and-forget: ukládání nikdy neblokuje odpověď calleru
- Platí pro main path i recovery path (viz ADR-001)

### Kdy se snapshot čte

- Na začátku `process()`, před normalizací a validací
- Pokud idempotency store není dostupný: **non-fatal**, pipeline pokračuje bez ochrany
  a loguje warning. Request je zpracován normálně — bez garance idempotency.

### TTL

24 hodin — odpovídá typickému retry oknu transportních mechanismů. Není totéž jako
duplicate store TTL (30 dní), který řídí business-level duplicate detection.

## Důsledky

**Pozitivní:**
- Transport retry je bezpečný: caller dostane vždy stejnou odpověď pro stejný logický request
- `REJECT` rozhodnutí jsou idempotentní — replay neupgraduje odmítnutý lead na `PASS`
- Snapshot obsahuje `request_id` původního requestu — caller může korelovat logy
- Verze policy jsou součástí snapshotu — audit trail je kompletní i pro replaye

**Omezení a vědomá kompromisy:**
- Idempotency je opt-in: bez `source_id` žádná ochrana
- 24h TTL znamená, že retry po více než 24 hodinách vytvoří nový lead
- Selhání snapshot store je non-fatal: v degraded režimu je idempotency nejlepší úsilí,
  ne garance — toto je záměrné a je konzistentní s celkovou degraded-mode filosofií pipeline

**Chráněno testy:**
- `tests/integration/test_pipeline.py::test_idempotency_returns_same_decision`
  — ověřuje `request_id`, `decision` i `reason_codes` na replay
- `tests/integration/test_pipeline.py::test_idempotency_preserves_reject_decision`
  — ověřuje že `REJECT` není upgradován na `PASS`
- `tests/integration/test_pipeline.py::test_recovery_path_stores_fingerprint_and_idempotency_snapshot`
  — ověřuje idempotency i přes recovery path (viz ADR-001)
