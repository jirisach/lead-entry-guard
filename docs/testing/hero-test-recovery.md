# Hero Test: Recovery Path Store Contract

**Umístění:** `tests/integration/test_pipeline.py::test_recovery_path_stores_fingerprint_and_idempotency_snapshot`
**Navazuje na:** [ADR-001 — Recovery Path Store Contract](../architecture/adr/adr-001-recovery-path-store-contract.md)

---

## Co tento test chrání

Tento test chrání **nejcitlivější correctness invariant pipeline**: lead zpracovaný přes
recovery path (QUEUE mode → Redis outage → zotavení) musí být z pohledu duplicate store
a idempotency store nerozeznatelný od leadu zpracovaného přes normální request path.

Bez tohoto testu je snadné nechtěně rozbít `_process_post_recovery()` zjednodušením
nebo refaktorem — a systém bude vypadat funkční při unit testech, ale selže v produkci
pod specifickými podmínkami (Redis výpadek + QUEUE mode + replay nebo duplicate lead).

## Proč existoval bug který tento test zachytí

V první implementaci `_process_post_recovery()` volalo `_finalize()` a rovnou vracelo výsledek.
Chyběly dvě operace, které jsou standardní součástí hlavní `process()` cesty:

1. `store_accepted()` — uložení fingerprinte do duplicate store
2. `_store_idempotency_snapshot()` — uložení snapshotu pro replay ochranu

Výsledek: lead zpracovaný přes recovery path prošel systémem bez stop — jako by neexistoval.
Následný identický lead (duplicate storm) prošel jako `PASS` místo `DUPLICATE_HINT`.
Replay stejného requestu vrátil nové rozhodnutí místo původního.

## Co test konkrétně testuje

Test ověřuje **dva invarianty** přes jeden scénář:

### Scénář

```
1. První request přijde → Redis selže → lead jde do QUEUE
2. Redis se zotaví → _process_post_recovery() zpracuje lead → vrátí PASS
3. [Invariant 1] Druhý request se stejným emailem → musí dostat DUPLICATE_HINT
4. [Invariant 2] Replay prvního requestu (stejný source_id) → musí vrátit původní request_id a decision
```

### Invariant 1: Fingerprint store

```python
result_dup = await pipeline.process(duplicate_lead)
assert result_dup.decision == DecisionClass.DUPLICATE_HINT
```

**Proč:** Pokud `_process_post_recovery()` neuloží fingerprint, duplicate lead projde jako `PASS`.
To je kritická correctness chyba — systém přestane detekovat duplikáty pro leady zpracované
přes recovery path.

### Invariant 2: Idempotency snapshot

```python
result_replay = await pipeline.process(lead)  # stejný lead, stejný source_id
assert result_replay.request_id == result1.request_id
assert result_replay.decision == result1.decision
```

**Proč:** Pokud `_process_post_recovery()` neuloží idempotency snapshot, replay requestu
vytvoří nové rozhodnutí s novým `request_id`. Caller dostane jinou odpověď na identický
retry — porušení idempotency kontraktu (viz ADR-004).

## Jak test simuluje Redis outage

Test používá `unittest.mock.patch` na `dup_tier.check`:

```python
call_count = 0
original_check = dup_tier.check

async def flaky_check(tenant_id, fingerprint, bloom_capacity):
    nonlocal call_count
    call_count += 1
    if call_count == 1:
        raise RedisUnavailableError("first call fails — triggers queue")
    return await original_check(tenant_id, fingerprint, bloom_capacity)

with patch.object(dup_tier, "check", side_effect=flaky_check):
    result1 = await pipeline.process(lead)
```

**Proč takhle:** První volání `check()` hodí `RedisUnavailableError` → pipeline vstoupí do
QUEUE mode. Druhé volání (z `_process_post_recovery()` po simulovaném zotavení) projde normálně.
Skutečný Redis (fakeredis) funguje celou dobu — test izoluje pouze chování pipeline, ne infrastrukturu.

Tenant je nakonfigurován s `degraded_mode_policy = QUEUE` a globální `queue_hold_timeout_seconds`
je nastaven na 5 sekund přes `monkeypatch` — bez toho by test čekal 15 minut na výchozí timeout.

## Co NEdělá

- Netestuje výkon ani throughput recovery path
- Netestuje chování při druhém selhání Redis uvnitř `_process_post_recovery()`
  (to je pokryto unit testem `_process_post_recovery` exception handling větve)
- Neověřuje TTL uložených dat

## Kdy může test selhat

Test selže v těchto situacích — každá z nich signalizuje reálný problém:

| Selhání | Příčina |
|---|---|
| `result_dup.decision != DUPLICATE_HINT` | `store_accepted()` chybí v recovery path |
| `result_replay.request_id != result1.request_id` | `_store_idempotency_snapshot()` chybí v recovery path |
| `result1.decision != PASS` | Recovery path vrací špatné rozhodnutí |
| Timeout (test trvá déle než ~10s) | `monkeypatch` na `queue_hold_timeout_seconds` nefunguje |

## Jak test spustit

```bash
# Jen tento test
pytest tests/integration/test_pipeline.py::test_recovery_path_stores_fingerprint_and_idempotency_snapshot -v

# Celá integration suite
pytest tests/integration/ -v

# S výpisem logů (užitečné při debugování recovery flow)
pytest tests/integration/test_pipeline.py::test_recovery_path_stores_fingerprint_and_idempotency_snapshot -v -s --log-cli-level=DEBUG
```

## Proč tento test nesmí být zjednodušen

Při code review nebo refaktoru může test vypadat zbytečně složitý — testuje dvě věci najednou,
používá `monkeypatch` i `patch`, má netriviální mock setup.

**Nezjednodušujte ho.** Každá část testu chrání konkrétní invariant z ADR-001:

- `flaky_check` mock — bez něj se pipeline nikdy nedostane do QUEUE mode a recovery path se netestuje
- `asyncio.sleep(0.1)` — dává čas fire-and-forget taskům dokončit store operace před assertion
- oba asserty (DUPLICATE_HINT + request_id) — každý chrání jiný invariant; odstranění jednoho
  ponechá část kontraktu nekrytou

Pokud test selže po refaktoru pipeline nebo recovery path, je to **záměrné** — test říká,
že invariant z ADR-001 byl porušen a je nutná oprava.
