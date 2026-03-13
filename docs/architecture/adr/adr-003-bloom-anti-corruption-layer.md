# ADR-003: Bloom Filter jako Anti-Corruption Layer

**Datum:** 2026-03-13
**Autoři:** core team
**Status:** accepted

---

## Kontext a důvod

`DuplicateLookupTier` orchestruje dvoustupňový duplicate check:

1. **Bloom filter** — rychlý in-memory negativní pre-check (best-effort)
2. **Redis** — autoritativní store (source of truth)

Bloom vrstva používá třetí knihovnu (`pybloom-live` / `ScalableBloomFilter`).
Při první implementaci `DuplicateLookupTier.check()` chytalo holé `Exception` kolem Bloom volání:

```python
try:
    bloom_filter = self._bloom.get_or_create(...)
    maybe_present = bloom_filter.check_and_add(fp)
except Exception as exc:
    logger.warning("Bloom filter unavailable", ...)
    return await self._redis_only_lookup(...)
```

Problém: `except Exception` zachytí **jakoukoliv** výjimku z Bloom vrstvy — včetně programátorských
chyb (`AttributeError`, `TypeError`, logických bugů v `BloomSlot` nebo `BloomFilterRegistry`).
Tyto chyby se maskují jako „Bloom unavailable, jedeme dál přes Redis", místo aby propadly jako
viditelný bug. Systém pak funguje „robustně", ale skrytě chybně.

## Alternativy

### Alternativa A: Whitelist konkrétních výjimek z `pybloom-live`

- Výhody: přesný catch bez adaptér vrstvy
- Nevýhody: těsná vazba na interní exception hierarchii třetí knihovny; výměna knihovny
  (např. za `rbloom`) by vyžadovala změny v orchestrační vrstvě; interní typy se mohou
  mezi verzemi knihovny měnit bez semver záruky

### Alternativa B: Anti-corruption layer v Bloom vrstvě (zvoleno)

- Výhody: orchestrační vrstva pracuje výhradně s doménovými výjimkami; knihovna je
  vyměnitelná bez dotyku `duplicate.py`; bugy mimo Bloom vrstvu propadají jako skutečné bugy
- Nevýhody: `BloomSlot` a `BloomFilterRegistry` musí obalovat volání try/except,
  mírně více kódu v Bloom vrstvě

### Alternativa C: Nechat `except Exception` a přijmout maskování

- Výhody: žádná změna
- Nevýhody: bugy v Bloom kódu jsou trvale neviditelné; debugging je obtížný;
  falešná jistota o „robustnosti"

## Rozhodnutí

**Bloom vrstva (`bloom.py`) je anti-corruption layer: překládá všechny výjimky třetí knihovny
na doménovou výjimku `BloomUnavailableError`. Orchestrační vrstva (`duplicate.py`) chytá
výhradně `BloomUnavailableError`.**

### Implementace v `bloom.py`

`BloomSlot` překládá výjimky na hranici každé operace:

```python
def __post_init__(self) -> None:
    try:
        if ScalableBloomFilter is None:
            raise ImportError("pybloom-live is required")
        self._filter = ScalableBloomFilter(...)
    except ImportError as exc:
        raise BloomUnavailableError("Bloom backend not available") from exc
    except Exception as exc:
        raise BloomUnavailableError("Bloom filter initialization failed") from exc

def add(self, key: str) -> None:
    try:
        self._filter.add(key)
        self._count += 1
    except Exception as exc:
        raise BloomUnavailableError("Bloom filter add failed") from exc

def __contains__(self, key: str) -> bool:
    try:
        return key in self._filter
    except Exception as exc:
        raise BloomUnavailableError("Bloom filter membership check failed") from exc
```

`BloomFilterRegistry.get_or_create()` re-raisuje `BloomUnavailableError` a překládá ostatní:

```python
except BloomUnavailableError:
    raise  # doménová výjimka — propustit čistě
except Exception as exc:
    raise BloomUnavailableError("Bloom registry unavailable") from exc
```

### Implementace v `duplicate.py`

```python
except BloomUnavailableError as exc:
    logger.warning(
        "Bloom filter unavailable — falling back to Redis direct lookup",
        extra={"tenant_id": tenant_id, "error_type": type(exc).__name__},
    )
    return await self._redis_only_lookup(tenant_id, fp)
```

### Důležitý rozdíl

`except Exception` uvnitř `bloom.py` je **v pořádku** — je to přesně na hranici adaptéru,
kde překládáme cizí chaos na vlastní kontrakt.

`except Exception` v `duplicate.py` by bylo **špatně** — tam by to maskovalo bugy
mimo Bloom vrstvu jako infra outage.

## Důsledky

**Pozitivní:**
- Bugy v Bloom kódu (`AttributeError`, logické chyby) propadají jako viditelné výjimky,
  nejsou tichý fallback
- `duplicate.py` pracuje výhradně s `BloomUnavailableError` a `RedisUnavailableError` —
  čistý exception kontrakt na orchestrační úrovni
- Bloom implementace je vyměnitelná (`pybloom-live` → `rbloom` nebo custom)
  bez dotyku `DuplicateLookupTier`
- Graceful fallback do Redis zůstává zachovaný — robustnost neklesla

**Architektonické pravidlo:**

> **Jakákoliv třetí knihovna použitá uvnitř Bloom vrstvy musí být obalena anti-corruption
> vrstvou, která překládá její výjimky na `BloomUnavailableError`.
> `DuplicateLookupTier` nesmí nikdy chytat výjimky z `pybloom-live` nebo jiné
> Bloom implementace přímo.**

**Volba knihovny:** `pybloom-live` je aktuálně správná volba — Bloom je jen rychlý negativní
pre-check, Redis je autoritativní. Výkonnostní bottleneck bude pravděpodobně Redis nebo
celkový harness, ne Bloom. Přechod na `rbloom` (Rust backend) dává smysl až po profiling
pod reálnou zátěží — a díky anti-corruption layer nevyžaduje změny mimo `bloom.py`.
