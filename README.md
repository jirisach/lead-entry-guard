# Lead Entry Guard v4

> Deterministická, privacy-safe, tenant-aware ingestion gateway pro CRM a marketingové pipeline.

## Architektura

```
Ingestion API
     ↓
Normalization Layer
     ↓
Validation Layer
     ↓
Fingerprint Builder (HMAC identity signal)
     ↓
Duplicate Lookup Tier (Bloom → Redis → Decision)
     ↓
Policy / Scoring Engine (active + async shadow)
     ↓                        ↓
Audit Meta             Async Telemetry Queue
(safe only)                   ↓
                      Telemetry Exporter
                      + OOB Heartbeat (UDP)
```

## Instalace

```bash
# development + testy
pip install -e ".[dev]"

# load testy
pip install -e ".[dev]" && pip install aiohttp
```

## Spuštění

```bash
# lokálně
uvicorn lead_entry_guard.api.app:app --reload

# docker
docker compose up
```

## Testy

```bash
# Unit + Integration + Resilience
pytest tests/unit tests/integration tests/resilience -v

# Chaos testy
pytest tests/chaos -v
```

## Load testy a benchmarky

```bash
# Hero benchmark — 100 000 messy leads (duplicate storms, retries, invalid data)
python load_tests/hero_benchmark.py

# Scale failure mode testy (retry storm, Redis outage, Bloom pressure...)
python load_tests/scale_scenarios.py --scenario all --output

# Generovat report
python load_tests/generate_report.py
```

Viz `docs/testing/` pro dokumentaci benchmarků a invariantů.

## Klíčové principy v4

| # | Princip |
|---|---------|
| 1 | Stateless-first zpracování requestů |
| 2 | Žádné raw PII v logách |
| 3 | Žádné fingerprint artefakty v telemetrii |
| 4 | Deterministické rozhodování s explicitním verzováním |
| 5 | Graceful degraded modes (ACCEPT_WITH_FLAG / REJECT / QUEUE) |
| 6 | Tenant izolace by design |
| 7 | Async side effects nesmí nikdy blokovat ingestion |
| 8 | Privacy-safe observability |

## Degraded modes

| Policy | Popis |
|--------|-------|
| `ACCEPT_WITH_FLAG` | Lead pokračuje s `duplicate_check_skipped=true` |
| `REJECT` | Request zamítnut (konzervativní, high-risk tenanti) |
| `QUEUE` | Čeká max 15 min na obnovu Redis, pak fallback policy |

## Konfigurace

Všechny parametry jsou konfigurovatelné přes environment variables s prefixem `LEG_`:

```env
LEG_REDIS_URL=redis://localhost:6379/0
LEG_VAULT_URL=http://vault:8200
LEG_VAULT_TOKEN=<token>
LEG_DUPLICATE_TTL_SECONDS=2592000
LEG_IDEMPOTENCY_TTL_SECONDS=86400
```

## Bezpečnost HMAC klíčů

- Klíče jsou uloženy výhradně v HashiCorp Vault / AWS KMS / GCP KMS / Azure Key Vault
- Zakázáno ukládat do zdrojového kódu, git history nebo config souborů v repozitáři
- Dual-key rotační model s overlap window >= Redis TTL (30 dní)
- Fingerprints se nikdy neobjevují v logách, telemetrii ani analytice

## Architektonická rozhodnutí

Každá větší změna designu je zdokumentována v `docs/architecture/adr/`.
