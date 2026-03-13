# Load Tests

Scénáře a nástroje pro testování Lead Entry Guard pod zátěží a failure conditions.

## Struktura

```
load_tests/
├── generate_dataset.py    # Generátor syntetických datasetů (duplicate storm, messy data...)
├── run_scenario.py        # HTTP runner — spouští scénáře proti běžící instanci
├── scale_scenarios.py     # Failure mode testy (in-process, bez HTTP)
├── hero_benchmark.py      # Hero benchmark — 100 000 messy leads
├── generate_report.py     # Generátor Markdown reportu z JSON výsledků
├── README.md
└── datasets/              # Vygenerované datasety a výsledky (gitignored)
```

| Typ | Soubor | Spouštěč | Kdy použít |
|---|---|---|---|
| HTTP load test | `run_scenario.py` | běžící instance + Docker | throughput, latence, integrace |
| Failure mode test | `scale_scenarios.py` | in-process (fakeredis) | correctness pod failure conditions |
| Hero benchmark | `hero_benchmark.py` | in-process (fakeredis / real Redis) | product proof, case study |

---

## Rychlý start

### HTTP load testy (proti běžící instanci)

```bash
# 1. Spusť instanci
docker compose up

# 2. Vygeneruj datasety
python load_tests/generate_dataset.py --scenario all --count 10000

# 3. Spusť scénář
python load_tests/run_scenario.py --scenario duplicate_storm --concurrency 50
```

### Failure mode testy (in-process)

```bash
# Všechny scénáře najednou
python load_tests/scale_scenarios.py --scenario all --output

# Jeden scénář
python load_tests/scale_scenarios.py --scenario retry_storm
python load_tests/scale_scenarios.py --scenario redis_latency
python load_tests/scale_scenarios.py --scenario queue_drain

# Sweep testy
python load_tests/scale_scenarios.py --scenario sweep_concurrency
python load_tests/scale_scenarios.py --scenario sweep_bloom
```

### Hero benchmark

```bash
# Plný run (generuje dataset + spustí benchmark)
python load_tests/hero_benchmark.py

# Jen generovat dataset
python load_tests/hero_benchmark.py --seed 2024 --generate-only

# Jen spustit benchmark (dataset musí existovat)
python load_tests/hero_benchmark.py --seed 2024 --run-only

# Spustit proti real Redis
python load_tests/hero_benchmark.py --seed 2024 --redis-url redis://localhost:6379

# Rychlý debug / CI smoke run
python load_tests/hero_benchmark.py --limit 500
```

### Report

```bash
python load_tests/generate_report.py
# → load_tests/datasets/scale_report.md
```

---

## HTTP load scénáře (`run_scenario.py`)

### `duplicate_storm`

Simuluje burst importu kde ~50 % leadů jsou duplikáty. Testuje Bloom → Redis lookup tier a throughput při vysokém podílu `DUPLICATE_HINT`.

```bash
python load_tests/run_scenario.py --scenario duplicate_storm --concurrency 50
```

Očekávané výsledky:
```
PASS              ~50%
DUPLICATE_HINT    ~50%
```

### `redis_outage`

Normální provoz během výpadku Redis, včetně replayů po zotavení. Vyžaduje manuální simulaci výpadku.

```bash
# Terminál 1
python load_tests/run_scenario.py --scenario redis_outage --concurrency 20

# Terminál 2 — simulace výpadku uprostřed testu
docker compose stop redis && sleep 30 && docker compose start redis
```

### `messy_data`

Import z nevalidovaného zdroje — uppercase emaily, broken phones, prázdná pole. Testuje early-reject path a normalizační robustnost.

```bash
python load_tests/run_scenario.py --scenario messy_data --concurrency 100
```

Očekávané výsledky:
```
REJECT            ~60%
PASS              ~35%
WARN               ~5%
```

---

## Failure mode scénáře (`scale_scenarios.py`)

Běží in-process (fakeredis) — nevyžadují běžící instanci.

| Scénář | Co testuje | Klíčový invariant |
|---|---|---|
| `retry_storm` | API gateway retry storm — 500 identických requestů | všechny vrátí stejné rozhodnutí |
| `queue_pressure` | Redis outage + 3 tenanti s různými policies | `ACCEPT_WITH_FLAG` tenant nikdy nedostane REJECT |
| `bloom_pressure` | Bloom fill ratio + rotation economics | duplikáty detekované i po rotaci |
| `queue_drain` | Recovery path store contract (ADR-001) | fingerprint + snapshot uloženy po recovery |
| `redis_latency` | Pomalý Redis (200ms latence) — ne unavailable | pipeline funguje, p99 bounded |
| `telemetry_backpressure` | Plná telemetry fronta | pipeline_errors == 0 |

### Sweep testy

```bash
# Kde začíná race window
python load_tests/scale_scenarios.py --scenario sweep_concurrency --concurrency 100 500 1000 2000

# Sizing guide pro bloom_capacity_override
python load_tests/scale_scenarios.py --scenario sweep_bloom --bloom-capacity 1000 5000 10000 20000
```

---

## Hero benchmark (`hero_benchmark.py`)

100 000 messy leads — product proof, case study.

**Kompozice datasetu:**

| Bucket | Počet | Garantovaný výsledek |
|---|---|---|
| valid unique | 45 000 | `PASS` nebo `WARN` |
| duplicate storm | 25 000 | 1 `PASS` + N `DUPLICATE_HINT` per skupina |
| retry / idempotency | 10 000 | stejný `request_id` na každém replaye |
| invalid email | 5 000 | `REJECT` |
| broken phone | 5 000 | `PASS` nebo `WARN` |
| missing fields | 5 000 | dle policy |
| near-duplicate eval | 5 000 | exploratory — bez hard assertu |

Hard invarianty:
1. všechny subsequent leads v duplicate storm skupině == `DUPLICATE_HINT`
2. všechny replaye stejného `source_id` vrátí identický `request_id`

Viz `docs/testing/hero-benchmark-100k.md` pro detailní dokumentaci.

---

## Výstup run_scenario.py

```
============================================================
Scenario:    duplicate_storm
Total leads: 10,000
Duration:    12.3s
Concurrency: 50
Throughput:  813.0 req/s
Success:     100.0%

Decision breakdown:
  PASS                       4,987  (49.9%)
  DUPLICATE_HINT             5,013  (50.1%)

Latency (successful requests):
  p50        4.21 ms
  p90        8.93 ms
  p95       12.44 ms
  p99       28.71 ms
  mean       5.02 ms
  max       87.33 ms
============================================================
```

---

## Parametry

### `generate_dataset.py`

```bash
python load_tests/generate_dataset.py \
    --scenario duplicate_storm \   # duplicate_storm | redis_outage | messy_data | all
    --count 50000
```

### `run_scenario.py`

```bash
python load_tests/run_scenario.py \
    --scenario duplicate_storm \
    --concurrency 50 \
    --limit 1000 \
    --base-url http://localhost:8000 \
    --no-wait
```

### `hero_benchmark.py`

```bash
python load_tests/hero_benchmark.py \
    --seed 2024 \                          # seed pro generátor (default: 2024)
    --limit 500 \                          # jen prvních N leadů (debug / CI)
    --redis-url redis://localhost:6379 \   # real Redis (default: fakeredis)
    --generate-only \                      # jen generovat dataset
    --run-only                             # jen spustit existující dataset
```

---

## CI integrace

```yaml
# Smoke load test
- name: Load test smoke run
  run: |
    python load_tests/generate_dataset.py --scenario duplicate_storm --count 1000
    python load_tests/run_scenario.py \
      --scenario duplicate_storm \
      --concurrency 10 \
      --limit 1000 \
      --no-wait

# Failure mode testy — bez běžící instance
- name: Scale correctness tests
  run: |
    python load_tests/scale_scenarios.py --scenario all --output
    python load_tests/generate_report.py
```
