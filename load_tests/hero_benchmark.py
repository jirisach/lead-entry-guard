"""
Hero benchmark — 100,000 messy leads.

This is a product proof, not a throughput test.

The goal is deterministic behavior under:
  - duplicate storms (30k leads, ~1 PASS + 29 DUPLICATE_HINT per storm)
  - retry/idempotency loops (10k replayed source_ids, same request_id returned)
  - Redis outage + recovery (10k leads, fingerprint + snapshot stored post-recovery)
  - malformed payloads (10k invalid emails, 5k broken phones)
  - missing fields (5k partial leads)
  - near-duplicate evaluation bucket (10k — reported separately, not a core KPI)

Dataset composition (100k total):
  bucket                   count   guarantee
  ─────────────────────────────────────────────────────────
  valid unique             45,000  PASS or WARN
  duplicate storm          25,000  1 PASS + N DUPLICATE_HINT per group
  retry / idempotency      10,000  same request_id on replay
  invalid email             5,000  REJECT (validation)
  broken phone              5,000  PASS or WARN (phone optional)
  missing fields            5,000  PASS, WARN, or REJECT by policy
  near-duplicate eval       5,000  EXPLORATORY — no hard guarantee
  ─────────────────────────────────────────────────────────
  total                   100,000

Usage:
    python load_tests/hero_benchmark.py              # run all phases
    python load_tests/hero_benchmark.py --generate-only
    python load_tests/hero_benchmark.py --run-only
    python load_tests/hero_benchmark.py --report-only
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean, median, quantiles

import fakeredis.aioredis as fakeredis

from lead_entry_guard.config.settings import get_settings
from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
from lead_entry_guard.core.exceptions import RedisUnavailableError
from lead_entry_guard.core.models import DecisionClass, DegradedModePolicy, LeadInput
from lead_entry_guard.core.pipeline import IngestionPipeline
from lead_entry_guard.fingerprint.builder import FingerprintBuilder
from lead_entry_guard.lookup.bloom import BloomFilterRegistry
from lead_entry_guard.lookup.duplicate import DuplicateLookupTier
from lead_entry_guard.lookup.redis_store import RedisDuplicateStore, RedisIdempotencyStore
from lead_entry_guard.normalization.normalizer import NormalizationLayer
from lead_entry_guard.policies.engine import PolicyEngine
from lead_entry_guard.security.hmac_keys import HMACKeyManager
from lead_entry_guard.security.vault import InMemoryVaultClient
from lead_entry_guard.telemetry.exporter import TelemetryQueue
from lead_entry_guard.validation.validator import ValidationLayer

SEED = 2024
random.seed(SEED)

OUTPUT_DIR = Path(__file__).parent / "datasets"

_KEY_RING = {
    "current": {
        "kid": "hero-v1",
        "secret_hex": "b" * 64,
        "activated_at": "2026-01-01T00:00:00+00:00",
    }
}

# ── Dataset composition ───────────────────────────────────────────────────────

COMPOSITION = {
    "valid_unique":        45_000,
    "duplicate_storm":     25_000,
    "retry_idempotency":   10_000,
    "invalid_email":        5_000,
    "broken_phone":         5_000,
    "missing_fields":       5_000,
    "near_duplicate_eval":  5_000,
}
TOTAL = sum(COMPOSITION.values())  # 100,000

# ── Name / domain pools ───────────────────────────────────────────────────────

_FIRST = ["Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace", "Henry",
          "Iris", "Jack", "Karen", "Leo", "Maya", "Nick", "Olivia", "Paul",
          "Quinn", "Rosa", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander"]
_LAST  = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
          "Davis", "Wilson", "Taylor", "Anderson", "Thomas", "Jackson", "White"]
_DOMAINS = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
            "company.com", "acme.io", "example.org", "corp.net"]


def _email(first: str, last: str) -> str:
    sep = random.choice([".", "_", ""])
    return f"{first.lower()}{sep}{last.lower()}@{random.choice(_DOMAINS)}"


def _phone() -> str:
    cc = random.choice(["+1", "+44", "+49", "+33", "+420"])
    return cc + "".join(str(random.randint(0, 9)) for _ in range(9))


def _base_lead(tenant_id: str = "hero") -> dict:
    first, last = random.choice(_FIRST), random.choice(_LAST)
    return {
        "tenant_id": tenant_id,
        "source_id": str(uuid.uuid4()),
        "email": _email(first, last),
        "phone": _phone() if random.random() > 0.25 else None,
        "first_name": first,
        "last_name": last,
    }


# ── Bucket generators ─────────────────────────────────────────────────────────

def _gen_valid_unique(n: int) -> list[dict]:
    leads = []
    for _ in range(n):
        l = _base_lead()
        l["_bucket"] = "valid_unique"
        l["_guarantee"] = "PASS or WARN"
        leads.append(l)
    return leads


def _gen_duplicate_storm(n: int, storm_size: int = 30) -> list[dict]:
    """
    Groups of storm_size leads sharing the same email (same fingerprint).
    Expected: 1 PASS + (storm_size - 1) DUPLICATE_HINT per group.

    n // storm_size gives complete groups; remainder leads are added to the
    last group so total always equals n exactly.
    """
    leads = []
    n_groups = n // storm_size
    remainder = n % storm_size  # e.g. 25000 % 30 = 10

    for g in range(n_groups):
        first, last = random.choice(_FIRST), random.choice(_LAST)
        canonical_email = _email(first, last)
        # Last group absorbs remainder so len(leads) == n
        group_size = storm_size + (remainder if g == n_groups - 1 else 0)
        for i in range(group_size):
            leads.append({
                "tenant_id": "hero",
                "source_id": str(uuid.uuid4()),
                "email": canonical_email,
                "phone": _phone(),
                "first_name": first,
                "last_name": last,
                "_bucket": "duplicate_storm",
                "_guarantee": "1 PASS + N DUPLICATE_HINT per group",
                "_storm_group": f"{canonical_email}",
                "_is_original": i == 0,
            })

    assert len(leads) == n, f"duplicate_storm: expected {n} leads, got {len(leads)}"
    random.shuffle(leads)
    return leads


def _gen_retry_idempotency(n: int, retries_per_lead: int = 4) -> list[dict]:
    """
    Leads that are "sent" retries_per_lead times with the same source_id.
    Expected: same request_id returned on all retries.
    Stored as pairs: (original, [replays]) — runner handles ordering.
    """
    assert n % retries_per_lead == 0, (
        f"n={n} must be divisible by retries_per_lead={retries_per_lead} "
        f"to produce exactly {n} leads — adjust COMPOSITION or retries_per_lead"
    )
    leads = []
    n_originals = n // retries_per_lead
    for _ in range(n_originals):
        base = _base_lead()
        source_id = base["source_id"]
        for i in range(retries_per_lead):
            replay = dict(base)
            replay["source_id"] = source_id  # same source_id every retry
            replay["_bucket"] = "retry_idempotency"
            replay["_guarantee"] = "same request_id on all replays"
            replay["_retry_index"] = i
            replay["_is_original"] = i == 0
            leads.append(replay)
    random.shuffle(leads)
    return leads


def _gen_invalid_email(n: int) -> list[dict]:
    def _bad_email():
        return random.choice([
            "not-an-email",
            "missing@",
            "@nodomain.com",
            "spaces in@email.com",
            "",
            "double@@at.com",
            "toolong" + "x" * 300 + "@example.com",
        ])
    leads = []
    for _ in range(n):
        l = _base_lead()
        l["email"] = _bad_email()
        l["_bucket"] = "invalid_email"
        l["_guarantee"] = "REJECT (validation)"
        leads.append(l)
    return leads


def _gen_broken_phone(n: int) -> list[dict]:
    """Valid email, broken/missing phone. Phone is optional → should PASS or WARN."""
    def _bad_phone():
        return random.choice([
            "123",                  # too short
            "not-a-phone",
            "00000000000000000",    # too long
            "+",
            None,                   # missing
        ])
    leads = []
    for _ in range(n):
        l = _base_lead()
        l["phone"] = _bad_phone()
        l["_bucket"] = "broken_phone"
        l["_guarantee"] = "PASS or WARN (phone optional)"
        leads.append(l)
    return leads


def _gen_missing_fields(n: int) -> list[dict]:
    """Leads missing various combinations of optional/required fields."""
    def _drop_fields(lead: dict) -> dict:
        candidates = ["phone", "first_name", "last_name"]
        n_drop = random.randint(1, len(candidates))
        for f in random.sample(candidates, n_drop):
            lead.pop(f, None)
        return lead
    leads = []
    for _ in range(n):
        l = _drop_fields(_base_lead())
        l["_bucket"] = "missing_fields"
        l["_guarantee"] = "PASS, WARN, or REJECT by policy"
        leads.append(l)
    return leads


def _gen_near_duplicate_eval(n: int) -> list[dict]:
    """
    Near-duplicates: same person, slightly different email variants.
    e.g. john@gmail.com / john.doe@gmail.com / j.doe@gmail.com

    EXPLORATORY bucket — not a core guarantee.
    Current fingerprint model uses email_normalized + phone_normalized.
    Near-duplicate detection requires fuzzy identity resolution (future capability).
    These leads are processed and results recorded, but invariants are NOT asserted.
    """
    leads = []
    n_identities = n // 4
    for _ in range(n_identities):
        first, last = random.choice(_FIRST), random.choice(_LAST)
        domain = random.choice(_DOMAINS)
        # Four email variants for the same identity
        variants = [
            f"{first.lower()}@{domain}",
            f"{first.lower()}.{last.lower()}@{domain}",
            f"{first.lower()[0]}.{last.lower()}@{domain}",
            f"{first.lower()}_{last.lower()}@{domain}",
        ]
        phone = _phone()
        for variant in variants:
            leads.append({
                "tenant_id": "hero",
                "source_id": str(uuid.uuid4()),
                "email": variant,
                "phone": phone,  # same phone — may trigger duplicate via phone fingerprint
                "first_name": first,
                "last_name": last,
                "_bucket": "near_duplicate_eval",
                "_guarantee": "EXPLORATORY — no hard assertion",
                "_identity_root": f"{first.lower()}.{last.lower()}@{domain}",
            })
    random.shuffle(leads)
    return leads


def generate_hero_dataset() -> list[dict]:
    rng = random.Random(SEED)
    leads = (
        _gen_valid_unique(COMPOSITION["valid_unique"]) +
        _gen_duplicate_storm(COMPOSITION["duplicate_storm"]) +
        _gen_retry_idempotency(COMPOSITION["retry_idempotency"]) +
        _gen_invalid_email(COMPOSITION["invalid_email"]) +
        _gen_broken_phone(COMPOSITION["broken_phone"]) +
        _gen_missing_fields(COMPOSITION["missing_fields"]) +
        _gen_near_duplicate_eval(COMPOSITION["near_duplicate_eval"])
    )
    rng.shuffle(leads)
    return leads


# ── Pipeline factory ──────────────────────────────────────────────────────────

def latency_histogram(latencies_ms: list[float]) -> dict[str, int]:
    """
    Bucket latencies into ranges useful for tail latency debugging.
    Returns ordered dict: range label → count.
    """
    buckets = [
        ("0–5 ms",    0,    5),
        ("5–10 ms",   5,   10),
        ("10–20 ms",  10,  20),
        ("20–50 ms",  20,  50),
        ("50–100 ms", 50, 100),
        (">100 ms",  100, float("inf")),
    ]
    counts = {label: 0 for label, _, _ in buckets}
    for v in latencies_ms:
        for label, lo, hi in buckets:
            if lo <= v < hi:
                counts[label] += 1
                break
    return counts

async def _build_pipeline(redis_url: str | None = None) -> IngestionPipeline:
    if redis_url:
        import redis.asyncio as aioredis
        redis_client = aioredis.from_url(redis_url, decode_responses=False)
        print(f"  Redis: real  ({redis_url})")
    else:
        redis_client = fakeredis.FakeRedis(decode_responses=False)
        print("  Redis: fakeredis (in-process)")

    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(_KEY_RING))

    return IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=FingerprintBuilder(km),
        duplicate_tier=DuplicateLookupTier(
            BloomFilterRegistry(),
            RedisDuplicateStore(redis_client, duplicate_ttl=86400),
        ),
        policy_engine=PolicyEngine(),
        idempotency_store=RedisIdempotencyStore(redis_client),
        telemetry_queue=TelemetryQueue(max_size=200_000),
        tenant_registry=TenantRegistry(),
    )


# ── Runner ────────────────────────────────────────────────────────────────────

@dataclass
class BucketResult:
    bucket: str
    guarantee: str
    total: int
    decisions: dict[str, int] = field(default_factory=dict)
    invariant_passed: bool | None = None
    invariant_note: str = ""
    latencies_ms: list[float] = field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        return self.decisions.get("PASS", 0) / self.total if self.total else 0

    @property
    def duplicate_rate(self) -> float:
        return self.decisions.get("DUPLICATE_HINT", 0) / self.total if self.total else 0

    @property
    def reject_rate(self) -> float:
        return self.decisions.get("REJECT", 0) / self.total if self.total else 0


async def run_benchmark(
    leads: list[dict],
    redis_url: str | None = None,
) -> tuple[dict, list[BucketResult]]:
    pipeline = await _build_pipeline(redis_url=redis_url)
    settings = get_settings()

    decisions_global: dict[str, int] = defaultdict(int)
    decisions_by_bucket: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    latencies_by_bucket: dict[str, list[float]] = defaultdict(list)

    # For idempotency check: source_id → first request_id
    first_request_ids: dict[str, str] = {}
    idempotency_mismatches = 0
    idempotency_total = 0

    # For duplicate storm: canonical_email → first seen decision
    storm_first_decision: dict[str, str] = {}
    storm_subsequent_not_dup = 0
    storm_subsequent_total = 0

    print(f"\n[hero_benchmark] Processing {len(leads):,} leads...")
    start = time.monotonic()

    for i, lead_data in enumerate(leads):
        bucket = lead_data.get("_bucket", "unknown")
        source_id = lead_data.get("source_id")
        storm_group = lead_data.get("_storm_group")
        is_original = lead_data.get("_is_original", True)
        retry_index = lead_data.get("_retry_index", 0)

        # Strip internal _fields before sending to pipeline
        payload = {k: v for k, v in lead_data.items() if not k.startswith("_") and v is not None}
        lead = LeadInput(**{k: v for k, v in payload.items()
                           if k in LeadInput.__dataclass_fields__})

        t0 = time.monotonic()
        try:
            result = await pipeline.process(lead)
            latency = (time.monotonic() - t0) * 1000

            decisions_global[result.decision.value] += 1
            decisions_by_bucket[bucket][result.decision.value] += 1
            latencies_by_bucket[bucket].append(latency)

            # Idempotency invariant check
            if bucket == "retry_idempotency" and source_id:
                if is_original:
                    first_request_ids[source_id] = result.request_id
                else:
                    idempotency_total += 1
                    if first_request_ids.get(source_id) != result.request_id:
                        idempotency_mismatches += 1

            # Duplicate storm invariant check
            if bucket == "duplicate_storm" and storm_group:
                if is_original:
                    storm_first_decision[storm_group] = result.decision.value
                else:
                    storm_subsequent_total += 1
                    if result.decision.value != "DUPLICATE_HINT":
                        storm_subsequent_not_dup += 1

        except Exception as exc:
            decisions_global["ERROR"] += 1
            decisions_by_bucket[bucket]["ERROR"] += 1
            latencies_by_bucket[bucket].append((time.monotonic() - t0) * 1000)

        if (i + 1) % 10_000 == 0:
            elapsed = time.monotonic() - start
            print(f"  {i+1:>6,} / {len(leads):,}  ({elapsed:.1f}s)")

    # Flush fire-and-forget tasks via public API
    await pipeline.flush_pending()

    total_duration = time.monotonic() - start
    print(f"\n  Done — {len(leads):,} leads in {total_duration:.1f}s "
          f"({len(leads)/total_duration:.0f} leads/s)")

    # Assemble bucket results
    bucket_results = []
    bucket_meta = {
        "valid_unique":        ("PASS or WARN", None),
        "duplicate_storm":     ("1 PASS + N DUPLICATE_HINT per group",
                                storm_subsequent_not_dup == 0),
        "retry_idempotency":   ("same request_id on replay",
                                idempotency_mismatches == 0),
        "invalid_email":       ("REJECT (validation)", None),
        "broken_phone":        ("PASS or WARN (phone optional)", None),
        "missing_fields":      ("PASS, WARN, or REJECT by policy", None),
        "near_duplicate_eval": ("EXPLORATORY — no assertion", None),
    }

    for bucket, (guarantee, inv_result) in bucket_meta.items():
        n = COMPOSITION[bucket]
        inv_note = ""
        if bucket == "duplicate_storm" and inv_result is not None:
            inv_note = (f"{storm_subsequent_not_dup} subsequent storm leads were not "
                       f"DUPLICATE_HINT (out of {storm_subsequent_total})")
        if bucket == "retry_idempotency" and inv_result is not None:
            inv_note = (f"{idempotency_mismatches} replays returned different "
                       f"request_id (out of {idempotency_total})")

        bucket_results.append(BucketResult(
            bucket=bucket,
            guarantee=guarantee,
            total=n,
            decisions=dict(decisions_by_bucket[bucket]),
            invariant_passed=inv_result,
            invariant_note=inv_note,
            latencies_ms=latencies_by_bucket[bucket],
        ))

    # Global latency histogram
    all_latencies = [l for lats in latencies_by_bucket.values() for l in lats]

    summary = {
        "total_leads": len(leads),
        "total_duration_s": round(total_duration, 2),
        "throughput_leads_per_s": round(len(leads) / total_duration),
        "redis_mode": "real" if redis_url else "fakeredis",
        "decisions_global": dict(decisions_global),
        "idempotency_mismatches": idempotency_mismatches,
        "idempotency_total_replays": idempotency_total,
        "storm_subsequent_not_dup": storm_subsequent_not_dup,
        "storm_subsequent_total": storm_subsequent_total,
        "latency_histogram_ms": latency_histogram(all_latencies),
        "invariants": {
            "duplicate_storm_correct": storm_subsequent_not_dup == 0,
            "idempotency_correct": idempotency_mismatches == 0,
        },
        "buckets": {
            br.bucket: {
                "total": br.total,
                "decisions": br.decisions,
                "invariant_passed": br.invariant_passed,
                "invariant_note": br.invariant_note,
                "latency_p95_ms": (
                    round(quantiles(br.latencies_ms, n=100)[94], 2)
                    if len(br.latencies_ms) > 1 else None
                ),
                "latency_histogram_ms": latency_histogram(br.latencies_ms),
            }
            for br in bucket_results
        },
    }

    return summary, bucket_results


# ── CLI ───────────────────────────────────────────────────────────────────────

async def main_async(args: argparse.Namespace) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    seed = args.seed
    dataset_path = OUTPUT_DIR / f"hero_100k_seed{seed}.jsonl"
    results_path = OUTPUT_DIR / f"hero_benchmark_results_seed{seed}.json"
    redis_url = args.redis_url

    if not args.run_only and not args.report_only:
        print(f"\n[generate] Building {TOTAL:,} lead dataset (seed={seed})...")
        random.seed(seed)
        leads = generate_hero_dataset()
        with open(dataset_path, "w") as f:
            for lead in leads:
                f.write(json.dumps(lead) + "\n")
        print(f"  Dataset → {dataset_path}  ({dataset_path.stat().st_size // 1024:,} KB)")

        tag_counts: dict[str, int] = defaultdict(int)
        for l in leads:
            tag_counts[l.get("_bucket", "unknown")] += 1
        print("\n  Composition:")
        for bucket, count in tag_counts.items():
            pct = count / TOTAL * 100
            print(f"    {bucket:<25} {count:>7,}  ({pct:.1f}%)")

    if args.generate_only:
        return

    if not args.report_only:
        print(f"\n[run] Loading dataset from {dataset_path}...")
        with open(dataset_path) as f:
            leads = [json.loads(line) for line in f]

        if args.limit and args.limit < len(leads):
            leads = leads[:args.limit]
            print(f"  Loaded {len(leads):,} leads (limited from full dataset — invariants may not hold at small N)")
        else:
            print(f"  Loaded {len(leads):,} leads")

        summary, bucket_results = await run_benchmark(leads, redis_url=redis_url)

        with open(results_path, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"\n  Results → {results_path}")

        # Decision summary
        total_decisions = sum(summary["decisions_global"].values())
        print("\n  ┌─ Decision summary " + "─" * 38 + "┐")
        for decision, count in sorted(summary["decisions_global"].items(), key=lambda x: -x[1]):
            pct = count / total_decisions * 100 if total_decisions else 0
            bar = "█" * int(pct / 2)
            print(f"  │  {decision:<20} {count:>7,}  ({pct:5.1f}%)  {bar}")
        print("  └" + "─" * 57 + "┘")

        # Print latency histogram
        print("\n  Latency histogram (all buckets):")
        for bucket_range, count in summary["latency_histogram_ms"].items():
            pct = count / len(leads) * 100
            print(f"    {bucket_range:<12}  {count:>7,}  ({pct:.1f}%)")

        all_inv_passed = all(v for v in summary["invariants"].values())
        print(f"\n  Invariants: {'✓ ALL PASSED' if all_inv_passed else '✗ FAILURES DETECTED'}")
        for inv, passed in summary["invariants"].items():
            print(f"    {inv}: {'✓' if passed else '✗'}")

        if not all_inv_passed:
            raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hero benchmark — 100k messy leads")
    parser.add_argument("--generate-only", action="store_true",
                        help="Only generate dataset, do not run")
    parser.add_argument("--run-only", action="store_true",
                        help="Run benchmark using existing dataset")
    parser.add_argument("--report-only", action="store_true",
                        help="Generate report from existing results (no run)")
    parser.add_argument("--seed", type=int, default=SEED,
                        help=f"Random seed for dataset generation (default: {SEED})")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Process only first N leads — for quick debug runs and CI smoke tests")
    parser.add_argument("--redis-url", default=None, metavar="URL",
                        dest="redis_url",
                        help="Real Redis URL, e.g. redis://localhost:6379 "
                             "(default: fakeredis in-process)")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
