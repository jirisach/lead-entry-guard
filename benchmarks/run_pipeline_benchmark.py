"""
Production-like benchmark — Lead Entry Guard v4.

Measures real-world pipeline performance with:
  - real Redis OR fakeredis (--redis-url to switch)
  - concurrent batch processing (--concurrency)
  - telemetry export layer active
  - memory growth tracking
  - latency p50/p95/p99
  - telemetry queue backlog monitoring

This is the production-credibility benchmark.
hero_benchmark.py tests correctness at 100k leads (sequential, fakeredis).
This benchmark tests performance under production-like conditions.

Usage:
    # fakeredis (no infrastructure needed)
    python benchmarks/run_pipeline_benchmark.py

    # real Redis via Docker Compose
    python benchmarks/run_pipeline_benchmark.py --redis-url redis://localhost:6379

    # custom concurrency and lead count
    python benchmarks/run_pipeline_benchmark.py --concurrency 100 --leads 10000

    # full production simulation
    python benchmarks/run_pipeline_benchmark.py \\
        --redis-url redis://localhost:6379 \\
        --concurrency 200 \\
        --leads 50000 \\
        --output benchmarks/results/
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import time
import tracemalloc
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, quantiles
from typing import Any

# ── Optional real Redis ───────────────────────────────────────────────────────

def _make_redis_client(redis_url: str | None):
    if redis_url:
        import redis.asyncio as aioredis
        return aioredis.from_url(redis_url, decode_responses=False)
    else:
        import fakeredis.aioredis as fakeredis
        return fakeredis.FakeRedis(decode_responses=False)


# ── Pipeline factory ──────────────────────────────────────────────────────────

from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
from lead_entry_guard.core.models import DecisionClass, LeadInput, SalvagePolicy
from lead_entry_guard.core.pipeline import IngestionPipeline
from lead_entry_guard.fingerprint.builder import FingerprintBuilder
from lead_entry_guard.lookup.bloom import BloomFilterRegistry
from lead_entry_guard.lookup.duplicate import DuplicateLookupTier
from lead_entry_guard.lookup.redis_store import RedisDuplicateStore, RedisIdempotencyStore
from lead_entry_guard.normalization.normalizer import NormalizationLayer
from lead_entry_guard.policies.engine import PolicyEngine
from lead_entry_guard.security.hmac_keys import HMACKeyManager
from lead_entry_guard.security.vault import InMemoryVaultClient
from lead_entry_guard.telemetry.exporter import TelemetryExporter, TelemetryQueue
from lead_entry_guard.validation.validator import ValidationLayer
from tests.fixtures.common import make_key_ring


async def _build_pipeline(
    redis_url: str | None,
    telemetry_queue_size: int = 5000,
) -> tuple[IngestionPipeline, TelemetryExporter | None]:
    """Build pipeline with real or fake Redis and active telemetry export."""
    redis_client = _make_redis_client(redis_url)
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))

    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)

    registry = TenantRegistry()
    registry.register(TenantConfig(
        tenant_id="bench_strict",
        salvage_policy=SalvagePolicy.STRICT,
    ))
    registry.register(TenantConfig(
        tenant_id="bench_salvage",
        salvage_policy=SalvagePolicy.SALVAGE,
    ))

    telemetry_queue = TelemetryQueue(max_size=telemetry_queue_size)

    # Start telemetry exporter if available
    exporter = None
    try:
        exporter = TelemetryExporter(telemetry_queue)
        await exporter.start()
    except Exception:
        exporter = None  # telemetry export optional — benchmark continues

    pipeline = IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=telemetry_queue,
        tenant_registry=registry,
    )

    return pipeline, exporter


# ── Dataset generator ─────────────────────────────────────────────────────────

def _make_leads(n: int) -> list[LeadInput]:
    """
    Realistic lead mix:
      50% clean unique (bench_strict tenant)
      25% duplicates (same email as earlier lead)
      15% dirty (invalid phone, valid email — triggers salvage)
      10% invalid (bad email → REJECT)
    """
    leads: list[LeadInput] = []
    originals: list[str] = []

    n_unique = int(n * 0.50)
    n_dupes = int(n * 0.25)
    n_dirty = int(n * 0.15)
    n_invalid = n - n_unique - n_dupes - n_dirty

    for i in range(n_unique):
        email = f"bench.unique.{i}.{uuid.uuid4().hex[:6]}@example.com"
        originals.append(email)
        leads.append(LeadInput(
            tenant_id="bench_strict",
            email=email,
            phone="+12025550100",
            source_id=str(uuid.uuid4()),
        ))

    for i in range(n_dupes):
        email = originals[i % len(originals)]
        leads.append(LeadInput(
            tenant_id="bench_strict",
            email=email,
            source_id=str(uuid.uuid4()),
        ))

    for i in range(n_dirty):
        email = f"bench.dirty.{i}@example.com"
        leads.append(LeadInput(
            tenant_id="bench_salvage",
            email=email,
            phone="not-a-phone",
            source_id=str(uuid.uuid4()),
        ))

    for i in range(n_invalid):
        leads.append(LeadInput(
            tenant_id="bench_strict",
            email=f"not-an-email-{i}",
            source_id=str(uuid.uuid4()),
        ))

    random.shuffle(leads)
    return leads


# ── Result model ──────────────────────────────────────────────────────────────

@dataclass
class BenchmarkResult:
    redis_mode: str
    n_leads: int
    concurrency: int
    duration_s: float
    decisions: dict[str, int]
    latencies_ms: list[float]
    memory_start_mb: float
    memory_peak_mb: float
    telemetry_queue_size_at_end: int
    errors: int
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def throughput_rps(self) -> float:
        return self.n_leads / self.duration_s if self.duration_s > 0 else 0

    @property
    def memory_growth_mb(self) -> float:
        return self.memory_peak_mb - self.memory_start_mb

    def latency_percentiles(self) -> dict[str, float]:
        if not self.latencies_ms:
            return {}
        qs = quantiles(self.latencies_ms, n=100)
        return {
            "p50": round(median(self.latencies_ms), 2),
            "p90": round(qs[89], 2),
            "p95": round(qs[94], 2),
            "p99": round(qs[98], 2),
            "mean": round(mean(self.latencies_ms), 2),
            "max": round(max(self.latencies_ms), 2),
        }

    def print_report(self) -> None:
        pct = self.latency_percentiles()
        total = self.n_leads
        print(f"\n{'═' * 62}")
        print(f"  Lead Entry Guard — Production Benchmark")
        print(f"{'─' * 62}")
        print(f"  Redis:        {self.redis_mode}")
        print(f"  Leads:        {self.n_leads:,}")
        print(f"  Concurrency:  {self.concurrency}")
        print(f"  Duration:     {self.duration_s:.2f}s")
        print(f"  Throughput:   {self.throughput_rps:,.0f} leads/s")
        print(f"  Errors:       {self.errors}")
        print()
        print(f"  Decision breakdown:")
        for decision, count in sorted(self.decisions.items(), key=lambda x: -x[1]):
            bar = "█" * int(count / total * 30)
            print(f"    {decision:<22} {count:>7,}  ({count/total*100:5.1f}%)  {bar}")
        print()
        print(f"  Latency (ms):")
        for pname, val in pct.items():
            print(f"    {pname:<6}  {val:>8.2f} ms")
        print()
        print(f"  Memory:")
        print(f"    start:   {self.memory_start_mb:.1f} MB")
        print(f"    peak:    {self.memory_peak_mb:.1f} MB")
        print(f"    growth:  {self.memory_growth_mb:.1f} MB")
        print()
        print(f"  Telemetry queue at end: {self.telemetry_queue_size_at_end}")
        print(f"{'═' * 62}\n")

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "redis_mode": self.redis_mode,
            "n_leads": self.n_leads,
            "concurrency": self.concurrency,
            "duration_s": round(self.duration_s, 3),
            "throughput_rps": round(self.throughput_rps, 1),
            "errors": self.errors,
            "decisions": self.decisions,
            "latency_ms": self.latency_percentiles(),
            "memory_mb": {
                "start": round(self.memory_start_mb, 2),
                "peak": round(self.memory_peak_mb, 2),
                "growth": round(self.memory_growth_mb, 2),
            },
            "telemetry_queue_at_end": self.telemetry_queue_size_at_end,
        }


# ── Benchmark runner ──────────────────────────────────────────────────────────

async def run_benchmark(
    redis_url: str | None,
    n_leads: int,
    concurrency: int,
) -> BenchmarkResult:
    redis_mode = f"real ({redis_url})" if redis_url else "fakeredis (in-process)"
    print(f"\n[setup] Building pipeline — {redis_mode}")

    pipeline, exporter = await _build_pipeline(redis_url)
    leads = _make_leads(n_leads)

    print(f"[run]   {n_leads:,} leads, concurrency={concurrency}")

    tracemalloc.start()
    mem_start = tracemalloc.get_traced_memory()[0] / 1024 / 1024

    semaphore = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    decisions: dict[str, int] = defaultdict(int)
    errors = 0

    async def process_one(lead: LeadInput) -> None:
        nonlocal errors
        async with semaphore:
            t0 = time.monotonic()
            try:
                result = await pipeline.process(lead)
                latencies.append((time.monotonic() - t0) * 1000)
                decisions[result.decision.value] += 1
            except Exception:
                errors += 1
                latencies.append((time.monotonic() - t0) * 1000)

    start = time.monotonic()
    tasks = [process_one(lead) for lead in leads]

    completed = 0
    for coro in asyncio.as_completed(tasks):
        await coro
        completed += 1
        if completed % max(1, n_leads // 20) == 0:
            elapsed = time.monotonic() - start
            rps = completed / elapsed if elapsed > 0 else 0
            print(f"  {completed:>7,}/{n_leads:,}  {rps:,.0f} req/s", end="\r")

    duration = time.monotonic() - start
    print()

    await pipeline.flush_pending()

    _, mem_peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    mem_peak = mem_peak_bytes / 1024 / 1024

    telemetry_backlog = pipeline._telemetry._queue.qsize()

    if exporter:
        try:
            await exporter.stop()
        except Exception:
            pass

    return BenchmarkResult(
        redis_mode=redis_mode,
        n_leads=n_leads,
        concurrency=concurrency,
        duration_s=duration,
        decisions=dict(decisions),
        latencies_ms=latencies,
        memory_start_mb=mem_start,
        memory_peak_mb=mem_peak,
        telemetry_queue_size_at_end=telemetry_backlog,
        errors=errors,
    )


# ── Concurrency sweep ─────────────────────────────────────────────────────────

async def run_concurrency_sweep(
    redis_url: str | None,
    levels: list[int],
    leads_per_level: int,
) -> list[BenchmarkResult]:
    """Run benchmark at multiple concurrency levels to find throughput ceiling."""
    results = []
    for level in levels:
        print(f"\n[sweep] concurrency={level}")
        result = await run_benchmark(redis_url, leads_per_level, level)
        result.print_report()
        results.append(result)
    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Lead Entry Guard — production-like benchmark"
    )
    parser.add_argument(
        "--redis-url", default=None, metavar="URL",
        help="Redis URL (default: fakeredis in-process). "
             "Example: redis://localhost:6379",
    )
    parser.add_argument(
        "--leads", type=int, default=5_000,
        help="Number of leads to process (default: 5000)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=50,
        help="Max concurrent pipeline.process() calls (default: 50)",
    )
    parser.add_argument(
        "--sweep", action="store_true",
        help="Run concurrency sweep (10, 50, 100, 200) instead of single run",
    )
    parser.add_argument(
        "--sweep-levels", type=int, nargs="+", default=[10, 50, 100, 200],
        metavar="N",
        help="Concurrency levels for sweep (default: 10 50 100 200)",
    )
    parser.add_argument(
        "--output", default=None, metavar="DIR",
        help="Directory to save JSON results (default: no file output)",
    )
    args = parser.parse_args()

    if args.sweep:
        results = await run_concurrency_sweep(
            redis_url=args.redis_url,
            levels=args.sweep_levels,
            leads_per_level=args.leads,
        )
        if args.output:
            out_dir = Path(args.output)
            out_dir.mkdir(parents=True, exist_ok=True)
            sweep_data = [r.to_dict() for r in results]
            out_path = out_dir / f"sweep_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
            with open(out_path, "w") as f:
                json.dump(sweep_data, f, indent=2)
            print(f"Sweep results → {out_path}")

        # Print sweep summary
        print(f"\n{'─' * 50}")
        print("  Concurrency sweep summary")
        print(f"{'─' * 50}")
        print(f"  {'Concurrency':>12}  {'Throughput':>12}  {'p99 ms':>8}")
        for r in results:
            pct = r.latency_percentiles()
            print(f"  {r.concurrency:>12}  {r.throughput_rps:>10,.0f}/s  {pct.get('p99', 0):>8.1f}")
        print(f"{'─' * 50}\n")

    else:
        result = await run_benchmark(
            redis_url=args.redis_url,
            n_leads=args.leads,
            concurrency=args.concurrency,
        )
        result.print_report()

        if args.output:
            out_dir = Path(args.output)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"benchmark_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
            with open(out_path, "w") as f:
                json.dump(result.to_dict(), f, indent=2)
            print(f"Results → {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
