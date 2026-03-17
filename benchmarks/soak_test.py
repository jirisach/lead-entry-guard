"""
Soak test — Lead Entry Guard v4.

Runs the pipeline continuously for a configurable duration to detect:
  - memory drift (leak or unbounded growth)
  - throughput drift (degradation over time)
  - telemetry queue backlog growth
  - latency drift (p99 degradation over time)

Not a throughput benchmark — stability over time is the goal.

Usage:
    # 5-minute soak (default)
    python benchmarks/soak_test.py --redis-url redis://localhost:6379

    # 30-minute soak
    python benchmarks/soak_test.py --redis-url redis://localhost:6379 --duration 1800

    # fakeredis (no infrastructure)
    python benchmarks/soak_test.py --duration 120

    # save results
    python benchmarks/soak_test.py --redis-url redis://localhost:6379 --output benchmarks/results/
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, quantiles
from typing import Any

import psutil

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
from lead_entry_guard.telemetry.exporter import TelemetryQueue
from lead_entry_guard.validation.validator import ValidationLayer
from tests.fixtures.common import make_key_ring


# ── Pipeline factory ──────────────────────────────────────────────────────────

def _make_redis_client(redis_url: str | None):
    if redis_url:
        import redis.asyncio as aioredis
        return aioredis.from_url(redis_url, decode_responses=False)
    else:
        import fakeredis.aioredis as fakeredis
        return fakeredis.FakeRedis(decode_responses=False)


async def _drain_telemetry(queue: TelemetryQueue, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set() or not queue._queue.empty():
        try:
            await asyncio.wait_for(queue._queue.get(), timeout=0.1)
            queue.task_done()
        except asyncio.TimeoutError:
            if stop_event.is_set():
                break


async def _build_pipeline(redis_url: str | None) -> tuple[IngestionPipeline, asyncio.Event]:
    redis_client = _make_redis_client(redis_url)
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))

    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)

    registry = TenantRegistry()
    registry.register(TenantConfig(tenant_id="soak_t1", salvage_policy=SalvagePolicy.STRICT))
    registry.register(TenantConfig(tenant_id="soak_t2", salvage_policy=SalvagePolicy.SALVAGE))

    telemetry_queue = TelemetryQueue(max_size=10_000)
    stop_event = asyncio.Event()
    asyncio.create_task(_drain_telemetry(telemetry_queue, stop_event), name="soak_telemetry_drain")

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
    return pipeline, stop_event


# ── Lead generator ────────────────────────────────────────────────────────────

_EMAIL_POOL = [f"soak.user.{i}@example.com" for i in range(500)]
_DIRTY_POOL = [f"soak.dirty.{i}@example.com" for i in range(200)]


def _next_lead() -> LeadInput:
    """Generate a realistic mix of leads for soak testing."""
    roll = random.random()
    if roll < 0.50:
        # Clean unique — may become duplicate if email reused
        return LeadInput(
            tenant_id="soak_t1",
            email=random.choice(_EMAIL_POOL),
            phone="+12025550100",
            source_id=str(uuid.uuid4()),
        )
    elif roll < 0.65:
        # Dirty phone — salvage path
        return LeadInput(
            tenant_id="soak_t2",
            email=random.choice(_DIRTY_POOL),
            phone="not-a-phone",
            source_id=str(uuid.uuid4()),
        )
    elif roll < 0.75:
        # Invalid email — early reject
        return LeadInput(
            tenant_id="soak_t1",
            email=f"bad-email-{random.randint(0, 999)}",
            source_id=str(uuid.uuid4()),
        )
    else:
        # Idempotency replay — same source_id
        return LeadInput(
            tenant_id="soak_t1",
            email=random.choice(_EMAIL_POOL),
            source_id=f"soak-replay-{random.randint(0, 50)}",
        )


# ── Snapshot model ────────────────────────────────────────────────────────────

@dataclass
class SoakSnapshot:
    elapsed_s: float
    throughput_rps: float
    p50_ms: float
    p99_ms: float
    memory_mb: float
    telemetry_backlog: int
    decisions: dict[str, int]
    errors: int


@dataclass
class SoakResult:
    redis_mode: str
    duration_s: float
    concurrency: int
    total_leads: int
    total_errors: int
    snapshots: list[SoakSnapshot] = field(default_factory=list)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def print_report(self) -> None:
        print(f"\n{'═' * 65}")
        print(f"  Lead Entry Guard — Soak Test Report")
        print(f"{'─' * 65}")
        print(f"  Redis:        {self.redis_mode}")
        print(f"  Duration:     {self.duration_s:.0f}s ({self.duration_s/60:.1f} min)")
        print(f"  Concurrency:  {self.concurrency}")
        print(f"  Total leads:  {self.total_leads:,}")
        print(f"  Total errors: {self.total_errors}")
        print(f"  Avg throughput: {self.total_leads/self.duration_s:.0f} leads/s")
        print()

        if len(self.snapshots) < 2:
            print("  Not enough snapshots for drift analysis.")
            return

        # Drift analysis
        first = self.snapshots[0]
        last = self.snapshots[-1]
        mid = self.snapshots[len(self.snapshots) // 2]

        print(f"  {'Metric':<25} {'Start':>10} {'Mid':>10} {'End':>10}  {'Drift':>10}")
        print(f"  {'─'*25} {'─'*10} {'─'*10} {'─'*10}  {'─'*10}")

        def _drift(a: float, b: float) -> str:
            if a == 0:
                return "n/a"
            pct = (b - a) / a * 100
            arrow = "↑" if pct > 5 else ("↓" if pct < -5 else "→")
            return f"{arrow} {abs(pct):.1f}%"

        print(f"  {'Throughput (req/s)':<25} {first.throughput_rps:>10.0f} {mid.throughput_rps:>10.0f} {last.throughput_rps:>10.0f}  {_drift(first.throughput_rps, last.throughput_rps):>10}")
        print(f"  {'p50 latency (ms)':<25} {first.p50_ms:>10.1f} {mid.p50_ms:>10.1f} {last.p50_ms:>10.1f}  {_drift(first.p50_ms, last.p50_ms):>10}")
        print(f"  {'p99 latency (ms)':<25} {first.p99_ms:>10.1f} {mid.p99_ms:>10.1f} {last.p99_ms:>10.1f}  {_drift(first.p99_ms, last.p99_ms):>10}")
        print(f"  {'Memory (MB)':<25} {first.memory_mb:>10.1f} {mid.memory_mb:>10.1f} {last.memory_mb:>10.1f}  {_drift(first.memory_mb, last.memory_mb):>10}")
        print(f"  {'Telemetry backlog':<25} {first.telemetry_backlog:>10} {mid.telemetry_backlog:>10} {last.telemetry_backlog:>10}")
        print()

        # Verdict
        mem_drift = (last.memory_mb - first.memory_mb) / max(first.memory_mb, 1) * 100
        tp_drift = (last.throughput_rps - first.throughput_rps) / max(first.throughput_rps, 1) * 100
        backlog_ok = last.telemetry_backlog < 100

        print(f"  Stability verdict:")
        print(f"    Memory drift:     {mem_drift:+.1f}%  {'✓ stable' if abs(mem_drift) < 20 else '⚠ investigate'}")
        print(f"    Throughput drift: {tp_drift:+.1f}%  {'✓ stable' if abs(tp_drift) < 20 else '⚠ investigate'}")
        print(f"    Telemetry backlog: {'✓ drained' if backlog_ok else '⚠ growing'}")
        print(f"{'═' * 65}\n")

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "redis_mode": self.redis_mode,
            "duration_s": round(self.duration_s, 1),
            "concurrency": self.concurrency,
            "total_leads": self.total_leads,
            "total_errors": self.total_errors,
            "avg_throughput_rps": round(self.total_leads / self.duration_s, 1) if self.duration_s > 0 else 0,
            "snapshots": [
                {
                    "elapsed_s": round(s.elapsed_s, 1),
                    "throughput_rps": round(s.throughput_rps, 1),
                    "p50_ms": round(s.p50_ms, 2),
                    "p99_ms": round(s.p99_ms, 2),
                    "memory_mb": round(s.memory_mb, 1),
                    "telemetry_backlog": s.telemetry_backlog,
                    "errors": s.errors,
                    "decisions": s.decisions,
                }
                for s in self.snapshots
            ],
        }


# ── Soak runner ───────────────────────────────────────────────────────────────

async def run_soak(
    redis_url: str | None,
    duration_s: float,
    concurrency: int,
    snapshot_interval_s: float = 10.0,
) -> SoakResult:
    redis_mode = f"real ({redis_url})" if redis_url else "fakeredis (in-process)"
    print(f"\n[soak] Starting {duration_s:.0f}s soak — {redis_mode}")
    print(f"[soak] Concurrency={concurrency}, snapshot every {snapshot_interval_s:.0f}s")
    print(f"[soak] Press Ctrl+C to stop early and get partial results\n")

    pipeline, stop_event = await _build_pipeline(redis_url)
    proc = psutil.Process(os.getpid())

    result = SoakResult(
        redis_mode=redis_mode,
        duration_s=duration_s,
        concurrency=concurrency,
        total_leads=0,
        total_errors=0,
    )

    semaphore = asyncio.Semaphore(concurrency)
    start = time.monotonic()
    deadline = start + duration_s

    # Per-snapshot accumulators
    window_latencies: list[float] = []
    window_decisions: dict[str, int] = defaultdict(int)
    window_errors = 0
    window_leads = 0
    window_start = start
    last_snapshot = start

    total_leads = 0
    total_errors = 0

    lock = asyncio.Lock()

    async def process_one() -> None:
        nonlocal window_leads, window_errors, total_leads, total_errors
        lead = _next_lead()
        async with semaphore:
            t0 = time.monotonic()
            try:
                res = await pipeline.process(lead)
                elapsed_ms = (time.monotonic() - t0) * 1000
                async with lock:
                    window_latencies.append(elapsed_ms)
                    window_decisions[res.decision.value] += 1
                    window_leads += 1
                    total_leads += 1
            except Exception:
                async with lock:
                    window_errors += 1
                    total_errors += 1

    try:
        while time.monotonic() < deadline:
            # Launch a batch of concurrent requests
            batch = [process_one() for _ in range(concurrency)]
            await asyncio.gather(*batch)

            now = time.monotonic()
            if now - last_snapshot >= snapshot_interval_s:
                async with lock:
                    elapsed = now - start
                    window_duration = now - window_start

                    if window_latencies and len(window_latencies) > 1:
                        qs = quantiles(window_latencies, n=100)
                        p50 = median(window_latencies)
                        p99 = qs[98]
                    elif window_latencies:
                        p50 = p99 = window_latencies[0]
                    else:
                        p50 = p99 = 0.0

                    snap = SoakSnapshot(
                        elapsed_s=elapsed,
                        throughput_rps=window_leads / window_duration if window_duration > 0 else 0,
                        p50_ms=round(p50, 2),
                        p99_ms=round(p99, 2),
                        memory_mb=proc.memory_info().rss / 1024 / 1024,
                        telemetry_backlog=pipeline._telemetry._queue.qsize(),
                        decisions=dict(window_decisions),
                        errors=window_errors,
                    )
                    result.snapshots.append(snap)

                    print(
                        f"  t={elapsed:>6.0f}s  "
                        f"{snap.throughput_rps:>6.0f} req/s  "
                        f"p50={snap.p50_ms:>6.1f}ms  "
                        f"p99={snap.p99_ms:>7.1f}ms  "
                        f"mem={snap.memory_mb:>6.1f}MB  "
                        f"backlog={snap.telemetry_backlog}"
                    )

                    # Reset window accumulators
                    window_latencies.clear()
                    window_decisions.clear()
                    window_errors = 0
                    window_leads = 0
                    window_start = now
                    last_snapshot = now

    except KeyboardInterrupt:
        print("\n[soak] Interrupted — generating partial report...")

    actual_duration = time.monotonic() - start
    result.duration_s = actual_duration
    result.total_leads = total_leads
    result.total_errors = total_errors

    stop_event.set()
    await pipeline.flush_pending()

    return result


# ── CLI ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Lead Entry Guard — soak test (stability over time)"
    )
    parser.add_argument(
        "--redis-url", default=None, metavar="URL",
        help="Redis URL (default: fakeredis). Example: redis://localhost:6379",
    )
    parser.add_argument(
        "--duration", type=float, default=300.0,
        help="Soak duration in seconds (default: 300 = 5 minutes)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=10,
        help="Concurrent pipeline.process() calls per batch (default: 10)",
    )
    parser.add_argument(
        "--snapshot-interval", type=float, default=10.0,
        help="Seconds between snapshot readings (default: 10)",
    )
    parser.add_argument(
        "--output", default=None, metavar="DIR",
        help="Directory to save JSON results",
    )
    args = parser.parse_args()

    result = await run_soak(
        redis_url=args.redis_url,
        duration_s=args.duration,
        concurrency=args.concurrency,
        snapshot_interval_s=args.snapshot_interval,
    )

    result.print_report()

    if args.output:
        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_path = out_dir / f"soak_{ts}.json"
        with open(out_path, "w") as f:
            json.dump(result.to_dict(), f, indent=2)
        print(f"Results → {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
