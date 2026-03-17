"""
Multi-tenant noisy neighbor test — Lead Entry Guard v4.

Verifies that a tenant generating a heavy storm does not degrade
the latency or correctness of other tenants running normal traffic.

This is a critical multi-tenant SaaS property:
  - tenant isolation must hold under pressure, not just under normal load
  - a misbehaving or high-volume tenant must not starve others

Test scenarios:
  1. noisy_neighbor_latency  — tenant A storms, tenant B measures latency impact
  2. noisy_neighbor_correctness — tenant A storms, tenant B checks decision accuracy
  3. noisy_neighbor_three_tenants — A storms, B normal, C quiet — all measured

Usage:
    python benchmarks/noisy_neighbor.py --redis-url redis://localhost:6379
    python benchmarks/noisy_neighbor.py --redis-url redis://localhost:6379 --output benchmarks/results/
    python benchmarks/noisy_neighbor.py  # fakeredis, no infrastructure needed
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import time
import uuid
from collections import Counter, defaultdict
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


# ── Infrastructure ────────────────────────────────────────────────────────────

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


async def _build_pipeline(
    redis_url: str | None,
    tenant_configs: list[TenantConfig],
) -> tuple[IngestionPipeline, asyncio.Event]:
    redis_client = _make_redis_client(redis_url)
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))

    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)

    registry = TenantRegistry()
    for cfg in tenant_configs:
        registry.register(cfg)

    telemetry_queue = TelemetryQueue(max_size=10_000)
    stop_event = asyncio.Event()
    asyncio.create_task(
        _drain_telemetry(telemetry_queue, stop_event),
        name="nn_telemetry_drain",
    )

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


# ── Result model ──────────────────────────────────────────────────────────────

@dataclass
class TenantMetrics:
    tenant_id: str
    role: str  # "noisy" | "normal" | "quiet"
    total_requests: int
    errors: int
    decisions: dict[str, int]
    latencies_ms: list[float]

    def percentiles(self) -> dict[str, float]:
        if not self.latencies_ms:
            return {}
        qs = quantiles(self.latencies_ms, n=100)
        return {
            "p50": round(median(self.latencies_ms), 2),
            "p95": round(qs[94], 2),
            "p99": round(qs[98], 2),
            "mean": round(mean(self.latencies_ms), 2),
            "max": round(max(self.latencies_ms), 2),
        }

    def throughput(self, duration_s: float) -> float:
        return self.total_requests / duration_s if duration_s > 0 else 0


@dataclass
class NoisyNeighborResult:
    scenario: str
    redis_mode: str
    duration_s: float
    tenant_metrics: list[TenantMetrics]
    isolation_verdict: str  # "PASS" | "WARN" | "FAIL"
    isolation_notes: list[str]
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def print_report(self) -> None:
        print(f"\n{'═' * 65}")
        print(f"  Noisy Neighbor Test — {self.scenario}")
        print(f"{'─' * 65}")
        print(f"  Redis:    {self.redis_mode}")
        print(f"  Duration: {self.duration_s:.2f}s")
        print()

        # Per-tenant table
        print(f"  {'Tenant':<20} {'Role':<8} {'Reqs':>7} {'Err':>5} {'p50':>8} {'p95':>8} {'p99':>8} {'req/s':>7}")
        print(f"  {'─'*20} {'─'*8} {'─'*7} {'─'*5} {'─'*8} {'─'*8} {'─'*8} {'─'*7}")
        for m in self.tenant_metrics:
            pct = m.percentiles()
            print(
                f"  {m.tenant_id:<20} {m.role:<8} {m.total_requests:>7} "
                f"{m.errors:>5} "
                f"{pct.get('p50', 0):>8.1f} "
                f"{pct.get('p95', 0):>8.1f} "
                f"{pct.get('p99', 0):>8.1f} "
                f"{m.throughput(self.duration_s):>7.0f}"
            )
        print()

        # Decision breakdown per tenant
        for m in self.tenant_metrics:
            print(f"  {m.tenant_id} ({m.role}) decisions:")
            for dec, count in sorted(m.decisions.items(), key=lambda x: -x[1]):
                pct_dec = count / m.total_requests * 100 if m.total_requests > 0 else 0
                print(f"    {dec:<22} {count:>6,}  ({pct_dec:5.1f}%)")
        print()

        # Isolation verdict
        verdict_icon = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}[self.isolation_verdict]
        print(f"  Tenant isolation: {verdict_icon} {self.isolation_verdict}")
        for note in self.isolation_notes:
            print(f"    {note}")
        print(f"{'═' * 65}\n")

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "scenario": self.scenario,
            "redis_mode": self.redis_mode,
            "duration_s": round(self.duration_s, 2),
            "isolation_verdict": self.isolation_verdict,
            "isolation_notes": self.isolation_notes,
            "tenants": [
                {
                    "tenant_id": m.tenant_id,
                    "role": m.role,
                    "total_requests": m.total_requests,
                    "errors": m.errors,
                    "throughput_rps": round(m.throughput(self.duration_s), 1),
                    "latency_ms": m.percentiles(),
                    "decisions": m.decisions,
                }
                for m in self.tenant_metrics
            ],
        }


# ── Isolation evaluator ───────────────────────────────────────────────────────

def _evaluate_isolation(
    noisy: TenantMetrics,
    normals: list[TenantMetrics],
    baseline_p99_ms: float = 500.0,
) -> tuple[str, list[str]]:
    """
    Evaluate whether normal tenants were impacted by the noisy neighbor.

    Verdict:
      PASS — normal tenants p99 within acceptable range, no errors
      WARN — latency elevated but within 3x baseline, or minor error rate
      FAIL — normal tenant p99 > 3x baseline, or error rate > 1%
    """
    notes = []
    worst_verdict = "PASS"

    noisy_pct = noisy.percentiles()
    notes.append(f"Noisy tenant ({noisy.tenant_id}): {noisy.total_requests:,} requests, p99={noisy_pct.get('p99', 0):.1f}ms")

    for m in normals:
        pct = m.percentiles()
        p99 = pct.get("p99", 0)
        error_rate = m.errors / m.total_requests if m.total_requests > 0 else 0

        if p99 > baseline_p99_ms * 3:
            verdict = "FAIL"
            notes.append(f"✗ {m.tenant_id} ({m.role}): p99={p99:.1f}ms — exceeds 3x baseline ({baseline_p99_ms*3:.0f}ms)")
        elif p99 > baseline_p99_ms * 1.5:
            verdict = "WARN"
            notes.append(f"⚠ {m.tenant_id} ({m.role}): p99={p99:.1f}ms — elevated (>{baseline_p99_ms*1.5:.0f}ms)")
        else:
            verdict = "PASS"
            notes.append(f"✓ {m.tenant_id} ({m.role}): p99={p99:.1f}ms — within acceptable range")

        if error_rate > 0.01:
            verdict = "FAIL"
            notes.append(f"✗ {m.tenant_id}: error rate {error_rate:.1%} exceeds 1%")
        elif error_rate > 0:
            notes.append(f"  {m.tenant_id}: {m.errors} errors ({error_rate:.2%})")

        if verdict == "FAIL" or (worst_verdict != "FAIL" and verdict == "WARN"):
            worst_verdict = verdict

    return worst_verdict, notes


# ── Scenario runners ──────────────────────────────────────────────────────────

async def scenario_two_tenants(
    redis_url: str | None,
    storm_requests: int = 500,
    normal_requests: int = 100,
) -> NoisyNeighborResult:
    """
    Tenant A (noisy): retry storm — same lead, N concurrent requests.
    Tenant B (normal): steady normal traffic concurrent with the storm.

    Key question: does tenant A's storm affect tenant B's latency?
    """
    redis_mode = f"real ({redis_url})" if redis_url else "fakeredis (in-process)"
    print(f"\n[noisy_neighbor] Two-tenant scenario — {redis_mode}")
    print(f"  Noisy tenant: {storm_requests} concurrent storm requests")
    print(f"  Normal tenant: {normal_requests} steady requests")

    pipeline, stop_event = await _build_pipeline(redis_url, [
        TenantConfig(tenant_id="tenant_noisy", salvage_policy=SalvagePolicy.STRICT),
        TenantConfig(tenant_id="tenant_normal", salvage_policy=SalvagePolicy.STRICT),
    ])

    noisy_metrics = TenantMetrics("tenant_noisy", "noisy", 0, 0, {}, [])
    normal_metrics = TenantMetrics("tenant_normal", "normal", 0, 0, {}, [])

    # Noisy lead — same email, many concurrent retries
    noisy_lead = LeadInput(
        tenant_id="tenant_noisy",
        source_id="storm-src",
        email="storm@noisy-tenant.com",
        phone="+12025550100",
    )

    async def storm_request() -> None:
        t0 = time.monotonic()
        try:
            result = await pipeline.process(noisy_lead)
            noisy_metrics.latencies_ms.append((time.monotonic() - t0) * 1000)
            noisy_metrics.decisions[result.decision.value] = \
                noisy_metrics.decisions.get(result.decision.value, 0) + 1
            noisy_metrics.total_requests += 1
        except Exception:
            noisy_metrics.errors += 1
            noisy_metrics.total_requests += 1

    async def normal_request(i: int) -> None:
        lead = LeadInput(
            tenant_id="tenant_normal",
            email=f"normal.user.{i}@example.com",
            source_id=str(uuid.uuid4()),
        )
        t0 = time.monotonic()
        try:
            result = await pipeline.process(lead)
            normal_metrics.latencies_ms.append((time.monotonic() - t0) * 1000)
            normal_metrics.decisions[result.decision.value] = \
                normal_metrics.decisions.get(result.decision.value, 0) + 1
            normal_metrics.total_requests += 1
        except Exception:
            normal_metrics.errors += 1
            normal_metrics.total_requests += 1

    start = time.monotonic()

    # Run both tenants concurrently
    storm_tasks = [storm_request() for _ in range(storm_requests)]
    normal_tasks = [normal_request(i) for i in range(normal_requests)]
    await asyncio.gather(*storm_tasks, *normal_tasks)

    duration = time.monotonic() - start
    stop_event.set()
    await pipeline.flush_pending()

    verdict, notes = _evaluate_isolation(noisy_metrics, [normal_metrics])

    return NoisyNeighborResult(
        scenario="two_tenant_storm",
        redis_mode=redis_mode,
        duration_s=duration,
        tenant_metrics=[noisy_metrics, normal_metrics],
        isolation_verdict=verdict,
        isolation_notes=notes,
    )


async def scenario_three_tenants(
    redis_url: str | None,
    storm_requests: int = 500,
    normal_requests: int = 100,
    quiet_requests: int = 20,
) -> NoisyNeighborResult:
    """
    Tenant A (noisy): heavy retry storm.
    Tenant B (normal): steady normal traffic.
    Tenant C (quiet): very low traffic — most sensitive to latency impact.

    Key question: does the storm degrade even the quietest tenant?
    """
    redis_mode = f"real ({redis_url})" if redis_url else "fakeredis (in-process)"
    print(f"\n[noisy_neighbor] Three-tenant scenario — {redis_mode}")
    print(f"  Noisy:  {storm_requests} concurrent storm requests")
    print(f"  Normal: {normal_requests} steady requests")
    print(f"  Quiet:  {quiet_requests} requests")

    pipeline, stop_event = await _build_pipeline(redis_url, [
        TenantConfig(tenant_id="tenant_A_noisy", salvage_policy=SalvagePolicy.STRICT),
        TenantConfig(tenant_id="tenant_B_normal", salvage_policy=SalvagePolicy.SALVAGE),
        TenantConfig(tenant_id="tenant_C_quiet", salvage_policy=SalvagePolicy.STRICT),
    ])

    metrics = {
        "tenant_A_noisy": TenantMetrics("tenant_A_noisy", "noisy", 0, 0, {}, []),
        "tenant_B_normal": TenantMetrics("tenant_B_normal", "normal", 0, 0, {}, []),
        "tenant_C_quiet": TenantMetrics("tenant_C_quiet", "quiet", 0, 0, {}, []),
    }

    noisy_lead = LeadInput(
        tenant_id="tenant_A_noisy",
        source_id="nn-storm-src",
        email="storm@tenant-a.com",
    )

    async def run_tenant(tenant_id: str, lead_fn, n: int) -> None:
        m = metrics[tenant_id]
        for i in range(n):
            lead = lead_fn(i)
            t0 = time.monotonic()
            try:
                result = await pipeline.process(lead)
                m.latencies_ms.append((time.monotonic() - t0) * 1000)
                m.decisions[result.decision.value] = m.decisions.get(result.decision.value, 0) + 1
                m.total_requests += 1
            except Exception:
                m.errors += 1
                m.total_requests += 1

    start = time.monotonic()

    await asyncio.gather(
        run_tenant(
            "tenant_A_noisy",
            lambda i: noisy_lead,
            storm_requests,
        ),
        run_tenant(
            "tenant_B_normal",
            lambda i: LeadInput(
                tenant_id="tenant_B_normal",
                email=f"user.{i}@tenant-b.com",
                phone="+12025550100",
                source_id=str(uuid.uuid4()),
            ),
            normal_requests,
        ),
        run_tenant(
            "tenant_C_quiet",
            lambda i: LeadInput(
                tenant_id="tenant_C_quiet",
                email=f"vip.{i}@tenant-c.com",
                source_id=str(uuid.uuid4()),
            ),
            quiet_requests,
        ),
    )

    duration = time.monotonic() - start
    stop_event.set()
    await pipeline.flush_pending()

    noisy = metrics["tenant_A_noisy"]
    normals = [metrics["tenant_B_normal"], metrics["tenant_C_quiet"]]
    verdict, notes = _evaluate_isolation(noisy, normals)

    return NoisyNeighborResult(
        scenario="three_tenant_storm",
        redis_mode=redis_mode,
        duration_s=duration,
        tenant_metrics=list(metrics.values()),
        isolation_verdict=verdict,
        isolation_notes=notes,
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Lead Entry Guard — multi-tenant noisy neighbor test"
    )
    parser.add_argument(
        "--redis-url", default=None, metavar="URL",
        help="Redis URL (default: fakeredis). Example: redis://localhost:6379",
    )
    parser.add_argument(
        "--scenario", choices=["two", "three", "all"], default="all",
        help="Which scenario to run (default: all)",
    )
    parser.add_argument(
        "--storm-requests", type=int, default=500,
        help="Concurrent storm requests from noisy tenant (default: 500)",
    )
    parser.add_argument(
        "--output", default=None, metavar="DIR",
        help="Directory to save JSON results",
    )
    args = parser.parse_args()

    results = []

    if args.scenario in ("two", "all"):
        r = await scenario_two_tenants(
            redis_url=args.redis_url,
            storm_requests=args.storm_requests,
        )
        r.print_report()
        results.append(r)

    if args.scenario in ("three", "all"):
        r = await scenario_three_tenants(
            redis_url=args.redis_url,
            storm_requests=args.storm_requests,
        )
        r.print_report()
        results.append(r)

    # Summary
    print(f"\n{'─' * 40}")
    print("  Noisy Neighbor — Summary")
    print(f"{'─' * 40}")
    for r in results:
        icon = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}[r.isolation_verdict]
        print(f"  {icon} {r.scenario:<30} {r.isolation_verdict}")
    print(f"{'─' * 40}\n")

    if args.output:
        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_path = out_dir / f"noisy_neighbor_{ts}.json"
        with open(out_path, "w") as f:
            json.dump([r.to_dict() for r in results], f, indent=2)
        print(f"Results → {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
