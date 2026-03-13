"""
Scale scenario tests for Lead Entry Guard.

Tests three bug classes that only manifest at 1M+ leads/day:

  1. idempotency_retry_storm     — concurrent retries on same source_id
  2. multi_tenant_queue_pressure — Redis outage + multiple tenants + QUEUE mode
  3. tenant_growth_bloom_pressure — Bloom economics under fast tenant growth

These are not load tests in the throughput sense — they test specific
failure modes that only appear at scale. Each scenario measures a different
invariant and reports metrics relevant to that invariant.

Usage:
    python load_tests/scale_scenarios.py --scenario retry_storm
    python load_tests/scale_scenarios.py --scenario queue_pressure
    python load_tests/scale_scenarios.py --scenario bloom_pressure
    python load_tests/scale_scenarios.py --scenario all

Requirements: pip install fakeredis pytest-asyncio (same as test suite)
These scenarios run against an in-process pipeline, not a live server,
so they can be run without docker compose.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean, median, quantiles
from unittest.mock import AsyncMock, patch

import fakeredis.aioredis as fakeredis

from lead_entry_guard.config.settings import get_settings
from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
from lead_entry_guard.core.exceptions import RedisUnavailableError
from lead_entry_guard.core.models import DecisionClass, DegradedModePolicy, LeadInput, SourceType
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

OUTPUT_DIR = Path(__file__).parent / "datasets"

# Fixed key ring for reproducible fingerprints
_KEY_RING = {
    "current": {
        "kid": "scale-v1",
        "secret_hex": "a" * 64,
        "activated_at": "2026-01-01T00:00:00+00:00",
    }
}


# ── Pipeline factory ──────────────────────────────────────────────────────────

async def _build_pipeline(
    redis_client=None,
    tenant_configs: list[TenantConfig] | None = None,
) -> IngestionPipeline:
    if redis_client is None:
        redis_client = fakeredis.FakeRedis(decode_responses=False)

    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(_KEY_RING))

    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)

    tenant_registry = TenantRegistry()
    for cfg in (tenant_configs or []):
        tenant_registry.register(cfg)

    return IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=TelemetryQueue(max_size=100_000),
        tenant_registry=tenant_registry,
    )


# ── Scenario 1: Idempotency Retry Storm ──────────────────────────────────────

@dataclass
class RetryStormResult:
    total_requests: int
    concurrency: int
    duration_s: float
    full_pipeline_count: int    # went through normalization → policy → store
    replay_count: int           # returned idempotency snapshot
    error_count: int
    decisions: dict[str, int]
    latencies_ms: list[float]

    def print_summary(self) -> None:
        qs = quantiles(self.latencies_ms, n=100) if len(self.latencies_ms) > 1 else [0] * 100
        print(f"\n{'='*60}")
        print("Scenario: idempotency_retry_storm")
        print(f"Total requests:   {self.total_requests:,}")
        print(f"Concurrency:      {self.concurrency}")
        print(f"Duration:         {self.duration_s:.2f}s")
        print(f"Throughput:       {self.total_requests / self.duration_s:.0f} req/s")
        print()
        print(f"Full pipeline:    {self.full_pipeline_count:,}  "
              f"({self.full_pipeline_count/self.total_requests*100:.1f}%)")
        print(f"Replay (idempotency hit): {self.replay_count:,}  "
              f"({self.replay_count/self.total_requests*100:.1f}%)")
        print(f"Errors:           {self.error_count:,}")
        print()
        print("Decision breakdown:")
        for d, c in sorted(self.decisions.items(), key=lambda x: -x[1]):
            print(f"  {d:<25} {c:>6,}")
        print()
        print("Latency (all requests):")
        print(f"  p50    {median(self.latencies_ms):>8.2f} ms")
        print(f"  p95    {qs[94]:>8.2f} ms")
        print(f"  p99    {qs[98]:>8.2f} ms")
        print(f"  max    {max(self.latencies_ms):>8.2f} ms")
        print()
        # Key invariant assertions
        print("Invariant checks:")
        all_same = len(self.decisions) == 1
        print(f"  All decisions identical:  {'✓ PASS' if all_same else '✗ FAIL'}")
        print(f"  (expected: 1 unique decision across {self.total_requests} requests)")
        print(f"{'='*60}\n")

    def to_dict(self) -> dict:
        qs = quantiles(self.latencies_ms, n=100) if len(self.latencies_ms) > 1 else [0] * 100
        return {
            "scenario": "idempotency_retry_storm",
            "total_requests": self.total_requests,
            "concurrency": self.concurrency,
            "duration_s": round(self.duration_s, 3),
            "full_pipeline_count": self.full_pipeline_count,
            "replay_count": self.replay_count,
            "error_count": self.error_count,
            "decisions": self.decisions,
            "latency_ms": {
                "p50": round(median(self.latencies_ms), 2),
                "p95": round(qs[94], 2),
                "p99": round(qs[98], 2),
                "max": round(max(self.latencies_ms), 2),
            },
            "invariant_all_decisions_identical": len(self.decisions) == 1,
        }


async def scenario_retry_storm(
    concurrency: int = 500,
    warmup_first: bool = True,
) -> RetryStormResult:
    """
    Send N concurrent requests with identical source_id + payload.

    Models: API gateway retry storm — upstream timeout causes gateway
    to fire the same request N times simultaneously.

    Key question: how many go through full pipeline vs idempotency replay?
    Invariant: all requests must return the same decision.

    Note on the race window: _store_idempotency_snapshot() is fire-and-forget.
    The snapshot may not be stored before concurrent requests arrive.
    This is a known trade-off (non-blocking response path) — this scenario
    measures the real-world size of that window.
    """
    print(f"\n[retry_storm] concurrency={concurrency}, warmup={warmup_first}")

    pipeline = await _build_pipeline(
        tenant_configs=[TenantConfig(tenant_id="t1")]
    )

    source_id = f"retry-storm-{uuid.uuid4()}"
    payload = LeadInput(
        tenant_id="t1",
        source_id=source_id,
        email="retry.storm@example.com",
        first_name="Retry",
        last_name="Storm",
    )

    if warmup_first:
        # Send one request first and wait for snapshot to be stored.
        # Models the case where first request succeeded but caller didn't
        # receive the response — subsequent retries should all be replays.
        warmup_result = await pipeline.process(payload)
        warmup_request_id = warmup_result.request_id
        # Flush fire-and-forget tasks so snapshot is stored before storm
        pending = set(pipeline._pending_tasks)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        print(f"  Warmup complete — snapshot stored (request_id={warmup_request_id[:8]}…), "
              f"firing {concurrency} concurrent retries")
    else:
        warmup_request_id = None
        print(f"  No warmup — all {concurrency} requests fire simultaneously (cold storm)")

    decisions: dict[str, int] = defaultdict(int)
    latencies: list[float] = []
    errors = 0
    full_pipeline = 0
    replays = 0

    async def send_one() -> None:
        nonlocal errors, full_pipeline, replays
        start = time.monotonic()
        try:
            lead = LeadInput(
                tenant_id=payload.tenant_id,
                source_id=payload.source_id,
                email=payload.email,
                first_name=payload.first_name,
                last_name=payload.last_name,
            )
            result = await pipeline.process(lead)
            latency = (time.monotonic() - start) * 1000
            latencies.append(latency)
            decisions[result.decision.value] += 1
            # Precise replay detection: snapshot hit returns the original request_id.
            # Fresh pipeline run produces a new request_id (uuid4 default_factory).
            if warmup_request_id and result.request_id == warmup_request_id:
                replays += 1
            else:
                full_pipeline += 1
        except Exception:
            errors += 1
            latencies.append((time.monotonic() - start) * 1000)

    start = time.monotonic()
    await asyncio.gather(*[send_one() for _ in range(concurrency)])
    duration = time.monotonic() - start

    return RetryStormResult(
        total_requests=concurrency + (1 if warmup_first else 0),
        concurrency=concurrency,
        duration_s=duration,
        full_pipeline_count=full_pipeline,
        replay_count=replays,
        error_count=errors,
        decisions=dict(decisions),
        latencies_ms=latencies,
    )


# ── Scenario 2: Multi-tenant Queue Pressure ───────────────────────────────────

@dataclass
class QueuePressureResult:
    duration_s: float
    tenant_results: dict[str, dict]  # tenant_id → metrics

    def print_summary(self) -> None:
        print(f"\n{'='*60}")
        print("Scenario: multi_tenant_queue_pressure")
        print(f"Duration: {self.duration_s:.2f}s")
        print()
        for tenant_id, metrics in self.tenant_results.items():
            print(f"Tenant: {tenant_id}  (policy={metrics['policy']})")
            for decision, count in metrics["decisions"].items():
                pct = count / metrics["total"] * 100
                print(f"  {decision:<25} {count:>5,}  ({pct:.1f}%)")
            print(f"  queue_cap_hits:   {metrics.get('queue_cap_hits', 'n/a')}")
            print()
        print("Invariant checks:")
        # Tenant C (ACCEPT_WITH_FLAG) should have no REJECT
        c = self.tenant_results.get("tenant_c", {})
        c_no_reject = c.get("decisions", {}).get("REJECT", 0) == 0
        print(f"  tenant_c no REJECT:  {'✓ PASS' if c_no_reject else '✗ FAIL'}")
        print(f"{'='*60}\n")

    def to_dict(self) -> dict:
        return {
            "scenario": "multi_tenant_queue_pressure",
            "duration_s": round(self.duration_s, 3),
            "tenant_results": self.tenant_results,
        }


async def scenario_queue_pressure(
    leads_per_tenant: int = 200,
    outage_duration_s: float = 3.0,
) -> QueuePressureResult:
    """
    Three tenants with different degraded policies under Redis outage + burst.

    tenant_a: QUEUE + ACCEPT_WITH_FLAG fallback
    tenant_b: QUEUE + REJECT fallback
    tenant_c: ACCEPT_WITH_FLAG (no queue)

    Models: planned Redis maintenance or unexpected outage during traffic burst.

    Key questions:
    - Does queue cap hit rate differ by tenant policy?
    - Does tenant_c behave correctly (WARN) while a/b are in QUEUE?
    - After recovery, do queued leads get correct decisions?
    """
    print(f"\n[queue_pressure] leads_per_tenant={leads_per_tenant}, "
          f"outage_duration={outage_duration_s}s")

    settings = get_settings()
    redis_client = fakeredis.FakeRedis(decode_responses=False)

    tenant_configs = [
        TenantConfig(
            tenant_id="tenant_a",
            degraded_mode_policy=DegradedModePolicy.QUEUE,
            queue_fallback_policy=DegradedModePolicy.ACCEPT_WITH_FLAG,
        ),
        TenantConfig(
            tenant_id="tenant_b",
            degraded_mode_policy=DegradedModePolicy.QUEUE,
            queue_fallback_policy=DegradedModePolicy.REJECT,
        ),
        TenantConfig(
            tenant_id="tenant_c",
            degraded_mode_policy=DegradedModePolicy.ACCEPT_WITH_FLAG,
        ),
    ]

    pipeline = await _build_pipeline(
        redis_client=redis_client,
        tenant_configs=tenant_configs,
    )

    # Patch settings for short queue timeout so scenario completes in seconds
    settings.queue_hold_timeout_seconds = max(int(outage_duration_s) + 2, 5)

    decisions_by_tenant: dict[str, dict[str, int]] = {
        "tenant_a": defaultdict(int),
        "tenant_b": defaultdict(int),
        "tenant_c": defaultdict(int),
    }

    def _make_leads(tenant_id: str, n: int) -> list[LeadInput]:
        return [
            LeadInput(
                tenant_id=tenant_id,
                email=f"lead{i}@{tenant_id}.example.com",
                source_id=str(uuid.uuid4()),
            )
            for i in range(n)
        ]

    leads_a = _make_leads("tenant_a", leads_per_tenant)
    leads_b = _make_leads("tenant_b", leads_per_tenant)
    leads_c = _make_leads("tenant_c", leads_per_tenant)

    # Simulate outage: patch dup_tier.check to raise for duration
    outage_start = time.monotonic()

    original_check = pipeline._dup_tier.check

    async def outage_check(tenant_id, fingerprint, bloom_capacity):
        if time.monotonic() - outage_start < outage_duration_s:
            raise RedisUnavailableError("simulated outage")
        return await original_check(tenant_id, fingerprint, bloom_capacity)

    start = time.monotonic()

    with patch.object(pipeline._dup_tier, "check", side_effect=outage_check):
        # Send all tenant leads concurrently (simulates burst)
        all_leads = (
            [(l, "tenant_a") for l in leads_a] +
            [(l, "tenant_b") for l in leads_b] +
            [(l, "tenant_c") for l in leads_c]
        )

        async def process_one(lead: LeadInput, tenant_id: str) -> None:
            try:
                result = await pipeline.process(lead)
                decisions_by_tenant[tenant_id][result.decision.value] += 1
            except Exception:
                decisions_by_tenant[tenant_id]["ERROR"] += 1

        await asyncio.gather(*[process_one(l, t) for l, t in all_leads])

    duration = time.monotonic() - start

    tenant_results = {}
    for tenant_id, decisions in decisions_by_tenant.items():
        policy = next(c.degraded_mode_policy.value for c in tenant_configs
                     if c.tenant_id == tenant_id)
        tenant_results[tenant_id] = {
            "policy": policy,
            "total": leads_per_tenant,
            "decisions": dict(decisions),
        }

    return QueuePressureResult(
        duration_s=duration,
        tenant_results=tenant_results,
    )


# ── Scenario 3: Tenant Growth / Bloom Pressure ────────────────────────────────

@dataclass
class BloomPressureResult:
    total_leads: int
    unique_leads: int
    duplicate_leads: int
    duration_s: float
    bloom_rotation_count: int
    estimated_fill_ratio: float
    bloom_capacity: int
    redis_lookup_count: int     # bloom MAYBE → Redis
    bloom_skip_count: int       # bloom NO → Redis skipped
    decisions: dict[str, int]
    latencies_ms: list[float]

    @property
    def redis_lookup_rate(self) -> float:
        """Fraction of leads that required Redis lookup (Bloom said MAYBE)."""
        return self.redis_lookup_count / self.total_leads if self.total_leads else 0

    def print_summary(self) -> None:
        qs = quantiles(self.latencies_ms, n=100) if len(self.latencies_ms) > 1 else [0] * 100
        print(f"\n{'='*60}")
        print("Scenario: tenant_growth_bloom_pressure")
        print(f"Total leads:       {self.total_leads:,}")
        print(f"  Unique:          {self.unique_leads:,}")
        print(f"  Duplicates:      {self.duplicate_leads:,}")
        print(f"Duration:          {self.duration_s:.2f}s")
        print()
        print("Bloom health:")
        print(f"  Capacity:        {self.bloom_capacity:,}")
        print(f"  Fill ratio:      {self.estimated_fill_ratio:.1%}")
        print(f"  Rotations:       {self.bloom_rotation_count}")
        print(f"  Redis skip rate: {1 - self.redis_lookup_rate:.1%}  "
              f"(Bloom said NO → skipped Redis)")
        print(f"  Redis lookup rate: {self.redis_lookup_rate:.1%}  "
              f"(Bloom said MAYBE → checked Redis)")
        print()
        print("Decision breakdown:")
        for d, c in sorted(self.decisions.items(), key=lambda x: -x[1]):
            print(f"  {d:<25} {c:>6,}")
        print()
        print("Latency:")
        print(f"  p50    {median(self.latencies_ms):>8.2f} ms")
        print(f"  p95    {qs[94]:>8.2f} ms")
        print(f"  p99    {qs[98]:>8.2f} ms")
        print()
        print("Invariant checks:")
        fpr_ok = self.estimated_fill_ratio < 0.90
        print(f"  Fill ratio < 90%:    {'✓ PASS' if fpr_ok else '✗ WARN — rotation needed'}")
        dup_detected = self.decisions.get("DUPLICATE_HINT", 0) > 0
        print(f"  Duplicates detected: {'✓ PASS' if dup_detected else '✗ FAIL'}")
        print(f"{'='*60}\n")

    def to_dict(self) -> dict:
        qs = quantiles(self.latencies_ms, n=100) if len(self.latencies_ms) > 1 else [0] * 100
        return {
            "scenario": "tenant_growth_bloom_pressure",
            "total_leads": self.total_leads,
            "unique_leads": self.unique_leads,
            "duplicate_leads": self.duplicate_leads,
            "duration_s": round(self.duration_s, 3),
            "bloom_rotation_count": self.bloom_rotation_count,
            "estimated_fill_ratio": round(self.estimated_fill_ratio, 4),
            "bloom_capacity": self.bloom_capacity,
            "redis_lookup_rate": round(self.redis_lookup_rate, 4),
            "decisions": self.decisions,
            "latency_ms": {
                "p50": round(median(self.latencies_ms), 2),
                "p95": round(qs[94], 2),
                "p99": round(qs[98], 2),
            },
            "invariants": {
                "fill_ratio_ok": self.estimated_fill_ratio < 0.90,
                "duplicates_detected": self.decisions.get("DUPLICATE_HINT", 0) > 0,
            },
        }


async def scenario_bloom_pressure(
    unique_leads: int = 5_000,
    duplicate_ratio: float = 0.30,
    bloom_capacity: int = 5_000,  # small capacity to force rotation
) -> BloomPressureResult:
    """
    One tenant with fast growth: stream unique leads until Bloom rotates,
    then mix in duplicates to measure detection accuracy post-rotation.

    Models: fast-growing tenant who quickly exceeds initial Bloom sizing.

    Key questions:
    - At what fill ratio does Bloom start rotating?
    - Does Redis lookup rate stay acceptable as Bloom fills?
    - Are duplicates still correctly detected after rotation?
    - What is the latency impact of increased Redis lookup rate?
    """
    print(f"\n[bloom_pressure] unique={unique_leads:,}, "
          f"dup_ratio={duplicate_ratio:.0%}, bloom_capacity={bloom_capacity:,}")

    tenant_cfg = TenantConfig(
        tenant_id="t1",
        bloom_capacity_override=bloom_capacity,
    )
    pipeline = await _build_pipeline(tenant_configs=[tenant_cfg])

    redis_lookups = 0
    original_redis_lookup = pipeline._dup_tier._redis_lookup

    async def counting_redis_lookup(tenant_id, fp):
        nonlocal redis_lookups
        redis_lookups += 1
        return await original_redis_lookup(tenant_id, fp)

    pipeline._dup_tier._redis_lookup = counting_redis_lookup

    decisions: dict[str, int] = defaultdict(int)
    latencies: list[float] = []

    scenario_start = time.monotonic()  # start before phase 1 — measure full scenario

    # Phase 1: unique leads (fill the Bloom)
    unique_emails = [f"user{i}@growth-tenant.example.com" for i in range(unique_leads)]

    print(f"  Phase 1: sending {unique_leads:,} unique leads...")
    for email in unique_emails:
        lead = LeadInput(tenant_id="t1", email=email, source_id=str(uuid.uuid4()))
        start = time.monotonic()
        result = await pipeline.process(lead)
        latencies.append((time.monotonic() - start) * 1000)
        decisions[result.decision.value] += 1

    bloom_stats_mid = pipeline._dup_tier._bloom._filters.get("t1")
    mid_fill = bloom_stats_mid.stats().estimated_fill_ratio if bloom_stats_mid else 0
    print(f"  After phase 1: fill_ratio={mid_fill:.1%}")

    # Phase 2: mix of duplicates + new unique leads
    n_dup = int(unique_leads * duplicate_ratio)
    dup_emails = [unique_emails[i % len(unique_emails)] for i in range(n_dup)]
    new_emails = [f"user{unique_leads + i}@growth-tenant.example.com" for i in range(n_dup)]
    phase2 = [(e, True) for e in dup_emails] + [(e, False) for e in new_emails]

    import random as _random
    _random.shuffle(phase2)

    print(f"  Phase 2: sending {len(phase2):,} mixed leads "
          f"({n_dup} dups + {n_dup} new)...")

    for email, _ in phase2:
        lead = LeadInput(tenant_id="t1", email=email, source_id=str(uuid.uuid4()))
        start = time.monotonic()
        result = await pipeline.process(lead)
        latencies.append((time.monotonic() - start) * 1000)
        decisions[result.decision.value] += 1

    duration = time.monotonic() - scenario_start

    bloom_filter = pipeline._dup_tier._bloom._filters.get("t1")
    stats = bloom_filter.stats() if bloom_filter else None

    return BloomPressureResult(
        total_leads=unique_leads + len(phase2),
        unique_leads=unique_leads + n_dup,
        duplicate_leads=n_dup,
        duration_s=duration,
        bloom_rotation_count=stats.rotation_count if stats else 0,
        estimated_fill_ratio=stats.estimated_fill_ratio if stats else 0,
        bloom_capacity=bloom_capacity,
        redis_lookup_count=redis_lookups,
        bloom_skip_count=(unique_leads + len(phase2)) - redis_lookups,
        decisions=dict(decisions),
        latencies_ms=latencies,
    )


# ── Scenario 4: Queue Drain Correctness ──────────────────────────────────────

@dataclass
class QueueDrainResult:
    total_queued: int
    duration_s: float
    decisions_during_outage: dict[str, int]
    decisions_realtime: dict[str, int]
    duplicate_parity: bool   # queued leads detected as dups after drain
    idempotency_parity: bool  # replayed queued leads return original decision

    def print_summary(self) -> None:
        print(f"\n{'='*60}")
        print("Scenario: queue_drain_correctness")
        print(f"Queued leads:  {self.total_queued}")
        print(f"Duration:      {self.duration_s:.2f}s")
        print()
        print("Decisions during outage (QUEUE mode):")
        for d, c in sorted(self.decisions_during_outage.items(), key=lambda x: -x[1]):
            print(f"  {d:<25} {c:>5}")
        print()
        print("Decisions realtime (control group, no outage):")
        for d, c in sorted(self.decisions_realtime.items(), key=lambda x: -x[1]):
            print(f"  {d:<25} {c:>5}")
        print()
        print("Invariant checks:")
        print(f"  Duplicate parity:    {'✓ PASS' if self.duplicate_parity else '✗ FAIL'}")
        print(f"    (queued leads appear as DUPLICATE_HINT after drain)")
        print(f"  Idempotency parity:  {'✓ PASS' if self.idempotency_parity else '✗ FAIL'}")
        print(f"    (replay of queued source_id returns original decision)")
        print(f"{'='*60}\n")

    def to_dict(self) -> dict:
        return {
            "scenario": "queue_drain_correctness",
            "total_queued": self.total_queued,
            "duration_s": round(self.duration_s, 3),
            "decisions_during_outage": self.decisions_during_outage,
            "decisions_realtime": self.decisions_realtime,
            "invariants": {
                "duplicate_parity": self.duplicate_parity,
                "idempotency_parity": self.idempotency_parity,
            },
        }


async def scenario_queue_drain_correctness(
    n_leads: int = 50,
    outage_duration_s: float = 3.0,
) -> QueueDrainResult:
    """
    Verify that leads processed via QUEUE → recovery path are
    indistinguishable from leads processed on the main path.

    Two invariants (mirrors ADR-001 at scale):

    1. Duplicate parity: after queue drain, sending the same lead again
       must produce DUPLICATE_HINT — fingerprint was stored post-recovery.

    2. Idempotency parity: replaying the same source_id after recovery
       must return the original request_id and decision — snapshot was
       stored post-recovery.

    This is the scale-level complement to the integration test
    test_recovery_path_stores_fingerprint_and_idempotency_snapshot.
    """
    print(f"\n[queue_drain_correctness] n_leads={n_leads}, "
          f"outage_duration={outage_duration_s}s")

    settings = get_settings()
    settings.queue_hold_timeout_seconds = max(int(outage_duration_s) + 3, 5)

    redis_client = fakeredis.FakeRedis(decode_responses=False)
    tenant_cfg = TenantConfig(
        tenant_id="t1",
        degraded_mode_policy=DegradedModePolicy.QUEUE,
        queue_fallback_policy=DegradedModePolicy.ACCEPT_WITH_FLAG,
    )
    pipeline = await _build_pipeline(
        redis_client=redis_client,
        tenant_configs=[tenant_cfg],
    )

    # Build leads with unique source_ids so we can replay them
    leads = [
        LeadInput(
            tenant_id="t1",
            email=f"drain{i}@correctness-test.example.com",
            source_id=f"drain-src-{i}-{uuid.uuid4()}",
        )
        for i in range(n_leads)
    ]

    # ── Phase 1: send leads during simulated outage ────────────────────────
    outage_start = time.monotonic()
    original_check = pipeline._dup_tier.check

    async def outage_check(tenant_id, fingerprint, bloom_capacity):
        if time.monotonic() - outage_start < outage_duration_s:
            raise RedisUnavailableError("simulated outage")
        return await original_check(tenant_id, fingerprint, bloom_capacity)

    decisions_during: dict[str, int] = defaultdict(int)
    original_results: dict[str, str] = {}  # source_id → request_id

    scenario_start = time.monotonic()

    print(f"  Phase 1: sending {n_leads} leads during {outage_duration_s}s outage...")
    with patch.object(pipeline._dup_tier, "check", side_effect=outage_check):
        tasks = []
        for lead in leads:
            tasks.append(pipeline.process(lead))
        results = await asyncio.gather(*tasks)

    for lead, result in zip(leads, results):
        decisions_during[result.decision.value] += 1
        original_results[lead.source_id] = result.request_id

    # Flush fire-and-forget store tasks (fingerprints + idempotency snapshots)
    pending = set(pipeline._pending_tasks)
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    print(f"  Phase 1 done — decisions: {dict(decisions_during)}")

    # ── Phase 2: send duplicate leads (same email, new source_id) ─────────
    # If fingerprints were stored correctly after recovery,
    # these must all come back as DUPLICATE_HINT.
    print(f"  Phase 2: sending {n_leads} duplicate leads (same emails, new source_ids)...")
    decisions_realtime: dict[str, int] = defaultdict(int)
    dup_leads = [
        LeadInput(
            tenant_id="t1",
            email=lead.email,          # same email → same fingerprint
            source_id=str(uuid.uuid4()),  # new source_id → not an idempotency hit
        )
        for lead in leads
    ]
    dup_results = await asyncio.gather(*[pipeline.process(l) for l in dup_leads])
    for result in dup_results:
        decisions_realtime[result.decision.value] += 1

    duplicate_parity = decisions_realtime.get("DUPLICATE_HINT", 0) == n_leads

    # ── Phase 3: replay original leads (same source_id + same payload) ────
    # If idempotency snapshots were stored correctly after recovery,
    # these must all return the original request_id.
    print(f"  Phase 3: replaying {n_leads} original source_ids...")
    idempotency_ok = 0
    replay_results = await asyncio.gather(*[pipeline.process(lead) for lead in leads])
    for lead, result in zip(leads, replay_results):
        if result.request_id == original_results[lead.source_id]:
            idempotency_ok += 1

    idempotency_parity = idempotency_ok == n_leads
    duration = time.monotonic() - scenario_start

    if not duplicate_parity:
        got = decisions_realtime.get("DUPLICATE_HINT", 0)
        print(f"  ✗ Duplicate parity FAILED: {got}/{n_leads} dups detected "
              f"(fingerprint not stored post-recovery)")
    if not idempotency_parity:
        print(f"  ✗ Idempotency parity FAILED: {idempotency_ok}/{n_leads} replays matched "
              f"(snapshot not stored post-recovery)")

    return QueueDrainResult(
        total_queued=n_leads,
        duration_s=duration,
        decisions_during_outage=dict(decisions_during),
        decisions_realtime=dict(decisions_realtime),
        duplicate_parity=duplicate_parity,
        idempotency_parity=idempotency_parity,
    )


# ── Scenario 5: Redis Latency Spike ──────────────────────────────────────────

@dataclass
class RedisLatencySpikeResult:
    n_leads: int
    injected_latency_ms: float
    duration_s: float
    queue_fallback_activations: int
    decisions: dict[str, int]
    latencies_ms: list[float]

    def print_summary(self) -> None:
        qs = quantiles(self.latencies_ms, n=100) if len(self.latencies_ms) > 1 else [0] * 100
        print(f"\n{'='*60}")
        print("Scenario: redis_latency_spike")
        print(f"Leads:             {self.n_leads}")
        print(f"Injected latency:  {self.injected_latency_ms:.0f}ms per Redis call")
        print(f"Duration:          {self.duration_s:.2f}s")
        print()
        print("Decision breakdown:")
        for d, c in sorted(self.decisions.items(), key=lambda x: -x[1]):
            print(f"  {d:<25} {c:>5}")
        print(f"  Queue fallback activations: {self.queue_fallback_activations}")
        print()
        print("Latency (pipeline, not Redis):")
        print(f"  p50    {median(self.latencies_ms):>8.2f} ms")
        print(f"  p95    {qs[94]:>8.2f} ms")
        print(f"  p99    {qs[98]:>8.2f} ms")
        print(f"  max    {max(self.latencies_ms):>8.2f} ms")
        print()
        print("Invariant checks:")
        # Pipeline must still produce decisions — slow Redis ≠ broken pipeline
        has_decisions = sum(self.decisions.values()) == self.n_leads
        print(f"  All leads decided:   {'✓ PASS' if has_decisions else '✗ FAIL'}")
        # p99 should not be unbounded — queue_hold_timeout must kick in
        p99_ok = qs[98] < (self.injected_latency_ms * 3 + 500)
        print(f"  p99 bounded:         {'✓ PASS' if p99_ok else '✗ WARN — p99 may be unbounded'}")
        print(f"{'='*60}\n")

    def to_dict(self) -> dict:
        qs = quantiles(self.latencies_ms, n=100) if len(self.latencies_ms) > 1 else [0] * 100
        return {
            "scenario": "redis_latency_spike",
            "n_leads": self.n_leads,
            "injected_latency_ms": self.injected_latency_ms,
            "duration_s": round(self.duration_s, 3),
            "queue_fallback_activations": self.queue_fallback_activations,
            "decisions": self.decisions,
            "latency_ms": {
                "p50": round(median(self.latencies_ms), 2),
                "p95": round(qs[94], 2),
                "p99": round(qs[98], 2),
                "max": round(max(self.latencies_ms), 2),
            },
            "invariants": {
                "all_leads_decided": sum(self.decisions.values()) == self.n_leads,
                "p99_bounded": qs[98] < (self.injected_latency_ms * 3 + 500),
            },
        }


async def scenario_redis_latency_spike(
    n_leads: int = 100,
    injected_latency_ms: float = 200.0,
    concurrency: int = 20,
) -> RedisLatencySpikeResult:
    """
    Inject per-call latency into the Redis duplicate check to simulate
    a slow (not unavailable) Redis — e.g. network saturation, noisy neighbor,
    or GC pause on the Redis instance.

    This is distinct from RedisUnavailableError: the call succeeds but slowly.
    The key question is whether the pipeline's queue_hold_timeout kicks in
    and bounds p99 latency, or whether slow Redis causes unbounded response times.

    Invariant 1: all leads must receive a decision (slow Redis ≠ stuck pipeline)
    Invariant 2: p99 pipeline latency must be bounded (timeout must fire)
    """
    print(f"\n[redis_latency_spike] n_leads={n_leads}, "
          f"latency={injected_latency_ms:.0f}ms, concurrency={concurrency}")

    settings = get_settings()
    # Set a tight timeout so we can observe it firing during the scenario
    settings.queue_hold_timeout_seconds = max(
        int(injected_latency_ms / 1000 * 3) + 1, 2
    )

    tenant_cfg = TenantConfig(
        tenant_id="t1",
        degraded_mode_policy=DegradedModePolicy.QUEUE,
        queue_fallback_policy=DegradedModePolicy.ACCEPT_WITH_FLAG,
    )
    pipeline = await _build_pipeline(tenant_configs=[tenant_cfg])

    queue_fallback_activations = 0
    original_check = pipeline._dup_tier.check

    async def slow_check(tenant_id, fingerprint, bloom_capacity):
        # Inject latency but let the call succeed — models slow Redis, not down Redis
        await asyncio.sleep(injected_latency_ms / 1000)
        return await original_check(tenant_id, fingerprint, bloom_capacity)

    decisions: dict[str, int] = defaultdict(int)
    latencies: list[float] = []

    leads = [
        LeadInput(
            tenant_id="t1",
            email=f"slow{i}@latency-spike.example.com",
            source_id=str(uuid.uuid4()),
        )
        for i in range(n_leads)
    ]

    sem = asyncio.Semaphore(concurrency)

    async def process_one(lead: LeadInput) -> None:
        nonlocal queue_fallback_activations
        async with sem:
            start = time.monotonic()
            try:
                result = await pipeline.process(lead)
                latency = (time.monotonic() - start) * 1000
                latencies.append(latency)
                decisions[result.decision.value] += 1
                if result.decision.value in ("PASS_DEGRADED", "WARN"):
                    queue_fallback_activations += 1
            except Exception:
                latencies.append((time.monotonic() - start) * 1000)
                decisions["ERROR"] += 1

    scenario_start = time.monotonic()
    with patch.object(pipeline._dup_tier, "check", side_effect=slow_check):
        await asyncio.gather(*[process_one(l) for l in leads])
    duration = time.monotonic() - scenario_start

    return RedisLatencySpikeResult(
        n_leads=n_leads,
        injected_latency_ms=injected_latency_ms,
        duration_s=duration,
        queue_fallback_activations=queue_fallback_activations,
        decisions=dict(decisions),
        latencies_ms=latencies,
    )


# ── Scenario 6: Telemetry Backpressure ───────────────────────────────────────

@dataclass
class TelemetryBackpressureResult:
    n_leads: int
    queue_max_size: int
    duration_s: float
    decisions: dict[str, int]
    telemetry_dropped: int
    pipeline_errors: int

    def print_summary(self) -> None:
        print(f"\n{'='*60}")
        print("Scenario: telemetry_backpressure")
        print(f"Leads:          {self.n_leads}")
        print(f"Queue max size: {self.queue_max_size}")
        print(f"Duration:       {self.duration_s:.2f}s")
        print()
        print("Decision breakdown:")
        for d, c in sorted(self.decisions.items(), key=lambda x: -x[1]):
            print(f"  {d:<25} {c:>5}")
        print()
        print(f"Telemetry dropped:  {self.telemetry_dropped}")
        print(f"Pipeline errors:    {self.pipeline_errors}")
        print()
        print("Invariant checks:")
        all_decided = sum(self.decisions.values()) - self.pipeline_errors == self.n_leads
        no_errors = self.pipeline_errors == 0
        print(f"  Pipeline unaffected: {'✓ PASS' if no_errors else '✗ FAIL'}")
        print(f"    (telemetry queue full must not block or error ingestion)")
        print(f"  All leads decided:   {'✓ PASS' if all_decided else '✗ FAIL'}")
        print(f"{'='*60}\n")

    def to_dict(self) -> dict:
        return {
            "scenario": "telemetry_backpressure",
            "n_leads": self.n_leads,
            "queue_max_size": self.queue_max_size,
            "duration_s": round(self.duration_s, 3),
            "decisions": self.decisions,
            "telemetry_dropped": self.telemetry_dropped,
            "pipeline_errors": self.pipeline_errors,
            "invariants": {
                "pipeline_unaffected": self.pipeline_errors == 0,
                "all_leads_decided": (
                    sum(self.decisions.values()) - self.pipeline_errors == self.n_leads
                ),
            },
        }


async def scenario_telemetry_backpressure(
    n_leads: int = 500,
    queue_max_size: int = 10,   # deliberately tiny to force overflow immediately
    exporter_delay_ms: float = 50.0,
) -> TelemetryBackpressureResult:
    """
    Fill the TelemetryQueue to capacity with a slow exporter, then send
    a burst of leads to confirm the pipeline is unaffected.

    The telemetry queue is fire-and-forget by design — a full queue must
    result in dropped events, never in a blocked or erroring pipeline.

    Invariant: pipeline_errors == 0, regardless of telemetry queue state.

    This tests the boundary between telemetry and ingestion paths.
    If TelemetryQueue.enqueue() ever raises or blocks on a full queue,
    this scenario will catch it.
    """
    print(f"\n[telemetry_backpressure] n_leads={n_leads}, "
          f"queue_max={queue_max_size}, exporter_delay={exporter_delay_ms:.0f}ms")

    tenant_cfg = TenantConfig(tenant_id="t1")

    # Tiny queue + slow exporter → queue saturates immediately
    telemetry_queue = TelemetryQueue(max_size=queue_max_size)
    pipeline = await _build_pipeline(tenant_configs=[tenant_cfg])
    # Replace pipeline's telemetry queue with our constrained one
    pipeline._telemetry_queue = telemetry_queue

    # Simulate slow exporter by patching flush/drain
    original_drain = telemetry_queue.drain

    async def slow_drain(*args, **kwargs):
        await asyncio.sleep(exporter_delay_ms / 1000)
        return await original_drain(*args, **kwargs)

    telemetry_queue.drain = slow_drain

    decisions: dict[str, int] = defaultdict(int)
    pipeline_errors = 0

    leads = [
        LeadInput(
            tenant_id="t1",
            email=f"backpressure{i}@telemetry-test.example.com",
            source_id=str(uuid.uuid4()),
        )
        for i in range(n_leads)
    ]

    scenario_start = time.monotonic()
    for lead in leads:
        try:
            result = await pipeline.process(lead)
            decisions[result.decision.value] += 1
        except Exception as exc:
            pipeline_errors += 1
            decisions["ERROR"] += 1
    duration = time.monotonic() - scenario_start

    # Measure how many telemetry events were dropped (queue overflow)
    telemetry_dropped = max(0, n_leads - telemetry_queue.size())

    print(f"  Telemetry queue size at end: {telemetry_queue.size()}/{queue_max_size}")

    return TelemetryBackpressureResult(
        n_leads=n_leads,
        queue_max_size=queue_max_size,
        duration_s=duration,
        decisions=dict(decisions),
        telemetry_dropped=telemetry_dropped,
        pipeline_errors=pipeline_errors,
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

SCENARIOS = {
    "retry_storm": lambda: scenario_retry_storm(concurrency=500),
    "queue_pressure": lambda: scenario_queue_pressure(leads_per_tenant=200),
    "bloom_pressure": lambda: scenario_bloom_pressure(unique_leads=5_000),
    "queue_drain": lambda: scenario_queue_drain_correctness(n_leads=50),
    "redis_latency": lambda: scenario_redis_latency_spike(n_leads=100),
    "telemetry_backpressure": lambda: scenario_telemetry_backpressure(n_leads=500),
}

# ── Sweep runners ─────────────────────────────────────────────────────────────

async def concurrency_sweep(
    levels: list[int] | None = None,
) -> None:
    """
    Run retry_storm at increasing concurrency levels to find the race window.

    At low concurrency, nearly all requests are replays (snapshot stored before
    they arrive). As concurrency grows, more requests hit the race window between
    process() returning and fire-and-forget snapshot being stored — showing up
    as full_pipeline_count > 1.

    Typical output shape:
        concurrency=100   full_pipeline=1   replays=99   race_window=small
        concurrency=500   full_pipeline=3   replays=497  race_window=medium
        concurrency=2000  full_pipeline=12  replays=1988 race_window=visible
    """
    levels = levels or [100, 500, 1000, 2000]
    print(f"\n{'='*60}")
    print("Sweep: concurrency vs race window (retry_storm)")
    print(f"{'concurrency':>12}  {'full_pipeline':>13}  {'replays':>8}  "
          f"{'p95_ms':>8}  {'p99_ms':>8}")
    print("-" * 60)

    sweep_results = []
    for c in levels:
        result = await scenario_retry_storm(concurrency=c, warmup_first=True)
        qs = quantiles(result.latencies_ms, n=100) if len(result.latencies_ms) > 1 else [0] * 100
        print(f"{c:>12}  {result.full_pipeline_count:>13}  {result.replay_count:>8}  "
              f"{qs[94]:>8.2f}  {qs[98]:>8.2f}")
        sweep_results.append({
            "concurrency": c,
            "full_pipeline": result.full_pipeline_count,
            "replays": result.replay_count,
            "p95_ms": round(qs[94], 2),
            "p99_ms": round(qs[98], 2),
        })

    print(f"{'='*60}")
    print("\nInterpretation:")
    print("  full_pipeline > 1 = race window visible at this concurrency level")
    print("  Watch for: full_pipeline growing faster than linearly with concurrency")
    return sweep_results


async def bloom_capacity_sweep(
    capacities: list[int] | None = None,
    unique_leads: int = 3_000,
) -> None:
    """
    Run bloom_pressure at different Bloom capacities to show rotation economics.

    Smaller capacity = more rotations = higher Redis lookup rate = higher latency.
    This sweep makes the trade-off between memory (capacity) and performance
    (Redis ops) visible and measurable.

    Typical output shape:
        capacity=1000   rotations=5  redis_rate=45%  p95=12ms
        capacity=5000   rotations=1  redis_rate=18%  p95=7ms
        capacity=20000  rotations=0  redis_rate=8%   p95=5ms
    """
    capacities = capacities or [1_000, 5_000, 10_000, 20_000]
    print(f"\n{'='*60}")
    print(f"Sweep: bloom capacity vs rotation economics ({unique_leads:,} leads)")
    print(f"{'capacity':>10}  {'rotations':>10}  {'fill_ratio':>10}  "
          f"{'redis_rate':>10}  {'p95_ms':>8}")
    print("-" * 60)

    sweep_results = []
    for cap in capacities:
        result = await scenario_bloom_pressure(
            unique_leads=unique_leads,
            bloom_capacity=cap,
        )
        qs = quantiles(result.latencies_ms, n=100) if len(result.latencies_ms) > 1 else [0] * 100
        print(f"{cap:>10,}  {result.bloom_rotation_count:>10}  "
              f"{result.estimated_fill_ratio:>10.1%}  "
              f"{result.redis_lookup_rate:>10.1%}  "
              f"{qs[94]:>8.2f}")
        sweep_results.append({
            "bloom_capacity": cap,
            "rotations": result.bloom_rotation_count,
            "fill_ratio": round(result.estimated_fill_ratio, 4),
            "redis_lookup_rate": round(result.redis_lookup_rate, 4),
            "p95_ms": round(qs[94], 2),
        })

    print(f"{'='*60}")
    print("\nInterpretation:")
    print("  redis_lookup_rate rising = Bloom filling, more MAYBE results")
    print("  Use this to size bloom_capacity_override per tenant tier")
    return sweep_results


async def main() -> None:
    parser = argparse.ArgumentParser(description="Scale scenario tests")
    parser.add_argument(
        "--scenario",
        choices=[*SCENARIOS.keys(), "all", "sweep_concurrency", "sweep_bloom"],
        default="all",
    )
    parser.add_argument(
        "--output",
        action="store_true",
        help="Write JSON results to datasets/",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        nargs="+",
        default=[100, 500, 1000, 2000],
        metavar="N",
        help="Concurrency levels for sweep_concurrency (default: 100 500 1000 2000)",
    )
    parser.add_argument(
        "--bloom-capacity",
        type=int,
        nargs="+",
        default=[1_000, 5_000, 10_000, 20_000],
        metavar="N",
        dest="bloom_capacity",
        help="Bloom capacities for sweep_bloom (default: 1000 5000 10000 20000)",
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_passed = True

    if args.scenario == "sweep_concurrency":
        results = await concurrency_sweep(levels=args.concurrency)
        if args.output:
            out = OUTPUT_DIR / "scale_sweep_concurrency.json"
            with open(out, "w") as f:
                json.dump(results, f, indent=2)
            print(f"\n  Results saved → {out}")
        return

    if args.scenario == "sweep_bloom":
        results = await bloom_capacity_sweep(capacities=args.bloom_capacity)
        if args.output:
            out = OUTPUT_DIR / "scale_sweep_bloom.json"
            with open(out, "w") as f:
                json.dump(results, f, indent=2)
            print(f"\n  Results saved → {out}")
        return

    scenarios = SCENARIOS if args.scenario == "all" else {args.scenario: SCENARIOS[args.scenario]}

    for name, runner in scenarios.items():
        result = await runner()
        result.print_summary()

        if args.output:
            out = OUTPUT_DIR / f"scale_{name}_result.json"
            with open(out, "w") as f:
                json.dump(result.to_dict(), f, indent=2)
            print(f"  Result saved → {out}")

        d = result.to_dict()
        invs = d.get("invariants") or {}
        if invs:
            for inv_name, passed in invs.items():
                if not passed:
                    print(f"  ✗ INVARIANT FAILED: {inv_name}")
                    all_passed = False

    if not all_passed:
        print("\n✗ One or more invariants failed.")
        raise SystemExit(1)
    else:
        print("\n✓ All invariants passed.")


if __name__ == "__main__":
    asyncio.run(main())
