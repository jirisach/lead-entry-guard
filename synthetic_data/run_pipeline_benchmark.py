from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import fakeredis.aioredis as fakeredis

from lead_entry_guard.config.tenant import TenantRegistry
from lead_entry_guard.core.models import LeadInput, SourceType
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

DATASET = Path("synthetic_data/output/messy_leads_100k.jsonl")


def load_dataset():
    with DATASET.open("r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


async def build_pipeline() -> IngestionPipeline:
    redis_client = fakeredis.FakeRedis(decode_responses=False)

    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))

    dup_store = RedisDuplicateStore(redis_client, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(redis_client, idempotency_ttl=3600)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)

    # NOTE: TelemetryExporter.export_loop() is intentionally NOT started here.
    # In production it runs as a long-lived background task draining the queue
    # into StatsD/UDP. In this benchmark we measure pure pipeline throughput
    # without the overhead of UDP emission.
    #
    # Consequence: the queue fills up during the run and P2_ALERT
    # ("Telemetry queue near capacity") will appear in logs once fill ratio
    # exceeds warn_threshold (0.80 → ~80k events). This is expected behavior,
    # not a bug. No events are dropped unless the queue hits max_size=100_000
    # — that would appear as "Telemetry event dropped" in DEBUG logs.
    return IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=TelemetryQueue(max_size=100_000),
        tenant_registry=TenantRegistry(),
    )


async def run() -> None:
    pipeline = await build_pipeline()

    total = 0
    decision_counts: dict[str, int] = {}
    latencies_ms: list[float] = []

    start = time.monotonic()

    for record in load_dataset():
        source_type_raw = record.get("source_type", "API")
        try:
            source_type = SourceType(source_type_raw)
        except ValueError:
            source_type = SourceType.API

        lead = LeadInput(
            tenant_id=record["tenant_id"],
            source_id=record["source_id"],
            source_type=source_type,
            email=record.get("email"),
            phone=record.get("phone"),
            first_name=record.get("first_name"),
            last_name=record.get("last_name"),
            company=record.get("company"),
            extra=record.get("extra", {}),
        )

        result = await pipeline.process(lead)

        key = result.decision.value
        decision_counts[key] = decision_counts.get(key, 0) + 1
        latencies_ms.append(result.latency_ms)
        total += 1

    duration = time.monotonic() - start

    latencies_ms.sort()
    p50 = latencies_ms[int(len(latencies_ms) * 0.50)]
    p95 = latencies_ms[int(len(latencies_ms) * 0.95)]
    p99 = latencies_ms[int(len(latencies_ms) * 0.99)]

    print(f"Processed: {total}")
    print(f"Time: {duration:.2f} seconds")
    print(f"Throughput: {total / duration:.2f} leads/sec")
    print(f"Latency p50: {p50:.2f} ms")
    print(f"Latency p95: {p95:.2f} ms")
    print(f"Latency p99: {p99:.2f} ms")
    print("Decisions:")
    for decision, count in sorted(decision_counts.items()):
        print(f"  {decision}: {count}")


if __name__ == "__main__":
    asyncio.run(run())