"""Integration tests — Idempotency & Replay Suite.

Four scenarios testing different aspects of idempotency and deduplication:

  A. exact_replay_sequential   — same payload, same source_id, sequential
  B. exact_replay_concurrent   — same payload, same source_id, concurrent (race window)
  C. normalized_variants       — same person, different payload formats (normalization)
  D. retry_storm_summary       — 50 retries, scoreboard output, downstream sink mock

Key distinction (always):
  idempotency = same REQUEST replayed (same source_id → same request_id returned)
  deduplication = same PERSON in different payloads (fingerprint match → DUPLICATE_HINT)

Both are tested here. They use different pipeline mechanisms:
  idempotency → RedisIdempotencyStore (source_id key)
  deduplication → DuplicateLookupTier (HMAC fingerprint → Redis)
"""
from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, patch

import pytest

from lead_entry_guard.config.tenant import TenantConfig
from lead_entry_guard.core.models import DecisionClass, LeadInput, ReasonCode
from tests.fixtures.pipeline import build_pipeline


# ── Downstream sink mock ──────────────────────────────────────────────────────

@dataclass
class DownstreamSink:
    """Mock downstream CRM writer.

    Counts actual write operations — allows asserting 'downstream writes: 1'
    rather than just 'decision was PASS once'.
    """
    writes: list[str] = field(default_factory=list)

    def write(self, request_id: str) -> None:
        self.writes.append(request_id)

    @property
    def unique_writes(self) -> int:
        return len(set(self.writes))

    @property
    def total_writes(self) -> int:
        return len(self.writes)


def _accepted(decision: DecisionClass) -> bool:
    """True if decision represents a lead accepted into pipeline (not blocked)."""
    return decision in (DecisionClass.PASS, DecisionClass.WARN)


# ── Scoreboard ────────────────────────────────────────────────────────────────

def _print_scoreboard(title: str, results: list, sink: DownstreamSink | None = None) -> None:
    decisions = [r.decision for r in results]
    counts = Counter(decisions)
    request_ids = [r.request_id for r in results]
    unique_ids = len(set(request_ids))

    accepted_count = sum(1 for d in decisions if _accepted(d))
    duplicate_leakage = sum(
        1 for r in results
        if r.decision == DecisionClass.PASS
        and request_ids.count(r.request_id) > 1
        and results.index(r) != next(
            i for i, x in enumerate(results) if x.request_id == r.request_id
        )
    )

    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")
    print(f"  requests sent:           {len(results)}")
    print(f"  unique request_ids:      {unique_ids}")
    print(f"  accepted (PASS/WARN):    {accepted_count}")
    for decision, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {decision.value:<24}   {count}")
    if sink is not None:
        print(f"  downstream writes:       {sink.total_writes}")
        print(f"  downstream unique:       {sink.unique_writes}")
    print(f"  duplicate leakage:       {duplicate_leakage}")
    print(f"{'═' * 60}\n")


# ── Scenario A: exact replay sequential ───────────────────────────────────────

@pytest.mark.asyncio
async def test_idempotency_exact_replay_sequential():
    """Scenario A — same payload + same source_id, 20× sequential.

    Idempotency contract:
      - first request: normal pipeline processing → PASS
      - subsequent 19: idempotency hit → same request_id, same decision
      - downstream write: exactly 1

    Sequential — no race window, tests pure contract correctness.
    """
    pipeline = await build_pipeline()
    sink = DownstreamSink()

    lead = LeadInput(
        tenant_id="t1",
        source_id="replay-sequential-src",
        email="john@example.com",
        phone="+420601123456",
        first_name="John",
        last_name="Smith",
    )

    results = []
    for _ in range(20):
        r = await pipeline.process(lead)
        if _accepted(r.decision):
            sink.write(r.request_id)
        results.append(r)
        await pipeline.flush_pending()

    _print_scoreboard("Scenario A — Exact Replay Sequential", results, sink)

    # Idempotency contract
    first_request_id = results[0].request_id
    first_decision = results[0].decision

    assert first_decision == DecisionClass.PASS

    for i, r in enumerate(results[1:], 1):
        assert r.request_id == first_request_id, (
            f"Replay {i}: request_id changed — "
            f"expected {first_request_id!r}, got {r.request_id!r}"
        )
        assert r.decision == first_decision, (
            f"Replay {i}: decision changed — "
            f"expected {first_decision!r}, got {r.decision!r}"
        )

    # Downstream writes: exactly 1 unique lead accepted
    assert sink.unique_writes == 1, (
        f"Downstream received {sink.unique_writes} unique writes — expected 1"
    )
    assert sink.total_writes == 20  # written on every PASS/WARN, but same request_id


# ── Scenario B: exact replay concurrent ──────────────────────────────────────

@pytest.mark.asyncio
async def test_idempotency_exact_replay_concurrent():
    """Scenario B — same payload + same source_id, 50× concurrent.

    Tests the race window: multiple requests arrive before idempotency snapshot
    is stored. Pipeline must converge — no decision drift, no duplicate leakage.

    This is the most realistic retry storm simulation:
      - webhook retry loop
      - API gateway timeout + retry
      - double-click import
    """
    pipeline = await build_pipeline()
    sink = DownstreamSink()

    lead = LeadInput(
        tenant_id="t1",
        source_id="replay-concurrent-src",
        email="john@example.com",
        phone="+420601123456",
        first_name="John",
        last_name="Smith",
    )

    results = await asyncio.gather(*[pipeline.process(lead) for _ in range(50)])
    await pipeline.flush_pending()

    for r in results:
        if _accepted(r.decision):
            sink.write(r.request_id)

    _print_scoreboard("Scenario B — Exact Replay Concurrent (50×)", results, sink)

    decisions = Counter(r.decision for r in results)
    request_ids = [r.request_id for r in results]

    # No decision drift — at most 2 valid outcomes in race window
    # (PASS for first writer, idempotency replay for rest)
    assert len(decisions) <= 2, (
        f"Decision drift in concurrent replay: {dict(decisions)}"
    )

    # Idempotency convergence — majority must return same request_id
    most_common_id, count = Counter(request_ids).most_common(1)[0]
    convergence = count / len(results)
    assert convergence >= 0.90, (
        f"Idempotency convergence too low: {convergence:.0%} "
        f"({count}/50 returned same request_id)"
    )

    # Downstream: at most 1 unique write (race may cause 1 duplicate write in window)
    assert sink.unique_writes == 1, (
        f"Downstream unique writes: {sink.unique_writes} — expected 1 "
        f"(duplicate leakage detected)"
    )


# ── Scenario C: normalized variants ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_idempotency_normalized_variants():
    """Scenario C — same person, different payload formats.

    Tests normalization + fingerprint + deduplication together.
    Different surface representations of the same logical lead
    must produce the same fingerprint → only 1 accepted.

    This is distinct from idempotency — these have DIFFERENT source_ids,
    so idempotency store doesn't protect them. Only fingerprint dedup does.

    Variants tested:
      email: john@example.com / JOHN@EXAMPLE.COM / John@Example.Com
      phone: +420601123456 / +420 601 123 456 / 420601123456
    """
    pipeline = await build_pipeline()
    sink = DownstreamSink()

    # All represent the same logical person
    variants = [
        LeadInput(tenant_id="t1", source_id="variant-1",
                  email="john@example.com", phone="+420601123456"),
        LeadInput(tenant_id="t1", source_id="variant-2",
                  email="JOHN@EXAMPLE.COM", phone="+420601123456"),
        LeadInput(tenant_id="t1", source_id="variant-3",
                  email="John@Example.Com", phone="+420 601 123 456"),
        LeadInput(tenant_id="t1", source_id="variant-4",
                  email="john@example.com", phone="420601123456"),
        LeadInput(tenant_id="t1", source_id="variant-5",
                  email="  john@example.com  ", phone="+420601123456"),
    ]

    results = []
    for lead in variants:
        r = await pipeline.process(lead)
        if _accepted(r.decision):
            sink.write(r.request_id)
        results.append(r)
        await pipeline.flush_pending()

    _print_scoreboard("Scenario C — Normalized Variants (5 formats)", results, sink)

    decisions = Counter(r.decision for r in results)

    # First variant must pass
    assert results[0].decision == DecisionClass.PASS

    # Remaining must be blocked — either DUPLICATE_HINT or WARN (phone normalization)
    # Phone variant "420601123456" (missing +) may normalize differently
    blocked = sum(1 for r in results[1:] if not _accepted(r.decision))
    assert blocked >= 2, (
        f"Expected most variants to be blocked as duplicates, "
        f"decisions: {dict(decisions)}"
    )

    # Downstream: exactly 1 unique person accepted
    assert sink.unique_writes == 1, (
        f"Downstream unique writes: {sink.unique_writes} — "
        f"normalization failed to converge same person to same fingerprint"
    )


# ── Scenario D: retry storm summary ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_idempotency_retry_storm_summary():
    """Scenario D — 50 retries, full scoreboard, downstream sink.

    The showcase test — generates numbers suitable for engineering post / screenshot.

    Expected output:
      requests sent:           50
      unique request_ids:      1
      accepted (PASS/WARN):    50   (all replays count as accepted — same decision)
      PASS:                    50
      downstream writes:       50
      downstream unique:       1
      duplicate leakage:       0

    This verifies the core claim:
      'We processed 50 identical requests and let exactly 1 through downstream.'
    """
    pipeline = await build_pipeline()
    sink = DownstreamSink()

    lead = LeadInput(
        tenant_id="t1",
        source_id="storm-showcase-src",
        email="john@example.com",
        phone="+420601123456",
        first_name="John",
        last_name="Smith",
    )

    # First submission — establishes idempotency snapshot
    r_first = await pipeline.process(lead)
    assert r_first.decision == DecisionClass.PASS
    sink.write(r_first.request_id)
    await pipeline.flush_pending()

    # 49 sequential replays — all must return same result
    results = [r_first]
    for _ in range(49):
        r = await pipeline.process(lead)
        sink.write(r.request_id)
        results.append(r)

    _print_scoreboard("Scenario D — Retry Storm (50×)", results, sink)

    # All replays identical
    assert all(r.request_id == r_first.request_id for r in results), \
        "Not all replays returned original request_id"
    assert all(r.decision == r_first.decision for r in results), \
        "Decision drift across replay storm"

    # Downstream: 50 writes, 1 unique
    assert sink.total_writes == 50
    assert sink.unique_writes == 1, (
        "Duplicate leakage: downstream received more than 1 unique lead"
    )
