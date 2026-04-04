"""
ADR-011 Idempotency Contract — Gap Coverage.

PURPOSE
-------
This file covers three idempotency gaps identified in the ADR-011
compliance review that were NOT covered by the existing test suite:

  GAP 1 — same source_id + DIFFERENT payload hash is NOT a replay.
    Existing tests only verify same source_id + same payload → replay.
    This test verifies the inverse: a changed payload is treated as a
    new, independent request — not silently replayed from the old snapshot.

  GAP 2 — REJECT decisions persist a snapshot and replay correctly.
    The existing test_idempotency_across_decisions.py covers REJECT at
    the integration level (requires Redis). This test covers the same
    contract at the unit level using fakeredis — no network dependency.
    Specifically: REJECT replay must return the *original* request_id,
    not produce a new one. A REJECT must not be silently upgraded to PASS.

  GAP 3 — write failure during snapshot store does not change the decision.
    The pipeline stores snapshots fire-and-forget. If the store fails
    (e.g. Redis timeout), the decision already returned to the caller must
    not be retroactively altered. This test injects a store failure and
    verifies the original response is unaffected.

CONTRACT (from ADR-011)
  - same source_id + same payload hash → same request_id + same decision
  - same source_id + DIFFERENT payload hash → independent decision (new request_id)
  - REJECT persists snapshot — replay returns original request_id
  - write failure is non-fatal — decision outcome is immutable after return

Placement: tests/contract/test_idempotency_contract.py
This is a contract test — it must pass before every merge.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import fakeredis.aioredis as fakeredis
import pytest

from lead_entry_guard.config.tenant import TenantConfig, TenantRegistry
from lead_entry_guard.core.exceptions import RedisUnavailableError
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

# ── Helpers ───────────────────────────────────────────────────────────────────

_KEY_RING = {
    "current": {
        "kid": "test-v1",
        "secret_hex": "a" * 64,
        "activated_at": "2026-01-01T00:00:00+00:00",
    }
}


async def _build_pipeline(
    salvage_policy: SalvagePolicy = SalvagePolicy.SALVAGE,
    redis_client: fakeredis.FakeRedis | None = None,
) -> IngestionPipeline:
    """Build a fully isolated pipeline with fakeredis. Each call is independent."""
    rc = redis_client or fakeredis.FakeRedis(decode_responses=False)
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(_KEY_RING))
    dup_store = RedisDuplicateStore(rc, duplicate_ttl=3600)
    idempotency_store = RedisIdempotencyStore(rc)
    bloom_registry = BloomFilterRegistry()
    dup_tier = DuplicateLookupTier(bloom_registry, dup_store)
    fp_builder = FingerprintBuilder(km)
    registry = TenantRegistry()
    registry.register(TenantConfig(tenant_id="t1", salvage_policy=salvage_policy))
    return IngestionPipeline(
        normalizer=NormalizationLayer(),
        validator=ValidationLayer(),
        fingerprint_builder=fp_builder,
        duplicate_tier=dup_tier,
        policy_engine=PolicyEngine(),
        idempotency_store=idempotency_store,
        telemetry_queue=TelemetryQueue(max_size=100),
        tenant_registry=registry,
    )


# ── GAP 1: different payload hash is NOT a replay ────────────────────────────

class TestDifferentPayloadHashIsNotReplay:
    """
    GAP 1: same source_id + different payload → independent decision.

    Contract: idempotency key = tenant_id + source_id + payload_hash.
    Changing any normalized field produces a different hash, which is a
    cache miss. The pipeline must process the second request independently
    and return a NEW request_id — not replay the first snapshot.

    This tests the critical boundary: idempotency is keyed on content,
    not just source_id. A transport retry of a genuinely different payload
    must not be silently replayed as if it were the original.
    """

    @pytest.mark.asyncio
    async def test_different_email_produces_new_request_id(self) -> None:
        """
        same source_id, different email → different hash → new request_id.

        This is the core gap: verifies that changing a payload field causes
        the idempotency lookup to miss and a fresh decision to be made.
        """
        pipeline = await _build_pipeline()

        first = LeadInput(
            tenant_id="t1",
            source_id="src-gap1",
            email="first@example.com",
        )
        r1 = await pipeline.process(first)
        await pipeline.flush_pending()

        # Same source_id, different email — different payload hash
        second = LeadInput(
            tenant_id="t1",
            source_id="src-gap1",
            email="second@example.com",
        )
        r2 = await pipeline.process(second)

        assert r2.request_id != r1.request_id, (
            "Different payload with same source_id must produce a NEW request_id.\n"
            "Idempotency key includes payload hash — a changed email is a new request,\n"
            "not a replay of the original snapshot.\n"
            f"  first request_id:  {r1.request_id}\n"
            f"  second request_id: {r2.request_id}\n"
            "(These should differ — if they match, the pipeline is replaying on source_id\n"
            "alone without validating the payload hash.)"
        )

    @pytest.mark.asyncio
    async def test_different_phone_produces_new_request_id(self) -> None:
        """same source_id, different phone → new request_id."""
        pipeline = await _build_pipeline()

        r1 = await pipeline.process(LeadInput(
            tenant_id="t1",
            source_id="src-gap1-phone",
            email="shared@example.com",
            phone="+420123456789",
        ))
        await pipeline.flush_pending()

        r2 = await pipeline.process(LeadInput(
            tenant_id="t1",
            source_id="src-gap1-phone",
            email="shared@example.com",
            phone="+420987654321",  # different phone
        ))

        assert r2.request_id != r1.request_id, (
            "Different phone with same source_id must produce a new request_id.\n"
            f"  first:  {r1.request_id}\n"
            f"  second: {r2.request_id}"
        )

    @pytest.mark.asyncio
    async def test_different_payload_does_not_replay_original_decision(self) -> None:
        """
        The second request (different payload, same source_id) must make an
        independent decision — it must not return the original reason_codes.

        This matters when the original was REJECT and the corrected payload would PASS:
        the correction must be processed, not blocked by the stale snapshot.
        """
        pipeline = await _build_pipeline(salvage_policy=SalvagePolicy.STRICT)

        # First: deliberately invalid email → REJECT
        reject_lead = LeadInput(
            tenant_id="t1",
            source_id="src-gap1-correction",
            email="not-an-email",
        )
        r1 = await pipeline.process(reject_lead)
        await pipeline.flush_pending()
        assert r1.decision == DecisionClass.REJECT, (
            f"Setup failed: expected REJECT, got {r1.decision}"
        )

        # Second: corrected email, same source_id → should NOT replay the REJECT
        corrected_lead = LeadInput(
            tenant_id="t1",
            source_id="src-gap1-correction",
            email="corrected@example.com",  # valid email — different payload hash
        )
        r2 = await pipeline.process(corrected_lead)

        assert r2.request_id != r1.request_id, (
            "Corrected payload (different hash) must produce a new request_id.\n"
            "The REJECT snapshot must not block a valid correction with the same source_id."
        )
        assert r2.decision != DecisionClass.REJECT, (
            "Corrected payload must not replay the original REJECT decision.\n"
            f"Got: {r2.decision} — expected PASS or WARN for a valid email."
        )

    @pytest.mark.asyncio
    async def test_same_payload_is_still_replayed(self) -> None:
        """
        Regression guard: verify the happy path is unbroken.

        After gap-1 tests verify the boundary, this test confirms that
        same source_id + IDENTICAL payload still produces a replay.
        This protects against accidentally breaking the normal idempotency path.
        """
        pipeline = await _build_pipeline()

        lead = LeadInput(
            tenant_id="t1",
            source_id="src-gap1-regression",
            email="identical@example.com",
        )
        r1 = await pipeline.process(lead)
        await pipeline.flush_pending()
        r2 = await pipeline.process(lead)  # identical — must replay

        assert r2.request_id == r1.request_id, (
            "Identical payload with same source_id must return original request_id (replay).\n"
            f"  first:  {r1.request_id}\n"
            f"  replay: {r2.request_id}"
        )
        assert r2.decision == r1.decision, (
            "Replay must return original decision.\n"
            f"  first:  {r1.decision}\n"
            f"  replay: {r2.decision}"
        )


# ── GAP 2: REJECT snapshot persists and replays correctly ─────────────────────

class TestRejectSnapshotPersistenceAndReplay:
    """
    GAP 2: REJECT decision persists a snapshot; replay returns original request_id.

    From ADR-011: "REJECT → only snapshot write (no fingerprint write)."

    The existing integration test covers REJECT replay with real Redis.
    This test covers the same contract using fakeredis — no network, no
    real Redis required. The contract is: a REJECT must not be silently
    upgraded to PASS on replay, and must return the original request_id.

    Why this matters: without a REJECT snapshot, a retry of an invalid lead
    would re-run the full pipeline and produce a second REJECT result with
    a *new* request_id — breaking idempotency and potentially double-counting
    the rejection in downstream systems.
    """

    @pytest.mark.asyncio
    async def test_reject_replay_returns_original_request_id(self) -> None:
        """
        REJECT replayed with same source_id+payload → original request_id.

        ADR-011: same source_id + same payload hash → same request_id on replay.
        This must hold for REJECT, not only PASS/WARN.
        """
        pipeline = await _build_pipeline(salvage_policy=SalvagePolicy.STRICT)
        lead = LeadInput(
            tenant_id="t1",
            source_id="src-reject-replay",
            email="not-an-email",  # guaranteed REJECT
        )

        r1 = await pipeline.process(lead)
        await pipeline.flush_pending()

        assert r1.decision == DecisionClass.REJECT, (
            f"Setup failed: expected REJECT for invalid email, got {r1.decision}"
        )

        # Replay — same source_id + same payload
        r2 = await pipeline.process(lead)

        assert r2.request_id == r1.request_id, (
            "REJECT replay must return the original request_id.\n"
            "A REJECT snapshot must be persisted (ADR-011: write semantics).\n"
            f"  original: {r1.request_id}\n"
            f"  replay:   {r2.request_id}\n"
            "(If these differ, the REJECT snapshot was not stored — "
            "the pipeline re-ran the full decision instead of replaying.)"
        )

    @pytest.mark.asyncio
    async def test_reject_replay_returns_original_decision(self) -> None:
        """REJECT replay must return REJECT — not silently upgraded to PASS."""
        pipeline = await _build_pipeline(salvage_policy=SalvagePolicy.STRICT)
        lead = LeadInput(
            tenant_id="t1",
            source_id="src-reject-decision",
            email="not-an-email",
        )
        r1 = await pipeline.process(lead)
        await pipeline.flush_pending()
        r2 = await pipeline.process(lead)

        assert r2.decision == DecisionClass.REJECT, (
            "REJECT replay must return REJECT.\n"
            "The snapshot must preserve the original decision — not fabricate a PASS.\n"
            f"  original: {r1.decision}\n"
            f"  replay:   {r2.decision}"
        )

    @pytest.mark.asyncio
    async def test_reject_replay_returns_original_reason_codes(self) -> None:
        """REJECT replay must return the original reason_codes verbatim."""
        pipeline = await _build_pipeline(salvage_policy=SalvagePolicy.STRICT)
        lead = LeadInput(
            tenant_id="t1",
            source_id="src-reject-codes",
            email="not-an-email",
        )
        r1 = await pipeline.process(lead)
        await pipeline.flush_pending()
        r2 = await pipeline.process(lead)

        assert r2.reason_codes == r1.reason_codes, (
            "REJECT replay must return original reason_codes.\n"
            f"  original: {[rc.value for rc in r1.reason_codes]}\n"
            f"  replay:   {[rc.value for rc in r2.reason_codes]}"
        )

    @pytest.mark.asyncio
    async def test_reject_without_source_id_has_no_idempotency_protection(self) -> None:
        """
        A REJECT without source_id is not protected by idempotency.

        Contract: idempotency is opt-in. Without source_id, every request
        runs the full pipeline. Two identical payloads without source_id
        MUST produce different request_ids (no snapshot to replay).

        This is not a bug — it is the documented behavior. Source_id is
        the caller's opt-in for transport-retry protection.
        """
        pipeline = await _build_pipeline(salvage_policy=SalvagePolicy.STRICT)
        lead = LeadInput(
            tenant_id="t1",
            # No source_id — idempotency opt-out
            email="not-an-email",
        )
        r1 = await pipeline.process(lead)
        r2 = await pipeline.process(lead)

        assert r2.request_id != r1.request_id, (
            "Without source_id, repeated submissions must produce different request_ids.\n"
            "Idempotency is opt-in — the caller must supply source_id to enable replay protection.\n"
            f"  first:  {r1.request_id}\n"
            f"  second: {r2.request_id}\n"
            "(If these match, idempotency is being applied globally, not opt-in.)"
        )


# ── GAP 3: write failure does not alter the returned decision ─────────────────

class TestWriteFailureDoesNotAlterDecision:
    """
    GAP 3: snapshot store failure is non-fatal — decision is already immutable.

    From ADR-011: "All writes are non-blocking and best-effort, but decision
    outcome is fully determined before any write attempt."

    The pipeline stores snapshots and fingerprints via fire-and-forget tasks.
    If a write fails (Redis timeout, unavailable), the response has already
    been returned. The decision must not be affected retroactively.

    These tests inject write failures and verify:
      1. The pipeline returns a valid decision despite the failure.
      2. The decision matches what a successful-write run would return.
      3. HTTP 200 is not converted to 503 by a background write failure.

    Note: this tests the pipeline layer, not the HTTP layer. The HTTP
    exception handlers in app.py guard the API boundary — here we verify
    the pipeline's own contract.
    """

    @pytest.mark.asyncio
    async def test_snapshot_store_failure_does_not_change_pass_decision(self) -> None:
        """
        PASS decision is unaffected when snapshot store raises RedisUnavailableError.

        The decision is determined synchronously before writes begin.
        A background write failure must never retroactively modify the result.
        """
        pipeline = await _build_pipeline()
        lead = LeadInput(
            tenant_id="t1",
            source_id="src-write-fail-pass",
            email="valid@example.com",
        )

        # Inject failure into the idempotency store's store() method.
        # get() succeeds (cache miss on first call) — store() fails.
        with patch.object(
            pipeline._idempotency,
            "store",
            new_callable=AsyncMock,
            side_effect=RedisUnavailableError("injected store failure"),
        ):
            result = await pipeline.process(lead)
            await pipeline.flush_pending()  # wait for fire-and-forget to complete

        # Decision must be valid — write failure is non-fatal
        assert result.decision in (
            DecisionClass.PASS,
            DecisionClass.WARN,
            DecisionClass.DUPLICATE_HINT,
        ), (
            f"Expected a valid affirmative decision, got: {result.decision}.\n"
            "Snapshot store failure must not convert a PASS into a REJECT or error."
        )
        assert result.request_id, "request_id must be set even when snapshot store fails"

    @pytest.mark.asyncio
    async def test_snapshot_store_failure_does_not_change_reject_decision(self) -> None:
        """
        REJECT decision is unaffected when snapshot store raises RedisUnavailableError.

        A REJECT is determined by validation — it is not caused by Redis.
        A subsequent Redis failure in the write path must not alter that REJECT
        into anything else (neither PASS nor a 503).
        """
        pipeline = await _build_pipeline(salvage_policy=SalvagePolicy.STRICT)
        lead = LeadInput(
            tenant_id="t1",
            source_id="src-write-fail-reject",
            email="not-an-email",  # guaranteed REJECT
        )

        with patch.object(
            pipeline._idempotency,
            "store",
            new_callable=AsyncMock,
            side_effect=RedisUnavailableError("injected store failure on REJECT path"),
        ):
            result = await pipeline.process(lead)
            await pipeline.flush_pending()

        assert result.decision == DecisionClass.REJECT, (
            f"REJECT decision must survive snapshot store failure. Got: {result.decision}.\n"
            "The decision was made before any write — a write failure is irrelevant to it."
        )
        assert result.request_id, "request_id must be set even when snapshot write fails"

    @pytest.mark.asyncio
    async def test_decision_matches_with_and_without_store_failure(self) -> None:
        """
        Decision must be identical whether or not the snapshot store succeeds.

        This is the strongest form of the write-isolation contract:
        the decision outcome is fully determined by the pipeline logic,
        never by the outcome of write side effects.
        """
        # Run 1: normal — writes succeed
        pipeline_ok = await _build_pipeline()
        lead = LeadInput(
            tenant_id="t1",
            source_id="src-isolation-check",
            email="isolation@example.com",
        )
        result_ok = await pipeline_ok.process(lead)
        await pipeline_ok.flush_pending()

        # Run 2: write fails — different pipeline instance, same lead payload
        pipeline_fail = await _build_pipeline()
        with patch.object(
            pipeline_fail._idempotency,
            "store",
            new_callable=AsyncMock,
            side_effect=RedisUnavailableError("injected"),
        ):
            result_fail = await pipeline_fail.process(lead)
            await pipeline_fail.flush_pending()

        assert result_fail.decision == result_ok.decision, (
            "Decision must be identical regardless of whether snapshot store succeeds.\n"
            f"  with store:    {result_ok.decision}\n"
            f"  without store: {result_fail.decision}\n"
            "Write failure must not influence decision outcome (ADR-011)."
        )
        assert result_fail.reason_codes == result_ok.reason_codes, (
            "reason_codes must be identical regardless of snapshot store outcome.\n"
            f"  with store:    {[rc.value for rc in result_ok.reason_codes]}\n"
            f"  without store: {[rc.value for rc in result_fail.reason_codes]}"
        )

    @pytest.mark.asyncio
    async def test_fingerprint_store_failure_does_not_change_decision(self) -> None:
        """
        Fingerprint store failure (duplicate store write path) is also non-fatal.

        ADR-011 write semantics: "PASS/WARN → fingerprint + snapshot".
        If the fingerprint store fails, the PASS decision stands.
        The write is a side effect — not a precondition for the decision.
        """
        pipeline = await _build_pipeline()
        lead = LeadInput(
            tenant_id="t1",
            source_id="src-fp-write-fail",
            email="fp-fail@example.com",
        )

        with patch.object(
            pipeline._dup_tier,
            "store_accepted",
            new_callable=AsyncMock,
            side_effect=RedisUnavailableError("injected fingerprint store failure"),
        ):
            result = await pipeline.process(lead)
            await pipeline.flush_pending()

        assert result.decision in (
            DecisionClass.PASS,
            DecisionClass.WARN,
        ), (
            f"Fingerprint store failure must not alter PASS/WARN decision. Got: {result.decision}.\n"
            "store_accepted is a fire-and-forget side effect — its failure is non-fatal."
        )
