"""
Duplicate Lookup Tier.

Lookup path:
  incoming lead
  → build runtime identity signal (fingerprint)
  → check tenant Bloom filter
  → Bloom = NO  : skip Redis (DEFINITELY NOT PRESENT)
  → Bloom = MAYBE: query Redis
  → evaluate duplicate hint
"""
from __future__ import annotations

import logging

from lead_entry_guard.core.exceptions import BloomUnavailableError, RedisUnavailableError
from lead_entry_guard.core.models import DuplicateHint, FingerprintResult, ReasonCode
from lead_entry_guard.lookup.bloom import BloomFilterRegistry
from lead_entry_guard.lookup.redis_store import RedisDuplicateStore

logger = logging.getLogger(__name__)

# Default capacity used in store_accepted() when tenant capacity is not available.
# store_accepted() is called after the decision — tenant config is not passed in.
# This matches the registry's get_or_create behaviour: if the filter already exists
# (created during check()), the capacity argument is ignored.
_DEFAULT_BLOOM_CAPACITY = 100_000


class DuplicateLookupTier:
    """
    Orchestrates Bloom → Redis duplicate lookup.

    Bloom filter: fast negative pre-check only.
    Redis: authoritative store.

    Bloom failure policy:
      Any exception from get_or_create() or check_and_add() triggers a
      fallback to Redis direct lookup. This includes BloomUnavailableError,
      RuntimeError, MemoryError, and any other runtime failure. Bloom is a
      performance optimisation — its failure must never block duplicate detection.
    """

    def __init__(
        self,
        bloom_registry: BloomFilterRegistry,
        redis_store: RedisDuplicateStore,
    ) -> None:
        self._bloom = bloom_registry
        self._redis = redis_store

    async def check(
        self,
        tenant_id: str,
        fingerprint: FingerprintResult,
        bloom_capacity: int,
    ) -> DuplicateHint:
        """
        Returns DuplicateHint.
        NEVER log fingerprint.fingerprint_id.
        """
        fp = fingerprint.fingerprint_id  # internal use only, NEVER log

        # Step 1a: Get or create Bloom filter instance
        try:
            bloom_filter = self._bloom.get_or_create(tenant_id, capacity=bloom_capacity)
        except Exception as exc:
            logger.warning(
                "Bloom get_or_create failed — falling back to Redis direct lookup",
                extra={"tenant_id": tenant_id, "error_type": type(exc).__name__},
            )
            return await self._redis_only_lookup(tenant_id, fp)

        # Step 1b: Check and add to Bloom filter
        try:
            maybe_present = bloom_filter.check_and_add(fp)
        except Exception as exc:
            logger.warning(
                "Bloom check_and_add failed — falling back to Redis direct lookup",
                extra={"tenant_id": tenant_id, "error_type": type(exc).__name__},
            )
            return await self._redis_only_lookup(tenant_id, fp)

        if not maybe_present:
            # DEFINITELY NOT PRESENT — skip Redis
            return DuplicateHint(
                is_duplicate=False,
                confidence="none",
                reason_code=ReasonCode.OK,
                lookup_path="bloom_negative",
            )

        # Step 2: Redis authoritative lookup
        return await self._redis_lookup(tenant_id, fp)

    async def _redis_lookup(self, tenant_id: str, fp: str) -> DuplicateHint:
        try:
            ref = await self._redis.lookup(tenant_id, fp)
            if ref:
                return DuplicateHint(
                    is_duplicate=True,
                    confidence="confirmed",
                    reason_code=ReasonCode.DUPLICATE_REDIS_CONFIRMED,
                    lookup_path="redis_confirmed",
                )
            return DuplicateHint(
                is_duplicate=False,
                confidence="none",
                reason_code=ReasonCode.OK,
                lookup_path="bloom_maybe_redis_miss",
            )
        except RedisUnavailableError:
            raise

    async def _redis_only_lookup(self, tenant_id: str, fp: str) -> DuplicateHint:
        """Fallback when Bloom is unavailable — go straight to Redis."""
        try:
            ref = await self._redis.lookup(tenant_id, fp)
            if ref:
                return DuplicateHint(
                    is_duplicate=True,
                    confidence="confirmed",
                    reason_code=ReasonCode.DUPLICATE_REDIS_CONFIRMED,
                    lookup_path="redis_direct",
                )
            return DuplicateHint(
                is_duplicate=False,
                confidence="none",
                reason_code=ReasonCode.OK,
                lookup_path="redis_direct_miss",
            )
        except RedisUnavailableError:
            raise

    async def store_accepted(
        self, tenant_id: str, fingerprint: FingerprintResult, lead_reference: str
    ) -> None:
        """Called after successful lead acceptance to register fingerprint.

        Write path: Redis (authoritative) → Bloom (cache hint).
        This mirrors the lookup path (Bloom → Redis) and ensures subsequent
        duplicate checks correctly identify this lead via Bloom fast path.

        Bloom update failure is non-fatal — Redis remains authoritative.
        """
        fp = fingerprint.fingerprint_id  # internal use only, NEVER log

        # 1. Store authoritative record in Redis
        await self._redis.store(tenant_id, fp, lead_reference)

        # 2. Update Bloom filter so next lookup hits Bloom MAYBE → Redis confirmed
        #    instead of Bloom negative → skip Redis (which would miss the duplicate)
        try:
            bloom_filter = self._bloom.get_or_create(
                tenant_id, capacity=_DEFAULT_BLOOM_CAPACITY
            )
            bloom_filter.check_and_add(fp)  # adds to active slot if not already present
        except Exception:
            # Bloom update failure must never break ingestion — Redis is authoritative
            logger.warning(
                "Bloom update failed during store_accepted — duplicate detection "
                "will fall back to Redis direct lookup for this fingerprint",
                extra={"tenant_id": tenant_id},
            )

    async def is_available(self) -> bool:
        """Probe Redis availability without exposing internal store details."""
        return await self._redis.ping()
