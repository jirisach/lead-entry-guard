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


class DuplicateLookupTier:
    """
    Orchestrates Bloom → Redis duplicate lookup.

    Bloom filter: fast negative pre-check only.
    Redis: authoritative store.
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

        # Step 1: Bloom filter pre-check
        try:
            bloom_filter = self._bloom.get_or_create(tenant_id, capacity=bloom_capacity)
            maybe_present = bloom_filter.check_and_add(fp)
        except BloomUnavailableError as exc:
            logger.warning(
                "Bloom filter unavailable — falling back to Redis direct lookup",
                extra={"tenant_id": tenant_id, "error_type": type(exc).__name__},
            )
            # Bloom unavailable → must fall back to Redis directly
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
        """Called after successful lead acceptance to register fingerprint."""
        await self._redis.store(tenant_id, fingerprint.fingerprint_id, lead_reference)

    async def is_available(self) -> bool:
        """Probe Redis availability without exposing internal store details."""
        return await self._redis.ping()
