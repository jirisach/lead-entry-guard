"""Redis-backed duplicate store with tenant key isolation."""
from __future__ import annotations

import logging

import redis.asyncio as aioredis
from pydantic import BaseModel

from lead_entry_guard.core.exceptions import RedisUnavailableError

logger = logging.getLogger(__name__)

# Key format: {tenant_id}:{fingerprint_id} → lead_reference
# NEVER use unscoped keys


class RedisDuplicateStore:
    """
    Authoritative duplicate store.

    Key format: {tenant_id}:{fingerprint_id}
    TTL: duplicate_ttl_seconds (default 30 days)

    Bloom filter is only a fast negative pre-check.
    Redis is the authoritative source of truth.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        duplicate_ttl: int = 60 * 60 * 24 * 30,
    ) -> None:
        self._redis = redis_client
        self._duplicate_ttl = duplicate_ttl

    def _key(self, tenant_id: str, fingerprint_id: str) -> str:
        # NEVER log fingerprint_id
        return f"{tenant_id}:{fingerprint_id}"

    async def lookup(self, tenant_id: str, fingerprint_id: str) -> str | None:
        """
        Returns lead_reference if duplicate found, else None.
        Raises RedisUnavailableError on connection failure.
        """
        key = self._key(tenant_id, fingerprint_id)
        try:
            value = await self._redis.get(key)
            return value.decode() if value else None
        except aioredis.RedisError as exc:
            raise RedisUnavailableError(f"Redis lookup failed: {exc}") from exc

    async def store(
        self, tenant_id: str, fingerprint_id: str, lead_reference: str
    ) -> None:
        """
        Store fingerprint → lead_reference mapping with TTL.
        Only called after successful lead acceptance.
        """
        key = self._key(tenant_id, fingerprint_id)
        try:
            await self._redis.set(key, lead_reference, ex=self._duplicate_ttl)
        except aioredis.RedisError as exc:
            raise RedisUnavailableError(f"Redis store failed: {exc}") from exc

    async def delete(self, tenant_id: str, fingerprint_id: str) -> bool:
        """Delete a duplicate record (used by reconciliation on correction events)."""
        key = self._key(tenant_id, fingerprint_id)
        try:
            deleted = await self._redis.delete(key)
            return bool(deleted)
        except aioredis.RedisError as exc:
            raise RedisUnavailableError(f"Redis delete failed: {exc}") from exc

    async def ping(self) -> bool:
        """Health check."""
        try:
            await self._redis.ping()
            return True
        except aioredis.RedisError:
            return False


class IdempotencySnapshot(BaseModel):
    """
    Minimal serializable decision snapshot stored for idempotency.

    Stores the actual decision outcome — not just a reference — so that
    replayed requests return the *same decision*, not a hardcoded PASS.
    """
    request_id: str
    decision: str
    reason_codes: list[str]
    duplicate_check_skipped: bool
    policy_version: str
    ruleset_version: str
    config_version: str


class RedisIdempotencyStore:
    """
    Separate idempotency store — protects against transport retries.

    TTL: 24 hours (much shorter than duplicate TTL of 30 days).
    Key format: idempotency:{tenant_id}:{source_id}:{request_hash}

    DISTINCT from duplicate store — different purpose, different TTL, different keyspace.

    Stores a full IdempotencySnapshot so that replays return the original
    decision verbatim, not a synthetic PASS constructed from a bare reference.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        idempotency_ttl: int = 60 * 60 * 24,
    ) -> None:
        self._redis = redis_client
        self._ttl = idempotency_ttl

    def _key(self, tenant_id: str, source_id: str, request_hash: str) -> str:
        return f"idempotency:{tenant_id}:{source_id}:{request_hash}"

    async def get(
        self, tenant_id: str, source_id: str, request_hash: str
    ) -> IdempotencySnapshot | None:
        """Return stored snapshot if this request was already processed, else None."""
        key = self._key(tenant_id, source_id, request_hash)
        try:
            raw = await self._redis.get(key)
            if raw is None:
                return None
            return IdempotencySnapshot.model_validate_json(raw)
        except aioredis.RedisError as exc:
            raise RedisUnavailableError(f"Idempotency get failed: {exc}") from exc

    async def store(
        self, tenant_id: str, source_id: str, request_hash: str, snapshot: IdempotencySnapshot
    ) -> None:
        """
        Persist decision snapshot.  TTL starts from first successful storage.
        Uses SET NX so a race between two concurrent identical requests only
        stores the first winner; subsequent calls are no-ops.
        """
        key = self._key(tenant_id, source_id, request_hash)
        try:
            await self._redis.set(key, snapshot.model_dump_json(), ex=self._ttl, nx=True)
        except aioredis.RedisError as exc:
            raise RedisUnavailableError(f"Idempotency store failed: {exc}") from exc
