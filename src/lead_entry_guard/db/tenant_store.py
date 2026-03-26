"""
Tenant store — async repository for tenant and API key operations.

Security:
  - API key comparison uses constant-time hmac.compare_digest (timing attack safe)
  - Max keys per tenant enforced (MAX_KEYS_PER_TENANT)
  - Revoke checks affected rows — logs warning if key not found
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from lead_entry_guard.db.models import (
    ApiKeyRow, TenantRow,
    generate_api_key, verify_api_key,
    MAX_KEYS_PER_TENANT,
)

logger = logging.getLogger(__name__)


class TenantNotFoundError(Exception):
    pass


class ApiKeyInvalidError(Exception):
    pass


class TooManyKeysError(Exception):
    pass


class TenantStore:
    """Async repository for tenant and API key management."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ── Tenant CRUD ───────────────────────────────────────────────────────────

    async def create_tenant(
        self,
        tenant_id: str,
        name: str,
        tier: str = "medium",
        salvage_policy: str = "STRICT",
    ) -> TenantRow:
        tenant = TenantRow(
            tenant_id=tenant_id,
            name=name,
            tier=tier,
            salvage_policy=salvage_policy,
        )
        self._session.add(tenant)
        await self._session.flush()
        logger.info("Tenant created", extra={"tenant_id": tenant_id, "tier": tier})
        return tenant

    async def get_tenant(self, tenant_id: str) -> TenantRow:
        result = await self._session.execute(
            select(TenantRow).where(
                TenantRow.tenant_id == tenant_id,
                TenantRow.is_active == True,  # noqa: E712
            )
        )
        tenant = result.scalar_one_or_none()
        if tenant is None:
            raise TenantNotFoundError(f"Tenant not found: {tenant_id!r}")
        return tenant

    async def list_tenants(self) -> list[TenantRow]:
        result = await self._session.execute(
            select(TenantRow)
            .where(TenantRow.is_active == True)  # noqa: E712
            .order_by(TenantRow.created_at)
        )
        return list(result.scalars().all())

    async def deactivate_tenant(self, tenant_id: str) -> None:
        await self._session.execute(
            update(TenantRow)
            .where(TenantRow.tenant_id == tenant_id)
            .values(is_active=False, updated_at=datetime.now(timezone.utc))
        )
        logger.info("Tenant deactivated", extra={"tenant_id": tenant_id})

    # ── API key management ────────────────────────────────────────────────────

    async def create_api_key(
        self,
        tenant_id: str,
        label: str | None = None,
        expires_at: datetime | None = None,
    ) -> tuple[str, ApiKeyRow]:
        """
        Generate and store a new API key for a tenant.

        Raises TooManyKeysError if tenant already has MAX_KEYS_PER_TENANT active keys.
        Returns (raw_key, ApiKeyRow). raw_key is returned ONCE — never stored.
        """
        await self.get_tenant(tenant_id)  # verify tenant exists and is active

        # Enforce max keys per tenant
        count_result = await self._session.execute(
            select(func.count(ApiKeyRow.id)).where(
                ApiKeyRow.tenant_id == tenant_id,
                ApiKeyRow.is_active == True,  # noqa: E712
            )
        )
        active_count = count_result.scalar_one()
        if active_count >= MAX_KEYS_PER_TENANT:
            raise TooManyKeysError(
                f"Tenant {tenant_id!r} already has {active_count} active keys "
                f"(max {MAX_KEYS_PER_TENANT}). Revoke unused keys first."
            )

        raw_key, key_hash, key_prefix = generate_api_key()
        api_key = ApiKeyRow(
            tenant_id=tenant_id,
            key_hash=key_hash,
            key_prefix=key_prefix,
            label=label,
            expires_at=expires_at,
        )
        self._session.add(api_key)
        await self._session.flush()

        logger.info(
            "API key created",
            extra={"tenant_id": tenant_id, "key_prefix": key_prefix},
        )
        return raw_key, api_key

    async def resolve_api_key(self, raw_key: str) -> TenantRow:
        """
        Resolve a raw API key to its tenant.

        Uses constant-time comparison (hmac.compare_digest) — timing attack safe.
        Raises ApiKeyInvalidError if key not found, inactive, expired, or tenant inactive.
        """
        from lead_entry_guard.db.models import hash_api_key
        key_hash = hash_api_key(raw_key)

        result = await self._session.execute(
            select(ApiKeyRow).where(
                ApiKeyRow.key_hash == key_hash,
                ApiKeyRow.is_active == True,  # noqa: E712
            )
        )
        api_key = result.scalar_one_or_none()

        if api_key is None:
            raise ApiKeyInvalidError("Invalid or inactive API key")

        # Constant-time verify (timing attack safe)
        from lead_entry_guard.db.models import verify_api_key
        if not verify_api_key(raw_key, api_key.key_hash):
            raise ApiKeyInvalidError("Invalid API key")

        # Check expiry
        if api_key.is_expired:
            raise ApiKeyInvalidError("API key has expired")

        # Update last_used_at (non-fatal)
        try:
            await self._session.execute(
                update(ApiKeyRow)
                .where(ApiKeyRow.id == api_key.id)
                .values(last_used_at=datetime.now(timezone.utc))
            )
        except Exception:
            pass

        return await self.get_tenant(api_key.tenant_id)

    async def revoke_api_key(self, key_prefix: str, tenant_id: str) -> bool:
        """
        Revoke an API key by its prefix for a specific tenant.

        Returns True if a key was revoked, False if no matching key was found.
        Logs a warning if nothing was revoked.
        """
        result = await self._session.execute(
            update(ApiKeyRow)
            .where(
                ApiKeyRow.key_prefix == key_prefix,
                ApiKeyRow.tenant_id == tenant_id,
                ApiKeyRow.is_active == True,  # noqa: E712
            )
            .values(is_active=False)
        )
        affected = result.rowcount
        if affected == 0:
            logger.warning(
                "Revoke API key — no matching active key found",
                extra={"tenant_id": tenant_id, "key_prefix": key_prefix},
            )
            return False

        logger.info(
            "API key revoked",
            extra={"tenant_id": tenant_id, "key_prefix": key_prefix},
        )
        return True
