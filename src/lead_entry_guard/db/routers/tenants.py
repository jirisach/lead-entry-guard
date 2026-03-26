"""
Tenant management endpoints — Phase 3A.

POST /tenants          — register new tenant, returns API key (shown once)
GET  /tenants          — list all active tenants (internal/admin)
GET  /tenants/{id}     — get single tenant
POST /tenants/{id}/keys — rotate / add API key
DELETE /tenants/{id}/keys/{prefix} — revoke API key

GET  /health           — liveness probe
GET  /ready            — readiness probe (DB + Redis)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from lead_entry_guard.db.database import get_engine, get_session
from lead_entry_guard.db.models import TenantRow
from lead_entry_guard.db.tenant_store import TenantNotFoundError, TenantStore

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Request / Response schemas ────────────────────────────────────────────────

class CreateTenantRequest(BaseModel):
    tenant_id: str = Field(..., min_length=2, max_length=64, pattern=r"^[a-z0-9_\-]+$")
    name: str = Field(..., min_length=1, max_length=255)
    tier: str = Field(default="medium", pattern=r"^(small|medium|large|enterprise)$")
    salvage_policy: str = Field(default="STRICT", pattern=r"^(STRICT|SALVAGE|QUARANTINE)$")
    key_label: str | None = Field(default=None, max_length=128)


class TenantResponse(BaseModel):
    tenant_id: str
    name: str
    tier: str
    salvage_policy: str
    is_active: bool
    created_at: datetime


class CreateTenantResponse(BaseModel):
    tenant: TenantResponse
    api_key: str = Field(..., description="Raw API key — shown once, store securely")
    key_prefix: str = Field(..., description="Key prefix for identification")


class AddKeyRequest(BaseModel):
    label: str | None = Field(default=None, max_length=128)


class AddKeyResponse(BaseModel):
    api_key: str = Field(..., description="Raw API key — shown once, store securely")
    key_prefix: str


def _tenant_response(tenant: TenantRow) -> TenantResponse:
    return TenantResponse(
        tenant_id=tenant.tenant_id,
        name=tenant.name,
        tier=tenant.tier,
        salvage_policy=tenant.salvage_policy,
        is_active=tenant.is_active,
        created_at=tenant.created_at,
    )


# ── Tenant endpoints ──────────────────────────────────────────────────────────

@router.post(
    "/tenants",
    response_model=CreateTenantResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new tenant",
    tags=["tenants"],
)
async def create_tenant(
    body: CreateTenantRequest,
    session: AsyncSession = Depends(get_session),
) -> CreateTenantResponse:
    """
    Register a new tenant and return its first API key.

    The API key is shown **once** — store it securely.
    It cannot be recovered; rotate via POST /tenants/{id}/keys if lost.
    """
    store = TenantStore(session)

    try:
        tenant = await store.create_tenant(
            tenant_id=body.tenant_id,
            name=body.name,
            tier=body.tier,
            salvage_policy=body.salvage_policy,
        )
        raw_key, api_key = await store.create_api_key(
            tenant_id=body.tenant_id,
            label=body.key_label or "default",
        )
    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Tenant already exists: {body.tenant_id!r}",
        )

    logger.info("Tenant registered", extra={"tenant_id": body.tenant_id, "tier": body.tier})

    return CreateTenantResponse(
        tenant=_tenant_response(tenant),
        api_key=raw_key,
        key_prefix=api_key.key_prefix,
    )


@router.get(
    "/tenants",
    response_model=list[TenantResponse],
    summary="List all active tenants",
    tags=["tenants"],
)
async def list_tenants(
    session: AsyncSession = Depends(get_session),
) -> list[TenantResponse]:
    store = TenantStore(session)
    tenants = await store.list_tenants()
    return [_tenant_response(t) for t in tenants]


@router.get(
    "/tenants/{tenant_id}",
    response_model=TenantResponse,
    summary="Get a single tenant",
    tags=["tenants"],
)
async def get_tenant(
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
) -> TenantResponse:
    store = TenantStore(session)
    try:
        tenant = await store.get_tenant(tenant_id)
    except TenantNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant not found")
    return _tenant_response(tenant)


@router.post(
    "/tenants/{tenant_id}/keys",
    response_model=AddKeyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add a new API key for a tenant",
    tags=["tenants"],
)
async def add_api_key(
    tenant_id: str,
    body: AddKeyRequest,
    session: AsyncSession = Depends(get_session),
) -> AddKeyResponse:
    store = TenantStore(session)
    try:
        raw_key, api_key = await store.create_api_key(
            tenant_id=tenant_id,
            label=body.label,
        )
    except TenantNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant not found")

    return AddKeyResponse(api_key=raw_key, key_prefix=api_key.key_prefix)


@router.delete(
    "/tenants/{tenant_id}/keys/{key_prefix}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke an API key",
    tags=["tenants"],
)
async def revoke_api_key(
    tenant_id: str,
    key_prefix: str,
    session: AsyncSession = Depends(get_session),
) -> None:
    store = TenantStore(session)
    await store.revoke_api_key(key_prefix=key_prefix, tenant_id=tenant_id)


# ── Health / Readiness ────────────────────────────────────────────────────────

@router.get(
    "/health",
    summary="Liveness probe",
    tags=["ops"],
)
async def health() -> dict:
    """Returns 200 if the process is alive."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@router.get(
    "/ready",
    summary="Readiness probe",
    tags=["ops"],
)
async def ready(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """
    Returns 200 if ready, 503 if degraded.
    Checks: database + Redis connectivity.
    """
    from sqlalchemy import text
    checks: dict[str, str] = {}

    # DB check
    try:
        await session.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as exc:
        checks["database"] = f"error: {exc}"

    # Redis check
    try:
        redis = getattr(request.app.state, "redis", None)
        if redis is not None:
            await redis.ping()
            checks["redis"] = "ok"
        else:
            checks["redis"] = "not configured"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"

    all_ok = all(v in ("ok", "not configured") for v in checks.values())

    if not all_ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "degraded", "checks": checks},
        )

    return {
        "status": "ready",
        "checks": checks,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
