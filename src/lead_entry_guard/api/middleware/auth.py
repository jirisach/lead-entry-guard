"""
API key middleware — Phase 3A.

Resolves X-API-Key header to a tenant on every protected request.
Injects tenant_id into request.state for downstream use.
"""
from __future__ import annotations

import logging

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from lead_entry_guard.db.database import get_session
from lead_entry_guard.db.models import TenantRow
from lead_entry_guard.db.tenant_store import ApiKeyInvalidError, TenantNotFoundError, TenantStore

logger = logging.getLogger(__name__)


async def require_tenant(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> TenantRow:
    """
    FastAPI dependency — resolve X-API-Key header to a TenantRow.

    Header is read from request directly (not via Header(...)) to ensure
    missing header returns 401, not 422 Unprocessable Entity.

    Returns the active TenantRow on success.
    Raises HTTP 401 on missing/invalid key.
    Raises HTTP 403 if tenant is inactive.
    """
    raw_key = request.headers.get("X-API-Key")

    if not raw_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    store = TenantStore(session)

    try:
        tenant = await store.resolve_api_key(raw_key)
    except ApiKeyInvalidError:
        logger.warning("Invalid API key", extra={"path": request.url.path})
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    except TenantNotFoundError:
        logger.warning("API key resolved but tenant inactive", extra={"path": request.url.path})
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Tenant is inactive",
        )
    except Exception:
        logger.exception("Unexpected error during API key resolution")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication error",
        )

    request.state.tenant_id = tenant.tenant_id
    return tenant
