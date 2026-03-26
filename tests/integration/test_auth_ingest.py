"""
Integration tests — Phase 3A API key auth on ingest endpoint.

Verifies:
  - missing X-API-Key → 401
  - invalid X-API-Key → 401
  - valid key, inactive tenant → 403
  - valid key, active tenant → ingest proceeds, tenant_id from auth
  - tenant cannot inject another tenant's id via body

Uses SQLite in-memory via aiosqlite (no Postgres required).
"""
from __future__ import annotations

import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from lead_entry_guard.db.models import Base
from lead_entry_guard.db.tenant_store import TenantStore
from lead_entry_guard.db.database import get_session


# ── SQLite in-memory DB fixture ───────────────────────────────────────────────

@pytest.fixture
async def db_session():
    """In-memory SQLite session — no Postgres needed."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with factory() as session:
        yield session

    await engine.dispose()


@pytest.fixture
async def tenant_with_key(db_session: AsyncSession):
    """Create an active tenant and return (tenant_row, raw_api_key)."""
    store = TenantStore(db_session)
    tenant = await store.create_tenant(
        tenant_id="test_tenant",
        name="Test Tenant",
        tier="medium",
        salvage_policy="STRICT",
    )
    raw_key, _ = await store.create_api_key(tenant_id="test_tenant", label="test")
    await db_session.commit()
    return tenant, raw_key


@pytest.fixture
async def inactive_tenant_with_key(db_session: AsyncSession):
    """Create a tenant, issue a key, then deactivate the tenant."""
    store = TenantStore(db_session)
    await store.create_tenant(
        tenant_id="inactive_tenant",
        name="Inactive Tenant",
    )
    raw_key, _ = await store.create_api_key(tenant_id="inactive_tenant", label="test")
    await store.deactivate_tenant("inactive_tenant")
    await db_session.commit()
    return raw_key


# ── App fixture with DB override ──────────────────────────────────────────────

@pytest.fixture
async def client(db_session: AsyncSession):
    """
    FastAPI test client with DB session injected and lifespan managed.

    - LifespanManager runs full app startup/shutdown per test
    - DB session overridden to SQLite in-memory
    - Redis unavailable — pipeline starts in degraded mode (expected)
    - _container reset before each test to avoid stale state between runs
    """
    from asgi_lifespan import LifespanManager
    from lead_entry_guard.api import app as app_module
    import lead_entry_guard.config.settings as settings_module

    # Reset global state before each test
    app_module._container = None
    settings_module._settings = None

    async def override_get_session():
        yield db_session

    app_module.app.dependency_overrides[get_session] = override_get_session

    async with LifespanManager(app_module.app) as manager:
        async with AsyncClient(
            transport=ASGITransport(app=manager.app),
            base_url="http://test",
        ) as ac:
            yield ac

    app_module.app.dependency_overrides.clear()
    app_module._container = None
    settings_module._settings = None


# ── Tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_ingest_missing_api_key_returns_401(client: AsyncClient):
    """No X-API-Key header → 401."""
    response = await client.post(
        "/v1/leads/ingest",
        json={"email": "test@example.com"},
    )
    assert response.status_code == 401
    assert "X-API-Key" in response.text or "Missing" in response.text


@pytest.mark.asyncio
async def test_ingest_invalid_api_key_returns_401(client: AsyncClient):
    """Invalid X-API-Key → 401."""
    response = await client.post(
        "/v1/leads/ingest",
        headers={"X-API-Key": "leg_invalid_key_that_does_not_exist"},
        json={"email": "test@example.com"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_ingest_inactive_tenant_returns_403(
    client: AsyncClient,
    inactive_tenant_with_key: str,
):
    """Valid key but inactive tenant → 403."""
    response = await client.post(
        "/v1/leads/ingest",
        headers={"X-API-Key": inactive_tenant_with_key},
        json={"email": "test@example.com"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_ingest_valid_key_returns_decision(
    client: AsyncClient,
    tenant_with_key: tuple,
):
    """Valid key → ingest proceeds, returns decision."""
    _, raw_key = tenant_with_key
    response = await client.post(
        "/v1/leads/ingest",
        headers={"X-API-Key": raw_key},
        json={"email": "valid@example.com", "source_id": "src-001"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["tenant_id"] == "test_tenant"
    assert data["decision"] in ("PASS", "WARN", "REJECT", "DUPLICATE_HINT")
    assert "request_id" in data


@pytest.mark.asyncio
async def test_ingest_tenant_id_comes_from_auth_not_body(
    client: AsyncClient,
    tenant_with_key: tuple,
):
    """
    ADR-007: tenant_id in response must match auth tenant, not any value
    from request body (body no longer accepts tenant_id at all).
    """
    _, raw_key = tenant_with_key
    response = await client.post(
        "/v1/leads/ingest",
        headers={"X-API-Key": raw_key},
        # tenant_id not in body — enforced by IngestRequest schema
        json={"email": "adr007@example.com"},
    )
    assert response.status_code == 200
    assert response.json()["tenant_id"] == "test_tenant"


@pytest.mark.asyncio
async def test_ingest_body_with_tenant_id_field_ignored(
    client: AsyncClient,
    tenant_with_key: tuple,
):
    """
    Extra fields in body (including tenant_id) are ignored by Pydantic.
    tenant_id in response must still come from auth.
    """
    _, raw_key = tenant_with_key
    response = await client.post(
        "/v1/leads/ingest",
        headers={"X-API-Key": raw_key},
        # Attempt to smuggle tenant_id — should be ignored (extra="ignore" in Pydantic)
        json={"email": "smuggle@example.com", "tenant_id": "evil_tenant"},
    )
    assert response.status_code == 200
    assert response.json()["tenant_id"] == "test_tenant"  # auth wins


@pytest.mark.asyncio
async def test_replay_same_source_id_returns_decision(
    client: AsyncClient,
    tenant_with_key: tuple,
):
    """
    Same source_id with valid key returns a valid decision on both calls.

    Full idempotency (same request_id on replay) requires Redis — tested
    in tests/integration/test_idempotency_across_decisions.py.
    Here we verify auth + pipeline integration: both calls succeed and
    return a valid decision for the authenticated tenant.
    """
    _, raw_key = tenant_with_key
    payload = {"email": "idempotent@example.com", "source_id": "idm-replay-001"}

    r1 = await client.post(
        "/v1/leads/ingest",
        headers={"X-API-Key": raw_key},
        json=payload,
    )
    r2 = await client.post(
        "/v1/leads/ingest",
        headers={"X-API-Key": raw_key},
        json=payload,
    )
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["tenant_id"] == "test_tenant"
    assert r2.json()["tenant_id"] == "test_tenant"
    assert r1.json()["decision"] in ("PASS", "WARN", "REJECT", "DUPLICATE_HINT")
