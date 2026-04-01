"""
Integration test — shutdown_executor wired into app lifespan.

This test exists because:
  - shutdown_executor() existence is tested in test_signal_check_api.py
  - But whether app.py actually calls it in lifespan is NOT tested there

Gap: if someone registers the signal_check_router but forgets to add
the shutdown hook, worker threads leak on every app restart.

What this test does:
  Patches shutdown_signal_check_executor in app.py's namespace and runs
  the full app lifespan (startup → yield → shutdown). Then asserts the
  patched function was called exactly once during shutdown.

Dependencies:
  - asgi_lifespan (pip install asgi-lifespan)
  - db_session fixture from conftest.py (SQLite in-memory, same as test_auth_ingest.py)
  - Redis unavailable is expected and handled — pipeline starts in degraded mode

This is an INTEGRATION test, not a unit test. It runs the full app startup
and shutdown cycle. Slower than unit tests and has more dependencies.
Run it as part of the integration suite, not on every commit if that is too slow.

Placement: tests/integration/test_app_lifespan_shutdown.py
"""
from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from lead_entry_guard.db.models import Base


# ── DB fixture ────────────────────────────────────────────────────────────────
# Defined here (not conftest.py) — same pattern as test_auth_ingest.py.
# SQLite in-memory, no Postgres required.

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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_signal_check_executor_shutdown_called_in_lifespan(db_session):
    """
    App lifespan must call shutdown_signal_check_executor() on exit.

    Uses the same db_session fixture pattern as test_auth_ingest.py.
    Redis unavailable is expected — pipeline starts in degraded mode.
    """
    from asgi_lifespan import LifespanManager
    from lead_entry_guard.api import app as app_module
    import lead_entry_guard.config.settings as settings_module
    from lead_entry_guard.db.database import get_session

    app_module._container = None
    settings_module._settings = None

    async def override_get_session():
        yield db_session

    app_module.app.dependency_overrides[get_session] = override_get_session

    with patch(
        # Patch in app.py's namespace — where the name is imported and used
        "lead_entry_guard.api.app.shutdown_signal_check_executor"
    ) as mock_shutdown:
        async with LifespanManager(app_module.app):
            pass  # startup → yield → shutdown

        mock_shutdown.assert_called_once(), (
            "shutdown_signal_check_executor() must be called in app lifespan shutdown. "
            "Without this, _eval_executor worker threads leak on every app restart. "
            "Add: shutdown_signal_check_executor() after _container.shutdown() in lifespan."
        )

    app_module.app.dependency_overrides.clear()
    app_module._container = None
    settings_module._settings = None
