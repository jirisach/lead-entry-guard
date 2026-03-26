"""
Database session factory — Phase 3A.

Provides async SQLAlchemy engine and session for FastAPI dependency injection.
"""
from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from lead_entry_guard.db.models import Base

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_db(database_url: str) -> None:
    """Initialize the async engine and session factory. Call once at startup."""
    global _engine, _session_factory
    _engine = create_async_engine(
        database_url,
        echo=False,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,  # detect stale connections
    )
    _session_factory = async_sessionmaker(
        _engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )


async def create_tables() -> None:
    """Create all tables. Used in dev/test — production uses Alembic migrations."""
    assert _engine is not None, "init_db() must be called before create_tables()"
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — yields an async DB session per request."""
    assert _session_factory is not None, "init_db() must be called at startup"
    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_engine() -> AsyncEngine:
    """Return the engine — used for health checks."""
    assert _engine is not None, "init_db() must be called at startup"
    return _engine
