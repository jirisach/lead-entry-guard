"""
Database models — Phase 3A tenant store.

Tables:
  tenants   — registered tenants with config
  api_keys  — hashed API keys linked to tenants

Security notes:
  - Raw API keys are never stored. Only SHA-256 hash is persisted.
  - Key comparison uses hmac.compare_digest to prevent timing attacks.
  - key_prefix is unique per tenant to prevent revoke ambiguity.
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean, DateTime, ForeignKey, Integer, String, UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# Max active API keys per tenant — prevents unbounded key accumulation
MAX_KEYS_PER_TENANT = 20


class Base(DeclarativeBase):
    pass


class TenantRow(Base):
    """Persistent tenant registration."""
    __tablename__ = "tenants"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    tier: Mapped[str] = mapped_column(String(32), nullable=False, default="medium")
    salvage_policy: Mapped[str] = mapped_column(String(32), nullable=False, default="STRICT")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    api_keys: Mapped[list["ApiKeyRow"]] = relationship(
        "ApiKeyRow", back_populates="tenant", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<TenantRow tenant_id={self.tenant_id!r} tier={self.tier!r}>"


class ApiKeyRow(Base):
    """
    Hashed API key linked to a tenant.

    Raw key is shown ONCE at creation and never stored.
    Only the SHA-256 hash is persisted.
    key_prefix is unique per tenant to ensure unambiguous revoke.
    """
    __tablename__ = "api_keys"
    __table_args__ = (
        # Unique prefix per tenant — prevents revoke ambiguity
        UniqueConstraint("tenant_id", "key_prefix", name="uq_api_keys_tenant_prefix"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("tenants.tenant_id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    key_prefix: Mapped[str] = mapped_column(String(12), nullable=False)
    label: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_used_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    tenant: Mapped["TenantRow"] = relationship("TenantRow", back_populates="api_keys")

    def __repr__(self) -> str:
        return f"<ApiKeyRow tenant_id={self.tenant_id!r} prefix={self.key_prefix!r}>"

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at


# ── Key generation helpers ────────────────────────────────────────────────────

def generate_api_key() -> tuple[str, str, str]:
    """
    Generate a new API key.

    Returns:
        (raw_key, key_hash, key_prefix)

    raw_key    — shown ONCE to the caller, never stored
    key_hash   — SHA-256 hex digest, stored in DB
    key_prefix — first 12 chars of raw key, stored for identification
    """
    raw = f"leg_{secrets.token_urlsafe(32)}"
    key_hash = hashlib.sha256(raw.encode()).hexdigest()
    key_prefix = raw[:12]
    return raw, key_hash, key_prefix


def hash_api_key(raw_key: str) -> str:
    """Hash an incoming API key for DB lookup."""
    return hashlib.sha256(raw_key.encode()).hexdigest()


def verify_api_key(raw_key: str, stored_hash: str) -> bool:
    """
    Constant-time comparison of API key hash.
    Prevents timing attacks — use this instead of == for key verification.
    """
    computed = hashlib.sha256(raw_key.encode()).hexdigest()
    return hmac.compare_digest(computed, stored_hash)
