"""Per-tenant configuration model."""
from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

from lead_entry_guard.core.models import DegradedModePolicy


class TenantTier(str, Enum):
    SMALL = "small"       # < 10k leads/month
    MEDIUM = "medium"     # < 100k
    LARGE = "large"       # < 1M
    ENTERPRISE = "enterprise"  # > 1M


TIER_BLOOM_CAPACITY: dict[TenantTier, int] = {
    TenantTier.SMALL: 500_000,
    TenantTier.MEDIUM: 2_000_000,
    TenantTier.LARGE: 10_000_000,
    TenantTier.ENTERPRISE: 50_000_000,
}


class TenantConfig(BaseModel):
    tenant_id: str
    tier: TenantTier = TenantTier.MEDIUM
    degraded_mode_policy: DegradedModePolicy = DegradedModePolicy.ACCEPT_WITH_FLAG
    # QUEUE fallback when cap or timeout hit
    queue_fallback_policy: DegradedModePolicy = DegradedModePolicy.ACCEPT_WITH_FLAG
    # Dedicated Redis cluster (sensitive tenants)
    dedicated_redis: bool = False
    dedicated_redis_url: str | None = None
    # Rate limits
    redis_ops_per_second: int = 1000
    reconciliation_corrections_per_hour: int = 1000
    # Bloom sizing override (None = derive from tier)
    bloom_capacity_override: int | None = None
    # Policy overrides
    active_policy_version: str | None = None

    @property
    def bloom_capacity(self) -> int:
        if self.bloom_capacity_override:
            return self.bloom_capacity_override
        return TIER_BLOOM_CAPACITY[self.tier]


class TenantRegistry:
    """In-memory tenant config registry. In production, backed by a config store."""

    def __init__(self) -> None:
        self._configs: dict[str, TenantConfig] = {}

    def register(self, config: TenantConfig) -> None:
        self._configs[config.tenant_id] = config

    def get(self, tenant_id: str) -> TenantConfig:
        if tenant_id not in self._configs:
            # Auto-register with defaults
            config = TenantConfig(tenant_id=tenant_id)
            self._configs[tenant_id] = config
        return self._configs[tenant_id]

    def all(self) -> list[TenantConfig]:
        return list(self._configs.values())
