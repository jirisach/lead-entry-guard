"""Application configuration via pydantic-settings."""
from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class VaultBackend(str, Enum):
    HASHICORP = "hashicorp"
    AWS_KMS = "aws_kms"
    GCP_KMS = "gcp_kms"
    AZURE_KEY_VAULT = "azure_key_vault"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LEG_", env_file=".env", extra="ignore")

    # App
    app_name: str = "lead-entry-guard"
    app_version: str = "4.0.0"
    environment: Literal["development", "staging", "production"] = "development"
    debug: bool = False

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_connect_timeout: float = 2.0
    redis_socket_timeout: float = 1.0
    redis_max_connections: int = 50

    # Duplicate TTL
    duplicate_ttl_seconds: int = 60 * 60 * 24 * 30  # 30 days
    idempotency_ttl_seconds: int = 60 * 60 * 24      # 24 hours

    # HMAC / Key Management
    vault_backend: VaultBackend = VaultBackend.HASHICORP
    vault_url: str = "http://localhost:8200"
    vault_token: str = ""
    vault_secret_path: str = "secret/lead-entry-guard/hmac-keys"
    key_poll_interval_seconds: int = 60
    key_confirmation_timeout_seconds: int = 300  # P1 alert threshold

    # Bloom filter defaults
    bloom_default_capacity: int = 2_000_000
    bloom_target_fpr: float = 0.001  # 0.1%
    bloom_fill_ratio_alert_threshold: float = 0.70
    bloom_fpr_alert_threshold: float = 0.01  # 1%

    # Policy engine
    policy_version: str = "v1.0.0"
    ruleset_version: str = "v1.0.0"
    config_version: str = "v1.0.0"
    config_mismatch_grace_period_seconds: int = 60

    # Shadow mode
    shadow_queue_max_size: int = 10_000
    shadow_cpu_budget_pct: float = 0.20
    shadow_sample_rate_low_traffic: float = 1.0    # < 1000 req/s
    shadow_sample_rate_high_traffic: float = 0.10  # > 1000 req/s
    shadow_traffic_threshold_rps: int = 1_000
    shadow_min_sample_size: int = 1_000
    shadow_max_duration_days: int = 30
    shadow_auto_escalate_days: int = 14

    # QUEUE degraded mode
    queue_hard_cap_per_tenant: int = 500
    queue_hold_timeout_seconds: int = 60 * 15  # 15 minutes

    # Reconciliation
    reconciliation_per_tenant_rate_limit: int = 1_000   # per hour
    reconciliation_global_rate_limit: int = 10_000       # per hour
    reconciliation_retry_max_age_hours: int = 24

    # Telemetry
    telemetry_queue_max_size: int = 50_000
    telemetry_queue_warn_threshold: float = 0.80
    statsd_host: str = "localhost"
    statsd_port: int = 8125
    heartbeat_interval_seconds: int = 60

    @field_validator("bloom_target_fpr")
    @classmethod
    def validate_fpr(cls, v: float) -> float:
        if not 0 < v < 1:
            raise ValueError("bloom_target_fpr must be between 0 and 1")
        return v


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
