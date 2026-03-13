"""Core domain models for Lead Entry Guard."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class DecisionClass(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    REJECT = "REJECT"
    DUPLICATE_HINT = "DUPLICATE_HINT"
    # Degraded variants
    PASS_DEGRADED = "PASS_DEGRADED"
    WARN_DEGRADED = "WARN_DEGRADED"


class ReasonCode(str, Enum):
    # Success
    OK = "OK"
    # Warnings
    WARN_INDEX_UNAVAILABLE = "WARN_INDEX_UNAVAILABLE"
    WARN_DUPLICATE_POSSIBLE = "WARN_DUPLICATE_POSSIBLE"
    WARN_POLICY_DEGRADED = "WARN_POLICY_DEGRADED"
    # Rejections
    REJECT_INVALID_EMAIL = "REJECT_INVALID_EMAIL"
    REJECT_INVALID_PHONE = "REJECT_INVALID_PHONE"
    REJECT_MISSING_REQUIRED = "REJECT_MISSING_REQUIRED"
    REJECT_POLICY_RULE = "REJECT_POLICY_RULE"
    REJECT_TENANT_BLOCKED = "REJECT_TENANT_BLOCKED"
    # Duplicate
    DUPLICATE_REDIS_CONFIRMED = "DUPLICATE_REDIS_CONFIRMED"
    # Degraded
    DEGRADED_REDIS_UNAVAILABLE = "DEGRADED_REDIS_UNAVAILABLE"
    DEGRADED_BLOOM_UNAVAILABLE = "DEGRADED_BLOOM_UNAVAILABLE"


class DegradedModePolicy(str, Enum):
    ACCEPT_WITH_FLAG = "ACCEPT_WITH_FLAG"
    REJECT = "REJECT"
    QUEUE = "QUEUE"


class SourceType(str, Enum):
    API = "API"
    WEBHOOK = "WEBHOOK"
    IMPORT = "IMPORT"
    FORM = "FORM"


class LeadInput(BaseModel):
    """Raw incoming lead payload."""
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str
    source_id: str | None = None
    source_type: SourceType = SourceType.API
    email: str | None = None
    phone: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    company: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class NormalizedLead(BaseModel):
    """Lead after normalization pass."""
    original: LeadInput
    email_normalized: str | None = None
    phone_normalized: str | None = None
    first_name_normalized: str | None = None
    last_name_normalized: str | None = None
    company_normalized: str | None = None


class ValidationError(BaseModel):
    field: str
    reason_code: ReasonCode
    message: str


class ValidationResult(BaseModel):
    valid: bool
    errors: list[ValidationError] = Field(default_factory=list)


class FingerprintResult(BaseModel):
    """HMAC identity signal — never exposed outside lookup internals."""
    fingerprint_id: str
    key_id: str  # kid used for generation


class DuplicateHint(BaseModel):
    is_duplicate: bool
    confidence: str  # "confirmed" | "possible" | "none"
    reason_code: ReasonCode
    lookup_path: str  # "bloom_negative" | "redis_confirmed" | "degraded"


class PolicyVersions(BaseModel):
    policy_version: str
    ruleset_version: str
    config_version: str


class DecisionResult(BaseModel):
    """Final decision emitted by the Policy/Scoring Engine."""
    request_id: str
    tenant_id: str
    decision: DecisionClass
    reason_codes: list[ReasonCode]
    duplicate_hint: DuplicateHint | None = None
    duplicate_check_skipped: bool = False
    versions: PolicyVersions
    latency_ms: float = 0.0
    decided_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AuditMeta(BaseModel):
    """Privacy-safe audit record — no PII, no fingerprints."""
    request_id: str
    tenant_id: str
    decision: DecisionClass
    reason_codes: list[ReasonCode]
    source_type: SourceType
    duplicate_check_skipped: bool
    versions: PolicyVersions
    recorded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
