"""
Phase 3B — Signal enforcement models.

Scope: 3B.0 (contract) + A3 (suspicious_domain) + A4 (source_conflict) + A6 (shared_inbox)

Models defined here:
  - VisibilityProjection   — downstream-facing fields, no PII
  - FallbackPolicy         — consequence if signal is ignored
  - SignalDefinition       — full signal contract (action + visibility + fallback)
  - SignalResult           — emitted instance of a signal (definition + runtime context)
  - FieldSourceRecord      — source provenance for a single field value
  - LeadSignalContext      — unified input for all signal rules

Invariants (ADR-008):
  - Every SignalDefinition must define action, visibility, and fallback.
  - VisibilityProjection must never contain raw PII.
  - Visibility is the minimum consequence — no signal may be consequence-free.
  - fallback-exempt signals (informational) must still define visibility.
"""
from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


# ── Enums ────────────────────────────────────────────────────────────────────

class SignalAction(str, Enum):
    """Immediate system behavior when signal fires."""
    ACCEPT_WITH_FLAG = "accept_with_flag"
    PRESERVE_MANUAL_VALUE = "preserve_manual_value"
    ACCEPT_LOW_QUALITY = "accept_low_quality"
    ROUTE_FOR_REVIEW = "route_for_review"


class FallbackMode(str, Enum):
    """What happens if no downstream system reacts to the signal."""
    NO_OVERWRITE = "no_overwrite"
    AUTO_EXPIRE_REVIEW = "auto_expire_review"
    KEEP_ACCEPTED_LOW_TRUST = "keep_accepted_low_trust"
    MANUAL_VALUE_AUTHORITATIVE = "manual_value_authoritative"


class SignalClass(str, Enum):
    """Signal importance class — determines whether fallback is required."""
    CRITICAL = "critical"
    INFORMATIONAL = "informational"


# ── Core signal models ────────────────────────────────────────────────────────

class VisibilityProjection(BaseModel):
    """
    Downstream-facing signal projection.

    Rules (ADR-008 — Data Exposure Invariant):
      - Must never contain raw PII (email, phone, name, free-form text from input).
      - Use status codes, tags, and boolean flags only.
      - Must affect fields downstream systems already rely on.

    At least one field must be set — a signal with no visibility is invalid.
    """
    crm_status: str | None = None
    routing_tags: list[str] = Field(default_factory=list)
    api_flags: dict[str, bool] = Field(default_factory=dict)

    @model_validator(mode="after")
    def at_least_one_field_set(self) -> "VisibilityProjection":
        has_crm_status = self.crm_status is not None
        has_tags = bool(self.routing_tags)
        has_flags = bool(self.api_flags)
        if not (has_crm_status or has_tags or has_flags):
            raise ValueError(
                "VisibilityProjection must set at least one field "
                "(crm_status, routing_tags, or api_flags). "
                "A signal with no visibility projection violates ADR-008."
            )
        return self


class FallbackPolicy(BaseModel):
    """
    Consequence if no human or downstream system reacts to the signal.

    Required for CRITICAL signals.
    For INFORMATIONAL signals: recommended, not enforced at model level.
    'then' must not contain PII.
    """
    mode: FallbackMode
    after_hours: int | None = None
    then: str


class SignalDefinition(BaseModel):
    """
    Full signal contract — action + visibility + fallback.

    ADR-008 invariant: every signal emitted by the pipeline must define
    all three properties. A SignalDefinition missing any of them is invalid
    and must not be emitted.

    Validation enforced at construction time.
    """
    code: str
    signal_class: SignalClass = SignalClass.CRITICAL
    action: SignalAction
    visibility: VisibilityProjection
    fallback: FallbackPolicy | None = None

    @model_validator(mode="after")
    def critical_signal_must_have_fallback(self) -> "SignalDefinition":
        if self.signal_class == SignalClass.CRITICAL and self.fallback is None:
            raise ValueError(
                f"SignalDefinition '{self.code}' is CRITICAL and must define fallback. "
                "A critical signal without fallback violates ADR-008. "
                "If this signal should be fallback-exempt, set signal_class=INFORMATIONAL."
            )
        return self


class SignalResult(BaseModel):
    """
    Emitted signal instance — definition bound to a specific decision context.

    This is what appears in DecisionResult.signals.
    Always a deep copy of the definition at emit time.
    """
    code: str
    action: SignalAction
    visibility: VisibilityProjection
    fallback: FallbackPolicy | None = None
    signal_class: SignalClass = SignalClass.CRITICAL

    @classmethod
    def from_definition(cls, definition: SignalDefinition) -> "SignalResult":
        # Deep copy — SignalResult is an immutable snapshot at emit time.
        return cls(
            code=definition.code,
            action=definition.action,
            visibility=definition.visibility.model_copy(deep=True),
            fallback=definition.fallback.model_copy(deep=True) if definition.fallback else None,
            signal_class=definition.signal_class,
        )


# ── Lead signal context ───────────────────────────────────────────────────────

@dataclass
class FieldSourceRecord:
    """
    Tracks the source of a specific field value on a lead.

    source_type: "manual" | "enrichment" | "api" | "import" | "form"
    value: the raw field value (used internally only — never emitted to visibility)
    """
    field_name: str
    source_type: str
    value: Any


@dataclass
class LeadSignalContext:
    """
    Unified input for all signal rules.

    Each rule takes what it needs:
      - A3, A6: use email (domain / prefix detection)
      - A4:     use fields (source provenance conflict detection)

    Field values in `fields` are internal only — never copied into
    SignalResult or VisibilityProjection (ADR-008 PII invariant).
    """
    tenant_id: str
    email: str | None = None
    fields: list[FieldSourceRecord] = dataclass_field(default_factory=list)

    def get_fields(self, field_name: str) -> list[FieldSourceRecord]:
        """Return all source records for a given field name."""
        return [f for f in self.fields if f.field_name == field_name]


# ── DecisionResultV2 ──────────────────────────────────────────────────────────

@dataclass
class DecisionResultV2:
    """
    DecisionResult extended with Phase 3B signal enforcement fields.

    Additive — all existing DecisionResult fields are preserved.
    New field:
      signals: list[SignalResult] — emitted signals, always a list (never None)

    Signals annotate the primary decision — they do not change it.
    Downstream systems must read signals[*].visibility for enforcement.
    """
    request_id: str
    tenant_id: str
    decision: str
    reason_codes: list[str]
    duplicate_check_skipped: bool = False
    latency_ms: float = 0.0
    signals: list[SignalResult] = dataclass_field(default_factory=list)

    def has_signal(self, code: str) -> bool:
        return any(s.code == code for s in self.signals)

    def conflict_detected(self) -> bool:
        """Convenience — True if any source_conflict signal was emitted."""
        return self.has_signal("source_conflict_manual_vs_enrichment")
