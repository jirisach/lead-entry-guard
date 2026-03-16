"""Policy and Scoring Engine."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from lead_entry_guard.core.models import (
    DecisionClass,
    DecisionResult,
    DuplicateHint,
    NormalizedLead,
    PolicyVersions,
    ReasonCode,
    RecoverabilityAssessment,
    SalvagePolicy,
    ValidationResult,
)
from lead_entry_guard.config.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass
class PolicyContext:
    normalized_lead: NormalizedLead
    validation_result: ValidationResult
    duplicate_hint: DuplicateHint | None
    recoverability: RecoverabilityAssessment | None = None
    salvage_policy: SalvagePolicy = SalvagePolicy.STRICT
    duplicate_check_skipped: bool = False


class PolicyRule:
    """Interface for a single policy rule."""

    @property
    def rule_id(self) -> str:
        raise NotImplementedError

    def evaluate(self, ctx: PolicyContext) -> tuple[DecisionClass, list[ReasonCode]] | None:
        """Return (decision, reason_codes) if this rule fires, else None."""
        raise NotImplementedError


class RejectOnFatalValidationError:
    """Reject immediately on fatal validation errors — not salvageable."""
    rule_id = "reject_on_fatal_validation_error"

    def evaluate(self, ctx: PolicyContext) -> tuple[DecisionClass, list[ReasonCode]] | None:
        if ctx.recoverability and ctx.recoverability.fatal_errors:
            codes = [e.reason_code for e in ctx.recoverability.fatal_errors]
            return DecisionClass.REJECT, codes
        # Fallback: if no recoverability assessment, use raw validation result
        if not ctx.recoverability and not ctx.validation_result.valid:
            codes = [e.reason_code for e in ctx.validation_result.errors]
            return DecisionClass.REJECT, codes
        return None


class HandleRecoverableErrors:
    """Apply SalvagePolicy to recoverable validation errors.

    Only fires AFTER duplicate signals have been evaluated — duplicate truth
    is a stronger business signal than a recoverable phone issue.

    STRICT     → REJECT (same as fatal)
    SALVAGE    → WARN with remapped reason codes
    QUARANTINE → WARN + WARN_MANUAL_REVIEW_REQUIRED (explicit review signal)
                 Note: this does not yet route to a quarantine queue —
                 it flags the lead for downstream ops review via reason code.
                 A full quarantine sink is a separate infrastructure concern.
    """
    rule_id = "handle_recoverable_errors"

    def evaluate(self, ctx: PolicyContext) -> tuple[DecisionClass, list[ReasonCode]] | None:
        if not ctx.recoverability or not ctx.recoverability.recoverable_errors:
            return None

        codes = [e.reason_code for e in ctx.recoverability.recoverable_errors]

        if ctx.salvage_policy == SalvagePolicy.STRICT:
            return DecisionClass.REJECT, codes

        if ctx.salvage_policy == SalvagePolicy.SALVAGE:
            warn_codes = [_remap_to_warn(c) for c in codes]
            return DecisionClass.WARN, warn_codes

        if ctx.salvage_policy == SalvagePolicy.QUARANTINE:
            warn_codes = [_remap_to_warn(c) for c in codes]
            warn_codes.append(ReasonCode.WARN_MANUAL_REVIEW_REQUIRED)
            return DecisionClass.WARN, warn_codes

        return None


class RejectOnConfirmedDuplicate:
    rule_id = "reject_on_confirmed_duplicate"

    def evaluate(self, ctx: PolicyContext) -> tuple[DecisionClass, list[ReasonCode]] | None:
        if ctx.duplicate_hint and ctx.duplicate_hint.is_duplicate:
            return DecisionClass.DUPLICATE_HINT, [ReasonCode.DUPLICATE_REDIS_CONFIRMED]
        return None


class WarnOnSkippedDuplicateCheck:
    rule_id = "warn_on_skipped_duplicate_check"

    def evaluate(self, ctx: PolicyContext) -> tuple[DecisionClass, list[ReasonCode]] | None:
        if ctx.duplicate_check_skipped:
            return DecisionClass.WARN, [ReasonCode.WARN_INDEX_UNAVAILABLE]
        return None


def _remap_to_warn(code: ReasonCode) -> ReasonCode:
    """Remap REJECT_ reason codes to WARN_ equivalents for salvage path."""
    _mapping = {
        ReasonCode.REJECT_INVALID_PHONE: ReasonCode.WARN_INVALID_OPTIONAL_PHONE,
    }
    return _mapping.get(code, ReasonCode.WARN_DATA_QUALITY)


DEFAULT_RULESET: list[PolicyRule] = [
    RejectOnFatalValidationError(),   # 1. fatal errors — always first
    RejectOnConfirmedDuplicate(),     # 2. duplicate signal — stronger than recoverable errors
    WarnOnSkippedDuplicateCheck(),    # 3. degraded duplicate check
    HandleRecoverableErrors(),        # 4. recoverable errors — only if not duplicate
]


class PolicyEngine:
    """Deterministic policy engine.

    - No long hidden condition chains
    - No network-dependent rule evaluation
    - No opaque heuristics without reason codes
    - Every decision carries version metadata
    - SalvagePolicy controls recoverable error handling per tenant
    """

    def __init__(
        self,
        rules: list[PolicyRule] | None = None,
        versions: PolicyVersions | None = None,
    ) -> None:
        self._rules = rules or DEFAULT_RULESET
        settings = get_settings()
        self._versions = versions or PolicyVersions(
            policy_version=settings.policy_version,
            ruleset_version=settings.ruleset_version,
            config_version=settings.config_version,
        )

    def decide(self, ctx: PolicyContext) -> tuple[DecisionClass, list[ReasonCode]]:
        for rule in self._rules:
            result = rule.evaluate(ctx)
            if result is not None:
                decision, codes = result
                logger.debug(
                    "Policy rule fired",
                    extra={"rule_id": rule.rule_id, "decision": decision},
                )
                return decision, codes

        # Default: PASS — with quality flags as informational reason codes
        quality_codes = list(ctx.recoverability.quality_flags) if ctx.recoverability else []
        return DecisionClass.PASS, [ReasonCode.OK, *quality_codes]

    @property
    def versions(self) -> PolicyVersions:
        return self._versions
