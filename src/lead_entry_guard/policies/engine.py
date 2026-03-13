"""Policy and Scoring Engine."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

from lead_entry_guard.core.models import (
    DecisionClass,
    DecisionResult,
    DuplicateHint,
    NormalizedLead,
    PolicyVersions,
    ReasonCode,
    ValidationResult,
)
from lead_entry_guard.config.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass
class PolicyContext:
    normalized_lead: NormalizedLead
    validation_result: ValidationResult
    duplicate_hint: DuplicateHint | None
    duplicate_check_skipped: bool = False


class PolicyRule(Protocol):
    """Interface for a single policy rule."""

    @property
    def rule_id(self) -> str: ...

    def evaluate(self, ctx: PolicyContext) -> tuple[DecisionClass, list[ReasonCode]] | None:
        """Return (decision, reason_codes) if this rule fires, else None."""


class RejectOnValidationFailure:
    rule_id = "reject_on_validation_failure"

    def evaluate(self, ctx: PolicyContext) -> tuple[DecisionClass, list[ReasonCode]] | None:
        if not ctx.validation_result.valid:
            codes = [e.reason_code for e in ctx.validation_result.errors]
            return DecisionClass.REJECT, codes
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


DEFAULT_RULESET: list[PolicyRule] = [
    RejectOnValidationFailure(),
    RejectOnConfirmedDuplicate(),
    WarnOnSkippedDuplicateCheck(),
]


class PolicyEngine:
    """
    Deterministic policy engine.

    - No long hidden condition chains
    - No network-dependent rule evaluation
    - No opaque heuristics without reason codes
    - Every decision carries version metadata
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

        # Default: PASS
        return DecisionClass.PASS, [ReasonCode.OK]

    @property
    def versions(self) -> PolicyVersions:
        return self._versions
