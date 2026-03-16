"""Recoverability / Salvage Layer.

Sits between ValidationLayer and PolicyEngine.
Classifies validation errors into fatal vs recoverable so the PolicyEngine
can apply the tenant's SalvagePolicy without hard-coding business logic
in the validator itself.

Fatal errors (always REJECT regardless of policy):
    - invalid or missing email
    - missing required fields
    - unknown/unexpected error codes (fail-safe)

Recoverable errors (handled per SalvagePolicy):
    - invalid phone when phone was provided but could not be normalized to E.164
      (email is valid → lead is salvageable under SALVAGE or QUARANTINE policy)

Quality flags (informational, never cause REJECT):
    - all-uppercase email that was normalized (suggests manual entry or legacy system)

Note: missing optional fields (company, first_name, last_name) are not currently
classified as recoverable errors — the validator does not produce errors for them.
If this changes in the future, add their reason codes to _RECOVERABLE_REASON_CODES.
"""
from __future__ import annotations

from lead_entry_guard.core.models import (
    NormalizedLead,
    ReasonCode,
    RecoverabilityAssessment,
    ValidationError,
    ValidationResult,
)

# Reason codes that are always fatal — lead cannot be salvaged
_FATAL_REASON_CODES = frozenset({
    ReasonCode.REJECT_INVALID_EMAIL,
    ReasonCode.REJECT_MISSING_REQUIRED,
    ReasonCode.REJECT_TENANT_BLOCKED,
    ReasonCode.REJECT_POLICY_RULE,
})

# Reason codes that are recoverable — lead may pass with WARN
_RECOVERABLE_REASON_CODES = frozenset({
    ReasonCode.REJECT_INVALID_PHONE,
})


class RecoverabilityLayer:
    """Classifies validation errors into fatal vs recoverable.

    Stateless — safe to share across requests and tenants.
    Does NOT make the final decision — that is the PolicyEngine's job.
    """

    def assess(
        self,
        validation_result: ValidationResult,
        normalized_lead: NormalizedLead,
    ) -> RecoverabilityAssessment:
        """Classify validation errors and detect quality flags.

        Args:
            validation_result: output of ValidationLayer.validate()
            normalized_lead: normalized lead for quality flag detection

        Returns:
            RecoverabilityAssessment with fatal_errors, recoverable_errors,
            quality_flags populated.
        """
        fatal: list[ValidationError] = []
        recoverable: list[ValidationError] = []
        quality_flags: list[ReasonCode] = []

        for error in validation_result.errors:
            if error.reason_code in _FATAL_REASON_CODES:
                fatal.append(error)
            elif error.reason_code in _RECOVERABLE_REASON_CODES:
                recoverable.append(error)
            else:
                # Unknown error codes are treated as fatal — fail safe
                fatal.append(error)

        # Quality flags — detectable from normalized lead regardless of errors
        quality_flags.extend(_detect_quality_flags(normalized_lead))

        return RecoverabilityAssessment(
            fatal_errors=fatal,
            recoverable_errors=recoverable,
            quality_flags=quality_flags,
        )


def _detect_quality_flags(lead: NormalizedLead) -> list[ReasonCode]:
    """Detect data quality signals that are not validation errors.

    Currently detects:
    - all-uppercase email (suggests manual entry or legacy system export)
    """
    flags: list[ReasonCode] = []

    # Email was all-uppercase — suggests manual entry or legacy system
    if (
        lead.original.email
        and lead.email_normalized
        and lead.original.email != lead.email_normalized
        and lead.original.email.upper() == lead.original.email
    ):
        flags.append(ReasonCode.WARN_DATA_QUALITY)

    return flags
