"""Validation layer — validates normalized lead fields."""
from __future__ import annotations

import re

import phonenumbers
from email_validator import EmailNotValidError, validate_email

from lead_entry_guard.core.models import (
    NormalizedLead,
    ReasonCode,
    ValidationError,
    ValidationResult,
)

_REQUIRED_FIELDS = ("email_normalized",)


class ValidationLayer:
    """Stateless validation of NormalizedLead."""

    def validate(self, lead: NormalizedLead) -> ValidationResult:
        errors: list[ValidationError] = []

        # Required fields check
        for field_name in _REQUIRED_FIELDS:
            if not getattr(lead, field_name, None):
                errors.append(
                    ValidationError(
                        field=field_name,
                        reason_code=ReasonCode.REJECT_MISSING_REQUIRED,
                        message=f"Field '{field_name}' is required",
                    )
                )

        # Email validation
        if lead.email_normalized:
            try:
                validate_email(lead.email_normalized, check_deliverability=False)
            except EmailNotValidError as exc:
                errors.append(
                    ValidationError(
                        field="email",
                        reason_code=ReasonCode.REJECT_INVALID_EMAIL,
                        message=str(exc),
                    )
                )

        # Phone validation (only if present and normalization produced a result)
        if lead.original.phone and not lead.phone_normalized:
            errors.append(
                ValidationError(
                    field="phone",
                    reason_code=ReasonCode.REJECT_INVALID_PHONE,
                    message="Phone number could not be parsed to E.164",
                )
            )

        return ValidationResult(valid=len(errors) == 0, errors=errors)
