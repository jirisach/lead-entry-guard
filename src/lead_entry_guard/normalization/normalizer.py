"""Normalization layer — email, phone, name, company."""
from __future__ import annotations

import logging
import re
import unicodedata

import phonenumbers

from lead_entry_guard.core.models import LeadInput, NormalizedLead

logger = logging.getLogger(__name__)

_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_email(raw: str) -> str:
    """Lowercase, strip whitespace. Does NOT validate — validation is separate."""
    return raw.strip().lower()


def _normalize_phone(raw: str, default_region: str = "US") -> str | None:
    """
    Parse and format phone to E.164 using libphonenumber.
    Returns None if unparseable (will be flagged at validation).
    """
    try:
        parsed = phonenumbers.parse(raw, default_region)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass
    return None


def _normalize_name(raw: str) -> str:
    """Unicode NFKC, strip, collapse whitespace, title-case."""
    normalized = unicodedata.normalize("NFKC", raw)
    collapsed = _WHITESPACE_RE.sub(" ", normalized).strip()
    return collapsed.title()


def _normalize_company(raw: str) -> str:
    """Unicode NFKC, strip, collapse whitespace. Preserve original casing."""
    normalized = unicodedata.normalize("NFKC", raw)
    return _WHITESPACE_RE.sub(" ", normalized).strip()


class NormalizationLayer:
    """Stateless normalization — transforms raw LeadInput into NormalizedLead."""

    def normalize(self, lead: LeadInput) -> NormalizedLead:
        return NormalizedLead(
            original=lead,
            email_normalized=_normalize_email(lead.email) if lead.email else None,
            phone_normalized=_normalize_phone(lead.phone) if lead.phone else None,
            first_name_normalized=_normalize_name(lead.first_name) if lead.first_name else None,
            last_name_normalized=_normalize_name(lead.last_name) if lead.last_name else None,
            company_normalized=_normalize_company(lead.company) if lead.company else None,
        )
