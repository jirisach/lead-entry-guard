"""Fingerprint builder — computes HMAC identity signal from normalized lead."""
from __future__ import annotations

import logging

from lead_entry_guard.core.exceptions import FingerprintError
from lead_entry_guard.core.models import FingerprintResult, NormalizedLead
from lead_entry_guard.security.hmac_keys import HMACKeyManager

logger = logging.getLogger(__name__)

# Primary identity anchor — must be present for a fingerprint to be built.
# Email is the authoritative identity signal for CRM ingestion.
_PRIMARY_IDENTITY_FIELDS = ("email_normalized",)

# Supplementary fields — included ONLY when primary fields are not available.
# Phone alone can identify a lead when email is absent.
# IMPORTANT: supplementary fields are never mixed with primary fields.
# This ensures that same-email leads always produce the same fingerprint
# regardless of whether an optional phone was provided or was invalid.
_SUPPLEMENTARY_IDENTITY_FIELDS = ("phone_normalized",)


def _build_identity_string(lead: NormalizedLead) -> str:
    """
    Compose deterministic identity string from normalized fields.

    Strategy:
      1. Use primary fields (email) if available — phone is intentionally excluded
         to prevent optional/invalid phone from fragmenting the identity.
      2. Fall back to supplementary fields (phone) only if no primary field present.

    This means:
      - same email + valid phone   → same fingerprint as same email + invalid phone
      - same email + valid phone   → same fingerprint as same email + no phone
      - phone only (no email)      → fingerprint based on phone
    """
    # Try primary fields first
    primary_parts = []
    for field_name in _PRIMARY_IDENTITY_FIELDS:
        value = getattr(lead, field_name, None)
        if value:
            primary_parts.append(f"{field_name}={value}")

    if primary_parts:
        return "|".join(sorted(primary_parts))

    # Fall back to supplementary fields only if no primary fields available
    supplementary_parts = []
    for field_name in _SUPPLEMENTARY_IDENTITY_FIELDS:
        value = getattr(lead, field_name, None)
        if value:
            supplementary_parts.append(f"{field_name}={value}")

    return "|".join(sorted(supplementary_parts))


class FingerprintBuilder:
    """
    Computes HMAC-SHA256 identity signal.

    Identity strategy:
      - Email is the primary anchor. Same email always produces the same fingerprint
        regardless of phone presence or validity.
      - Phone is used as fallback only when email is absent.
      - This prevents optional phone fragmentation in CRM deduplication.

    IMPORTANT: The fingerprint_id must NEVER appear in logs, telemetry,
    or any output outside the duplicate lookup subsystem.
    """

    def __init__(self, key_manager: HMACKeyManager) -> None:
        self._km = key_manager

    def build(self, lead: NormalizedLead) -> FingerprintResult:
        tenant_id = lead.original.tenant_id
        identity_str = _build_identity_string(lead)

        if not identity_str:
            raise FingerprintError(
                f"Cannot build fingerprint for tenant={tenant_id}: no identity fields present"
            )

        # generate_fingerprint returns (hex_digest, kid) — NEVER log hex_digest
        fingerprint_id, kid = self._km.generate_fingerprint(tenant_id, identity_str)

        return FingerprintResult(fingerprint_id=fingerprint_id, key_id=kid)
