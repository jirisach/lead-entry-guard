"""Fingerprint builder — computes HMAC identity signal from normalized lead."""
from __future__ import annotations

import logging

from lead_entry_guard.core.exceptions import FingerprintError
from lead_entry_guard.core.models import FingerprintResult, NormalizedLead
from lead_entry_guard.security.hmac_keys import HMACKeyManager

logger = logging.getLogger(__name__)

# Fields used for identity signal composition
_IDENTITY_FIELDS = ("email_normalized", "phone_normalized")


def _build_identity_string(lead: NormalizedLead) -> str:
    """
    Compose deterministic identity string from normalized fields.
    Only non-None fields are included to maintain consistency.
    """
    parts = []
    for field_name in _IDENTITY_FIELDS:
        value = getattr(lead, field_name, None)
        if value:
            parts.append(f"{field_name}={value}")
    return "|".join(sorted(parts))


class FingerprintBuilder:
    """
    Computes HMAC-SHA256 identity signal.

    IMPORTANT: The fingerprint_id must NEVER appear in logs, telemetry,
    or any output outside the duplicate lookup subsystem.
    """

    def __init__(self, key_manager: HMACKeyManager) -> None:
        self._km = key_manager

    def build(self, lead: NormalizedLead) -> FingerprintResult:
        tenant_id = lead.original.tenant_id
        identity_str = _build_identity_string(lead)

        if not identity_str:
            # Raise domain exception — not generic ValueError — so pipeline
            # error mapping and future exception handlers can distinguish
            # "no identity fields" from unrelated runtime errors.
            raise FingerprintError(
                f"Cannot build fingerprint for tenant={tenant_id}: no identity fields present"
            )

        # generate_fingerprint returns (hex_digest, kid) — NEVER log hex_digest
        fingerprint_id, kid = self._km.generate_fingerprint(tenant_id, identity_str)

        return FingerprintResult(fingerprint_id=fingerprint_id, key_id=kid)
