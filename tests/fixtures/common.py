"""Shared test fixtures."""
from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any

import pytest

from lead_entry_guard.core.models import LeadInput, SourceType
from lead_entry_guard.security.hmac_keys import HMACKeyManager
from lead_entry_guard.security.vault import InMemoryVaultClient


def make_key_ring(
    current_kid: str = "v1",
    previous_kid: str | None = None,
    previous_expires: str | None = None,
) -> dict[str, Any]:
    ring: dict[str, Any] = {
        "current": {
            "kid": current_kid,
            "secret_hex": secrets.token_hex(32),
            "activated_at": "2026-01-01T00:00:00+00:00",
        }
    }
    if previous_kid:
        ring["previous"] = {
            "kid": previous_kid,
            "secret_hex": secrets.token_hex(32),
            "activated_at": "2025-11-01T00:00:00+00:00",
            "expires_at": previous_expires or "2026-04-01T00:00:00+00:00",
        }
    return ring


@pytest.fixture
async def key_manager() -> HMACKeyManager:
    km = HMACKeyManager()
    vault = InMemoryVaultClient(make_key_ring())
    await km.load_from_vault(vault)
    return km


@pytest.fixture
def sample_lead() -> LeadInput:
    return LeadInput(
        tenant_id="tenant_test",
        source_id="src_001",
        source_type=SourceType.API,
        email="test@example.com",
        phone="+12025550100",
        first_name="John",
        last_name="Doe",
        company="Acme Corp",
    )


@pytest.fixture
def invalid_lead() -> LeadInput:
    return LeadInput(
        tenant_id="tenant_test",
        email="not-an-email",
        phone="invalid",
    )
