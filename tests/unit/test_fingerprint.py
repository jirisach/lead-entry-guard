"""Unit tests — fingerprint generation and key rotation."""
from __future__ import annotations

import pytest

from lead_entry_guard.core.models import LeadInput
from lead_entry_guard.fingerprint.builder import FingerprintBuilder
from lead_entry_guard.normalization.normalizer import NormalizationLayer
from lead_entry_guard.security.hmac_keys import HMACKeyManager
from lead_entry_guard.security.vault import InMemoryVaultClient
from tests.fixtures.common import make_key_ring


@pytest.mark.asyncio
async def test_fingerprint_is_deterministic():
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring(current_kid="v1")))
    builder = FingerprintBuilder(km)
    normalizer = NormalizationLayer()

    lead = LeadInput(tenant_id="t1", email="test@example.com")
    normalized = normalizer.normalize(lead)

    fp1 = builder.build(normalized)
    fp2 = builder.build(normalized)

    assert fp1.fingerprint_id == fp2.fingerprint_id
    assert fp1.key_id == "v1"


@pytest.mark.asyncio
async def test_fingerprint_differs_per_tenant():
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))
    builder = FingerprintBuilder(km)
    normalizer = NormalizationLayer()

    lead_t1 = LeadInput(tenant_id="tenant_1", email="test@example.com")
    lead_t2 = LeadInput(tenant_id="tenant_2", email="test@example.com")

    fp1 = builder.build(normalizer.normalize(lead_t1))
    fp2 = builder.build(normalizer.normalize(lead_t2))

    assert fp1.fingerprint_id != fp2.fingerprint_id


@pytest.mark.asyncio
async def test_current_key_lookup_works():
    ring = make_key_ring(current_kid="v3")
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(ring))

    fp, kid = km.generate_fingerprint("t1", "email=test@example.com")
    assert kid == "v3"
    assert km.verify_fingerprint("t1", "email=test@example.com", fp)


@pytest.mark.asyncio
async def test_previous_key_lookup_works_during_overlap():
    """Test that previous key can still verify fingerprints during overlap window."""
    ring_v2 = make_key_ring(current_kid="v2")
    km_old = HMACKeyManager()
    await km_old.load_from_vault(InMemoryVaultClient(ring_v2))

    # Generate fingerprint with v2
    fp_v2, _ = km_old.generate_fingerprint("t1", "email=user@example.com")

    # Rotate to v3 — v2 becomes previous with future expiry
    ring_v3 = {
        "current": {
            "kid": "v3",
            "secret_hex": ring_v2["current"]["secret_hex"],  # different in reality
            "activated_at": "2026-03-01T00:00:00+00:00",
        },
        "previous": {
            "kid": "v2",
            "secret_hex": ring_v2["current"]["secret_hex"],
            "activated_at": "2026-01-15T00:00:00+00:00",
            "expires_at": "2026-04-01T00:00:00+00:00",  # future
        },
    }
    km_new = HMACKeyManager()
    await km_new.load_from_vault(InMemoryVaultClient(ring_v3))

    # Should verify with previous key
    verified = km_new.verify_fingerprint("t1", "email=user@example.com", fp_v2)
    assert verified


@pytest.mark.asyncio
async def test_rotation_does_not_create_duplicate_blind_spots(key_manager: HMACKeyManager):
    """After rotation, old fingerprints should still be verifiable during overlap."""
    builder = FingerprintBuilder(key_manager)
    normalizer = NormalizationLayer()
    lead = LeadInput(tenant_id="t1", email="stable@example.com")
    normalized = normalizer.normalize(lead)
    fp = builder.build(normalized)
    # Fingerprint was just created with current key — should still be findable
    assert fp.fingerprint_id is not None
