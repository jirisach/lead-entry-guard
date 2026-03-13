"""Unit test fixtures."""
import pytest

from lead_entry_guard.security.hmac_keys import HMACKeyManager
from lead_entry_guard.security.vault import InMemoryVaultClient
from tests.fixtures.common import make_key_ring


@pytest.fixture
async def key_manager() -> HMACKeyManager:
    km = HMACKeyManager()
    await km.load_from_vault(InMemoryVaultClient(make_key_ring()))
    return km
