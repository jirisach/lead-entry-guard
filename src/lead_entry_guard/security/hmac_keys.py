"""HMAC key management with dual-key rotation model."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from lead_entry_guard.core.exceptions import KeyNotFoundError, KeyRotationError

logger = logging.getLogger(__name__)


@dataclass
class HMACKey:
    kid: str
    secret: bytes
    activated_at: datetime
    expires_at: datetime | None = None

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at


@dataclass
class KeyRing:
    current: HMACKey
    previous: HMACKey | None = None

    def active_keys(self) -> list[HMACKey]:
        """Return all keys that should be tried for lookup verification."""
        keys = [self.current]
        if self.previous and not self.previous.is_expired():
            keys.append(self.previous)
        return keys


class HMACKeyManager:
    """
    Manages dual-key HMAC rotation.

    - current key: used for all new fingerprint generation
    - previous key: used for lookup verification during overlap window
    - overlap window >= Redis TTL (30 days)
    """

    def __init__(self, confirmation_timeout_seconds: int = 300) -> None:
        self._key_ring: KeyRing | None = None
        self._confirmation_timeout = confirmation_timeout_seconds
        self._confirmed = False
        self._lock = asyncio.Lock()

    async def load_from_vault(self, vault_client: "VaultClient") -> None:  # type: ignore[name-defined]
        """Pull key ring from Vault/KMS. Called by Key Distribution Service."""
        async with self._lock:
            try:
                key_data = await vault_client.get_key_ring()
                self._key_ring = self._parse_key_ring(key_data)
                self._confirmed = True
                logger.info("Key ring loaded", extra={"kid": self._key_ring.current.kid})
            except Exception as exc:
                raise KeyRotationError(f"Failed to load key ring: {exc}") from exc

    def _parse_key_ring(self, data: dict) -> KeyRing:
        current_data = data["current"]
        current = HMACKey(
            kid=current_data["kid"],
            secret=bytes.fromhex(current_data["secret_hex"]),
            activated_at=datetime.fromisoformat(current_data["activated_at"]),
        )
        previous: HMACKey | None = None
        if "previous" in data:
            prev_data = data["previous"]
            previous = HMACKey(
                kid=prev_data["kid"],
                secret=bytes.fromhex(prev_data["secret_hex"]),
                activated_at=datetime.fromisoformat(prev_data["activated_at"]),
                expires_at=datetime.fromisoformat(prev_data["expires_at"]),
            )
        return KeyRing(current=current, previous=previous)

    def generate_fingerprint(self, tenant_id: str, identity_fields: str) -> tuple[str, str]:
        """
        Generate HMAC fingerprint using current key.

        Returns (fingerprint_hex, kid).
        NEVER log the fingerprint_hex.
        """
        if self._key_ring is None:
            raise KeyNotFoundError("Key ring not loaded")
        key = self._key_ring.current
        material = f"{tenant_id}:{identity_fields}".encode()
        digest = hmac.new(key.secret, material, hashlib.sha256).hexdigest()
        return digest, key.kid

    def verify_fingerprint(self, tenant_id: str, identity_fields: str, candidate: str) -> bool:
        """
        Try current + previous keys (dual-key verification for rotation overlap).
        """
        if self._key_ring is None:
            raise KeyNotFoundError("Key ring not loaded")
        material = f"{tenant_id}:{identity_fields}".encode()
        for key in self._key_ring.active_keys():
            digest = hmac.new(key.secret, material, hashlib.sha256).hexdigest()
            if hmac.compare_digest(digest, candidate):
                return True
        return False

    def has_confirmed_key(self) -> bool:
        return self._confirmed and self._key_ring is not None

    def current_kid(self) -> str:
        if not self._key_ring:
            raise KeyNotFoundError("Key ring not loaded")
        return self._key_ring.current.kid


class KeyDistributionService:
    """
    Coordinates key rotation across nodes.

    - Polls Vault/KMS every poll_interval seconds (or event-driven)
    - Broadcasts new key ring to all registered nodes
    - Tracks confirmation; raises P1 alert if node does not confirm within timeout
    """

    def __init__(
        self,
        vault_client: "VaultClient",  # type: ignore[name-defined]
        nodes: list[HMACKeyManager],
        poll_interval: int = 60,
        confirmation_timeout: int = 300,
    ) -> None:
        self._vault = vault_client
        self._nodes = nodes
        self._poll_interval = poll_interval
        self._confirmation_timeout = confirmation_timeout

    async def poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._poll_interval)
            try:
                await self._distribute()
            except Exception:
                logger.exception("Key distribution failed")

    async def _distribute(self) -> None:
        await self._vault.get_key_ring()  # validate vault reachable before broadcasting
        distribution_start = time.monotonic()
        tasks = [node.load_from_vault(self._vault) for node in self._nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        elapsed = time.monotonic() - distribution_start
        failed = [i for i, r in enumerate(results) if isinstance(r, Exception)]
        if failed:
            if elapsed > self._confirmation_timeout:
                logger.critical(
                    "P1_ALERT: Nodes did not confirm key rotation within timeout",
                    extra={
                        "failed_node_indices": failed,
                        "elapsed_seconds": round(elapsed, 2),
                        "timeout_seconds": self._confirmation_timeout,
                    },
                )
            else:
                logger.warning(
                    "Some nodes failed key rotation — within timeout window",
                    extra={
                        "failed": failed,
                        "elapsed_seconds": round(elapsed, 2),
                    },
                )
