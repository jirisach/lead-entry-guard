"""Vault / KMS client abstraction."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class VaultClient(ABC):
    """Abstract interface for secret backends."""

    @abstractmethod
    async def get_key_ring(self) -> dict[str, Any]:
        """Return key ring dict with current + optional previous key."""


class HashiCorpVaultClient(VaultClient):
    """HashiCorp Vault KV v2 backend."""

    def __init__(self, vault_url: str, token: str, secret_path: str) -> None:
        self._url = vault_url
        self._token = token
        self._path = secret_path

    async def get_key_ring(self) -> dict[str, Any]:
        import hvac  # type: ignore[import-untyped]

        client = hvac.Client(url=self._url, token=self._token)
        response = client.secrets.kv.v2.read_secret_version(path=self._path)
        return response["data"]["data"]  # type: ignore[no-any-return]


class AWSKMSVaultClient(VaultClient):
    """AWS KMS + Secrets Manager backend."""

    def __init__(self, secret_arn: str, region: str = "us-east-1") -> None:
        self._secret_arn = secret_arn
        self._region = region

    async def get_key_ring(self) -> dict[str, Any]:
        import boto3
        import json

        client = boto3.client("secretsmanager", region_name=self._region)
        response = client.get_secret_value(SecretId=self._secret_arn)
        return json.loads(response["SecretString"])  # type: ignore[no-any-return]


class InMemoryVaultClient(VaultClient):
    """In-memory vault for testing."""

    def __init__(self, key_ring: dict[str, Any]) -> None:
        self._key_ring = key_ring

    async def get_key_ring(self) -> dict[str, Any]:
        return self._key_ring
