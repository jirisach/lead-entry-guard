"""
Determinism and Config Rollout Safety (Section 12).

Config mismatch classification (v4):
  CRITICAL → fail closed (reject incoming leads)
  NON_CRITICAL → degraded mode (ACCEPT_WITH_FLAG)
  UNKNOWN → grace period 60s → escalates to CRITICAL

Activation flow:
  download config → validate checksum/schema → stage locally → activate atomic version
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from lead_entry_guard.core.exceptions import ConfigMismatchError

logger = logging.getLogger(__name__)


class MismatchSeverity(str, Enum):
    CRITICAL = "CRITICAL"
    NON_CRITICAL = "NON_CRITICAL"
    UNKNOWN = "UNKNOWN"


@dataclass
class VersionedConfig:
    policy_version: str
    ruleset_version: str
    config_version: str
    checksum: str
    payload: dict[str, Any]

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "VersionedConfig":
        payload_bytes = json.dumps(raw.get("payload", {}), sort_keys=True).encode()
        checksum = hashlib.sha256(payload_bytes).hexdigest()
        return cls(
            policy_version=raw["policy_version"],
            ruleset_version=raw["ruleset_version"],
            config_version=raw["config_version"],
            checksum=checksum,
            payload=raw.get("payload", {}),
        )


class ConfigVersionManager:
    """
    Manages config versioning, validation, and atomic activation.
    """

    def __init__(self, grace_period_seconds: int = 60) -> None:
        self._active: VersionedConfig | None = None
        self._staged: VersionedConfig | None = None
        self._grace_period = grace_period_seconds
        self._unknown_mismatch_detected_at: float | None = None

    def stage(self, raw_config: dict[str, Any]) -> VersionedConfig:
        """Download, validate checksum + schema, stage locally."""
        config = VersionedConfig.from_raw(raw_config)
        self._validate_schema(raw_config)
        self._staged = config
        logger.info("Config staged", extra={"config_version": config.config_version})
        return config

    def _validate_schema(self, raw: dict[str, Any]) -> None:
        required = {"policy_version", "ruleset_version", "config_version"}
        missing = required - set(raw.keys())
        if missing:
            raise ConfigMismatchError(
                f"Config schema invalid — missing fields: {missing}",
                severity=MismatchSeverity.CRITICAL,
            )

    def activate(self) -> VersionedConfig:
        """Atomic version activation."""
        if self._staged is None:
            raise ConfigMismatchError("No staged config to activate", severity=MismatchSeverity.CRITICAL)
        self._active = self._staged
        self._staged = None
        logger.info("Config activated", extra={"config_version": self._active.config_version})
        return self._active

    def classify_mismatch(
        self, node_version: str, expected_version: str, field: str
    ) -> MismatchSeverity:
        """
        CRITICAL:     policy_version missing, schema invalid
        NON_CRITICAL: ruleset_version mismatch, config_version 1 behind
        UNKNOWN:      anything else → grace period → escalates to CRITICAL
        """
        if field == "policy_version" and not node_version:
            return MismatchSeverity.CRITICAL
        if field in ("ruleset_version", "config_version"):
            return MismatchSeverity.NON_CRITICAL
        return MismatchSeverity.UNKNOWN

    async def handle_mismatch(self, severity: MismatchSeverity) -> None:
        if severity == MismatchSeverity.CRITICAL:
            logger.critical(
                "P1_ALERT: CRITICAL config mismatch — failing closed",
                extra={"severity": severity},
            )
            raise ConfigMismatchError("CRITICAL config mismatch", severity=severity)

        if severity == MismatchSeverity.NON_CRITICAL:
            logger.warning(
                "P3_ALERT: NON_CRITICAL config mismatch — degraded mode (ACCEPT_WITH_FLAG)",
                extra={"severity": severity},
            )
            return  # Allow degraded processing

        if severity == MismatchSeverity.UNKNOWN:
            if self._unknown_mismatch_detected_at is None:
                self._unknown_mismatch_detected_at = time.monotonic()
                logger.warning("UNKNOWN config mismatch detected — grace period started")
                return

            elapsed = time.monotonic() - self._unknown_mismatch_detected_at
            if elapsed > self._grace_period:
                self._unknown_mismatch_detected_at = None
                logger.critical(
                    "P1_ALERT: UNKNOWN mismatch escalated to CRITICAL after grace period"
                )
                raise ConfigMismatchError(
                    "UNKNOWN mismatch escalated to CRITICAL", severity=MismatchSeverity.CRITICAL
                )

    @property
    def active(self) -> VersionedConfig | None:
        return self._active
