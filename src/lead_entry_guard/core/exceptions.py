"""Domain exceptions for Lead Entry Guard."""


class LeadEntryGuardError(Exception):
    """Base exception."""


class ValidationError(LeadEntryGuardError):
    pass


class FingerprintError(LeadEntryGuardError):
    pass


class KeyRotationError(LeadEntryGuardError):
    pass


class KeyNotFoundError(KeyRotationError):
    pass


class RedisUnavailableError(LeadEntryGuardError):
    pass


class BloomUnavailableError(LeadEntryGuardError):
    pass


class PolicyEngineError(LeadEntryGuardError):
    pass


class ConfigMismatchError(LeadEntryGuardError):
    def __init__(self, message: str, severity: str = "UNKNOWN") -> None:
        super().__init__(message)
        self.severity = severity  # CRITICAL | NON_CRITICAL | UNKNOWN


class TelemetryQueueFullError(LeadEntryGuardError):
    pass


class ReconciliationRateLimitError(LeadEntryGuardError):
    pass


class IdempotencyStoreError(LeadEntryGuardError):
    pass


class QueueCapExceededError(LeadEntryGuardError):
    pass
