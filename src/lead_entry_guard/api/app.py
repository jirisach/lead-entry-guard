"""FastAPI application — Ingestion API."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from contextlib import asynccontextmanager
from typing import Annotated, Any

import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from lead_entry_guard.config.settings import Settings, get_settings
from lead_entry_guard.config.tenant import TenantRegistry
from lead_entry_guard.core.exceptions import (
    ConfigMismatchError,
    KeyNotFoundError,
    LeadEntryGuardError,
    RedisUnavailableError,
)
from lead_entry_guard.core.models import DecisionResult, LeadInput, SourceType
from lead_entry_guard.core.pipeline import IngestionPipeline
from lead_entry_guard.fingerprint.builder import FingerprintBuilder
from lead_entry_guard.lookup.bloom import BloomFilterRegistry
from lead_entry_guard.lookup.duplicate import DuplicateLookupTier
from lead_entry_guard.lookup.redis_store import RedisDuplicateStore, RedisIdempotencyStore
from lead_entry_guard.normalization.normalizer import NormalizationLayer
from lead_entry_guard.policies.engine import PolicyEngine
from lead_entry_guard.security.hmac_keys import HMACKeyManager
from lead_entry_guard.security.vault import (
    AWSKMSVaultClient,
    HashiCorpVaultClient,
    InMemoryVaultClient,
    VaultClient,
)
from lead_entry_guard.telemetry.exporter import OOBHeartbeat, StatsDClient, TelemetryExporter, TelemetryQueue
from lead_entry_guard.validation.validator import ValidationLayer

logger = logging.getLogger(__name__)


# ── Privacy-safe logging filter ──────────────────────────────────────────────

_PII_FIELDS = frozenset({
    "email", "phone", "first_name", "last_name", "company",
    "body", "payload", "raw_email", "raw_phone",
})

# Patterns that suggest a stringified payload ended up in an error/message field.
# Checked against field *values* (not keys) — e.g. error="email=foo@bar.com ..."
_PII_VALUE_MARKERS = ("@", "email=", "phone=", "name=")


def _scrub_value(value: object) -> object:
    """
    Recursively scrub a log field value.

    - dict  → scrub keys matching _PII_FIELDS and values containing PII markers
    - str   → replace if it looks like it contains a PII payload
    - other → pass through unchanged
    """
    if isinstance(value, dict):
        return {
            k: "[REDACTED]" if k in _PII_FIELDS else _scrub_value(v)
            for k, v in value.items()
        }
    if isinstance(value, str):
        if any(marker in value for marker in _PII_VALUE_MARKERS):
            return "[REDACTED]"
    return value


class PIIRedactingFilter(logging.Filter):
    """
    Last-line-of-defence PII guard for the lead_entry_guard logger tree.

    Belt-and-suspenders: pipeline and normalizer layers must already ensure
    PII never reaches log calls, but this filter catches anything that slips
    through — including stringified payloads in `error` fields, nested dicts,
    and PII markers inside `record.msg`.

    Deliberately NOT a complete GDPR scrubber (that requires a full regex
    pass over arbitrary strings). Its purpose is to catch the most likely
    accidental leaks, not to replace upstream hygiene.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        # Scrub known PII attribute names on the record itself
        for field_name in _PII_FIELDS:
            if hasattr(record, field_name):
                setattr(record, field_name, "[REDACTED]")

        # Scrub `extra` fields that may carry nested dicts or stringified payloads
        # (covers: error="...", detail={...}, context={...})
        for attr in ("error", "detail", "context", "extra", "data"):
            if hasattr(record, attr):
                setattr(record, attr, _scrub_value(getattr(record, attr)))

        # Scrub record.msg if it looks like it contains a raw PII value.
        # We check for markers rather than blanket-redacting to preserve
        # useful log messages.
        if isinstance(record.msg, str) and any(
            marker in record.msg for marker in _PII_VALUE_MARKERS
        ):
            record.msg = "[REDACTED — PII detected in log message]"
            record.args = ()

        return True


def _configure_logging() -> None:
    root = logging.getLogger("lead_entry_guard")
    root.addFilter(PIIRedactingFilter())


# ── Vault factory ─────────────────────────────────────────────────────────────

_DEV_FIXED_SECRET_HEX = "a" * 64  # stable across restarts in dev — NOT random


def _build_vault_client(settings: Settings) -> VaultClient:
    """
    Select vault backend by environment.

    dev  → InMemoryVaultClient with a FIXED (not random) secret so
           fingerprints are stable across process restarts.
    prod → HashiCorp Vault or AWS KMS.  Startup fails fast if unreachable.

    A random secret (secrets.token_hex) is explicitly forbidden here because
    it breaks the dual-key overlap model: every restart produces a new current
    key, making all existing Redis fingerprints unverifiable.
    """
    env = settings.environment

    if env == "development":
        logger.warning(
            "DEV MODE: using fixed in-memory vault key. "
            "Fingerprints are stable within this key only. "
            "DO NOT use in production."
        )
        return InMemoryVaultClient({
            "current": {
                "kid": "dev-v1",
                "secret_hex": _DEV_FIXED_SECRET_HEX,
                "activated_at": "2026-01-01T00:00:00+00:00",
            }
        })

    # staging / production — require real vault
    from lead_entry_guard.config.settings import VaultBackend
    if settings.vault_backend == VaultBackend.AWS_KMS:
        return AWSKMSVaultClient(secret_arn=settings.vault_token)

    # Default: HashiCorp Vault
    if not settings.vault_token:
        raise RuntimeError(
            "LEG_VAULT_TOKEN is required in non-development environments. "
            "Refusing to start without a valid HMAC key source."
        )
    return HashiCorpVaultClient(
        vault_url=settings.vault_url,
        token=settings.vault_token,
        secret_path=settings.vault_secret_path,
    )


# ── Dependency injection container ──────────────────────────────────────────

class Container:
    """
    Dependency container assembled at startup.
    Holds all shared infrastructure — Redis, key manager, pipeline, telemetry.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.redis_client: aioredis.Redis | None = None
        self.key_manager: HMACKeyManager | None = None
        self.pipeline: IngestionPipeline | None = None
        self.telemetry_queue: TelemetryQueue | None = None
        self.statsd: StatsDClient | None = None
        # Readiness flags — each set only after the component is truly ready
        self._redis_ready = False
        self._keys_ready = False
        self._pipeline_ready = False
        # Registry of background tasks — set so done-callback removal is O(1)
        # and we never hold stale references after tasks complete.
        self._background_tasks: set[asyncio.Task[Any]] = set()
        # Separate registry for long-running daemon tasks (exporter, heartbeat).
        # Unlike _background_tasks (short-lived store ops), these should live
        # for the entire process lifetime. Tracked by name so is_ready() can
        # check health without relying on set membership (which is cleared on done).
        self._long_running_tasks: dict[str, asyncio.Task[Any]] = {}

    def _spawn(self, coro: Coroutine[Any, Any, Any], *, name: str) -> asyncio.Task[Any]:
        """
        Create a tracked background task.

        - Stores a strong reference in a set so the task isn't garbage-collected
          and is removed automatically via done-callback (O(1) discard on set).
        - Logs any unexpected exception so failures are never silent.
        - Name is set on the Task for easier introspection (asyncio.all_tasks()).
        """
        task: asyncio.Task[Any] = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)

        def _on_done(t: asyncio.Task[Any]) -> None:
            # Remove from registry as soon as the task is done — keeps the set lean.
            self._background_tasks.discard(t)
            if t.cancelled():
                logger.debug("Background task cancelled", extra={"task": name})
                return
            exc = t.exception()
            if exc is not None:
                logger.error(
                    "Background task failed unexpectedly",
                    extra={"task": name, "error_type": type(exc).__name__, "error": str(exc)},
                    exc_info=exc,
                )

        task.add_done_callback(_on_done)
        return task

    def _spawn_daemon(self, coro: Coroutine[Any, Any, Any], *, name: str) -> asyncio.Task[Any]:
        """
        Spawn a long-running daemon task and register it for health tracking.

        Unlike _spawn() (short-lived fire-and-forget ops), daemon tasks are
        expected to run for the full process lifetime. They are stored in
        _long_running_tasks by name so is_ready() can detect post-startup crashes:
        a task that exits unexpectedly will have task.done() == True, which
        is_ready() treats as unhealthy.

        The done-callback intentionally does NOT remove from _long_running_tasks —
        the stale reference is kept precisely so is_ready() can detect the failure.
        """
        task: asyncio.Task[Any] = asyncio.create_task(coro, name=name)
        self._long_running_tasks[name] = task

        def _on_done(t: asyncio.Task[Any]) -> None:
            if t.cancelled():
                logger.debug("Daemon task cancelled", extra={"task": name})
                return
            exc = t.exception()
            if exc is not None:
                logger.error(
                    "Daemon task failed unexpectedly — readiness will report unhealthy",
                    extra={"task": name, "error_type": type(exc).__name__, "error": str(exc)},
                    exc_info=exc,
                )

        task.add_done_callback(_on_done)
        return task

    async def startup(self) -> None:
        _configure_logging()

        # ── Telemetry (independent of Redis / vault) ──────────────────────
        self.statsd = StatsDClient(
            host=self.settings.statsd_host,
            port=self.settings.statsd_port,
        )
        self.telemetry_queue = TelemetryQueue(
            max_size=self.settings.telemetry_queue_max_size,
            warn_threshold=self.settings.telemetry_queue_warn_threshold,
            statsd_client=self.statsd,
        )
        # Start async exporter — drains telemetry queue in the background.
        # Start OOB heartbeat — independent UDP signal to infra monitoring.
        # _spawn() tracks both tasks and logs any unexpected failure.
        exporter = TelemetryExporter(queue=self.telemetry_queue, statsd=self.statsd)
        heartbeat = OOBHeartbeat(
            statsd=self.statsd,
            telemetry_queue=self.telemetry_queue,
            interval_seconds=self.settings.heartbeat_interval_seconds,
        )
        self._spawn_daemon(exporter.export_loop(), name="telemetry_exporter")
        self._spawn_daemon(heartbeat.heartbeat_loop(), name="oob_heartbeat")
        logger.info("Telemetry exporter and OOB heartbeat started")

        # ── Redis ─────────────────────────────────────────────────────────
        self.redis_client = aioredis.from_url(
            self.settings.redis_url,
            socket_connect_timeout=self.settings.redis_connect_timeout,
            socket_timeout=self.settings.redis_socket_timeout,
            max_connections=self.settings.redis_max_connections,
            decode_responses=False,
        )
        try:
            await self.redis_client.ping()
            self._redis_ready = True
            logger.info("Redis connection established")
        except Exception as exc:
            # Not fatal at startup — pipeline handles degraded mode.
            # But we log it clearly so ops knows from the first line.
            logger.warning(
                "Redis unavailable at startup — pipeline will operate in degraded mode",
                extra={"error": str(exc)},
            )

        # ── HMAC key manager ──────────────────────────────────────────────
        self.key_manager = HMACKeyManager(
            confirmation_timeout_seconds=self.settings.key_confirmation_timeout_seconds
        )
        vault_client = _build_vault_client(self.settings)
        # Fail fast if vault is unreachable in prod. In dev this always succeeds.
        await self.key_manager.load_from_vault(vault_client)
        self._keys_ready = True
        logger.info("HMAC key ring loaded", extra={"kid": self.key_manager.current_kid()})

        # ── Pipeline ──────────────────────────────────────────────────────
        duplicate_store = RedisDuplicateStore(
            self.redis_client,
            duplicate_ttl=self.settings.duplicate_ttl_seconds,
        )
        idempotency_store = RedisIdempotencyStore(
            self.redis_client,
            idempotency_ttl=self.settings.idempotency_ttl_seconds,
        )
        bloom_registry = BloomFilterRegistry()
        dup_tier = DuplicateLookupTier(bloom_registry, duplicate_store)
        fp_builder = FingerprintBuilder(self.key_manager)
        tenant_registry = TenantRegistry()

        self.pipeline = IngestionPipeline(
            normalizer=NormalizationLayer(),
            validator=ValidationLayer(),
            fingerprint_builder=fp_builder,
            duplicate_tier=dup_tier,
            policy_engine=PolicyEngine(),
            idempotency_store=idempotency_store,
            telemetry_queue=self.telemetry_queue,
            tenant_registry=tenant_registry,
        )
        self._pipeline_ready = True
        logger.info("Ingestion pipeline ready")

    async def shutdown(self) -> None:
        # Snapshot both task registries before cancellation.
        # _background_tasks done-callbacks discard tasks as they complete,
        # so we capture the set now to have stable counts for logging.
        pending = set(self._background_tasks)
        daemon_tasks = set(self._long_running_tasks.values())

        for task in pending | daemon_tasks:
            task.cancel()

        all_tasks = pending | daemon_tasks
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)
            logger.info(
                "Background and daemon tasks cancelled",
                extra={
                    "background_count": len(pending),
                    "daemon_count": len(daemon_tasks),
                },
            )

        # Clear daemon registry after shutdown — avoids is_ready() seeing
        # cancelled tasks as crashes if the container is inspected post-shutdown.
        self._long_running_tasks.clear()

        if self.redis_client:
            await self.redis_client.aclose()

    def is_ready(self) -> tuple[bool, dict[str, str]]:
        """
        Returns (ready, component_statuses).
        Ready = pipeline exists + keys loaded + daemon tasks still running.
        Redis unavailable is NOT a readiness blocker — pipeline handles degraded mode.
        """
        # Check daemon task health: a task that has exited (done=True, not cancelled)
        # after startup is a crash — report it as unhealthy so the orchestrator can restart.
        daemon_statuses: dict[str, str] = {}
        all_daemons_healthy = True
        for task_name, task in self._long_running_tasks.items():
            if task.done() and not task.cancelled():
                daemon_statuses[task_name] = "crashed"
                all_daemons_healthy = False
            elif task.cancelled():
                daemon_statuses[task_name] = "cancelled"
                # Cancelled during shutdown is expected — not a readiness failure.
                # During normal operation, cancelled = unexpected → unhealthy.
                all_daemons_healthy = False
            else:
                daemon_statuses[task_name] = "ok"

        statuses: dict[str, str] = {
            "pipeline": "ok" if self._pipeline_ready else "not_ready",
            "keys": "ok" if self._keys_ready else "not_ready",
            "redis": "ok" if self._redis_ready else "degraded",
            "telemetry": "ok" if self.telemetry_queue is not None else "not_ready",
            **daemon_statuses,
        }
        ready = (
            self._pipeline_ready
            and self._keys_ready
            and self.telemetry_queue is not None
            and all_daemons_healthy
        )
        return ready, statuses


_container: Container | None = None


def get_container() -> Container:
    if _container is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "SERVICE_NOT_INITIALIZED", "message": "Container not ready"},
        )
    return _container


def _require_pipeline(container: Container) -> IngestionPipeline:
    """Explicit runtime guard — no assert in production code."""
    if container.pipeline is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "PIPELINE_NOT_READY", "message": "Pipeline is not initialized"},
        )
    return container.pipeline


# ── App lifecycle ────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    global _container
    settings = get_settings()
    _container = Container(settings)
    try:
        await _container.startup()
    except Exception as exc:
        # Startup failure is always fatal — log clearly, then re-raise so the
        # process exits and the orchestrator can restart / alert.
        logger.critical(
            "Startup failed — process will exit",
            extra={"error": str(exc)},
        )
        raise
    logger.info(
        "Lead Entry Guard started",
        extra={"version": settings.app_version, "environment": settings.environment},
    )
    yield
    await _container.shutdown()
    logger.info("Lead Entry Guard stopped")


app = FastAPI(
    title="Lead Entry Guard",
    version="4.0.0",
    description="Real-time ingestion gateway for CRM and marketing pipelines",
    lifespan=lifespan,
)


# ── Global exception handlers ─────────────────────────────────────────────────
#
# Goal: every error path returns a stable, safe response body.
# Rules:
#   - No request body in error logs (PII guard)
#   - No raw exception detail in response body (information leak guard)
#   - Always include request_id when available
#   - Domain exceptions map to explicit HTTP status + error code

@app.exception_handler(RedisUnavailableError)
async def _handle_redis_unavailable(request: Request, exc: RedisUnavailableError) -> JSONResponse:
    # This should normally be caught and handled by the pipeline's degraded mode.
    # If it bubbles here, something bypassed the pipeline — treat as 503.
    logger.error(
        "Unhandled RedisUnavailableError reached API layer",
        extra={"path": request.url.path},
        # Deliberately NOT logging request body
    )
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"error": "DEPENDENCY_UNAVAILABLE", "message": "Upstream dependency unavailable"},
    )


@app.exception_handler(ConfigMismatchError)
async def _handle_config_mismatch(request: Request, exc: ConfigMismatchError) -> JSONResponse:
    logger.critical(
        "Config mismatch — failing closed",
        extra={"severity": exc.severity, "path": request.url.path},
    )
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"error": "CONFIG_MISMATCH", "severity": exc.severity},
    )


@app.exception_handler(KeyNotFoundError)
async def _handle_key_not_found(request: Request, exc: KeyNotFoundError) -> JSONResponse:
    logger.critical("HMAC key not found — failing closed", extra={"path": request.url.path})
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"error": "KEY_UNAVAILABLE", "message": "Identity subsystem unavailable"},
    )


@app.exception_handler(LeadEntryGuardError)
async def _handle_domain_error(request: Request, exc: LeadEntryGuardError) -> JSONResponse:
    logger.error(
        "Domain error in request path",
        extra={"error_type": type(exc).__name__, "path": request.url.path},
    )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"error": "PROCESSING_ERROR", "error_type": type(exc).__name__},
    )


@app.exception_handler(Exception)
async def _handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
    # Fallback for anything not caught above.
    # Never include exc detail in response — potential info leak.
    logger.exception(
        "Unexpected error in request path — no request body logged",
        extra={"error_type": type(exc).__name__, "path": request.url.path},
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "INTERNAL_ERROR"},
    )


# ── Request / Response schemas ───────────────────────────────────────────────

class IngestRequest(BaseModel):
    tenant_id: str
    source_id: str | None = None
    source_type: SourceType = SourceType.API
    email: str | None = None
    phone: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    company: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class IngestResponse(BaseModel):
    request_id: str
    tenant_id: str
    decision: str
    reason_codes: list[str]
    duplicate_check_skipped: bool
    policy_version: str
    ruleset_version: str
    config_version: str
    latency_ms: float


# ── Routes ───────────────────────────────────────────────────────────────────

@app.post("/v1/leads/ingest", response_model=IngestResponse, status_code=status.HTTP_200_OK)
async def ingest_lead(
    body: IngestRequest,
    container: Annotated[Container, Depends(get_container)],
) -> IngestResponse:
    pipeline = _require_pipeline(container)
    lead = LeadInput(
        tenant_id=body.tenant_id,
        source_id=body.source_id,
        source_type=body.source_type,
        email=body.email,
        phone=body.phone,
        first_name=body.first_name,
        last_name=body.last_name,
        company=body.company,
        extra=body.extra,
    )
    result: DecisionResult = await pipeline.process(lead)
    return IngestResponse(
        request_id=result.request_id,
        tenant_id=result.tenant_id,
        decision=result.decision.value,
        reason_codes=[rc.value for rc in result.reason_codes],
        duplicate_check_skipped=result.duplicate_check_skipped,
        policy_version=result.versions.policy_version,
        ruleset_version=result.versions.ruleset_version,
        config_version=result.versions.config_version,
        latency_ms=result.latency_ms,
    )


@app.get("/health")
async def health(container: Annotated[Container, Depends(get_container)]) -> JSONResponse:
    """
    Liveness probe — answers whether the process is alive and minimally functional.
    Redis unavailable = degraded, not dead.
    """
    redis_ok = False
    if container.redis_client:
        try:
            await container.redis_client.ping()
            redis_ok = True
        except Exception:
            pass

    overall = "ok" if redis_ok else "degraded"
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "status": overall,
            "components": {
                "redis": "ok" if redis_ok else "unavailable",
                "keys": "ok" if container._keys_ready else "not_ready",
                "pipeline": "ok" if container._pipeline_ready else "not_ready",
            },
            "version": get_settings().app_version,
        },
    )


@app.get("/ready")
async def readiness(container: Annotated[Container, Depends(get_container)]) -> JSONResponse:
    """
    Readiness probe — answers whether this instance should receive traffic.

    NOT ready if:
      - pipeline is not assembled
      - HMAC keys are not loaded
      - telemetry queue is not initialized

    Redis unavailable is NOT a readiness blocker — pipeline operates in
    degraded mode and will handle it per-tenant policy.
    """
    ready, statuses = container.is_ready()
    http_status = status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(
        status_code=http_status,
        content={"ready": ready, "components": statuses},
    )
