"""
Signal Check Router — Phase 3B Validation Surface

POST /v1/leads/signal-check

────────────────────────────────────────────────────────────
ARCHITECTURAL NOTE

This endpoint is a non-production validation surface.
Its purpose: demo, design partner feedback, scenario testing.

It is NOT a production ingest boundary.
It does NOT enforce tenant auth or write to any store.
ADR-007 ("tenant identity from auth only") applies to write paths —
this endpoint is read-only and side-effect-free, so no auth boundary
is needed or appropriate here.

Production trust boundary → /v1/leads/ingest  (with require_tenant)
Signal sandbox            → /v1/leads/signal-check  (this file)

SCOPE GUARD — do not add:
  - tenant config lookup or DB access
  - real auth / require_tenant
  - enrichment calls
  - duplicate checks
  - explainability text generation
  - any write side effect

This endpoint must stay a pure pass-through to SignalEvaluator.
That is what keeps signal semantics identical between sandbox and production.
See: tests/contract/test_signal_check_parity.py
────────────────────────────────────────────────────────────

Determinism contract:
  Signals are sorted by code (ascending) before returning.
  This is an explicit API contract — independent of evaluator rule order.
  SignalEvaluator currently executes A3 → A4 → A6, which is stable but
  is an implementation detail that may change. The sort here is the contract.
  Same input → same response, always.

Rate limiting:
  Per-IP token bucket: _RATE_LIMIT_REQUESTS per _RATE_LIMIT_WINDOW_SECONDS.
  No external dependency — implemented in-process with threading.Lock.
  Returns HTTP 429 with Retry-After header on exhaustion.

  IP resolution: fail-safe default — always uses direct connection IP
  (request.client.host). X-Forwarded-For is read only when the connecting
  IP is present in LEG_TRUSTED_PROXY_IPS. Without that config, the header is
  ignored entirely to prevent spoofing. See _get_client_ip() for details.

  TODO(scale): In-process rate limiting does not survive restarts and does
  not coordinate across replicas. For production multi-replica deployment,
  replace with Redis-backed rate limiting or an API gateway layer
  (e.g. Cloudflare, nginx limit_req). See ADR-002 degraded-mode patterns
  for Redis-backed approach.

Evaluation timeout:
  SignalEvaluator runs on a module-level shared ThreadPoolExecutor
  (_eval_executor) with _EVAL_TIMEOUT_SECONDS budget per request.
  The executor is created once at import time and shared across requests,
  eliminating per-request thread creation overhead.
  Exceeding the budget returns HTTP 503 SIGNAL_EVALUATION_TIMEOUT.
  The executor must be shut down on app exit — see shutdown_executor().

Logging:
  NOTE: current logging is file-based and not integrity-protected.
  Production deployment requires an append-only or WORM-style log sink
  with access controls. See docs/compliance/security-incident-response.md.

Error code taxonomy:
  SIGNAL_EVALUATION_FAILED   — evaluator raised an unexpected exception
  SIGNAL_EVALUATION_TIMEOUT  — evaluator exceeded timeout budget
  SIGNAL_CONTEXT_INVALID     — context could not be constructed (reserved)
  SIGNAL_RULE_ERROR          — specific rule failure (reserved)
"""
from __future__ import annotations

import collections
import concurrent.futures
import logging
import os
import threading
import time
from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator, model_validator

from lead_entry_guard.core.signal_models import (
    FieldSourceRecord,
    FallbackPolicy,
    LeadSignalContext,
    SignalResult,
    VisibilityProjection,
)
from lead_entry_guard.policies.signal_evaluator import SignalEvaluator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/leads", tags=["signals"])


# ── Constants ─────────────────────────────────────────────────────────────────

_ALLOWED_SOURCE_TYPES = frozenset({"manual", "enrichment", "api", "import", "form"})

# Guards against oversized string payloads reaching the evaluator.
# A4 conflict detection only needs to know whether values differ — not their
# full content. This is a string-specific guard: large ints/floats are
# naturally bounded by their types and are not a practical concern here.
_MAX_VALUE_LENGTH = 512

# Rate limiting — per-IP token bucket.
_RATE_LIMIT_REQUESTS = 30        # max requests per window per IP
_RATE_LIMIT_WINDOW_SECONDS = 60  # sliding window length in seconds

# Memory guard for the rate limiter bucket dictionary.
# Caps the number of distinct IPs tracked simultaneously.
# When the cap is hit, the oldest-seen IP is evicted (LRU).
# At ~200 bytes per entry this caps memory at roughly 2MB.
_RATE_LIMIT_MAX_TRACKED_IPS = 10_000

# Evaluation timeout — hard budget for SignalEvaluator.evaluate().
# Pure in-process signal evaluation should complete well under 10ms.
# 200ms is a generous ceiling that still protects against pathological inputs.
#
# Starvation note: with max_workers=1, this budget covers queue wait + execution.
# Under concurrent load, requests may timeout before the evaluator even starts.
# If moving to real traffic, tune both together:
#   LEG_SIGNAL_CHECK_WORKERS = min(4, cpu_count())
#   _EVAL_TIMEOUT_SECONDS    = 0.300–0.500
# Do not increase one without reviewing the other.
#
# Demo-safe config (before public exposure, reduces false timeouts):
#   LEG_SIGNAL_CHECK_WORKERS=2  +  _EVAL_TIMEOUT_SECONDS = 0.300
_EVAL_TIMEOUT_SECONDS = 0.200


# ── Rate limiter ──────────────────────────────────────────────────────────────

class _TokenBucket:
    """
    Per-IP sliding-window rate limiter with LRU eviction.

    Thread-safe. No external dependencies.

    Memory model:
      Tracks a deque of request timestamps per IP key.
      When _max_tracked_ips is reached, the least-recently-seen IP is evicted
      to prevent unbounded memory growth from scanning or unique-IP floods.
      Evicted IPs start with a fresh bucket on their next request.

    Known limitations (see module docstring TODOs):
      - In-process only — does not survive restarts
      - Does not coordinate across replicas
      - IP source can be spoofed if proxy chain is not trusted
    """

    def __init__(
        self,
        max_requests: int,
        window_seconds: float,
        max_tracked_ips: int = _RATE_LIMIT_MAX_TRACKED_IPS,
    ) -> None:
        self._max = max_requests
        self._window = window_seconds
        self._max_tracked = max_tracked_ips
        # OrderedDict used as LRU: move_to_end on access, popitem(last=False) for eviction.
        self._buckets: collections.OrderedDict[str, collections.deque[float]] = (
            collections.OrderedDict()
        )
        self._lock = threading.Lock()

    def is_allowed(self, key: str) -> tuple[bool, int]:
        """
        Returns (allowed, retry_after_seconds).
        retry_after_seconds is 0 when allowed.
        """
        now = time.monotonic()
        cutoff = now - self._window

        with self._lock:
            if key in self._buckets:
                # Move to end = mark as most-recently used
                self._buckets.move_to_end(key)
            else:
                # Evict LRU entry if at capacity before inserting new key
                if len(self._buckets) >= self._max_tracked:
                    self._buckets.popitem(last=False)
                self._buckets[key] = collections.deque()

            bucket = self._buckets[key]

            # Evict expired timestamps from this bucket
            while bucket and bucket[0] < cutoff:
                bucket.popleft()

            if len(bucket) >= self._max:
                retry_after = int(self._window - (now - bucket[0])) + 1
                return False, retry_after

            bucket.append(now)
            return True, 0


_rate_limiter = _TokenBucket(_RATE_LIMIT_REQUESTS, _RATE_LIMIT_WINDOW_SECONDS)


# ── Trusted proxy configuration ───────────────────────────────────────────────
#
# Fail-safe default: X-Forwarded-For is IGNORED unless the direct connecting
# IP is in this set. Without trusted proxy config, rate limiting always uses
# request.client.host — the actual TCP connection source.
#
# Populated from env var LEG_TRUSTED_PROXY_IPS — a comma-separated list of IPs.
# Empty env var (default) = no trusted proxies = X-Forwarded-For always ignored.
#
# IMPORTANT: only set LEG_TRUSTED_PROXY_IPS if you fully control the proxy
# (e.g. your own nginx, load balancer, or cloud gateway). Never trust public
# proxies or unknown infrastructure — a malicious proxy can forge any IP.
#
# Example (single nginx proxy):
#   LEG_TRUSTED_PROXY_IPS=10.0.0.1
# Example (multiple proxies / load balancer pool):
#   LEG_TRUSTED_PROXY_IPS=10.0.0.1,10.0.0.2,10.0.0.3
#
# This is deployment config, not code config — change it via env without
# redeploying the application. Never hardcode IPs here.
_TRUSTED_PROXY_IPS: frozenset[str] = frozenset(
    ip.strip()
    for ip in os.getenv("LEG_TRUSTED_PROXY_IPS", "").split(",")
    if ip.strip()
)


def _get_client_ip(request: Request) -> str:
    """
    Extract client IP for rate limiting — fail-safe default.

    Security model:
      X-Forwarded-For is only read when the direct connecting IP
      (request.client.host) is in LEG_TRUSTED_PROXY_IPS.

      Without trusted proxy configuration, the header is ignored entirely.
      This prevents IP spoofing by clients who set the header themselves.

      When trusted proxy is configured, take the leftmost IP from
      X-Forwarded-For — this is the original client IP as seen by the proxy.
      The rightmost IP(s) are added by intermediate proxies and must not
      be used for rate limiting.

    Returns:
      The IP string to use as the rate limit key.
      Falls back to "unknown" only if request.client is None (test context).
    """
    direct_ip = request.client.host if request.client else "unknown"

    if direct_ip not in _TRUSTED_PROXY_IPS:
        # Not coming from a trusted proxy — use direct connection IP.
        # X-Forwarded-For is ignored, regardless of whether it is present.
        return direct_ip

    # Coming from a trusted proxy — honour X-Forwarded-For.
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()

    # Trusted proxy didn't set the header — fall back to direct IP.
    return direct_ip


# ── Shared executor ───────────────────────────────────────────────────────────
#
# Single module-level executor shared across all requests.
# Eliminates per-request thread creation overhead.
# Must be shut down on app exit — call shutdown_executor() from app lifespan.
#
# max_workers=1: intentional for sandbox load.
#   With a single worker, concurrent requests queue behind each other.
#   Timeout budget (_EVAL_TIMEOUT_SECONDS) covers queue wait + execution time,
#   so under heavy concurrency some requests may timeout before the evaluator
#   even starts. This is acceptable for demo/validation traffic.
#
def _parse_workers(raw: str | None, default: int = 1) -> int:
    """
    Parse LEG_SIGNAL_CHECK_WORKERS env var into a valid worker count.

    Rules:
      - Empty string or None → default (safe fallback, no crash on import)
      - Non-integer string   → fail fast with clear ValueError at import time
      - Value < 1            → fail fast (ThreadPoolExecutor requires >= 1)
      - Valid integer >= 1   → use as-is

    Fail-fast for non-integer and < 1: a misconfigured worker count will produce
    subtle starvation bugs that are hard to diagnose at runtime. Better to crash
    loudly at startup than silently degrade under load.

    Fallback for empty/None: empty env var is a common deployment artefact
    (e.g. `LEG_SIGNAL_CHECK_WORKERS=` in .env without a value). Treating it as
    "use default" is safer than crashing on an accidental blank.
    """
    if not raw or not raw.strip():
        return default
    stripped = raw.strip()
    try:
        value = int(stripped)
    except ValueError:
        raise ValueError(
            f"LEG_SIGNAL_CHECK_WORKERS must be a positive integer, got: {stripped!r}"
        ) from None
    if value < 1:
        raise ValueError(
            f"LEG_SIGNAL_CHECK_WORKERS must be >= 1, got: {value}"
        )
    return value


# max_workers: configurable via LEG_SIGNAL_CHECK_WORKERS env var.
# Default 1 is correct for sandbox — see starvation note above.
# Before higher-traffic exposure: set LEG_SIGNAL_CHECK_WORKERS=<cpu_count+4>
#
# WARNING: higher values increase concurrency but do not extend the timeout
# budget. Under load, queued requests still count against _EVAL_TIMEOUT_SECONDS
# (queue wait + execution). Increasing workers without also reviewing the
# timeout budget may produce false-negative timeouts — the evaluator never
# ran, but the request timed out waiting in the queue.
_eval_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=_parse_workers(os.getenv("LEG_SIGNAL_CHECK_WORKERS"))
)


def shutdown_executor() -> None:
    """
    Shut down the shared executor on app exit.

    Call this from the app lifespan shutdown hook:

        from lead_entry_guard.api.routers.signal_check import shutdown_executor

        @asynccontextmanager
        async def lifespan(app):
            yield
            shutdown_executor()

    wait=False: do not block shutdown waiting for in-flight evaluations.
    Any in-flight evaluation that was already past its timeout budget is
    abandoned — the request already returned 503 to the caller.
    """
    _eval_executor.shutdown(wait=False, cancel_futures=True)


# ── Request models ────────────────────────────────────────────────────────────

class FieldIn(BaseModel):
    """
    Single field + source provenance.

    value is used internally by A4 conflict detection — never echoed in response.
    The length guard on value applies to strings only; numeric types are
    naturally bounded and do not require an explicit limit here.
    """
    field_name: str = Field(..., min_length=1, max_length=64)
    source_type: str = Field(..., min_length=1, max_length=32)
    value: str | int | float | bool | None = Field(default=None)

    @field_validator("source_type")
    @classmethod
    def source_type_known(cls, v: str) -> str:
        if v not in _ALLOWED_SOURCE_TYPES:
            raise ValueError(
                f"Unknown source_type '{v}'. Allowed: {sorted(_ALLOWED_SOURCE_TYPES)}"
            )
        return v

    @field_validator("field_name")
    @classmethod
    def field_name_no_whitespace(cls, v: str) -> str:
        if v != v.strip():
            raise ValueError("field_name must not have leading/trailing whitespace")
        return v

    @field_validator("value", mode="before")
    @classmethod
    def value_length_guard(cls, v: object) -> object:
        """
        Reject string values exceeding _MAX_VALUE_LENGTH.
        Numeric types pass through — they are bounded by their own type semantics.
        """
        if isinstance(v, str) and len(v) > _MAX_VALUE_LENGTH:
            raise ValueError(
                f"field value exceeds maximum length of {_MAX_VALUE_LENGTH} characters"
            )
        return v


class SignalCheckRequest(BaseModel):
    """
    Input for signal evaluation.

    scenario_id: caller-supplied label for grouping related demo requests.
    Explicitly NOT named tenant_id to prevent it being treated as identity.

    It is:
      - unverified (not authenticated)
      - unsanitised beyond length/empty checks
      - safe to log (not PII, not identity)
      - NOT suitable as an access control key or trust boundary

    Use it for: scenario naming, log correlation, dashboard grouping in demos.
    Do NOT use it for: access decisions, data isolation, billing, identity.
    """
    scenario_id: str = Field(..., min_length=1, max_length=128)
    email: str | None = Field(default=None, max_length=254)
    fields: list[FieldIn] = Field(default_factory=list, max_length=50)

    @model_validator(mode="after")
    def has_something_to_evaluate(self) -> "SignalCheckRequest":
        if self.email is None and not self.fields:
            raise ValueError(
                "Provide at least 'email' or one entry in 'fields' — "
                "the evaluator has no data to work with."
            )
        return self


# ── Response models ───────────────────────────────────────────────────────────

class VisibilityOut(BaseModel):
    crm_status: str | None
    routing_tags: list[str]
    api_flags: dict[str, bool]

    @classmethod
    def from_model(cls, v: VisibilityProjection) -> "VisibilityOut":
        return cls(
            crm_status=v.crm_status,
            routing_tags=list(v.routing_tags),
            api_flags=dict(v.api_flags),
        )


class FallbackOut(BaseModel):
    mode: str
    after_hours: int | None
    then: str

    @classmethod
    def from_model(cls, f: FallbackPolicy) -> "FallbackOut":
        return cls(mode=f.mode.value, after_hours=f.after_hours, then=f.then)


class SignalOut(BaseModel):
    """
    Serialised signal for API response.

    action, signal_class, mode are str (not domain enums) intentionally:
    the response layer must not import domain enums — it is a serialisation
    boundary, not a domain boundary. Values are guaranteed by the domain
    models that produce them; correctness is asserted in parity tests.
    """
    code: str
    action: str
    signal_class: str
    visibility: VisibilityOut
    fallback: FallbackOut | None

    @classmethod
    def from_signal(cls, s: SignalResult) -> "SignalOut":
        return cls(
            code=s.code,
            action=s.action.value,
            signal_class=s.signal_class.value,
            visibility=VisibilityOut.from_model(s.visibility),
            fallback=FallbackOut.from_model(s.fallback) if s.fallback else None,
        )


class SignalCheckResponse(BaseModel):
    request_id: str
    scenario_id: str
    # Literal enforces the two-value contract at model construction time.
    # OpenAPI schema will reflect this as an enum — no ambiguity for consumers.
    status: Literal["clean", "flagged"]
    has_signals: bool
    signal_count: int
    signals: list[SignalOut]
    latency_ms: float = Field(..., ge=0)


# ── Handler ───────────────────────────────────────────────────────────────────

@router.post(
    "/signal-check",
    response_model=SignalCheckResponse,
    status_code=status.HTTP_200_OK,
    summary="Evaluate signals for a lead (no auth, no persistence)",
    description=(
        "Deterministic signal sandbox. "
        "Evaluates A3 (domain trust), A4 (source conflict), A6 (shared inbox) rules. "
        "No writes, no auth, no external calls. "
        "Signals are sorted by code — same input always produces same output. "
        "Empty signals list means clean lead — this is not an error. "
        "Rate limited: 30 requests / 60 seconds per IP."
    ),
)
def signal_check(request: Request, body: SignalCheckRequest) -> SignalCheckResponse:
    """
    Pure signal evaluation — synchronous, stateless, side-effect-free.

    Calls the same SignalEvaluator used by the production ingest pipeline.
    This is the parity guarantee: sandbox and production cannot diverge
    as long as this handler does not substitute or wrap the evaluator.
    See: tests/contract/test_signal_check_parity.py

    scenario_id: caller-supplied demo label, not authenticated identity.
    Used for log correlation and scenario grouping only.

    Rate limiting: per-IP token bucket with LRU eviction, returns 429 + Retry-After.
    Timeout: evaluator has a hard budget of _EVAL_TIMEOUT_SECONDS → 503 on breach.

    Ordering guarantee:
      Signals sorted by code (ascending) — explicit API contract,
      independent of evaluator rule execution order.

    Empty signals = clean lead, not an error.
    """
    request_id = str(uuid4())
    t_start = time.monotonic()

    # ── Rate limit ────────────────────────────────────────────────────────────
    client_ip = _get_client_ip(request)
    allowed, retry_after = _rate_limiter.is_allowed(client_ip)
    if not allowed:
        logger.warning(
            "Rate limit exceeded",
            extra={"request_id": request_id, "client_ip": client_ip},
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error": "RATE_LIMIT_EXCEEDED",
                "message": f"Too many requests. Retry after {retry_after} seconds.",
                "retry_after_seconds": retry_after,
            },
            headers={"Retry-After": str(retry_after)},
        )

    # ── Build context ─────────────────────────────────────────────────────────
    context = LeadSignalContext(
        tenant_id=body.scenario_id,  # non-authoritative demo label — not identity
        email=body.email,
        fields=[
            FieldSourceRecord(
                field_name=f.field_name,
                source_type=f.source_type,
                value=f.value,
            )
            for f in body.fields
        ],
    )

    # ── Evaluate with timeout ─────────────────────────────────────────────────
    # Shared module-level executor — no per-request thread creation overhead.
    try:
        future = _eval_executor.submit(SignalEvaluator().evaluate, context)
        try:
            raw_signals = future.result(timeout=_EVAL_TIMEOUT_SECONDS)
        except concurrent.futures.TimeoutError:
            logger.error(
                "Signal evaluation timed out",
                extra={
                    "request_id": request_id,
                    "scenario_id": body.scenario_id,
                    "timeout_seconds": _EVAL_TIMEOUT_SECONDS,
                },
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "SIGNAL_EVALUATION_TIMEOUT",
                    "message": "Signal evaluation exceeded the time budget.",
                    "request_id": request_id,
                },
            )
    except HTTPException:
        raise
    except Exception as exc:
        # exc message deliberately excluded — may contain input field values.
        # error_type is safe: always a class name from our own codebase.
        logger.error(
            "Signal evaluation failed",
            extra={
                "request_id": request_id,
                "scenario_id": body.scenario_id,
                "error_type": type(exc).__name__,
            },
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "SIGNAL_EVALUATION_FAILED",
                "message": "Signal evaluation encountered an internal error.",
                "request_id": request_id,
            },
        )

    # ── Sort + respond ────────────────────────────────────────────────────────
    signals = sorted(raw_signals, key=lambda s: s.code)

    latency_ms = round((time.monotonic() - t_start) * 1000, 3)

    # signal_codes are safe to log: static identifiers from rule modules,
    # never derived from input field values.
    # scenario_id is logged as-is — caller-supplied, unverified, not PII.
    # NOTE: these logs are not integrity-protected. Production requires
    # an append-only log sink. See docs/compliance/security-incident-response.md
    logger.info(
        "Signal check completed",
        extra={
            "request_id": request_id,
            "scenario_id": body.scenario_id,
            "signal_count": len(signals),
            "signal_codes": [s.code for s in signals],
            "latency_ms": latency_ms,
        },
    )

    return SignalCheckResponse(
        request_id=request_id,
        scenario_id=body.scenario_id,
        status="flagged" if signals else "clean",
        has_signals=bool(signals),
        signal_count=len(signals),
        signals=[SignalOut.from_signal(s) for s in signals],
        latency_ms=latency_ms,
    )
