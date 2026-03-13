"""
Bloom filter — rolling two-slot lifecycle model with per-tenant isolation.

Architecture:
  slot_A (active) → write + read
  slot_B (draining) → read only
  Rotation: slot_A reaches capacity → becomes slot_B → new slot_A created
  Lookup: always checks both slots simultaneously
"""
from __future__ import annotations

import logging
import math
import threading
from dataclasses import dataclass, field
from typing import NamedTuple

try:
    from pybloom_live import ScalableBloomFilter
except ImportError:  # pragma: no cover
    ScalableBloomFilter = None  # type: ignore[assignment, misc]

from lead_entry_guard.core.exceptions import BloomUnavailableError

logger = logging.getLogger(__name__)


def calculate_bloom_params(
    expected_items: int, target_fpr: float = 0.001
) -> tuple[int, int]:
    """
    Calculate required bits and hash functions.

    n = expected_leads_per_tenant_per_slot_lifetime
    p = target_false_positive_rate
    required_bits = -n * ln(p) / (ln(2))^2
    required_hashes = (bits / n) * ln(2)
    """
    required_bits = int(-expected_items * math.log(target_fpr) / (math.log(2) ** 2))
    required_hashes = int((required_bits / expected_items) * math.log(2))
    return required_bits, required_hashes


class BloomStats(NamedTuple):
    estimated_fill_ratio: float
    estimated_count: int
    capacity: int
    rotation_count: int


@dataclass
class BloomSlot:
    capacity: int
    error_rate: float
    _filter: object = field(init=False)
    _count: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        try:
            if ScalableBloomFilter is None:
                raise ImportError("pybloom-live is required")
            self._filter = ScalableBloomFilter(
                initial_capacity=self.capacity,
                error_rate=self.error_rate,
            )
        except ImportError as exc:
            raise BloomUnavailableError("Bloom backend not available") from exc
        except Exception as exc:
            raise BloomUnavailableError("Bloom filter initialization failed") from exc

    def add(self, key: str) -> None:
        try:
            self._filter.add(key)  # type: ignore[union-attr]
            self._count += 1
        except Exception as exc:
            raise BloomUnavailableError("Bloom filter add failed") from exc

    def __contains__(self, key: str) -> bool:
        try:
            return key in self._filter  # type: ignore[operator]
        except Exception as exc:
            raise BloomUnavailableError("Bloom filter membership check failed") from exc

    @property
    def estimated_fill_ratio(self) -> float:
        return min(self._count / self.capacity, 1.0)

    @property
    def count(self) -> int:
        return self._count


class TenantBloomFilter:
    """
    Per-tenant rolling two-slot Bloom filter.

    - slot_active: receives writes + reads
    - slot_draining: read-only (previous slot during overlap)
    - Rotation triggered when active slot fill_ratio exceeds fill_ratio_threshold
    """

    def __init__(
        self,
        tenant_id: str,
        capacity: int,
        error_rate: float = 0.001,
        fill_ratio_threshold: float = 0.70,
    ) -> None:
        self.tenant_id = tenant_id
        self._capacity = capacity
        self._error_rate = error_rate
        self._fill_threshold = fill_ratio_threshold
        self._rotation_count = 0
        self._lock = threading.Lock()
        self._slot_active = BloomSlot(capacity=capacity, error_rate=error_rate)
        self._slot_draining: BloomSlot | None = None

    def check_and_add(self, key: str) -> bool:
        """
        Returns True if key is POSSIBLY present (MAYBE), False if DEFINITELY NOT.

        Adds key to active slot if not found in either slot.
        """
        with self._lock:
            # Check both slots
            maybe_present = key in self._slot_active or (
                self._slot_draining is not None and key in self._slot_draining
            )
            if not maybe_present:
                self._slot_active.add(key)
                self._maybe_rotate()
            return maybe_present

    def _maybe_rotate(self) -> None:
        if self._slot_active.estimated_fill_ratio >= self._fill_threshold:
            logger.info(
                "Bloom rotation triggered",
                extra={
                    "tenant_id": self.tenant_id,
                    "fill_ratio": self._slot_active.estimated_fill_ratio,
                    "rotation_count": self._rotation_count + 1,
                },
            )
            self._slot_draining = self._slot_active
            self._slot_active = BloomSlot(
                capacity=self._capacity, error_rate=self._error_rate
            )
            self._rotation_count += 1

    def stats(self) -> BloomStats:
        with self._lock:
            return BloomStats(
                estimated_fill_ratio=self._slot_active.estimated_fill_ratio,
                estimated_count=self._slot_active.count,
                capacity=self._capacity,
                rotation_count=self._rotation_count,
            )


class BloomFilterRegistry:
    """Registry of per-tenant Bloom filter instances."""

    def __init__(self) -> None:
        self._filters: dict[str, TenantBloomFilter] = {}
        self._lock = threading.Lock()

    def get_or_create(
        self,
        tenant_id: str,
        capacity: int,
        error_rate: float = 0.001,
    ) -> TenantBloomFilter:
        try:
            with self._lock:
                if tenant_id not in self._filters:
                    self._filters[tenant_id] = TenantBloomFilter(
                        tenant_id=tenant_id,
                        capacity=capacity,
                        error_rate=error_rate,
                    )
            return self._filters[tenant_id]
        except BloomUnavailableError:
            raise  # already domain exception — let it propagate cleanly
        except Exception as exc:
            raise BloomUnavailableError("Bloom registry unavailable") from exc

    def all_stats(self) -> dict[str, BloomStats]:
        return {tid: f.stats() for tid, f in self._filters.items()}
