"""Enums used by the synthetic messy lead generator."""
from __future__ import annotations

from enum import Enum


class VariantType(str, Enum):
    """Type of generated lead variant."""
    CLEAN = "clean"
    DIRTY = "dirty"
    EXACT_DUPLICATE = "exact_duplicate"
    NEAR_DUPLICATE = "near_duplicate"
    BROKEN = "broken"
    EDGE_CASE = "edge_case"


class ExpectedDecision(str, Enum):
    """Expected pipeline outcome (for validation later)."""
    PASS = "PASS"
    WARN = "WARN"
    REJECT = "REJECT"
    DUPLICATE_HINT = "DUPLICATE_HINT"