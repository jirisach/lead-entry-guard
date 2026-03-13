"""Data models for synthetic lead generation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .enums import VariantType, ExpectedDecision


@dataclass
class SyntheticIdentity:
    """Base identity used to derive multiple lead variants."""
    identity_id: str
    email: str
    phone: str
    first_name: str
    last_name: str
    company: str


@dataclass
class LeadEventVariant:
    """Concrete generated lead event."""
    tenant_id: str
    source_id: str
    source_type: str

    email: Optional[str]
    phone: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    company: Optional[str]

    variant_type: VariantType
    expected_decision: ExpectedDecision
    identity_id: str


@dataclass
class DatasetManifest:
    """Metadata about generated dataset."""
    seed: int
    total_records: int
    duplicate_rate: float
    invalid_rate: float