"""Unit tests — normalization layer."""
import pytest

from lead_entry_guard.core.models import LeadInput, SourceType
from lead_entry_guard.normalization.normalizer import NormalizationLayer, _normalize_email, _normalize_phone


def test_email_normalized_to_lowercase():
    assert _normalize_email("  TEST@EXAMPLE.COM  ") == "test@example.com"


def test_email_strips_whitespace():
    assert _normalize_email(" user@domain.org ") == "user@domain.org"


def test_phone_e164_us():
    result = _normalize_phone("202-555-0100", default_region="US")
    assert result == "+12025550100"


def test_phone_e164_international():
    # +420 601 123 456 — valid Czech mobile number format
    result = _normalize_phone("+420 601 123 456")
    assert result is not None
    assert result.startswith("+420")


def test_phone_invalid_returns_none():
    result = _normalize_phone("not-a-phone")
    assert result is None


def test_full_normalization():
    layer = NormalizationLayer()
    lead = LeadInput(
        tenant_id="t1",
        email="  HELLO@WORLD.COM  ",
        phone="+12025550100",
        first_name="john",
        last_name="DOE",
        company="  acme corp  ",
    )
    normalized = layer.normalize(lead)
    assert normalized.email_normalized == "hello@world.com"
    assert normalized.phone_normalized == "+12025550100"
    assert normalized.first_name_normalized == "John"
    assert normalized.last_name_normalized == "Doe"
    assert normalized.company_normalized == "acme corp"


def test_normalization_handles_none_fields():
    layer = NormalizationLayer()
    lead = LeadInput(tenant_id="t1", email="a@b.com")
    normalized = layer.normalize(lead)
    assert normalized.phone_normalized is None
    assert normalized.first_name_normalized is None
