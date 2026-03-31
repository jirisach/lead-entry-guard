"""
Phase 3B — Tests for A3 signal: suspicious_domain + low_trust_domain

Covers:
  - Hard A3 (suspicious_domain) — TLD detection, unchanged behavior
  - Soft A3 (low_trust_domain) — structural heuristics
  - Mutual exclusion — hard takes priority, both never fire
  - Visibility invariants — no PII, correct fields
  - Boundary cases
"""
from __future__ import annotations

import pytest

from lead_entry_guard.core.signal_models import (
    FallbackMode,
    LeadSignalContext,
    SignalAction,
    SignalClass,
    SignalResult,
)
from lead_entry_guard.policies.signal_a3 import (
    A3_SIGNAL,
    A3_SOFT_SIGNAL,
    A3SignalRule,
    SUSPICIOUS_TLDS,
    _extract_domain,
    _extract_label,
    has_soft_domain_risk,
    has_suspicious_tld,
)


def ctx(email: str | None) -> LeadSignalContext:
    return LeadSignalContext(tenant_id="t1", email=email)


# ── _extract_domain ───────────────────────────────────────────────────────────

class TestExtractDomain:

    def test_standard_email(self):
        assert _extract_domain("user@example.com") == "example.com"

    def test_uppercase_lowercased(self):
        assert _extract_domain("user@EXAMPLE.COM") == "example.com"

    def test_empty_string(self):
        assert _extract_domain("") is None

    def test_no_at_sign(self):
        assert _extract_domain("notanemail") is None

    def test_at_sign_at_end(self):
        assert _extract_domain("user@") is None

    def test_subdomain(self):
        assert _extract_domain("user@mail.example.com") == "mail.example.com"


# ── _extract_label ────────────────────────────────────────────────────────────

class TestExtractLabel:

    def test_simple_domain(self):
        assert _extract_label("example.com") == "example"

    def test_hyphenated_domain(self):
        assert _extract_label("newco-mail.com") == "newco-mail"

    def test_multi_hyphen_domain(self):
        assert _extract_label("newco-mail-online.com") == "newco-mail-online"

    def test_subdomain_returns_rightmost_label(self):
        assert _extract_label("mail.example.com") == "example"


# ── has_suspicious_tld (hard A3) ──────────────────────────────────────────────

class TestHasSuspiciousTld:

    @pytest.mark.parametrize("email", [
        "user@example.xyz", "user@example.top", "user@example.click",
        "user@example.loan", "user@example.gq", "user@example.ml",
        "user@example.cf", "user@example.tk",
    ])
    def test_all_suspicious_tlds_match(self, email):
        assert has_suspicious_tld(email) is True

    @pytest.mark.parametrize("email", [
        "user@example.com", "user@example.cz", "user@example.org",
        "user@example.net", "user@example.io",
    ])
    def test_legitimate_tlds_do_not_match(self, email):
        assert has_suspicious_tld(email) is False

    def test_all_tlds_in_set_covered(self):
        for tld in SUSPICIOUS_TLDS:
            assert has_suspicious_tld(f"user@example{tld}") is True

    @pytest.mark.parametrize("email,reason", [
        (None, "None"), ("", "empty"), ("notanemail", "no @"), ("user@", "empty domain"),
    ])
    def test_invalid_inputs_return_false(self, email, reason):
        assert has_suspicious_tld(email) is False


# ── has_soft_domain_risk (soft A3) ────────────────────────────────────────────

class TestHasSoftDomainRisk:

    # Rule 1: more than one hyphen
    def test_multiple_hyphens_fires(self):
        assert has_soft_domain_risk("user@newco-mail-online.com") is True

    def test_single_hyphen_does_not_fire(self):
        """One hyphen is common in legit B2B domains."""
        assert has_soft_domain_risk("user@newco-mail.com") is False

    def test_no_hyphen_does_not_fire(self):
        assert has_soft_domain_risk("user@example.com") is False

    # Rule 2: label longer than 20 chars
    def test_long_label_fires(self):
        assert has_soft_domain_risk("user@verylongsyntheticdomain.com") is True

    def test_exactly_20_chars_does_not_fire(self):
        # "abcdefghijklmnopqrst" = exactly 20 chars — must not fire
        assert has_soft_domain_risk("user@abcdefghijklmnopqrst.com") is False

    def test_21_chars_fires(self):
        # "abcdefghijklmnopqrstu" = exactly 21 chars — must fire
        assert has_soft_domain_risk("user@abcdefghijklmnopqrstu.com") is True

    # Mutual exclusion — hard TLD must not trigger soft
    def test_suspicious_tld_does_not_trigger_soft(self):
        """Hard A3 domain must not also fire soft — mutual exclusion."""
        assert has_soft_domain_risk("user@newco-mail-online.xyz") is False
        assert has_soft_domain_risk("user@verylongsyntheticdomain.ml") is False

    # Boundary cases
    @pytest.mark.parametrize("email,reason", [
        (None, "None"), ("", "empty"), ("notanemail", "no @"),
    ])
    def test_invalid_inputs_return_false(self, email, reason):
        assert has_soft_domain_risk(email) is False

    def test_normal_b2b_domains_do_not_fire(self):
        """Common legitimate B2B domains must not trigger soft signal."""
        assert has_soft_domain_risk("user@b2b.com") is False
        assert has_soft_domain_risk("user@salesforce.com") is False
        assert has_soft_domain_risk("user@hubspot.com") is False
        assert has_soft_domain_risk("user@api2crm.com") is False


# ── A3SignalRule — mutual exclusion ───────────────────────────────────────────

class TestA3SignalRuleMutualExclusion:

    def test_hard_tld_emits_hard_signal(self):
        rule = A3SignalRule()
        signals = rule.evaluate(ctx("user@example.xyz"))
        assert len(signals) == 1
        assert signals[0].code == "suspicious_domain"

    def test_soft_risk_emits_soft_signal(self):
        rule = A3SignalRule()
        signals = rule.evaluate(ctx("user@newco-mail-online.com"))
        assert len(signals) == 1
        assert signals[0].code == "low_trust_domain"

    def test_hard_and_soft_never_both_fire(self):
        """When hard fires, soft must not also fire — at most one signal."""
        rule = A3SignalRule()
        signals = rule.evaluate(ctx("user@newco-mail-online.xyz"))
        assert len(signals) == 1
        assert signals[0].code == "suspicious_domain"

    def test_clean_domain_emits_no_signal(self):
        rule = A3SignalRule()
        assert rule.evaluate(ctx("user@example.com")) == []

    def test_none_email_emits_no_signal(self):
        assert A3SignalRule().evaluate(ctx(None)) == []

    def test_prianka_case_com_now_fires_soft(self):
        """
        Prianka feedback (March 2026):
        marek@newco-mail.com had no signal — single hyphen does not fire.
        A domain with multiple hyphens on .com would fire soft signal.
        """
        rule = A3SignalRule()
        # Single hyphen — still no signal (intentional)
        assert rule.evaluate(ctx("marek@newco-mail.com")) == []
        # Multiple hyphens — soft signal
        signals = rule.evaluate(ctx("marek@newco-mail-online.com"))
        assert len(signals) == 1
        assert signals[0].code == "low_trust_domain"


# ── Hard A3 visibility ────────────────────────────────────────────────────────

class TestHardA3Visibility:

    def _get(self) -> SignalResult:
        return A3SignalRule().evaluate(ctx("user@example.xyz"))[0]

    def test_crm_status_needs_review(self):
        assert self._get().visibility.crm_status == "needs_review"

    def test_routing_tag_suspicious_domain(self):
        assert "suspicious_domain" in self._get().visibility.routing_tags

    def test_api_flag_requires_review(self):
        assert self._get().visibility.api_flags.get("requires_review") is True

    def test_no_pii_in_visibility(self):
        v = self._get().visibility
        assert "@" not in (v.crm_status or "")
        for tag in v.routing_tags:
            assert "@" not in tag
        for val in v.api_flags.values():
            assert isinstance(val, bool)


# ── Soft A3 visibility ────────────────────────────────────────────────────────

class TestSoftA3Visibility:

    def _get(self) -> SignalResult:
        return A3SignalRule().evaluate(ctx("user@newco-mail-online.com"))[0]

    def test_crm_status_low_trust_lead(self):
        assert self._get().visibility.crm_status == "low_trust_lead"

    def test_routing_tag_low_trust_domain(self):
        assert "low_trust_domain" in self._get().visibility.routing_tags

    def test_api_flag_low_trust(self):
        assert self._get().visibility.api_flags.get("low_trust") is True

    def test_no_pii_in_visibility(self):
        v = self._get().visibility
        assert "@" not in (v.crm_status or "")
        for tag in v.routing_tags:
            assert "@" not in tag
        for val in v.api_flags.values():
            assert isinstance(val, bool)

    def test_signal_class_is_informational(self):
        assert self._get().signal_class == SignalClass.INFORMATIONAL

    def test_action_is_accept_low_quality(self):
        assert self._get().action == SignalAction.ACCEPT_LOW_QUALITY

    def test_fallback_is_keep_accepted_low_trust(self):
        assert self._get().fallback.mode == FallbackMode.KEEP_ACCEPTED_LOW_TRUST

    def test_soft_does_not_trigger_needs_review(self):
        """Soft signal must not produce needs_review — that's hard A3 only."""
        v = self._get().visibility
        assert v.crm_status != "needs_review"
        assert not v.api_flags.get("requires_review", False)

    def test_deep_copy(self):
        original = A3_SOFT_SIGNAL.visibility.crm_status
        result = SignalResult.from_definition(A3_SOFT_SIGNAL)
        A3_SOFT_SIGNAL.visibility.crm_status = "mutated"
        assert result.visibility.crm_status == original
        A3_SOFT_SIGNAL.visibility.crm_status = original
