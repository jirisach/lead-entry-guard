"""
Phase 3B — Tests for A6 signal: shared_inbox

Covers:
  - Detection logic (is_shared_inbox, _extract_local_part)
  - A6SignalRule — correct fire / no-fire conditions
  - Visibility invariants — no PII, correct fields set
  - INFORMATIONAL class — fallback present but class is exempt
  - Deep copy — mutation of A6_SIGNAL does not affect emitted result
  - Boundary cases — empty email, None, no-@ email, substring non-match
"""
from __future__ import annotations

import pytest

from lead_entry_guard.core.signal_models import (
    LeadSignalContext,
    FallbackMode,
    SignalAction,
    SignalClass,
    SignalResult,
)
from lead_entry_guard.policies.signal_a6 import (
    A6_SIGNAL,
    A6SignalRule,
    SHARED_INBOX_PREFIXES,
    _extract_local_part,
    is_shared_inbox,
)


# ── _extract_local_part ───────────────────────────────────────────────────────

class TestExtractLocalPart:

    def test_standard_email(self):
        assert _extract_local_part("info@example.com") == "info"

    def test_uppercase_is_lowercased(self):
        assert _extract_local_part("INFO@example.com") == "info"

    def test_empty_string(self):
        assert _extract_local_part("") is None

    def test_no_at_sign(self):
        assert _extract_local_part("notanemail") is None

    def test_at_sign_at_start(self):
        assert _extract_local_part("@example.com") is None

    def test_subdomain_email(self):
        assert _extract_local_part("support@mail.example.com") == "support"


# ── is_shared_inbox ───────────────────────────────────────────────────────────

class TestIsSharedInbox:

    @pytest.mark.parametrize("email", [
        "info@example.com",
        "support@example.com",
        "sales@example.com",
        "contact@example.com",
        "hello@example.com",
    ])
    def test_known_prefixes_match(self, email: str):
        assert is_shared_inbox(email) is True

    @pytest.mark.parametrize("email", [
        "INFO@example.com",
        "SUPPORT@example.com",
        "Sales@example.com",
        "HELLO@example.com",
    ])
    def test_case_insensitive(self, email: str):
        assert is_shared_inbox(email) is True

    @pytest.mark.parametrize("email", [
        "information@example.com",   # substring of "info" — must NOT match
        "supporter@example.com",     # substring of "support" — must NOT match
        "salesteam@example.com",     # substring of "sales" — must NOT match
    ])
    def test_substring_does_not_match(self, email: str):
        """Exact match only — substring of a known prefix must not fire."""
        assert is_shared_inbox(email) is False

    @pytest.mark.parametrize("email", [
        "john@example.com",
        "lucie@example.com",
        "sara@example.com",
        "peter@example.com",
    ])
    def test_personal_emails_do_not_match(self, email: str):
        assert is_shared_inbox(email) is False

    @pytest.mark.parametrize("email,reason", [
        (None, "None input"),
        ("", "empty string"),
        ("notanemail", "no @ sign"),
        ("@example.com", "empty local part"),
    ])
    def test_invalid_inputs_return_false(self, email, reason):
        assert is_shared_inbox(email) is False, f"Expected False for: {reason}"

    @pytest.mark.parametrize("excluded_prefix", [
        "noreply",
        "no-reply",
        "admin",
        "team",
        "office",
    ])
    def test_intentionally_excluded_prefixes_do_not_match(self, excluded_prefix: str):
        """
        Prefixes intentionally excluded from SHARED_INBOX_PREFIXES must not fire.
        Documents the scope decision — narrow set to minimize false positives.
        """
        email = f"{excluded_prefix}@example.com"
        assert is_shared_inbox(email) is False, (
            f"'{excluded_prefix}' should not match — intentionally excluded from A6 scope. "
            "If this changes, update SHARED_INBOX_PREFIXES and this test together."
        )


# ── A6SignalRule ──────────────────────────────────────────────────────────────

class TestA6SignalRule:

    def test_shared_inbox_emits_signal(self):
        rule = A6SignalRule()
        signals = rule.evaluate(LeadSignalContext(tenant_id="t1", email="info@example.com"))
        assert len(signals) == 1
        assert signals[0].code == "shared_inbox"

    def test_personal_email_emits_no_signal(self):
        rule = A6SignalRule()
        signals = rule.evaluate(LeadSignalContext(tenant_id="t1", email="john@example.com"))
        assert signals == []

    def test_none_email_emits_no_signal(self):
        rule = A6SignalRule()
        signals = rule.evaluate(LeadSignalContext(tenant_id="t1", email=None))
        assert signals == []

    def test_empty_email_emits_no_signal(self):
        rule = A6SignalRule()
        signals = rule.evaluate(LeadSignalContext(tenant_id="t1", email=""))
        assert signals == []

    def test_signal_action_is_accept_low_quality(self):
        rule = A6SignalRule()
        signals = rule.evaluate(LeadSignalContext(tenant_id="t1", email="support@example.com"))
        assert signals[0].action == SignalAction.ACCEPT_LOW_QUALITY

    def test_signal_class_is_informational(self):
        """A6 is INFORMATIONAL — fallback-exempt but visibility still required."""
        rule = A6SignalRule()
        signals = rule.evaluate(LeadSignalContext(tenant_id="t1", email="sales@example.com"))
        assert signals[0].signal_class == SignalClass.INFORMATIONAL

    def test_all_prefixes_fire(self):
        """Every odsouhlasený prefix must produce a signal."""
        rule = A6SignalRule()
        for prefix in SHARED_INBOX_PREFIXES:
            email = f"{prefix}@example.com"
            signals = rule.evaluate(LeadSignalContext(tenant_id="t1", email=email))
            assert len(signals) == 1, (
                f"Expected signal for '{email}' — prefix '{prefix}' "
                "is in SHARED_INBOX_PREFIXES but no signal was emitted."
            )


# ── Visibility invariants ─────────────────────────────────────────────────────

class TestA6Visibility:

    def _get_signal(self) -> SignalResult:
        return A6SignalRule().evaluate(LeadSignalContext(tenant_id="t1", email="info@example.com"))[0]

    def test_crm_status_is_low_quality_lead(self):
        assert self._get_signal().visibility.crm_status == "low_quality_lead"

    def test_routing_tag_is_shared_inbox(self):
        assert "shared_inbox" in self._get_signal().visibility.routing_tags

    def test_api_flag_low_quality_is_true(self):
        assert self._get_signal().visibility.api_flags.get("low_quality") is True

    def test_visibility_contains_no_email_address(self):
        """
        ADR-008 PII invariant — email address must not appear in visibility.
        The rule fires on 'info@example.com' but must never copy that value downstream.
        """
        signal = self._get_signal()
        v = signal.visibility

        # crm_status must not contain email
        assert "@" not in (v.crm_status or "")

        # tags must not contain email
        for tag in v.routing_tags:
            assert "@" not in tag
            assert "info" not in tag  # local part must not leak into tag

        # api_flags values must be bool
        for key, val in v.api_flags.items():
            assert isinstance(val, bool), (
                f"api_flags['{key}'] is {type(val).__name__}, expected bool — "
                "non-bool values may contain PII (ADR-008)"
            )

    def test_visibility_is_minimum_consequence(self):
        """
        INFORMATIONAL signal may be fallback-exempt but must define visibility.
        Visibility is the minimum consequence — no signal may be consequence-free.
        """
        signal = self._get_signal()
        v = signal.visibility
        has_any = (
            v.crm_status is not None
            or bool(v.routing_tags)
            or bool(v.api_flags)
        )
        assert has_any, (
            "A6 signal has no visibility fields — violates ADR-008. "
            "INFORMATIONAL class is fallback-exempt, not consequence-free."
        )


# ── INFORMATIONAL class — fallback semantics ──────────────────────────────────

class TestA6FallbackSemantics:

    def test_fallback_is_defined(self):
        """
        A6 defines fallback even though INFORMATIONAL class does not require it.
        This is intentional — provides downstream guidance even for low-priority signals.
        """
        signal = A6SignalRule().evaluate(LeadSignalContext(tenant_id="t1", email="info@example.com"))[0]
        assert signal.fallback is not None

    def test_fallback_mode_is_keep_accepted_low_trust(self):
        signal = A6SignalRule().evaluate(LeadSignalContext(tenant_id="t1", email="contact@example.com"))[0]
        assert signal.fallback.mode == FallbackMode.KEEP_ACCEPTED_LOW_TRUST

    def test_fallback_then_describes_low_trust(self):
        signal = A6SignalRule().evaluate(LeadSignalContext(tenant_id="t1", email="hello@example.com"))[0]
        assert "trust" in signal.fallback.then


# ── Deep copy ─────────────────────────────────────────────────────────────────

class TestA6DeepCopy:

    def test_mutation_of_definition_does_not_affect_emitted_result(self):
        """
        SignalResult.from_definition() must deep copy visibility.
        Mutating A6_SIGNAL after emit must not change historical results.
        """
        original_status = A6_SIGNAL.visibility.crm_status

        result = SignalResult.from_definition(A6_SIGNAL)

        # Simulate accidental mutation of the definition
        A6_SIGNAL.visibility.crm_status = "mutated"

        # Emitted result must be unaffected
        assert result.visibility.crm_status == original_status

        # Restore
        A6_SIGNAL.visibility.crm_status = original_status

    def test_two_emissions_are_independent(self):
        """Two calls to from_definition produce independent objects."""
        r1 = SignalResult.from_definition(A6_SIGNAL)
        r2 = SignalResult.from_definition(A6_SIGNAL)

        r1.visibility.crm_status = "modified"
        assert r2.visibility.crm_status != "modified"
