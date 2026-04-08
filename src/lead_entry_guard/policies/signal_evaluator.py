"""
Phase 3B — Signal Evaluator

Unified entry point for all signal rules.

Called after the main pipeline decision is made.
Signals annotate DecisionResult — they do not change the primary decision.

Rules:
  Data signals (A-series — concrete field observations):
    - A3SignalRule  (suspicious_domain, low_trust_domain)
    - A4SignalRule  (source_conflict_manual_vs_enrichment)
    - A6SignalRule  (shared_inbox)

  Context signals (C-series — decision readiness conclusions):
    - C1C2C3SignalRule  (missing_context, conflicting_context, false_clarity)

Input:  LeadSignalContext (unified — each rule takes what it needs)
Output: list[SignalResult] — empty if no signals fire, never None

ADR-008: every emitted signal has action + visibility + fallback.
No signal without operational consequence may be returned.

signal_family in SignalResult distinguishes data observations from
context readiness conclusions. Downstream systems may filter by family.
"""
from __future__ import annotations

from lead_entry_guard.core.signal_models import LeadSignalContext, SignalResult
from lead_entry_guard.policies.signal_a3 import A3SignalRule
from lead_entry_guard.policies.signal_a4 import A4SignalRule
from lead_entry_guard.policies.signal_a6 import A6SignalRule
from lead_entry_guard.policies.signal_c1 import C1C2C3SignalRule


class SignalEvaluator:
    """
    Evaluates all signal rules against a unified LeadSignalContext.

    Each rule is responsible for extracting what it needs from the context:
      - A3: context.email (domain TLD check)
      - A6: context.email (shared inbox prefix check)
      - A4: context.fields (source conflict detection)
      - C1C2C3: context.email + context.fields (context quality evaluation)

    Rules are independent — order does not matter between families.
    Within C-series, C3 depends on C1/C2 outcome (handled inside C1C2C3SignalRule).

    A data signal and a context signal may fire on the same lead.
    Example: A6 (shared_inbox) + C3 (false_clarity) is valid and expected.
    Example: A4 (source_conflict) + C2 (conflicting_context) is valid and expected.
    """

    def __init__(self) -> None:
        self._a3 = A3SignalRule()
        self._a4 = A4SignalRule()
        self._a6 = A6SignalRule()
        self._c1c2c3 = C1C2C3SignalRule()

    def evaluate(self, context: LeadSignalContext) -> list[SignalResult]:
        """
        Run all rules and collect emitted signals.
        Returns empty list if no signals fire — never returns None.
        """
        signals: list[SignalResult] = []
        signals.extend(self._a3.evaluate(context))
        signals.extend(self._a4.evaluate(context))
        signals.extend(self._a6.evaluate(context))
        signals.extend(self._c1c2c3.evaluate(context))
        return signals
