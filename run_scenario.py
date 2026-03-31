"""
run_scenario.py — Signal engine walkthrough for Prianka's feedback cases.

NOTE: This is a signal walkthrough, not a full pipeline simulation.
Primary decisions are assumed (based on pipeline behavior) — they are not
computed by the policy engine here. Only the signal layer runs live.

What this validates:
  - which signals fire for each scenario
  - what downstream systems would see (visibility projection)
  - what fallback behavior is defined
  - where scope boundaries are (e.g. A3 .com vs .xyz)

Usage:
    python run_scenario.py
"""
from __future__ import annotations

from lead_entry_guard.core.signal_models import (
    DecisionResultV2,
    FieldSourceRecord,
    LeadSignalContext,
)
from lead_entry_guard.policies.signal_evaluator import SignalEvaluator


# ── Scenarios ────────────────────────────────────────────────────────────────

SCENARIOS = [
    {
        "case": "A1",
        "description": "First submission with phone — clean lead",
        "context": LeadSignalContext(
            tenant_id="tenant_A",
            email="lucie@example.com",
            fields=[
                FieldSourceRecord("phone", "manual", "+420777111000"),
            ],
        ),
        "primary_decision": "PASS",
        "expected_signals": [],
        "expected_crm_status": [],
    },
    {
        "case": "A2",
        "description": "Same lead later from API without phone (assumed: duplicate, incomplete payload)",
        "context": LeadSignalContext(
            tenant_id="tenant_A",
            email="lucie@example.com",
            fields=[],
        ),
        "primary_decision": "DUPLICATE_HINT",
        "expected_signals": [],
        "expected_crm_status": [],
        "scope_note": "Duplicate detection is pipeline-level — not in signal layer scope",
    },
    {
        "case": "A3",
        "description": "Suspicious-looking domain — .com TLD (not in suspicious set)",
        "context": LeadSignalContext(
            tenant_id="tenant_A",
            email="marek@newco-mail.com",
            fields=[
                FieldSourceRecord("phone", "manual", "+420777222000"),
            ],
        ),
        "primary_decision": "PASS",
        "expected_signals": [],
        "expected_crm_status": [],
        "scope_note": ".com not in SUSPICIOUS_TLDS — Phase 3B TLD-only detection",
    },
    {
        "case": "A3x",
        "description": "Same scenario — .xyz TLD (in suspicious set)",
        "context": LeadSignalContext(
            tenant_id="tenant_A",
            email="marek@newco-mail.xyz",
            fields=[
                FieldSourceRecord("phone", "manual", "+420777222000"),
            ],
        ),
        "primary_decision": "PASS",
        "expected_signals": ["suspicious_domain"],
        "expected_crm_status": ["needs_review"],
        "scope_note": "Shows TLD boundary — .xyz fires, .com does not",
    },
    {
        "case": "A4",
        "description": "Enrichment overwrites a manually collected field",
        "context": LeadSignalContext(
            tenant_id="tenant_A",
            email="sara@example.com",
            fields=[
                FieldSourceRecord("phone", "manual", "+420777333000"),
                FieldSourceRecord("phone", "enrichment", "+420777999999"),
            ],
        ),
        "primary_decision": "WARN",
        "expected_signals": ["source_conflict_manual_vs_enrichment"],
        "expected_crm_status": ["conflict_flagged"],
    },
    {
        "case": "A6",
        "description": "Shared inbox submitted through import",
        "context": LeadSignalContext(
            tenant_id="tenant_A",
            email="info@acme.com",
            fields=[
                FieldSourceRecord("phone", "manual", "+420777555000"),
            ],
        ),
        "primary_decision": "PASS",
        "expected_signals": ["shared_inbox"],
        "expected_crm_status": ["low_quality_lead"],
    },
    {
        "case": "A7",
        "description": "Same person as A1 but different tenant",
        "context": LeadSignalContext(
            tenant_id="tenant_B",
            email="lucie@example.com",
            fields=[
                FieldSourceRecord("phone", "manual", "+420777111000"),
            ],
        ),
        "primary_decision": "PASS",
        "expected_signals": [],
        "expected_crm_status": [],
        "scope_note": "Tenant isolation — separate namespace, no cross-tenant signal",
    },
]


# ── Engine ────────────────────────────────────────────────────────────────────

def run(scenario: dict) -> dict:
    evaluator = SignalEvaluator()
    context = scenario["context"]
    signals = evaluator.evaluate(context)

    result = DecisionResultV2(
        request_id=f"scenario-{scenario['case']}",
        tenant_id=context.tenant_id,
        decision=scenario["primary_decision"],
        reason_codes=[],
        signals=signals,
    )

    actual_signals = [s.code for s in result.signals]
    actual_crm = [s.visibility.crm_status for s in result.signals]
    expected_signals = scenario.get("expected_signals", [])
    expected_crm = scenario.get("expected_crm_status", [])

    signal_match = sorted(actual_signals) == sorted(expected_signals)
    crm_match = sorted(str(x) for x in actual_crm) == sorted(str(x) for x in expected_crm)
    ok = signal_match and crm_match

    return {
        "case": scenario["case"],
        "description": scenario["description"],
        "email": context.email,
        "primary_decision": result.decision,
        "signals_emitted": actual_signals or ["—"],
        "crm_status": [str(x) for x in actual_crm] or ["—"],
        "routing_tags": [t for s in result.signals for t in s.visibility.routing_tags] or ["—"],
        "api_flags": {k: v for s in result.signals for k, v in s.visibility.api_flags.items()},
        "fallback": [s.fallback.mode.value if s.fallback else "exempt" for s in result.signals] or ["—"],
        "scope_note": scenario.get("scope_note", ""),
        "ok": ok,
        "mismatch": None if ok else {
            "expected_signals": expected_signals,
            "actual_signals": actual_signals,
            "expected_crm": expected_crm,
            "actual_crm": actual_crm,
        },
    }


# ── Output ────────────────────────────────────────────────────────────────────

def print_table(results: list[dict]) -> None:
    sep = "─" * 120

    print()
    print("Signal Engine — Scenario Walkthrough")
    print("Prianka feedback cases, March 2026")
    print("NOTE: primary decision is assumed — only signal layer runs live")
    print(sep)
    print(f"{'Case':<5} {'Email':<26} {'Decision':<16} {'Signals emitted':<36} {'CRM status':<20} {'Check':<6} {'Scope note'}")
    print(sep)

    for r in results:
        check = "✓" if r["ok"] else "✗ MISMATCH"
        print(
            f"{r['case']:<5} "
            f"{(r['email'] or '—'):<26} "
            f"{r['primary_decision']:<16} "
            f"{', '.join(r['signals_emitted']):<36} "
            f"{', '.join(r['crm_status']):<20} "
            f"{check:<6} "
            f"{r['scope_note']}"
        )

    print(sep)

    # Mismatches
    mismatches = [r for r in results if not r["ok"]]
    if mismatches:
        print()
        print("MISMATCHES:")
        for r in mismatches:
            m = r["mismatch"]
            print(f"  [{r['case']}] expected signals={m['expected_signals']} got={m['actual_signals']}")
            print(f"         expected crm={m['expected_crm']} got={m['actual_crm']}")
    else:
        print()
        print("All scenarios match expected output. ✓")

    # Signal detail
    print()
    print("Signal detail (cases with signals only):")
    print()
    for r in results:
        if r["signals_emitted"] == ["—"]:
            continue
        print(f"  [{r['case']}] {r['description']}")
        print(f"       signals : {', '.join(r['signals_emitted'])}")
        print(f"       crm     : {', '.join(r['crm_status'])}")
        print(f"       tags    : {', '.join(r['routing_tags'])}")
        print(f"       flags   : {r['api_flags']}")
        print(f"       fallback: {', '.join(r['fallback'])}")
        if r["scope_note"]:
            print(f"       note    : {r['scope_note']}")
        print()


if __name__ == "__main__":
    results = [run(s) for s in SCENARIOS]
    print_table(results)
