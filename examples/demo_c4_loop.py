"""
demo_c4_loop.py — C4 end-to-end feedback loop walkthrough

Demonstrates the complete lifecycle for a compound-routed lead:

  1. Signal evaluation (A3 soft + A4 → compound fires)
  2. PendingReview created (routing moment)
  3. Human submits review outcome via store directly (simulates POST /v1/review-events)
  4. Analytics: action_breakdown, expired_ratio, low_insight_ratio
  5. Second run: expiry job fires on unreviewed lead

This is the demo to show Priyanka.
The question to ask after: "Does this make sense without me explaining it?"

NOT a load test. NOT a unit test.
This is a human-readable walkthrough of the feedback loop.

Usage:
    python demo_c4_loop.py
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

from lead_entry_guard.core.signal_models import (
    FieldSourceRecord,
    LeadSignalContext,
)
from lead_entry_guard.policies.compound_signal_evaluator import CompoundSignalEvaluator
from lead_entry_guard.policies.signal_evaluator import SignalEvaluator
from lead_entry_guard.core.review_event import (
    ReviewAction,
    ReviewEventStore,
    create_human_review_event,
    create_expired_review_event,
    create_pending_review,
)
from lead_entry_guard.policies.compound_signal_result import CompoundSignalResult

TENANT = "demo_tenant"
SEP = "─" * 65


def header(title: str) -> None:
    print()
    print(SEP)
    print(f"  {title}")
    print(SEP)


def run_demo() -> None:
    store = ReviewEventStore()
    compound_evaluator = CompoundSignalEvaluator(enabled=True)
    evaluator = SignalEvaluator(compound=compound_evaluator)

    # ── Step 1: Signal evaluation ─────────────────────────────────────────────

    header("Step 1 — Signal evaluation")

    context = LeadSignalContext(
        tenant_id=TENANT,
        email="info@verylongsyntheticdomain.com",
        fields=[
            FieldSourceRecord("phone", "manual", "+420777444000"),
            FieldSourceRecord("phone", "enrichment", "+420777000111"),
        ],
    )

    print(f"\n  Lead:  {context.email}")
    print(f"  Phone: manual +420777444000  /  enrichment +420777000111")

    # Base signals
    from lead_entry_guard.policies.signal_a3 import A3SignalRule
    from lead_entry_guard.policies.signal_a4 import A4SignalRule
    from lead_entry_guard.policies.signal_a6 import A6SignalRule

    base_signals = []
    base_signals.extend(A3SignalRule().evaluate(context))
    base_signals.extend(A4SignalRule().evaluate(context))
    base_signals.extend(A6SignalRule().evaluate(context))

    print(f"\n  Base signals ({len(base_signals)}):")
    for s in sorted(base_signals, key=lambda x: x.code):
        print(f"    · {s.code}")
        print(f"      action:     {s.action.value}")
        print(f"      crm_status: {s.visibility.crm_status}")

    # Compound
    compound_results = compound_evaluator.evaluate_with_metadata(tuple(base_signals))

    if compound_results:
        print(f"\n  Compound signal:")
        for cr in compound_results:
            print(f"    · {cr.signal.code}")
            print(f"      triggered_by: {sorted(cr.triggered_by)}")
            print(f"      crm_status:   {cr.signal.visibility.crm_status}")
            print(f"      action:       {cr.signal.action.value}")
            print(f"      reason:       {cr.human_reason()}")
            print()
            print(f"      log payload:")
            for k, v in cr.to_log_dict().items():
                print(f"        {k}: {v}")
    else:
        print("\n  Compound signal: did not fire")

    # ── Step 2: Routing — PendingReview created ───────────────────────────────

    header("Step 2 — Routing (PendingReview created)")

    pending = create_pending_review(
        fingerprint_id="fp_c4_demo_001",
        compound_code="compound_signal_alignment",
        triggered_by=frozenset({"low_trust_domain", "source_conflict_manual_vs_enrichment"}),
        tenant_id=TENANT,
        expiry_hours=24,
    )
    store.add_pending(pending)

    print(f"\n  pending_id:     {pending.pending_id}")
    print(f"  fingerprint_id: {pending.fingerprint_id}")
    print(f"  compound_code:  {pending.compound_code}")
    print(f"  routed_at:      {pending.routed_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  expires_at:     {pending.expires_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  resolved:       {pending.resolved}")
    print()
    print("  → Lead is now in review queue. Owner: SDR manager / RevOps.")

    # ── Step 3: Human review (simulates POST /v1/review-events) ──────────────

    header("Step 3 — Human review submitted (action: reject)")

    event = create_human_review_event(
        fingerprint_id=pending.fingerprint_id,
        action=ReviewAction.REJECT,
        actor="sdr_manager",
        compound_code=pending.compound_code,
        triggered_by=pending.triggered_by,
        tenant_id=TENANT,
        expires_at=pending.expires_at,
        reason="No company found — domain appears synthetic. Likely low intent.",
        pending_id=pending.pending_id,
    )
    store.append(event)

    print(f"\n  review_id:      {event.review_id}")
    print(f"  fingerprint_id: {event.fingerprint_id}")
    print(f"  action:         {event.action.value}")
    print(f"  actor:          {event.actor}")
    print(f"  reason:         {event.reason}")
    print(f"  low_insight:    {event.low_insight}")
    print(f"  recorded_at:    {event.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()

    # Verify pending auto-resolved
    resolved = store._pending[pending.pending_id]
    print(f"  PendingReview resolved: {resolved.resolved}  ← auto-resolved on append")

    # ── Step 4: Analytics ─────────────────────────────────────────────────────

    header("Step 4 — Analytics (after 1 review)")

    breakdown = store.action_breakdown(TENANT, "compound_signal_alignment")
    low_insight = store.low_insight_ratio(TENANT)
    expired_r = store.expired_ratio(TENANT)

    print(f"\n  action_breakdown (compound_signal_alignment):")
    for action, count in breakdown.items():
        bar = "█" * count if count else "·"
        print(f"    {action:<25} {bar} ({count})")

    print(f"\n  low_insight_ratio:  {low_insight:.0%}  ← {'good — reason provided' if low_insight < 1.0 else 'warning — no reasons given'}")
    print(f"  expired_ratio:      {expired_r:.0%}  ← {'OK' if expired_r <= 0.20 else 'ALERT: queue may be broken'}")

    # ── Step 5: Expiry job — second lead, no human action ────────────────────

    header("Step 5 — Expiry job (second lead, no human action)")

    # Simulate a second lead that expired without review
    pending_expired = create_pending_review(
        fingerprint_id="fp_c4_demo_002",
        compound_code="compound_signal_alignment",
        triggered_by=frozenset({"low_trust_domain", "source_conflict_manual_vs_enrichment"}),
        tenant_id=TENANT,
        expiry_hours=24,
    )
    # Manually backdate expires_at to simulate expiry
    from dataclasses import replace
    pending_expired_past = PendingReview(
        pending_id=pending_expired.pending_id,
        fingerprint_id=pending_expired.fingerprint_id,
        compound_code=pending_expired.compound_code,
        triggered_by=pending_expired.triggered_by,
        tenant_id=pending_expired.tenant_id,
        routed_at=pending_expired.routed_at,
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        resolved=False,
    )
    store._pending[pending_expired_past.pending_id] = pending_expired_past

    print(f"\n  Pending lead fp_c4_demo_002 — expires_at in the past, no human action.")
    print(f"  Running expiry job...")

    expired_found = list(store.get_expired_pending())
    print(f"\n  Expired pending found: {len(expired_found)}")

    for p in expired_found:
        expired_event = create_expired_review_event(
            fingerprint_id=p.fingerprint_id,
            compound_code=p.compound_code,
            triggered_by=p.triggered_by,
            tenant_id=p.tenant_id,
            expires_at=p.expires_at,
            pending_id=p.pending_id,
        )
        store.append(expired_event)
        print(f"\n  expired_no_review event created:")
        print(f"    review_id:  {expired_event.review_id}")
        print(f"    actor:      {expired_event.actor}")
        print(f"    low_insight:{expired_event.low_insight}")

    # ── Step 6: Final analytics ───────────────────────────────────────────────

    header("Step 6 — Final analytics (after expiry)")

    breakdown2 = store.action_breakdown(TENANT, "compound_signal_alignment")
    expired_r2 = store.expired_ratio(TENANT)

    print(f"\n  action_breakdown (compound_signal_alignment):")
    for action, count in breakdown2.items():
        bar = "█" * count if count else "·"
        print(f"    {action:<25} {bar} ({count})")

    print(f"\n  expired_ratio: {expired_r2:.0%}  ← {'OK' if expired_r2 <= 0.20 else 'ALERT: > 20% — check routing ownership'}")
    print()

    if expired_r2 > 0.20:
        print("  ⚠️  ALERT: expired_ratio above threshold.")
        print("     Action: check if owner is assigned and queue is manageable.")
    else:
        print("  ✓  expired_ratio within threshold. Routing is working.")

    print()
    print(SEP)
    print("  Demo complete.")
    print(f"  Total ReviewEvents stored: {len(store._events)}")
    print(SEP)
    print()


if __name__ == "__main__":
    from lead_entry_guard.core.review_event import PendingReview
    run_demo()
