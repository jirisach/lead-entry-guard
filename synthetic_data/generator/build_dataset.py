"""Build a synthetic messy lead dataset for Lead Entry Guard."""
from __future__ import annotations

import json
import random
from pathlib import Path

from .enums import ExpectedDecision, VariantType
from .models import DatasetManifest, LeadEventVariant, SyntheticIdentity
from .mutations import (
    corrupt_email,
    messy_phone,
    near_duplicate_string,
    random_case,
    random_company_suffix,
    random_whitespace,
)

FIRST_NAMES = [
    "John", "Jane", "Alice", "Bob", "Eva", "Lukas", "Anna", "Marek", "Tereza", "David",
]
LAST_NAMES = [
    "Smith", "Johnson", "Brown", "Novak", "Svoboda", "Taylor", "Miller", "Kral", "Prochazka", "White",
]
COMPANIES = [
    "Acme", "Northwind", "Globex", "Initech", "Umbrella", "Contoso", "Stark", "Wayne", "Aperture", "Vertex",
]
DOMAINS = [
    "example.com", "mail.com", "company.io", "business.cz", "demo.org",
]
SOURCE_TYPES = ["API", "WEBHOOK", "IMPORT", "FORM"]


def make_identity(identity_id: int) -> SyntheticIdentity:
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    company = random.choice(COMPANIES)
    domain = random.choice(DOMAINS)

    email = f"{first_name.lower()}.{last_name.lower()}{identity_id}@{domain}"
    phone = f"+420777{identity_id:06d}"[-13:]

    return SyntheticIdentity(
        identity_id=f"id_{identity_id}",
        email=email,
        phone=phone,
        first_name=first_name,
        last_name=last_name,
        company=company,
    )


def to_clean_event(identity: SyntheticIdentity, index: int) -> LeadEventVariant:
    return LeadEventVariant(
        tenant_id="t1",
        source_id=f"src_{index:06d}",
        source_type=random.choice(SOURCE_TYPES),
        email=identity.email,
        phone=identity.phone,
        first_name=identity.first_name,
        last_name=identity.last_name,
        company=identity.company,
        variant_type=VariantType.CLEAN,
        expected_decision=ExpectedDecision.PASS,
        identity_id=identity.identity_id,
    )


def to_dirty_event(identity: SyntheticIdentity, index: int) -> LeadEventVariant:
    return LeadEventVariant(
        tenant_id="t1",
        source_id=f"src_{index:06d}",
        source_type=random.choice(SOURCE_TYPES),
        email=random_case(random_whitespace(identity.email)),
        phone=messy_phone(identity.phone),
        first_name=random_case(random_whitespace(identity.first_name)),
        last_name=random_case(random_whitespace(identity.last_name)),
        company=random_company_suffix(random_whitespace(identity.company)),
        variant_type=VariantType.DIRTY,
        expected_decision=ExpectedDecision.WARN,
        identity_id=identity.identity_id,
    )


def to_exact_duplicate_event(identity: SyntheticIdentity, index: int) -> LeadEventVariant:
    return LeadEventVariant(
        tenant_id="t1",
        source_id=f"src_{index:06d}",
        source_type=random.choice(SOURCE_TYPES),
        email=identity.email,
        phone=identity.phone,
        first_name=identity.first_name,
        last_name=identity.last_name,
        company=identity.company,
        variant_type=VariantType.EXACT_DUPLICATE,
        expected_decision=ExpectedDecision.DUPLICATE_HINT,
        identity_id=identity.identity_id,
    )


def to_near_duplicate_event(identity: SyntheticIdentity, index: int) -> LeadEventVariant:
    return LeadEventVariant(
        tenant_id="t1",
        source_id=f"src_{index:06d}",
        source_type=random.choice(SOURCE_TYPES),
        email=random_case(random_whitespace(identity.email)),
        phone=messy_phone(identity.phone),
        first_name=near_duplicate_string(identity.first_name),
        last_name=identity.last_name,
        company=random_company_suffix(identity.company),
        variant_type=VariantType.NEAR_DUPLICATE,
        # EXPLORATORY — email fingerprint differs from original (mutated string),
        # so pipeline will NOT confirm as duplicate. Expected outcome is PASS or WARN,
        # not DUPLICATE_HINT. This bucket is intentionally exploratory — no hard assertion.
        expected_decision=ExpectedDecision.WARN,
        identity_id=identity.identity_id,
    )


def to_broken_event(identity: SyntheticIdentity, index: int) -> LeadEventVariant:
    return LeadEventVariant(
        tenant_id="t1",
        source_id=f"src_{index:06d}",
        source_type=random.choice(SOURCE_TYPES),
        email=corrupt_email(identity.email),
        phone=messy_phone(identity.phone),
        first_name=identity.first_name,
        last_name=identity.last_name,
        company=None,
        variant_type=VariantType.BROKEN,
        expected_decision=ExpectedDecision.REJECT,
        identity_id=identity.identity_id,
    )


def to_edge_case_event(identity: SyntheticIdentity, index: int) -> LeadEventVariant:
    return LeadEventVariant(
        tenant_id="t1",
        source_id=f"src_{index:06d}",
        source_type=random.choice(SOURCE_TYPES),
        email=random_case(identity.email),
        phone=identity.phone,
        first_name=f"{identity.first_name}Ž",
        last_name=f"{identity.last_name}-X",
        company=f"{identity.company} 🚀",
        variant_type=VariantType.EDGE_CASE,
        expected_decision=ExpectedDecision.WARN,
        identity_id=identity.identity_id,
    )


# Valid decisions per variant type.
# Strict buckets have a single allowed outcome (hard assertion).
# Wide buckets reflect real non-determinism in mutations — analyzed but not
# hard-asserted. Will be narrowed to per-subtype contracts in a future step
# once the generator imports NormalizationLayer to classify mutation outcomes.
_VALID_DECISIONS: dict[str, list[str]] = {
    "clean":           ["PASS"],
    "broken":          ["REJECT"],
    "exact_duplicate": ["DUPLICATE_HINT"],
    "dirty":           ["PASS", "WARN", "REJECT", "DUPLICATE_HINT"],  # wide — exploratory
    "near_duplicate":  ["PASS", "WARN", "REJECT", "DUPLICATE_HINT"],  # exploratory
    "edge_case":       ["PASS", "WARN"],
}


def event_to_dict(event: LeadEventVariant) -> dict:
    variant = event.variant_type.value
    return {
        "tenant_id": event.tenant_id,
        "source_id": event.source_id,
        "source_type": event.source_type,
        "email": event.email,
        "phone": event.phone,
        "first_name": event.first_name,
        "last_name": event.last_name,
        "company": event.company,
        "extra": {},
        "_truth": {
            "identity_id": event.identity_id,
            "variant_type": variant,
            "expected_decision": event.expected_decision.value,
            "valid_decisions": _VALID_DECISIONS.get(variant, [event.expected_decision.value]),
        },
    }


def build_dataset(
    total_records: int = 100_000,
    seed: int = 42,
    clean_ratio: float = 0.55,
    dirty_ratio: float = 0.20,
    exact_duplicate_ratio: float = 0.05,
    near_duplicate_ratio: float = 0.10,
    broken_ratio: float = 0.07,
    edge_ratio: float = 0.03,
) -> tuple[list[dict], DatasetManifest]:
    """
    Build a synthetic messy lead dataset with guaranteed _truth correctness.

    Identity pool design — no cross-bucket sharing:
      - clean, dirty, broken, edge_case each get their own dedicated identity
        pool drawn without replacement. This guarantees that a "clean" lead is
        truly unique and will not collide with another bucket's fingerprint.
      - exact_duplicate and near_duplicate share a separate duplicate pool.
        Within each family the original (PASS) is always emitted before its
        duplicate copies so the pipeline sees the original first, regardless
        of the global shuffle applied to non-duplicate buckets.

    Shuffle strategy:
      - Non-duplicate records are globally shuffled for realism.
      - Duplicate families are inserted into random positions in the final
        list but their internal order (original → copies) is preserved.
    """
    random.seed(seed)

    if abs(
        clean_ratio
        + dirty_ratio
        + exact_duplicate_ratio
        + near_duplicate_ratio
        + broken_ratio
        + edge_ratio
        - 1.0
    ) > 1e-9:
        raise ValueError("Ratios must sum to 1.0")

    counts = {
        VariantType.CLEAN:           int(total_records * clean_ratio),
        VariantType.DIRTY:           int(total_records * dirty_ratio),
        VariantType.EXACT_DUPLICATE: int(total_records * exact_duplicate_ratio),
        VariantType.NEAR_DUPLICATE:  int(total_records * near_duplicate_ratio),
        VariantType.BROKEN:          int(total_records * broken_ratio),
    }
    counts[VariantType.EDGE_CASE] = total_records - sum(counts.values())

    # Total identities needed across all buckets — each gets its own slice.
    n_clean      = counts[VariantType.CLEAN]
    n_dirty      = counts[VariantType.DIRTY]
    n_broken     = counts[VariantType.BROKEN]
    n_edge       = counts[VariantType.EDGE_CASE]

    if counts[VariantType.EXACT_DUPLICATE] % 2 != 0:
        raise ValueError("EXACT_DUPLICATE count must be even for origin+duplicate families")
    if counts[VariantType.NEAR_DUPLICATE] % 2 != 0:
        raise ValueError("NEAR_DUPLICATE count must be even for origin+near-duplicate families")

    n_dup_orig   = counts[VariantType.EXACT_DUPLICATE] // 2
    n_near_orig  = counts[VariantType.NEAR_DUPLICATE] // 2

    total_identities = n_clean + n_dirty + n_broken + n_edge + n_dup_orig + n_near_orig
    all_identities = [make_identity(i) for i in range(total_identities)]

    # Slice dedicated pools — no overlap between buckets
    offset = 0
    clean_pool      = all_identities[offset: offset + n_clean];      offset += n_clean
    dirty_pool      = all_identities[offset: offset + n_dirty];      offset += n_dirty
    broken_pool     = all_identities[offset: offset + n_broken];     offset += n_broken
    edge_pool       = all_identities[offset: offset + n_edge];       offset += n_edge
    dup_pool        = all_identities[offset: offset + n_dup_orig];   offset += n_dup_orig
    near_pool       = all_identities[offset: offset + n_near_orig];  offset += n_near_orig

    idx = 0

    # ── Non-duplicate buckets (shuffle freely) ────────────────────────────────
    non_dup_records: list[dict] = []

    for identity in clean_pool:
        idx += 1
        non_dup_records.append(event_to_dict(to_clean_event(identity, idx)))

    for identity in dirty_pool:
        idx += 1
        non_dup_records.append(event_to_dict(to_dirty_event(identity, idx)))

    for identity in broken_pool:
        idx += 1
        non_dup_records.append(event_to_dict(to_broken_event(identity, idx)))

    for identity in edge_pool:
        idx += 1
        non_dup_records.append(event_to_dict(to_edge_case_event(identity, idx)))

    random.shuffle(non_dup_records)

    # ── Duplicate families (original always before copies) ────────────────────
    # Each family is a list [original, duplicate].
    # Families are inserted at random positions in the final list so the
    # dataset looks realistic, but within each family order is preserved.
    dup_families: list[list[dict]] = []

    for identity in dup_pool:
        # Original — emitted first, pipeline stores fingerprint
        idx += 1
        original = event_to_dict(to_clean_event(identity, idx))
        original["_truth"]["is_original"] = True
        original["_truth"]["family_id"] = identity.identity_id

        # Duplicate copy — arrives after original
        idx += 1
        duplicate = event_to_dict(to_exact_duplicate_event(identity, idx))
        duplicate["_truth"]["is_original"] = False
        duplicate["_truth"]["family_id"] = identity.identity_id

        dup_families.append([original, duplicate])

    for identity in near_pool:
        idx += 1
        original = event_to_dict(to_clean_event(identity, idx))
        original["_truth"]["is_original"] = True
        original["_truth"]["family_id"] = identity.identity_id

        idx += 1
        near_dup = event_to_dict(to_near_duplicate_event(identity, idx))
        near_dup["_truth"]["is_original"] = False
        near_dup["_truth"]["family_id"] = identity.identity_id

        dup_families.append([original, near_dup])

    # Interleave duplicate families into non-duplicate records at random positions,
    # keeping each family's internal order intact.
    final_records: list[dict] = list(non_dup_records)
    for family in dup_families:
        # Pick a random insertion point for the family head; tail follows immediately
        pos = random.randint(0, len(final_records))
        for offset_in_family, record in enumerate(family):
            final_records.insert(pos + offset_in_family, record)

    assert len(final_records) == total_records, (
        f"Record count mismatch: expected {total_records}, got {len(final_records)}"
    )

    manifest = DatasetManifest(
        seed=seed,
        total_records=total_records,
        duplicate_rate=exact_duplicate_ratio + near_duplicate_ratio,
        invalid_rate=broken_ratio,
    )
    return final_records, manifest


def write_jsonl(records: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_manifest(manifest: DatasetManifest, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "seed": manifest.seed,
                "total_records": manifest.total_records,
                "duplicate_rate": manifest.duplicate_rate,
                "invalid_rate": manifest.invalid_rate,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[2]
    output_dir = project_root / "synthetic_data" / "output"

    records, manifest = build_dataset(total_records=100_000, seed=42)

    write_jsonl(records, output_dir / "messy_leads_100k.jsonl")
    write_manifest(manifest, output_dir / "messy_leads_100k_manifest.json")

    print(f"Generated {manifest.total_records} records")
    print(f"Output: {output_dir / 'messy_leads_100k.jsonl'}")