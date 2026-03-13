"""
Synthetic dataset generator for Lead Entry Guard load tests.

Generates realistic lead datasets with controlled duplicate ratios,
near-duplicates, messy data, and missing fields.

Usage:
    python load_tests/generate_dataset.py --scenario duplicate_storm --count 10000
    python load_tests/generate_dataset.py --scenario redis_outage --count 1000
    python load_tests/generate_dataset.py --scenario messy_data --count 5000
    python load_tests/generate_dataset.py --scenario all --count 10000
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import string
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterator

SEED = 42  # fixed seed for reproducible datasets
random.seed(SEED)

OUTPUT_DIR = Path(__file__).parent / "datasets"


# ── Realistic base data ────────────────────────────────────────────────────────

FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace", "Henry",
    "Iris", "Jack", "Karen", "Leo", "Maya", "Nick", "Olivia", "Paul",
    "Quinn", "Rosa", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Wilson", "Taylor", "Anderson", "Thomas", "Jackson", "White",
    "Harris", "Martin", "Thompson", "Robinson", "Clark", "Lewis",
]

DOMAINS = [
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "company.com", "acme.io", "example.org", "corp.net",
]

COMPANIES = [
    "Acme Corp", "Globex", "Initech", "Umbrella", "Hooli",
    "Pied Piper", "Dunder Mifflin", "Vandelay Industries",
    None, None, None,  # ~27% without company
]

SOURCE_TYPES = ["api", "webhook", "csv_import", "crm_sync"]

TENANTS = ["tenant_alpha", "tenant_beta", "tenant_gamma"]

# ── Lead model ────────────────────────────────────────────────────────────────

@dataclass
class SyntheticLead:
    tenant_id: str
    source_id: str
    source_type: str
    email: str | None = None
    phone: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    company: str | None = None
    scenario_tag: str = ""  # for analysis — not part of API payload

    def to_api_payload(self) -> dict:
        """Return only the fields the API accepts."""
        return {
            k: v for k, v in {
                "tenant_id": self.tenant_id,
                "source_id": self.source_id,
                "source_type": self.source_type,
                "email": self.email,
                "phone": self.phone,
                "first_name": self.first_name,
                "last_name": self.last_name,
                "company": self.company,
            }.items() if v is not None
        }


# ── Generators ────────────────────────────────────────────────────────────────

def _random_email(first: str, last: str) -> str:
    domain = random.choice(DOMAINS)
    sep = random.choice([".", "_", ""])
    return f"{first.lower()}{sep}{last.lower()}@{domain}"


def _random_phone() -> str:
    """Generate E164-ish phone numbers."""
    country = random.choice(["+1", "+44", "+49", "+33", "+420"])
    number = "".join(random.choices(string.digits, k=9))
    return f"{country}{number}"


def _make_base_lead(tenant_id: str | None = None) -> SyntheticLead:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return SyntheticLead(
        tenant_id=tenant_id or random.choice(TENANTS),
        source_id=str(uuid.uuid4()),
        source_type=random.choice(SOURCE_TYPES),
        email=_random_email(first, last),
        phone=_random_phone() if random.random() > 0.3 else None,
        first_name=first,
        last_name=last,
        company=random.choice(COMPANIES),
    )


# ── Scenario: Duplicate Storm ─────────────────────────────────────────────────

def generate_duplicate_storm(count: int, duplicate_ratio: float = 0.50) -> list[SyntheticLead]:
    """
    Simulate a burst of leads with ~50% duplicates.

    Pattern: CRM import or CSV re-upload where half the leads already exist.
    Tests: duplicate detection accuracy, Bloom filter hit rate, Redis lookup rate.

    duplicate_ratio: fraction of leads that are exact duplicates of an earlier lead.
    """
    originals: list[SyntheticLead] = []
    result: list[SyntheticLead] = []

    n_unique = int(count * (1 - duplicate_ratio))
    n_duplicate = count - n_unique

    for _ in range(n_unique):
        lead = _make_base_lead(tenant_id="tenant_alpha")
        lead.scenario_tag = "duplicate_storm:original"
        originals.append(lead)
        result.append(lead)

    for _ in range(n_duplicate):
        original = random.choice(originals)
        dupe = SyntheticLead(
            tenant_id=original.tenant_id,
            source_id=str(uuid.uuid4()),  # different source_id — not a retry
            source_type=original.source_type,
            email=original.email,          # same email — will be fingerprint match
            phone=original.phone,
            first_name=original.first_name,
            last_name=original.last_name,
            company=original.company,
            scenario_tag="duplicate_storm:duplicate",
        )
        result.append(dupe)

    random.shuffle(result)
    return result


# ── Scenario: Redis Outage ────────────────────────────────────────────────────

def generate_redis_outage(count: int) -> list[SyntheticLead]:
    """
    Leads designed to be sent during a simulated Redis outage window.

    Pattern: normal traffic mix, but sent while Redis is intentionally down.
    Tests: QUEUE mode behavior, recovery path store contract (ADR-001),
           degraded mode policy routing (ADR-002).

    All leads use tenant with degraded_mode_policy=QUEUE.
    Includes a subset that will be replayed after recovery (same source_id).
    """
    result: list[SyntheticLead] = []
    replay_candidates: list[SyntheticLead] = []

    n_normal = int(count * 0.80)
    n_replay = count - n_normal

    for _ in range(n_normal):
        lead = _make_base_lead(tenant_id="tenant_beta")
        lead.scenario_tag = "redis_outage:during_outage"
        result.append(lead)
        if random.random() < 0.20:  # 20% of outage leads will be replayed
            replay_candidates.append(lead)

    # Add replay leads — same source_id, same payload, should return original decision
    for original in random.sample(replay_candidates, min(n_replay, len(replay_candidates))):
        replay = SyntheticLead(
            tenant_id=original.tenant_id,
            source_id=original.source_id,  # same source_id → idempotency hit
            source_type=original.source_type,
            email=original.email,
            phone=original.phone,
            first_name=original.first_name,
            last_name=original.last_name,
            company=original.company,
            scenario_tag="redis_outage:replay_after_recovery",
        )
        result.append(replay)

    return result


# ── Scenario: Messy Data ──────────────────────────────────────────────────────

def generate_messy_data(count: int) -> list[SyntheticLead]:
    """
    Leads with realistic data quality issues.

    Pattern: CSV import from non-validated source (webform, data broker).
    Tests: validation rejection rates, normalization robustness,
           pipeline early-reject path (saves Redis round-trip).
    """

    def _messy_email(email: str) -> str:
        mutations = [
            lambda e: e.upper(),
            lambda e: e.replace("@", " @ "),
            lambda e: e + " ",
            lambda e: "not-an-email",
            lambda e: e.replace(".", ""),
            lambda e: "",
            lambda e: e,  # valid — keeps some clean ones
        ]
        return random.choice(mutations)(email)

    def _messy_phone(phone: str | None) -> str | None:
        if phone is None:
            return None
        mutations = [
            lambda p: p,               # valid
            lambda p: p.replace("+", "00"),
            lambda p: "123",           # too short
            lambda p: "not-a-phone",
            lambda p: None,            # missing
        ]
        return random.choice(mutations)(phone)

    result = []
    for _ in range(count):
        base = _make_base_lead(tenant_id="tenant_gamma")
        lead = SyntheticLead(
            tenant_id=base.tenant_id,
            source_id=base.source_id,
            source_type=base.source_type,
            email=_messy_email(base.email or ""),
            phone=_messy_phone(base.phone),
            first_name=base.first_name if random.random() > 0.15 else None,
            last_name=base.last_name if random.random() > 0.10 else None,
            company=base.company,
            scenario_tag="messy_data",
        )
        result.append(lead)

    return result


# ── Output writers ────────────────────────────────────────────────────────────

def write_csv(leads: list[SyntheticLead], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(SyntheticLead.__dataclass_fields__.keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(asdict(lead) for lead in leads)
    print(f"  Written {len(leads):,} leads → {path}")


def write_jsonl(leads: list[SyntheticLead], path: Path) -> None:
    """JSONL — one API payload per line, ready for load test runner."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for lead in leads:
            f.write(json.dumps(lead.to_api_payload()) + "\n")
    print(f"  Written {len(leads):,} payloads → {path}")


def write_summary(leads: list[SyntheticLead], path: Path) -> None:
    tags = {}
    for lead in leads:
        tags[lead.scenario_tag] = tags.get(lead.scenario_tag, 0) + 1

    summary = {
        "total": len(leads),
        "tenants": list({l.tenant_id for l in leads}),
        "tags": tags,
        "email_missing": sum(1 for l in leads if not l.email),
        "phone_missing": sum(1 for l in leads if not l.phone),
        "company_missing": sum(1 for l in leads if not l.company),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  Summary → {path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

SCENARIOS = {
    "duplicate_storm": lambda n: generate_duplicate_storm(n),
    "redis_outage": lambda n: generate_redis_outage(n),
    "messy_data": lambda n: generate_messy_data(n),
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic lead datasets")
    parser.add_argument(
        "--scenario",
        choices=[*SCENARIOS.keys(), "all"],
        default="all",
        help="Which scenario to generate",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10_000,
        help="Number of leads per scenario",
    )
    args = parser.parse_args()

    scenarios = SCENARIOS if args.scenario == "all" else {args.scenario: SCENARIOS[args.scenario]}

    for name, generator in scenarios.items():
        print(f"\n[{name}] Generating {args.count:,} leads...")
        leads = generator(args.count)
        base = OUTPUT_DIR / name
        write_csv(leads, base.with_suffix(".csv"))
        write_jsonl(leads, base.with_suffix(".jsonl"))
        write_summary(leads, base.with_name(f"{name}_summary.json"))

    print(f"\nDone. Datasets in {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
