"""Mutation functions that introduce realistic CRM data messiness."""
from __future__ import annotations

import random
import string


def random_whitespace(value: str) -> str:
    """Add leading/trailing whitespace."""
    prefix = " " * random.randint(0, 2)
    suffix = " " * random.randint(0, 2)
    return f"{prefix}{value}{suffix}"


def random_case(value: str) -> str:
    """Randomize casing."""
    return "".join(
        c.upper() if random.random() < 0.5 else c.lower()
        for c in value
    )


def corrupt_email(email: str) -> str | None:
    """Introduce common email corruption patterns.

    Every branch produces an invalid or missing email — no pass-through.
    This ensures BROKEN variants always map to REJECT in _truth.
    """
    r = random.random()

    if r < 0.25:
        return email.replace("@", "")       # missing @
    if r < 0.50:
        return email + "."                  # trailing dot
    if r < 0.75:
        return email.replace(".", "..")     # double dot
    return None                             # completely missing


def messy_phone(phone: str) -> str | None:
    """Create messy phone formatting."""
    r = random.random()

    if r < 0.2:
        return phone.replace("+", "")
    if r < 0.4:
        return f"{phone[:4]} {phone[4:7]} {phone[7:]}"
    if r < 0.6:
        return f"({phone[1:4]}) {phone[4:]}"
    if r < 0.8:
        return None
    return phone


def random_company_suffix(company: str) -> str:
    """Random company suffix variation."""
    suffixes = ["s.r.o.", "sro", "a.s.", "Ltd", "LLC", ""]
    suffix = random.choice(suffixes)

    if suffix:
        return f"{company} {suffix}"
    return company


def near_duplicate_string(value: str) -> str:
    """Create small mutation for near-duplicate."""
    if len(value) < 2:
        return value

    i = random.randint(0, len(value) - 1)
    return value[:i] + random.choice(string.ascii_letters) + value[i + 1 :]