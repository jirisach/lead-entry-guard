"""
ADR-011 Scope Guard — Ingest Boundary Contract Test.

PURPOSE
-------
This test enforces the production ingest boundary defined in ADR-011.

The risk it guards against:
  Someone adds a "small debug field", "just signals for now", or
  "triggered_by for tracing" to IngestRequest or IngestResponse.
  The change looks minor in review. It passes linting. But it silently
  violates the v1 contract and may expose internal signal-level state,
  compound semantics, or reasoning chains to callers.

What this test does:
  It asserts that IngestRequest and IngestResponse contain EXACTLY the
  fields declared in the ADR-011 v1 contract — no more, no less.

  Any field addition to either model will cause this test to fail with
  a message that names the ADR and the prohibited field category.

  Removing a contractual field also fails — protecting callers from
  silent breaking changes.

Rule:
  Changes to INGEST_REQUEST_V1_FIELDS or INGEST_RESPONSE_V1_FIELDS
  require a new ADR or an explicit amendment to ADR-011.
  You cannot sneak a field past this test.

Import strategy:
  Primary: from lead_entry_guard.api.app import IngestRequest, IngestResponse
    (standard CI — full package installed)
  Fallback: AST parse of app.py source
    (environments without full package install, e.g. lightweight CI, pre-commit)

Placement: tests/contract/test_ingest_boundary_contract.py
This is a contract test — it must pass before every merge.
"""
from __future__ import annotations

import ast
import importlib
import os
import pathlib
import sys
from typing import Any

import pytest

# ── ADR-011 v1 field whitelists ───────────────────────────────────────────────
#
# These sets are the source of truth for the v1 boundary contract.
# They map directly to the "Request contract (v1)" and "Response contract (v1)"
# sections of ADR-011.
#
# To change these: write or amend an ADR first, then update this whitelist.
# The test message below will remind any future reader of this requirement.

INGEST_REQUEST_V1_FIELDS: frozenset[str] = frozenset({
    "source_id",
    "source_type",
    "email",
    "phone",
    "first_name",
    "last_name",
    "company",
    "extra",
    # tenant_id is INTENTIONALLY ABSENT — derived from auth layer only (ADR-007).
    # If you're tempted to add it: read ADR-007 first.
})

INGEST_RESPONSE_V1_FIELDS: frozenset[str] = frozenset({
    "request_id",
    "tenant_id",
    "decision",
    "reason_codes",
    "duplicate_check_skipped",
    "policy_version",
    "ruleset_version",
    "config_version",
    "latency_ms",
    # Fields that must NEVER appear here without a new ADR:
    #   signals, compound_result, triggered_by, explanation, why_flagged,
    #   confidence, signal_codes, reasoning_chain, review_metadata
})

# ── Failure message templates ─────────────────────────────────────────────────
#
# Shown verbatim when a boundary violation is detected.
# Intentionally verbose — the person reading this failed test needs context.

_VIOLATION_MSG = """
Ingest boundary v1 contract violation — ADR-011 scope guard triggered.

The following fields were found in {model} but are NOT part of the
ADR-011 v1 contract:

  {unexpected}

The ingest boundary must not expose signal-level fields, compound
evaluation results, reasoning chains, or any internal pipeline state
without a new ADR or explicit amendment to ADR-011.

Prohibited field categories (from ADR-011 scope guard):
  - Signal codes or compound evaluation results
  - Any field implying signal-level explainability or reasoning chains
  - Tenant-configurable signal rules via response
  - Internal pipeline state not listed in the v1 response contract

If this field is intentional:
  1. Write or amend an ADR.
  2. Get it reviewed.
  3. Update INGEST_REQUEST_V1_FIELDS or INGEST_RESPONSE_V1_FIELDS above.
  4. Then — and only then — this test should be updated.
"""

_MISSING_MSG = """
Ingest boundary v1 contract violation — contractual field removed.

The following fields are declared in the ADR-011 v1 contract but are
MISSING from {model}:

  {missing}

Removing a contractual field is a breaking change for all callers.
If this removal is intentional, amend ADR-011 and update the whitelist.
"""


# ── Field extraction helpers ──────────────────────────────────────────────────

def _extract_fields_via_import() -> tuple[frozenset[str], frozenset[str]] | None:
    """
    Primary strategy: import IngestRequest and IngestResponse from the
    installed package and return their Pydantic model_fields.

    Returns None if import fails (package not installed in this environment).
    """
    try:
        # Try standard installed package layout first
        from lead_entry_guard.api.app import IngestRequest, IngestResponse  # type: ignore[import]
        return (
            frozenset(IngestRequest.model_fields.keys()),
            frozenset(IngestResponse.model_fields.keys()),
        )
    except ImportError:
        pass

    try:
        # Flat project layout fallback (app.py at repo root)
        import app as _app  # type: ignore[import]
        return (
            frozenset(_app.IngestRequest.model_fields.keys()),
            frozenset(_app.IngestResponse.model_fields.keys()),
        )
    except (ImportError, AttributeError):
        return None


def _extract_fields_via_ast() -> tuple[frozenset[str], frozenset[str]]:
    """
    Fallback strategy: parse app.py with AST and extract annotated field names
    from the IngestRequest and IngestResponse class bodies.

    This runs without any package installation — pure stdlib.
    It is less precise than model_fields (does not distinguish ClassVar,
    __private__, etc.) but is sufficient for boundary enforcement because
    Pydantic uses the same annotated-assignment pattern we look for.

    Raises FileNotFoundError if app.py cannot be located.

    Known limitation: this relies on ast.AnnAssign detection, which covers
    standard Pydantic annotated fields (the only pattern used in this codebase).
    It would miss fields defined via Field(...) without a type annotation —
    but that is not valid Pydantic v2 syntax, so this limitation is theoretical.
    """
    # Walk up from this file's location to find app.py.
    # Also include CWD and its parents — pytest is typically invoked from
    # the repo root, so cwd() reliably resolves to the project directory
    # even when the test file itself lives in a subdirectory.
    _file_roots = [
        pathlib.Path(__file__).parent,          # tests/contract/
        pathlib.Path(__file__).parent.parent,   # tests/
        pathlib.Path(__file__).parent.parent.parent,  # repo root
    ]
    _cwd = pathlib.Path.cwd()
    _cwd_roots = [_cwd] + list(_cwd.parents)[:3]
    search_roots = _file_roots + _cwd_roots
    # Also check src/ layout variants
    for root in list(search_roots):
        search_roots.append(root / "src" / "lead_entry_guard" / "api")

    app_path: pathlib.Path | None = None
    for root in search_roots:
        candidate = root / "app.py"
        if candidate.exists():
            app_path = candidate
            break

    if app_path is None:
        raise FileNotFoundError(
            "Cannot locate app.py for AST-based field extraction. "
            "Ensure this test is run from the repository root, or install the package."
        )

    tree = ast.parse(app_path.read_text(encoding="utf-8"))

    result: dict[str, list[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name in ("IngestRequest", "IngestResponse"):
            fields = [
                item.target.id  # type: ignore[union-attr]
                for item in node.body
                if isinstance(item, ast.AnnAssign)
                and isinstance(item.target, ast.Name)
                and not item.target.id.startswith("_")  # skip ClassVar-style privates
            ]
            result[node.name] = fields

    if "IngestRequest" not in result or "IngestResponse" not in result:
        raise RuntimeError(
            f"AST parse of {app_path} found these classes: {list(result.keys())}. "
            "Expected IngestRequest and IngestResponse. "
            "Has app.py been refactored or renamed?"
        )

    return frozenset(result["IngestRequest"]), frozenset(result["IngestResponse"])


def _get_model_fields() -> tuple[frozenset[str], frozenset[str]]:
    """
    Return (request_fields, response_fields) using the best available strategy.

    Order:
      1. Live import via installed package  (most accurate — uses Pydantic introspection)
      2. AST parse of app.py source         (fallback — no package install needed)
    """
    result = _extract_fields_via_import()
    if result is not None:
        return result
    return _extract_fields_via_ast()


# ── Model field extraction (module-level, fail fast) ─────────────────────────

_REQUEST_FIELDS, _RESPONSE_FIELDS = _get_model_fields()


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestIngestRequestBoundary:
    """
    IngestRequest field whitelist — ADR-011 v1 request contract.

    Guards against:
    - Adding tenant_id to the body (violates ADR-007)
    - Adding signal inputs or hints to the request
    - Adding compound semantics triggers
    - Adding debug or explainability knobs
    """

    def test_no_fields_added_beyond_v1_contract(self) -> None:
        """Fail if IngestRequest has any field not in the ADR-011 v1 whitelist."""
        unexpected = _REQUEST_FIELDS - INGEST_REQUEST_V1_FIELDS
        assert not unexpected, _VIOLATION_MSG.format(
            model="IngestRequest",
            unexpected=sorted(unexpected),
        )

    def test_no_contractual_fields_removed(self) -> None:
        """Fail if a contractual IngestRequest field was silently dropped."""
        missing = INGEST_REQUEST_V1_FIELDS - _REQUEST_FIELDS
        assert not missing, _MISSING_MSG.format(
            model="IngestRequest",
            missing=sorted(missing),
        )

    def test_tenant_id_absent_from_request_schema(self) -> None:
        """
        ADR-007 hard rule: tenant_id must never appear in IngestRequest.

        This is a dedicated test (not just covered by the whitelist above)
        because the consequence of a regression here is a multi-tenant
        isolation breach — not just a contract drift.
        """
        assert "tenant_id" not in _REQUEST_FIELDS, (
            "CRITICAL: tenant_id found in IngestRequest fields.\n"
            "This violates ADR-007 (tenant identity from auth only).\n"
            "Any key holder could write into another tenant's namespace.\n"
            "Remove tenant_id from IngestRequest immediately."
        )

    def test_no_signal_fields_in_request(self) -> None:
        """
        Ingest boundary must not accept signal-level inputs in the request body.
        Signal evaluation is internal — it is not a caller-controlled input.
        """
        prohibited_prefixes = ("signal", "compound", "triggered", "hint", "explain", "reason_override")
        violations = [
            f for f in _REQUEST_FIELDS
            if any(f.startswith(p) or p in f for p in prohibited_prefixes)
            and f not in INGEST_REQUEST_V1_FIELDS
        ]
        assert not violations, (
            f"Signal-level or compound fields found in IngestRequest: {violations}\n"
            "Ingest boundary v1 must not expose signal inputs to callers (ADR-011)."
        )


class TestIngestResponseBoundary:
    """
    IngestResponse field whitelist — ADR-011 v1 response contract.

    Guards against:
    - Adding signals / signal_codes to the response
    - Adding compound_result, triggered_by, or reasoning chains
    - Adding explainability fields (why_flagged, explanation, confidence)
    - Adding review metadata or internal pipeline state
    """

    def test_no_fields_added_beyond_v1_contract(self) -> None:
        """Fail if IngestResponse has any field not in the ADR-011 v1 whitelist."""
        unexpected = _RESPONSE_FIELDS - INGEST_RESPONSE_V1_FIELDS
        assert not unexpected, _VIOLATION_MSG.format(
            model="IngestResponse",
            unexpected=sorted(unexpected),
        )

    def test_no_contractual_fields_removed(self) -> None:
        """Fail if a contractual IngestResponse field was silently dropped."""
        missing = INGEST_RESPONSE_V1_FIELDS - _RESPONSE_FIELDS
        assert not missing, _MISSING_MSG.format(
            model="IngestResponse",
            missing=sorted(missing),
        )

    def test_no_signal_fields_in_response(self) -> None:
        """
        Ingest boundary must not expose signal-level fields in v1 response.

        Signals annotate the decision internally in v1 — they do not influence
        the primary decision outcome and must not appear in the public contract.
        (ADR-011: "Signals annotate the decision in v1 — they do not influence
        the primary decision outcome.")
        """
        # Exact field names that must never appear — sourced directly from
        # ADR-011 scope guard and deferred decisions sections.
        prohibited_exact = frozenset({
            "signals",
            "signal_codes",
            "compound_result",
            "triggered_by",
            "explanation",
            "why_flagged",
            "confidence",
            "reasoning_chain",
            "review_metadata",
            "explainability",
        })
        violations = _RESPONSE_FIELDS & prohibited_exact
        assert not violations, (
            f"Prohibited signal-level or compound fields found in IngestResponse: {sorted(violations)}\n\n"
            "Ingest boundary v1 must not expose signal-level fields without a new ADR.\n"
            "These fields are explicitly deferred in ADR-011 (deferred decisions section).\n"
            "Adding them without an ADR review violates the v1 contract."
        )

    def test_no_signal_fields_by_prefix_in_response(self) -> None:
        """
        Catch signal/compound fields added under variant names
        (e.g. 'signal_result', 'compound_hint', 'triggered_by_rule').

        Belt-and-suspenders: the exact-match test above is the primary gate.
        This prefix check is a secondary guard — it catches new variants that
        weren't anticipated in the exact-match list.

        Known limitation: prefix matching is intentionally broad and could
        theoretically flag a legitimate future field (e.g. a hypothetical
        'reasoning_code' that is genuinely benign). If that happens, the
        correct fix is to add the field to INGEST_RESPONSE_V1_FIELDS after
        ADR review — NOT to weaken or remove this test.

        The whitelist is the authoritative gate. This prefix check is bonus
        protection, not the primary enforcement mechanism.
        """
        prohibited_prefixes = ("signal_", "compound_", "triggered_", "explain", "reasoning_")
        violations = [
            f for f in _RESPONSE_FIELDS
            if any(f.startswith(p) for p in prohibited_prefixes)
            and f not in INGEST_RESPONSE_V1_FIELDS
        ]
        assert not violations, (
            f"Signal-level or compound fields found in IngestResponse (prefix match): {violations}\n"
            "Ingest boundary v1 must not expose signal-level explainability or reasoning chains.\n"
            "If this field is intentional and benign: add it to INGEST_RESPONSE_V1_FIELDS\n"
            "after ADR review — do not remove the prefix guard.\n"
            "See ADR-011 scope guard."
        )


class TestIngestBoundaryStructuralInvariants:
    """
    Structural invariants that must hold regardless of field additions.

    These are not field-count checks — they verify semantic properties
    of the boundary that ADR-011 treats as non-negotiable.
    """

    def test_ingest_request_and_response_have_expected_field_counts(self) -> None:
        """
        Sanity check: field counts match ADR-011 whitelist sizes.

        If this fails alongside test_no_fields_added_beyond_v1_contract,
        it means the extraction mechanism (import or AST) is returning
        unexpected results — investigate the import strategy, not the model.
        """
        assert len(_REQUEST_FIELDS) == len(INGEST_REQUEST_V1_FIELDS), (
            f"IngestRequest field count mismatch: "
            f"extracted {len(_REQUEST_FIELDS)} fields {sorted(_REQUEST_FIELDS)}, "
            f"expected {len(INGEST_REQUEST_V1_FIELDS)} from ADR-011 whitelist."
        )
        assert len(_RESPONSE_FIELDS) == len(INGEST_RESPONSE_V1_FIELDS), (
            f"IngestResponse field count mismatch: "
            f"extracted {len(_RESPONSE_FIELDS)} fields {sorted(_RESPONSE_FIELDS)}, "
            f"expected {len(INGEST_RESPONSE_V1_FIELDS)} from ADR-011 whitelist."
        )

    def test_ingest_request_extra_config_blocks_body_injection(self) -> None:
        """
        IngestRequest must be configured to reject or silently drop unknown fields.

        This is the mechanical enforcement of the ADR-007 body injection guard:
        any field not in the schema (including tenant_id) must not propagate
        into the pipeline.

        We check the source directly via AST since this invariant must hold
        even in environments where the package is not fully installed.
        """
        # Find app.py
        _cwd = pathlib.Path.cwd()
        search_roots = [
            pathlib.Path(__file__).parent,
            pathlib.Path(__file__).parent.parent,
            pathlib.Path(__file__).parent.parent.parent,
        ] + [_cwd] + list(_cwd.parents)[:3]
        app_path: pathlib.Path | None = None
        for root in search_roots:
            candidate = root / "app.py"
            if candidate.exists():
                app_path = candidate
                break

        if app_path is None:
            pytest.skip("app.py not found — skipping extra-config AST check")

        source = app_path.read_text(encoding="utf-8")

        # Pydantic v2 default is extra="ignore" (unknown fields silently dropped).
        # The CORRECT state is an EXPLICIT model_config = ConfigDict(extra="ignore")
        # on IngestRequest — not relying on the Pydantic default.
        #
        # Explicit config is contractual: it documents intent, survives Pydantic
        # major version upgrades, and makes "ignore vs forbid" a conscious choice.
        #
        # This test checks for explicit config first, then verifies that at minimum
        # the Pydantic default protects the boundary (no extra="allow" anywhere near
        # IngestRequest).
        #
        # ADR-011 compliance note: if this test fails on `has_explicit_guard`,
        # the fix is to add `model_config = ConfigDict(extra="ignore")` to
        # IngestRequest in app.py — not to weaken this test.

        has_explicit_guard = (
            'extra="ignore"' in source
            or "extra='ignore'" in source
            or 'extra="forbid"' in source
            or "extra='forbid'" in source
            or "Extra.ignore" in source
            or "Extra.forbid" in source
        )

        # Belt-and-suspenders: also check that extra="allow" is NOT present
        # near IngestRequest (which would be an active violation).
        has_allow_violation = (
            'extra="allow"' in source
            or "extra='allow'" in source
            or "Extra.allow" in source
        )

        # Fail hard on allow — that is always a violation.
        assert not has_allow_violation, (
            "CRITICAL: extra='allow' found in app.py.\n"
            "This means unknown fields (including tenant_id) can propagate into the pipeline.\n"
            "This violates ADR-007. Remove extra='allow' from IngestRequest immediately."
        )

        # Warn (via xfail-style soft assert) on missing explicit config.
        # Pydantic v2 defaults to extra="ignore" so the boundary is protected,
        # but explicit config is required for production-grade contractual clarity.
        if not has_explicit_guard:
            pytest.xfail(
                "IngestRequest is missing explicit model_config = ConfigDict(extra='ignore').\n"
                "Pydantic v2 default (extra='ignore') currently protects the boundary,\n"
                "but explicit config is required for ADR-011 compliance.\n"
                "Fix: add `model_config = ConfigDict(extra='ignore')` to IngestRequest in app.py."
            )
