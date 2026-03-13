"""
Scale report generator for Lead Entry Guard.

Reads JSON result files produced by:
    python load_tests/scale_scenarios.py --scenario all --output
    python load_tests/scale_scenarios.py --scenario sweep_concurrency --output
    python load_tests/scale_scenarios.py --scenario sweep_bloom --output

Produces:
    load_tests/datasets/scale_report.md

Usage:
    python load_tests/generate_report.py
    python load_tests/generate_report.py --output docs/scale_report.md
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

DATASETS = Path(__file__).parent / "datasets"
BAR_WIDTH = 40


# ── ASCII chart primitives ────────────────────────────────────────────────────

def bar(value: float, max_value: float, width: int = BAR_WIDTH) -> str:
    filled = round(value / max_value * width) if max_value else 0
    return "█" * filled + "░" * (width - filled)


def hbar_chart(
    rows: list[tuple[str, float]],
    unit: str = "",
    width: int = BAR_WIDTH,
    fmt: str = ".1f",
) -> str:
    if not rows:
        return "_no data_"
    max_val = max(v for _, v in rows)
    label_w = max(len(l) for l, _ in rows)
    lines = []
    for label, value in rows:
        b = bar(value, max_val, width)
        lines.append(f"  {label:<{label_w}}  {b}  {value:{fmt}}{unit}")
    return "\n".join(lines)


def sparkline(values: list[float], width: int = 40) -> str:
    if not values:
        return ""
    blocks = "▁▂▃▄▅▆▇█"
    mn, mx = min(values), max(values)
    rng = mx - mn or 1
    return "".join(blocks[min(7, int((v - mn) / rng * 8))] for v in values)


def table(headers: list[str], rows: list[list[str]], align: list[str] | None = None) -> str:
    widths = [max(len(h), max((len(str(r[i])) for r in rows), default=0))
              for i, h in enumerate(headers)]
    align = align or ["<"] * len(headers)

    def fmt_row(cells):
        return "| " + " | ".join(
            f"{str(c):{a}{w}}" for c, a, w in zip(cells, align, widths)
        ) + " |"

    sep = "|-" + "-|-".join("-" * w for w in widths) + "-|"
    return "\n".join([fmt_row(headers), sep] + [fmt_row(r) for r in rows])


# ── Section builders ──────────────────────────────────────────────────────────

def section_retry_storm(data: dict) -> str:
    lines = ["## Idempotency retry storm\n"]
    lines.append(
        "Models an API gateway retry storm: upstream timeout causes the gateway "
        "to fire the same request N times simultaneously. "
        "Measures how many requests go through full pipeline vs return a cached "
        "idempotency replay.\n"
    )

    total = data["total_requests"]
    full = data["full_pipeline_count"]
    replay = data["replay_count"]
    lms = data["latency_ms"]
    inv = data.get("invariant_all_decisions_identical", False)

    lines.append(f"**Concurrency:** {data['concurrency']:,} simultaneous requests  ")
    lines.append(f"**Duration:** {data['duration_s']:.2f}s  ")
    lines.append(f"**Throughput:** {total / data['duration_s']:.0f} req/s\n")

    lines.append("### Pipeline path breakdown\n")
    rows = [
        ("Full pipeline", full, f"{full/total*100:.1f}%"),
        ("Idempotency replay", replay, f"{replay/total*100:.1f}%"),
    ]
    lines.append(hbar_chart([(r[0], r[1]) for r in rows], unit=" requests"))
    lines.append("")
    lines.append(table(
        ["Path", "Count", "Share"],
        [[r[0], f"{r[1]:,}", r[2]] for r in rows],
        align=["<", ">", ">"],
    ))

    lines.append("\n### Latency distribution\n")
    lines.append(hbar_chart([
        ("p50", lms["p50"]),
        ("p95", lms["p95"]),
        ("p99", lms["p99"]),
        ("max", lms["max"]),
    ], unit=" ms"))

    lines.append(f"\n**Invariant — all decisions identical:** {'✓ PASS' if inv else '✗ FAIL'}\n")
    lines.append(
        "> `full_pipeline_count > 1` indicates the size of the race window between "
        "`process()` returning and fire-and-forget snapshot being stored. "
        "This is a known trade-off — the window is measurable and bounded.\n"
    )
    return "\n".join(lines)


def section_concurrency_sweep(data: list[dict]) -> str:
    lines = ["## Concurrency sweep — race window\n"]
    lines.append(
        "Shows how the idempotency race window scales with concurrency. "
        "Each level fires that many simultaneous retries after a warm snapshot is stored. "
        "`full_pipeline > 1` means some requests hit the window before the snapshot was saved.\n"
    )

    lines.append(table(
        ["Concurrency", "Full pipeline", "Replays", "p95 (ms)", "p99 (ms)"],
        [[
            f"{r['concurrency']:,}",
            str(r["full_pipeline"]),
            f"{r['replays']:,}",
            f"{r['p95_ms']:.2f}",
            f"{r['p99_ms']:.2f}",
        ] for r in data],
        align=[">", ">", ">", ">", ">"],
    ))

    lines.append("\n### p99 latency vs concurrency\n")
    max_p99 = max(r["p99_ms"] for r in data)
    for r in data:
        b = bar(r["p99_ms"], max_p99)
        lines.append(f"  {r['concurrency']:>6,}  {b}  {r['p99_ms']:.2f} ms")

    lines.append(
        "\n> Use this table to decide the maximum safe retry concurrency "
        "for upstream API gateways. The inflection point where `full_pipeline` "
        "starts growing is the empirical race window boundary.\n"
    )
    return "\n".join(lines)


def section_redis_latency(data: dict) -> str:
    lines = ["## Redis latency spike\n"]
    lines.append(
        "Models slow Redis (not unavailable) — e.g. network saturation or GC pause. "
        f"Each Redis call had {data['injected_latency_ms']:.0f}ms injected latency. "
        "Key question: does the pipeline remain functional and bounded?\n"
    )

    lms = data["latency_ms"]
    invs = data.get("invariants", {})

    lines.append(f"**Leads:** {data['n_leads']:,}  ")
    lines.append(f"**Injected latency:** {data['injected_latency_ms']:.0f}ms  ")
    lines.append(f"**Queue fallback activations:** {data['queue_fallback_activations']}\n")

    lines.append("### Pipeline latency under slow Redis\n")
    lines.append(hbar_chart([
        ("p50", lms["p50"]),
        ("p95", lms["p95"]),
        ("p99", lms["p99"]),
        ("max", lms["max"]),
    ], unit=" ms"))

    lines.append("\n### Decision breakdown\n")
    total = sum(data["decisions"].values())
    lines.append(hbar_chart([
        (k, v) for k, v in sorted(data["decisions"].items(), key=lambda x: -x[1])
    ], unit=" leads"))

    lines.append("\n**Invariants:**")
    for k, v in invs.items():
        lines.append(f"- {k}: {'✓ PASS' if v else '✗ FAIL'}")

    lines.append(
        "\n> `queue_fallback_activations` shows how often the `queue_hold_timeout` fired. "
        "A high count here means Redis latency exceeded the timeout threshold — "
        "consider tuning `queue_hold_timeout_seconds` for your Redis SLA.\n"
    )
    return "\n".join(lines)


def section_bloom_pressure(data: dict) -> str:
    lines = ["## Bloom filter economics\n"]
    lines.append(
        "Models a fast-growing tenant who quickly exceeds initial Bloom sizing. "
        "Phase 1 streams unique leads until Bloom rotates; phase 2 mixes duplicates "
        "and new leads to measure detection accuracy and Redis lookup rate post-rotation.\n"
    )

    lines.append(f"**Total leads:** {data['total_leads']:,}  ")
    lines.append(f"**Bloom capacity:** {data['bloom_capacity']:,}  ")
    lines.append(f"**Rotations:** {data['bloom_rotation_count']}  ")
    lines.append(f"**Fill ratio at end:** {data['estimated_fill_ratio']:.1%}\n")

    lines.append("### Redis lookup rate (Bloom said MAYBE → checked Redis)\n")
    skip_rate = 1 - data["redis_lookup_rate"]
    lines.append(hbar_chart([
        ("Redis skipped (Bloom=NO)", skip_rate * data["total_leads"]),
        ("Redis checked (Bloom=MAYBE)", data["redis_lookup_rate"] * data["total_leads"]),
    ], unit=" leads"))

    lms = data["latency_ms"]
    lines.append("\n### Latency\n")
    lines.append(hbar_chart([
        ("p50", lms["p50"]),
        ("p95", lms["p95"]),
        ("p99", lms["p99"]),
    ], unit=" ms"))

    invs = data.get("invariants", {})
    lines.append("\n**Invariants:**")
    for k, v in invs.items():
        lines.append(f"- {k}: {'✓ PASS' if v else '✗ FAIL'}")

    lines.append(
        "\n> Redis skip rate is the key Bloom economics metric. "
        "A healthy system skips 80–95% of Redis lookups. "
        "As fill ratio approaches 100%, false positive rate rises and skip rate drops — "
        "use `bloom_capacity_override` per tenant tier to maintain this target.\n"
    )
    return "\n".join(lines)


def section_bloom_sweep(data: list[dict]) -> str:
    lines = ["## Bloom capacity sweep — sizing guide\n"]
    lines.append(
        "Shows how Bloom capacity affects rotation frequency, Redis lookup rate, "
        "and p95 latency. Use this table to set `bloom_capacity_override` per tenant tier.\n"
    )

    lines.append(table(
        ["Capacity", "Rotations", "Fill ratio", "Redis lookup rate", "p95 (ms)"],
        [[
            f"{r['bloom_capacity']:,}",
            str(r["rotations"]),
            f"{r['fill_ratio']:.1%}",
            f"{r['redis_lookup_rate']:.1%}",
            f"{r['p95_ms']:.2f}",
        ] for r in data],
        align=[">", ">", ">", ">", ">"],
    ))

    lines.append("\n### Redis lookup rate vs capacity\n")
    max_rate = max(r["redis_lookup_rate"] for r in data)
    for r in data:
        b = bar(r["redis_lookup_rate"], max_rate)
        lines.append(
            f"  {r['bloom_capacity']:>8,}  {b}  {r['redis_lookup_rate']:.1%}"
        )

    lines.append(
        "\n> Pick the capacity where Redis lookup rate drops below your target "
        "(typically 15–20%). Smaller capacity = more rotations + higher Redis load. "
        "Larger capacity = more memory per tenant.\n"
    )
    return "\n".join(lines)


def section_queue_drain(data: dict) -> str:
    lines = ["## Queue drain correctness\n"]
    lines.append(
        "Verifies that leads processed via QUEUE → recovery path are indistinguishable "
        "from leads processed on the main path. Tests two contracts from ADR-001 at scale:\n"
    )
    lines.append(
        "1. **Duplicate parity** — after queue drain, sending the same lead again "
        "must return `DUPLICATE_HINT` (fingerprint stored post-recovery)\n"
    )
    lines.append(
        "2. **Idempotency parity** — replaying the same `source_id` after recovery "
        "must return the original `request_id` (snapshot stored post-recovery)\n"
    )

    invs = data.get("invariants", {})
    lines.append(table(
        ["Invariant", "Result"],
        [[k, "✓ PASS" if v else "✗ FAIL"] for k, v in invs.items()],
        align=["<", "<"],
    ))

    lines.append("\n### Decisions during outage\n")
    total = data["total_queued"]
    lines.append(hbar_chart([
        (k, v) for k, v in sorted(data["decisions_during_outage"].items(), key=lambda x: -x[1])
    ], unit=" leads"))

    lines.append("\n### Decisions after recovery (duplicate re-send)\n")
    lines.append(hbar_chart([
        (k, v) for k, v in sorted(data["decisions_realtime"].items(), key=lambda x: -x[1])
    ], unit=" leads"))
    lines.append("")
    return "\n".join(lines)


def section_telemetry(data: dict) -> str:
    lines = ["## Telemetry backpressure\n"]
    lines.append(
        "Verifies the ingestion pipeline is unaffected when the telemetry queue "
        f"is saturated (max_size={data['queue_max_size']}). "
        "Telemetry is fire-and-forget — a full queue must drop events silently, "
        "never block or error the pipeline.\n"
    )

    invs = data.get("invariants", {})
    lines.append(table(
        ["Invariant", "Result"],
        [[k, "✓ PASS" if v else "✗ FAIL"] for k, v in invs.items()],
        align=["<", "<"],
    ))

    lines.append(f"\n**Leads processed:** {data['n_leads']:,}  ")
    lines.append(f"**Pipeline errors:** {data['pipeline_errors']}  ")
    lines.append(f"**Telemetry events dropped:** {data['telemetry_dropped']}\n")

    lines.append("### Decision breakdown\n")
    lines.append(hbar_chart([
        (k, v) for k, v in sorted(data["decisions"].items(), key=lambda x: -x[1])
    ], unit=" leads"))

    lines.append(
        "\n> Dropped telemetry is acceptable — it means the queue was full and "
        "events were shed. What is not acceptable is `pipeline_errors > 0`, "
        "which would indicate telemetry blocking the ingestion path.\n"
    )
    return "\n".join(lines)


# ── Report assembly ───────────────────────────────────────────────────────────

def load_json(name: str) -> dict | list | None:
    path = DATASETS / name
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def _build_summary(available: list[str], scenario_files: dict[str, str]) -> str:
    """
    Compute top-level summary across all available result files.
    Extracts: invariant status, max throughput, worst p99, Bloom skip rate.
    """
    lines = ["## Summary\n"]

    # Collect all invariants across scenarios
    all_invariants: dict[str, bool] = {}
    for name in available:
        d = load_json(scenario_files[name])
        if not d or isinstance(d, list):
            continue
        for k, v in (d.get("invariants") or {}).items():
            all_invariants[f"{name} / {k}"] = v

    all_passing = all(all_invariants.values()) if all_invariants else None

    # Max throughput (retry storm has it most naturally)
    max_throughput = None
    d = load_json(scenario_files.get("retry_storm", ""))
    if d and isinstance(d, dict) and d.get("duration_s"):
        max_throughput = round(d["total_requests"] / d["duration_s"])

    # Worst p99 across scenarios with latency data
    worst_p99 = 0.0
    worst_p99_scenario = ""
    for name in available:
        d = load_json(scenario_files[name])
        if not d or isinstance(d, list):
            continue
        p99 = (d.get("latency_ms") or {}).get("p99")
        if p99 and p99 > worst_p99:
            worst_p99 = p99
            worst_p99_scenario = name

    # Bloom skip rate
    bloom_skip_rate = None
    d = load_json(scenario_files.get("bloom_pressure", ""))
    if d and isinstance(d, dict):
        bloom_skip_rate = 1 - d.get("redis_lookup_rate", 0)

    # Status line
    if all_passing is True:
        status = "✓ HEALTHY"
    elif all_passing is False:
        failed = [k for k, v in all_invariants.items() if not v]
        status = f"✗ FAILURES ({len(failed)} invariant{'s' if len(failed) > 1 else ''})"
    else:
        status = "— no invariants measured"

    lines.append(f"**Scenarios run:** {len(available)} / {len(scenario_files)}  ")
    lines.append(f"**All invariants passing:** {'yes' if all_passing else 'NO' if all_passing is False else '—'}  ")
    lines.append(f"**System status:** {status}\n")

    metrics = []
    if max_throughput:
        metrics.append(("Max throughput observed", f"{max_throughput:,} req/s"))
    if worst_p99:
        metrics.append(("Worst p99 latency", f"{worst_p99:.2f} ms  ({worst_p99_scenario})"))
    if bloom_skip_rate is not None:
        metrics.append(("Redis skip rate (Bloom)", f"{bloom_skip_rate:.1%}"))

    if metrics:
        lines.append(table(
            ["Metric", "Value"],
            metrics,
            align=["<", "<"],
        ))
        lines.append("")

    if all_invariants:
        lines.append("\n**Invariant results:**\n")
        for inv_name, passed in all_invariants.items():
            lines.append(f"- {'✓' if passed else '✗'} `{inv_name}`")
        lines.append("")

    return "\n".join(lines)


def build_report() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    sections = [f"# Lead Entry Guard — Scale test report\n\n_Generated: {ts}_\n"]

    sections.append(
        "This report documents scale correctness testing for the Lead Entry Guard "
        "ingestion pipeline. These are not throughput benchmarks — they are "
        "**failure mode tests**: scenarios that only manifest at production scale "
        "and that most ingestion systems never test.\n"
    )

    # Summary table of available results
    available = []
    scenario_files = {
        "retry_storm": "scale_retry_storm_result.json",
        "redis_latency": "scale_redis_latency_result.json",
        "bloom_pressure": "scale_bloom_pressure_result.json",
        "queue_drain": "scale_queue_drain_result.json",
        "telemetry_backpressure": "scale_telemetry_backpressure_result.json",
        "concurrency_sweep": "scale_sweep_concurrency.json",
        "bloom_sweep": "scale_sweep_bloom.json",
    }
    for name, fname in scenario_files.items():
        d = load_json(fname)
        if d:
            available.append(name)

    if not available:
        sections.append(
            "> **No result files found.** Run scenarios first:\n"
            "> ```\n"
            "> python load_tests/scale_scenarios.py --scenario all --output\n"
            "> python load_tests/scale_scenarios.py --scenario sweep_concurrency --output\n"
            "> python load_tests/scale_scenarios.py --scenario sweep_bloom --output\n"
            "> ```\n"
        )
        return "\n".join(sections)

    # Summary first
    sections.append(_build_summary(available, scenario_files))

    sections.append("## Scenarios run\n")
    sections.append(table(
        ["Scenario", "Status", "File"],
        [[
            name,
            "✓ results available" if name in available else "— not run",
            scenario_files[name],
        ] for name in scenario_files],
        align=["<", "<", "<"],
    ))
    sections.append("")

    # Individual sections
    d = load_json("scale_retry_storm_result.json")
    if d:
        sections.append(section_retry_storm(d))

    d = load_json("scale_sweep_concurrency.json")
    if d:
        sections.append(section_concurrency_sweep(d))

    d = load_json("scale_redis_latency_result.json")
    if d:
        sections.append(section_redis_latency(d))

    d = load_json("scale_bloom_pressure_result.json")
    if d:
        sections.append(section_bloom_pressure(d))

    d = load_json("scale_sweep_bloom.json")
    if d:
        sections.append(section_bloom_sweep(d))

    d = load_json("scale_queue_drain_result.json")
    if d:
        sections.append(section_queue_drain(d))

    d = load_json("scale_telemetry_backpressure_result.json")
    if d:
        sections.append(section_telemetry(d))

    # Closing
    sections.append("---\n")
    sections.append(
        "_Lead Entry Guard test suite — scale correctness tests. "
        "For architecture decisions behind these tests see "
        "`docs/architecture/adr/` and `docs/testing/hero-test-recovery.md`._\n"
    )

    return "\n".join(sections)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate scale_report.md")
    parser.add_argument(
        "--output",
        type=Path,
        default=DATASETS / "scale_report.md",
        help="Output path (default: load_tests/datasets/scale_report.md)",
    )
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = build_report()
    args.output.write_text(report, encoding="utf-8")
    print(f"Report written → {args.output}")
    print(f"  {len(report.splitlines())} lines, {len(report):,} bytes")


if __name__ == "__main__":
    main()
