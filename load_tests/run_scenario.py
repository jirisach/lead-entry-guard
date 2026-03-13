"""
Load test scenario runner for Lead Entry Guard.

Sends pre-generated datasets against a running instance and reports
throughput, latency distribution, and decision breakdown.

Usage:
    # Generate datasets first
    python load_tests/generate_dataset.py --scenario all --count 10000

    # Run a scenario
    python load_tests/run_scenario.py --scenario duplicate_storm --concurrency 50
    python load_tests/run_scenario.py --scenario redis_outage --concurrency 20
    python load_tests/run_scenario.py --scenario messy_data --concurrency 100

Requirements:
    pip install aiohttp

The runner expects a Lead Entry Guard instance at LEG_BASE_URL (default: http://localhost:8000).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean, median, quantiles

import aiohttp

BASE_URL_DEFAULT = "http://localhost:8000"
DATASET_DIR = Path(__file__).parent / "datasets"
INGEST_ENDPOINT = "/v1/leads/ingest"


# ── Result model ──────────────────────────────────────────────────────────────

@dataclass
class RequestResult:
    status_code: int
    decision: str | None
    latency_ms: float
    error: str | None = None
    scenario_tag: str = ""


@dataclass
class ScenarioReport:
    scenario: str
    total: int
    duration_s: float
    concurrency: int
    results: list[RequestResult] = field(default_factory=list)

    @property
    def throughput_rps(self) -> float:
        return self.total / self.duration_s if self.duration_s > 0 else 0

    @property
    def success_rate(self) -> float:
        ok = sum(1 for r in self.results if r.status_code == 200)
        return ok / self.total if self.total > 0 else 0

    @property
    def decision_breakdown(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for r in self.results:
            counts[r.decision or f"http_{r.status_code}"] += 1
        return dict(counts)

    @property
    def latency_percentiles(self) -> dict[str, float]:
        latencies = [r.latency_ms for r in self.results if r.status_code == 200]
        if not latencies:
            return {}
        qs = quantiles(latencies, n=100)
        return {
            "p50": round(median(latencies), 2),
            "p90": round(qs[89], 2),
            "p95": round(qs[94], 2),
            "p99": round(qs[98], 2),
            "mean": round(mean(latencies), 2),
            "max": round(max(latencies), 2),
        }

    def print_summary(self) -> None:
        print(f"\n{'='*60}")
        print(f"Scenario:    {self.scenario}")
        print(f"Total leads: {self.total:,}")
        print(f"Duration:    {self.duration_s:.1f}s")
        print(f"Concurrency: {self.concurrency}")
        print(f"Throughput:  {self.throughput_rps:.1f} req/s")
        print(f"Success:     {self.success_rate*100:.1f}%")
        print()
        print("Decision breakdown:")
        for decision, count in sorted(self.decision_breakdown.items(), key=lambda x: -x[1]):
            pct = count / self.total * 100
            print(f"  {decision:<25} {count:>6,}  ({pct:.1f}%)")
        print()
        print("Latency (successful requests):")
        for pct, val in self.latency_percentiles.items():
            print(f"  {pct:<6} {val:>8.2f} ms")
        print(f"{'='*60}\n")


# ── Worker ────────────────────────────────────────────────────────────────────

async def send_lead(
    session: aiohttp.ClientSession,
    payload: dict,
    base_url: str,
    scenario_tag: str = "",
) -> RequestResult:
    url = f"{base_url}{INGEST_ENDPOINT}"
    start = time.monotonic()
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            latency_ms = (time.monotonic() - start) * 1000
            if resp.status == 200:
                body = await resp.json()
                return RequestResult(
                    status_code=200,
                    decision=body.get("decision"),
                    latency_ms=latency_ms,
                    scenario_tag=scenario_tag,
                )
            else:
                return RequestResult(
                    status_code=resp.status,
                    decision=None,
                    latency_ms=latency_ms,
                    error=f"HTTP {resp.status}",
                    scenario_tag=scenario_tag,
                )
    except asyncio.TimeoutError:
        latency_ms = (time.monotonic() - start) * 1000
        return RequestResult(status_code=0, decision=None, latency_ms=latency_ms, error="timeout")
    except Exception as exc:
        latency_ms = (time.monotonic() - start) * 1000
        return RequestResult(status_code=0, decision=None, latency_ms=latency_ms, error=str(exc))


async def run_scenario(
    scenario: str,
    base_url: str,
    concurrency: int,
    limit: int | None = None,
) -> ScenarioReport:
    dataset_path = DATASET_DIR / f"{scenario}.jsonl"
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"Dataset not found: {dataset_path}\n"
            f"Run: python load_tests/generate_dataset.py --scenario {scenario}"
        )

    payloads: list[dict] = []
    with open(dataset_path) as f:
        for line in f:
            payloads.append(json.loads(line.strip()))
            if limit and len(payloads) >= limit:
                break

    print(f"Loaded {len(payloads):,} leads from {dataset_path.name}")
    print(f"Starting scenario '{scenario}' — concurrency={concurrency}")

    results: list[RequestResult] = []
    semaphore = asyncio.Semaphore(concurrency)
    start = time.monotonic()

    async def bounded_send(payload: dict) -> RequestResult:
        async with semaphore:
            return await send_lead(session, payload, base_url)

    connector = aiohttp.TCPConnector(limit=concurrency + 10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [bounded_send(p) for p in payloads]
        completed = 0
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            completed += 1
            if completed % 500 == 0:
                elapsed = time.monotonic() - start
                rps = completed / elapsed
                print(f"  {completed:>6,}/{len(payloads):,}  {rps:.0f} req/s", end="\r")

    duration = time.monotonic() - start
    print()

    return ScenarioReport(
        scenario=scenario,
        total=len(payloads),
        duration_s=duration,
        concurrency=concurrency,
        results=results,
    )


# ── Readiness check ────────────────────────────────────────────────────────────

async def wait_for_ready(base_url: str, timeout: float = 30.0) -> None:
    print(f"Waiting for {base_url}/ready ...")
    deadline = time.monotonic() + timeout
    async with aiohttp.ClientSession() as session:
        while time.monotonic() < deadline:
            try:
                async with session.get(f"{base_url}/ready", timeout=aiohttp.ClientTimeout(total=2)) as r:
                    if r.status == 200:
                        print("  Instance is ready.")
                        return
            except Exception:
                pass
            await asyncio.sleep(1)
    raise TimeoutError(f"Instance not ready after {timeout}s")


# ── CLI ───────────────────────────────────────────────────────────────────────

SCENARIOS = ["duplicate_storm", "redis_outage", "messy_data"]


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run load test scenario")
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    parser.add_argument("--base-url", default=BASE_URL_DEFAULT)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--limit", type=int, default=None, help="Max leads to send (default: all)")
    parser.add_argument("--no-wait", action="store_true", help="Skip readiness check")
    args = parser.parse_args()

    if not args.no_wait:
        await wait_for_ready(args.base_url)

    report = await run_scenario(
        scenario=args.scenario,
        base_url=args.base_url,
        concurrency=args.concurrency,
        limit=args.limit,
    )
    report.print_summary()

    # Write JSON report for CI / diff
    out = DATASET_DIR / f"{args.scenario}_report.json"
    with open(out, "w") as f:
        json.dump({
            "scenario": report.scenario,
            "total": report.total,
            "duration_s": round(report.duration_s, 2),
            "throughput_rps": round(report.throughput_rps, 1),
            "success_rate": round(report.success_rate, 4),
            "decision_breakdown": report.decision_breakdown,
            "latency_ms": report.latency_percentiles,
        }, f, indent=2)
    print(f"Report saved → {out}")


if __name__ == "__main__":
    asyncio.run(main())
