import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import random
import statistics
import time
import tracemalloc
from typing import Any


@dataclass(frozen=True, slots=True)
class BenchmarkScenario:
    clients: int = 10_000
    symbols_per_client: int = 20
    symbol_universe: int = 500
    events_per_symbol: int = 100
    provider_events_per_second: int = 10
    seed: int = 207


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    mode: str
    provider_subscriptions: int
    provider_events: int
    delivered_events: int
    elapsed_seconds: float
    cpu_seconds: float
    peak_memory_bytes: int
    p50_processing_us: float
    p95_processing_us: float


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * percentile)))
    return ordered[index]


def _build_clients(scenario: BenchmarkScenario) -> list[tuple[str, tuple[int, ...]]]:
    rng = random.Random(scenario.seed)
    tiers = (
        ["basic"] * 70
        + ["pro"] * 25
        + ["premium"] * 5
    )
    population = list(range(scenario.symbol_universe))
    weights = [1.0 / (index + 1) ** 0.8 for index in population]
    clients: list[tuple[str, tuple[int, ...]]] = []
    for client_index in range(scenario.clients):
        tier = tiers[client_index % len(tiers)]
        chosen: set[int] = set()
        while len(chosen) < min(scenario.symbols_per_client, scenario.symbol_universe):
            chosen.add(rng.choices(population, weights=weights, k=1)[0])
        clients.append((tier, tuple(sorted(chosen))))
    return clients


def _run_mode(
    scenario: BenchmarkScenario,
    clients: list[tuple[str, tuple[int, ...]]],
    *,
    optimized: bool,
) -> BenchmarkResult:
    subscribers: dict[int, list[str]] = {}
    for tier, symbols in clients:
        for symbol in symbols:
            subscribers.setdefault(symbol, []).append(tier)

    provider_subscriptions = (
        len(subscribers)
        if optimized
        else sum(len(symbols) for _tier, symbols in clients)
    )
    provider_events = len(subscribers) * scenario.events_per_symbol
    delivered = 0
    processing_samples: list[float] = []
    tier_divisor = {
        "basic": max(1, round(scenario.provider_events_per_second / 1)),
        "pro": max(1, round(scenario.provider_events_per_second / 5)),
        "premium": 1,
    }

    tracemalloc.start()
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    checksum = 0
    for symbol, tiers in subscribers.items():
        for event_number in range(scenario.events_per_symbol):
            event_start = time.perf_counter_ns()
            if optimized:
                for tier in tiers:
                    if event_number % tier_divisor[tier] == 0:
                        delivered += 1
                        checksum ^= symbol + delivered
            else:
                for _tier in tiers:
                    delivered += 1
                    checksum ^= symbol + delivered
            processing_samples.append((time.perf_counter_ns() - event_start) / 1000)

    cpu_seconds = time.process_time() - cpu_start
    elapsed = time.perf_counter() - wall_start
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    if checksum == -1:  # pragma: no cover - prevents over-aggressive optimization
        raise AssertionError("unreachable checksum")

    return BenchmarkResult(
        mode="smart" if optimized else "baseline",
        provider_subscriptions=provider_subscriptions,
        provider_events=provider_events,
        delivered_events=delivered,
        elapsed_seconds=elapsed,
        cpu_seconds=cpu_seconds,
        peak_memory_bytes=peak,
        p50_processing_us=statistics.median(processing_samples) if processing_samples else 0.0,
        p95_processing_us=_percentile(processing_samples, 0.95),
    )


def run_benchmark(scenario: BenchmarkScenario) -> dict[str, Any]:
    clients = _build_clients(scenario)
    baseline = _run_mode(scenario, clients, optimized=False)
    smart = _run_mode(scenario, clients, optimized=True)

    def savings(before: float, after: float) -> float:
        return (1.0 - after / before) * 100 if before else 0.0

    return {
        "scenario": asdict(scenario),
        "baseline": asdict(baseline),
        "smart": asdict(smart),
        "comparison": {
            "provider_subscription_reduction_percent": savings(
                baseline.provider_subscriptions,
                smart.provider_subscriptions,
            ),
            "delivered_event_reduction_percent": savings(
                baseline.delivered_events,
                smart.delivered_events,
            ),
            "cpu_reduction_percent": savings(baseline.cpu_seconds, smart.cpu_seconds),
            "peak_memory_reduction_percent": savings(
                baseline.peak_memory_bytes,
                smart.peak_memory_bytes,
            ),
        },
        "limitations": [
            "This benchmark measures deterministic in-process routing work, not exchange latency.",
            "Node-count and cost conclusions require a deployed network benchmark.",
            "Real-provider results depend on licensing, rate limits, payload shape, and market activity.",
        ],
    }


def _markdown(result: dict[str, Any]) -> str:
    baseline = result["baseline"]
    smart = result["smart"]
    comparison = result["comparison"]
    return f"""# Smart Market Data Gateway benchmark

## Scenario

```json
{json.dumps(result['scenario'], indent=2)}
```

## Measured results

| Metric | Baseline | Smart |
|---|---:|---:|
| Provider subscriptions | {baseline['provider_subscriptions']:,} | {smart['provider_subscriptions']:,} |
| Provider events | {baseline['provider_events']:,} | {smart['provider_events']:,} |
| Delivered events | {baseline['delivered_events']:,} | {smart['delivered_events']:,} |
| CPU seconds | {baseline['cpu_seconds']:.4f} | {smart['cpu_seconds']:.4f} |
| Peak memory bytes | {baseline['peak_memory_bytes']:,} | {smart['peak_memory_bytes']:,} |
| P95 processing, μs | {baseline['p95_processing_us']:.2f} | {smart['p95_processing_us']:.2f} |

## Difference

- Provider subscription reduction: {comparison['provider_subscription_reduction_percent']:.2f}%
- Delivered event reduction: {comparison['delivered_event_reduction_percent']:.2f}%
- CPU reduction in this run: {comparison['cpu_reduction_percent']:.2f}%
- Peak-memory reduction in this run: {comparison['peak_memory_reduction_percent']:.2f}%

## Limitations

""" + "\n".join(f"- {item}" for item in result["limitations"]) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run reproducible baseline vs smart benchmark")
    parser.add_argument("--clients", type=int, default=10_000)
    parser.add_argument("--symbols-per-client", type=int, default=20)
    parser.add_argument("--symbol-universe", type=int, default=500)
    parser.add_argument("--events-per-symbol", type=int, default=100)
    parser.add_argument("--provider-events-per-second", type=int, default=10)
    parser.add_argument("--seed", type=int, default=207)
    parser.add_argument("--output", type=Path, default=Path("benchmark-results"))
    args = parser.parse_args()

    scenario = BenchmarkScenario(
        clients=args.clients,
        symbols_per_client=args.symbols_per_client,
        symbol_universe=args.symbol_universe,
        events_per_symbol=args.events_per_symbol,
        provider_events_per_second=args.provider_events_per_second,
        seed=args.seed,
    )
    result = run_benchmark(scenario)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "results.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    (args.output / "report.md").write_text(_markdown(result), encoding="utf-8")
    print(json.dumps(result["comparison"], indent=2))


if __name__ == "__main__":
    main()
