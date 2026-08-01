import argparse
import asyncio
from datetime import UTC, datetime
import json
from pathlib import Path
import random
import statistics
import time
from typing import Any

import websockets


async def run_client(
    client_id: int,
    *,
    url: str,
    token: str,
    symbols: list[str],
    messages: int,
) -> dict[str, Any]:
    latencies_ms: list[float] = []
    received = 0
    started = time.perf_counter()
    async with websockets.connect(f"{url}?token={token}", open_timeout=10) as socket:
        await socket.recv()  # connected
        await socket.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "symbols": symbols,
                    "channels": ["quote"],
                    "request_id": f"client-{client_id}",
                }
            )
        )
        while received < messages:
            raw = await asyncio.wait_for(socket.recv(), timeout=30)
            payload = json.loads(raw)
            if payload.get("type") != "quote":
                continue
            quote = payload["data"]["quote"]
            provider_timestamp = datetime.fromisoformat(quote["provider_timestamp"])
            if provider_timestamp.tzinfo is None:
                provider_timestamp = provider_timestamp.replace(tzinfo=UTC)
            latency = (datetime.now(UTC) - provider_timestamp).total_seconds() * 1000
            latencies_ms.append(max(0.0, latency))
            received += 1
    return {
        "client_id": client_id,
        "received": received,
        "elapsed_seconds": time.perf_counter() - started,
        "latencies_ms": latencies_ms,
    }


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * fraction))
    return ordered[index]


async def execute(args: argparse.Namespace) -> dict[str, Any]:
    rng = random.Random(args.seed)
    universe = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
    tiers = ["basic"] * 70 + ["pro"] * 25 + ["premium"] * 5
    tasks = []
    for client_id in range(args.clients):
        selected = rng.sample(universe, k=min(args.symbols_per_client, len(universe)))
        tier = tiers[client_id % len(tiers)]
        tasks.append(
            asyncio.create_task(
                run_client(
                    client_id,
                    url=args.url,
                    token=f"dev-{tier}:load-{client_id}",
                    symbols=selected,
                    messages=args.messages,
                )
            )
        )
    started = time.perf_counter()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.perf_counter() - started
    successful = [result for result in results if isinstance(result, dict)]
    errors = [str(result) for result in results if isinstance(result, Exception)]
    latencies = [latency for result in successful for latency in result["latencies_ms"]]
    return {
        "scenario": {
            "url": args.url,
            "clients": args.clients,
            "symbols_per_client": args.symbols_per_client,
            "messages_per_client": args.messages,
            "seed": args.seed,
        },
        "results": {
            "successful_clients": len(successful),
            "failed_clients": len(errors),
            "received_messages": sum(result["received"] for result in successful),
            "wall_seconds": elapsed,
            "throughput_messages_per_second": (
                sum(result["received"] for result in successful) / elapsed if elapsed else 0
            ),
            "latency_p50_ms": statistics.median(latencies) if latencies else 0,
            "latency_p95_ms": percentile(latencies, 0.95),
            "latency_p99_ms": percentile(latencies, 0.99),
        },
        "errors": errors[:100],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Network WebSocket load benchmark")
    parser.add_argument("--url", default="ws://localhost:8000/v1/stream")
    parser.add_argument("--clients", type=int, default=100)
    parser.add_argument("--symbols-per-client", type=int, default=5)
    parser.add_argument("--messages", type=int, default=20)
    parser.add_argument(
        "--symbols",
        default="AAPL,TSLA,NVDA,MSFT,GOOG,AMZN,META,AMD,INTC,ORCL",
    )
    parser.add_argument("--seed", type=int, default=207)
    parser.add_argument("--output", type=Path, default=Path("benchmark-results/network.json"))
    args = parser.parse_args()
    result = asyncio.run(execute(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result["results"], indent=2))


if __name__ == "__main__":
    main()
