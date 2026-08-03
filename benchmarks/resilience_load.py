from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import random
import time
from typing import Any

import httpx

from smart_market_data_gateway.resilience_client import (
    ResilienceSocket,
    connect_and_subscribe,
    receive_quote,
    receive_quote_after_marker,
)


async def close_sockets(sockets: list[ResilienceSocket]) -> None:
    await asyncio.gather(
        *(socket.close() for socket in sockets),
        return_exceptions=True,
    )


async def fetch_stats(url: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(url)
        response.raise_for_status()
        return dict(response.json())


async def reconnect_storm(
    args: argparse.Namespace,
    assignments: list[list[str]],
) -> dict[str, Any]:
    initial = await asyncio.gather(
        *(
            connect_and_subscribe(index, url=args.url, symbols=symbols)
            for index, symbols in enumerate(assignments)
        ),
        return_exceptions=True,
    )
    initial_sockets = [
        socket for socket in initial if not isinstance(socket, BaseException)
    ]
    initial_errors = [
        str(error) for error in initial if isinstance(error, BaseException)
    ]
    await close_sockets(initial_sockets)

    started = time.perf_counter()
    reconnected = await asyncio.gather(
        *(
            connect_and_subscribe(index, url=args.url, symbols=symbols)
            for index, symbols in enumerate(assignments)
        ),
        return_exceptions=True,
    )
    elapsed = time.perf_counter() - started
    reconnected_sockets = [
        socket for socket in reconnected if not isinstance(socket, BaseException)
    ]
    reconnect_errors = [
        str(error) for error in reconnected if isinstance(error, BaseException)
    ]
    quote_results = await asyncio.gather(
        *(receive_quote(socket, args.timeout) for socket in reconnected_sockets),
        return_exceptions=True,
    )
    delivered = sum(isinstance(result, dict) for result in quote_results)
    await close_sockets(reconnected_sockets)

    return {
        "initial_connections": len(initial_sockets),
        "initial_errors": initial_errors[:100],
        "reconnected_clients": len(reconnected_sockets),
        "reconnect_errors": reconnect_errors[:100],
        "reconnect_wall_seconds": elapsed,
        "reconnects_per_second": (
            len(reconnected_sockets) / elapsed if elapsed else 0
        ),
        "clients_receiving_after_reconnect": delivered,
    }


async def zombie_cleanup(
    args: argparse.Namespace,
    assignments: list[list[str]],
) -> dict[str, Any]:
    connected = await asyncio.gather(
        *(
            connect_and_subscribe(index, url=args.url, symbols=symbols)
            for index, symbols in enumerate(assignments)
        ),
        return_exceptions=True,
    )
    sockets = [
        socket for socket in connected if not isinstance(socket, BaseException)
    ]
    before = await fetch_stats(args.stats_url)
    abrupt_count = (
        max(1, round(len(sockets) * args.zombie_fraction)) if sockets else 0
    )
    abrupt = sockets[:abrupt_count]
    graceful = sockets[abrupt_count:]

    for socket in abrupt:
        transport = getattr(socket, "transport", None)
        if transport is not None:
            transport.abort()
        else:
            await socket.close()
    await close_sockets(graceful)
    await asyncio.sleep(args.cleanup_wait)
    after = await fetch_stats(args.stats_url)

    return {
        "connected_clients": len(sockets),
        "abruptly_dropped_clients": len(abrupt),
        "cleanup_wait_seconds": args.cleanup_wait,
        "stats_before": before,
        "stats_after": after,
    }


async def frozen_stream(
    args: argparse.Namespace,
    assignments: list[list[str]],
) -> dict[str, Any]:
    connected = await asyncio.gather(
        *(
            connect_and_subscribe(index, url=args.url, symbols=symbols)
            for index, symbols in enumerate(assignments)
        ),
        return_exceptions=True,
    )
    sockets = [
        socket for socket in connected if not isinstance(socket, BaseException)
    ]
    first_results = await asyncio.gather(
        *(receive_quote(socket, args.timeout) for socket in sockets),
        return_exceptions=True,
    )
    clients_with_initial_quote = sum(
        isinstance(result, dict) for result in first_results
    )
    await asyncio.sleep(args.pause_seconds)

    # A matching pong on each ordered WebSocket is the recovery boundary. Messages
    # received before that pong are discarded as backlog. This avoids comparing clocks
    # between the benchmark host and the gateway host.
    resumed = await asyncio.gather(
        *(
            receive_quote_after_marker(socket, args.timeout)
            for socket in sockets
        ),
        return_exceptions=True,
    )
    recovered = sum(isinstance(result, dict) for result in resumed)
    errors = [
        str(result) for result in resumed if isinstance(result, BaseException)
    ]
    await close_sockets(sockets)

    return {
        "connected_clients": len(sockets),
        "clients_with_initial_quote": clients_with_initial_quote,
        "pause_seconds": args.pause_seconds,
        "clients_with_quote_after_pong_marker": recovered,
        "failed_or_frozen_clients": len(sockets) - recovered,
        "errors": errors[:100],
    }


async def execute(args: argparse.Namespace) -> dict[str, Any]:
    rng = random.Random(args.seed)
    universe = [
        symbol.strip().upper()
        for symbol in args.symbols.split(",")
        if symbol.strip()
    ]
    if not universe:
        raise ValueError("at least one symbol is required")
    assignments = [
        rng.sample(universe, k=min(args.symbols_per_client, len(universe)))
        for _ in range(args.clients)
    ]

    if args.scenario == "reconnect-storm":
        results = await reconnect_storm(args, assignments)
    elif args.scenario == "zombie-cleanup":
        results = await zombie_cleanup(args, assignments)
    else:
        results = await frozen_stream(args, assignments)

    return {
        "scenario": {
            "name": args.scenario,
            "clients": args.clients,
            "symbols_per_client": args.symbols_per_client,
            "symbol_universe": len(universe),
            "url": args.url,
            "seed": args.seed,
        },
        "results": results,
        "limitations": [
            "Run against an isolated environment; resilience tests intentionally "
            "disconnect clients.",
            "Provider-backed publication remains gated by Tradernet and exchange "
            "licensing terms.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gateway resilience benchmark scenarios"
    )
    parser.add_argument(
        "--scenario",
        choices=["reconnect-storm", "zombie-cleanup", "frozen-stream"],
        required=True,
    )
    parser.add_argument("--url", default="ws://localhost:8000/v1/stream")
    parser.add_argument(
        "--stats-url",
        default="http://localhost:8000/internal/stats",
    )
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--symbols-per-client", type=int, default=5)
    parser.add_argument(
        "--symbols",
        default=(
            "AAPL.US,MSFT.US,NVDA.US,TSLA.US,AMZN.US,"
            "GOOG.US,META.US,AMD.US,INTC.US,ORCL.US"
        ),
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--pause-seconds", type=float, default=10.0)
    parser.add_argument("--cleanup-wait", type=float, default=50.0)
    parser.add_argument("--zombie-fraction", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=207)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmark-results/resilience.json"),
    )
    args = parser.parse_args()
    result = asyncio.run(execute(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result["results"], indent=2))


if __name__ == "__main__":
    main()
