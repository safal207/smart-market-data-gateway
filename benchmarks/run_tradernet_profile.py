from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
from typing import Any

from smart_market_data_gateway.provider_attestation import (
    load_attestation,
    require_publication_approval,
)

if __package__:
    from benchmarks.ws_load import execute
else:
    from ws_load import execute


def load_profiles(path: Path) -> dict[str, dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("benchmark profile file must contain an object")
    return {str(name): dict(config) for name, config in payload.items()}


def load_symbols(args: argparse.Namespace) -> list[str]:
    if args.symbols_file is not None:
        raw = args.symbols_file.read_text(encoding="utf-8")
        candidates = raw.replace("\n", ",").split(",")
    else:
        candidates = args.symbols.split(",")
    symbols = list(
        dict.fromkeys(symbol.strip().upper() for symbol in candidates if symbol.strip())
    )
    if not symbols:
        raise ValueError("at least one symbol is required")
    return symbols


async def run(args: argparse.Namespace) -> dict[str, Any]:
    attestation = load_attestation(
        args.attestation,
        expected_gateway_url=args.url,
        max_age_hours=args.attestation_max_age_hours,
    )
    require_publication_approval(args.profile, attestation)
    profiles = load_profiles(args.profiles)
    if args.profile not in profiles:
        raise ValueError(
            f"unknown profile {args.profile!r}; "
            f"available: {', '.join(sorted(profiles))}"
        )
    profile = profiles[args.profile]
    symbols = load_symbols(args)
    required_universe = int(profile.get("required_symbol_universe", 0))
    if required_universe and len(symbols) < required_universe:
        raise ValueError(
            f"profile {args.profile} requires at least {required_universe} "
            f"real symbols; received {len(symbols)}"
        )

    benchmark_args = argparse.Namespace(
        url=args.url,
        clients=int(profile["clients"]),
        symbols_per_client=int(profile["symbols_per_client"]),
        messages=int(profile["messages"]),
        heartbeat_seconds=float(profile["heartbeat_seconds"]),
        symbols=",".join(symbols),
        seed=args.seed,
    )
    result = await execute(benchmark_args)
    result["profile"] = {
        "name": args.profile,
        "description": profile.get("description"),
        "symbol_universe": len(symbols),
    }
    provenance = asdict(attestation)
    provenance["issued_at"] = attestation.issued_at.isoformat()
    provenance["market_session"] = args.market_session
    result["provenance"] = provenance
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a staged Tradernet gateway benchmark"
    )
    parser.add_argument("--profile", default="smoke")
    parser.add_argument(
        "--profiles",
        type=Path,
        default=Path("benchmarks/tradernet_profiles.json"),
    )
    parser.add_argument("--url", default="ws://localhost:8000/v1/stream")
    parser.add_argument(
        "--symbols",
        default=(
            "AAPL.US,MSFT.US,NVDA.US,TSLA.US,AMZN.US,"
            "GOOG.US,META.US,AMD.US,INTC.US,ORCL.US"
        ),
    )
    parser.add_argument("--symbols-file", type=Path)
    parser.add_argument("--seed", type=int, default=207)
    parser.add_argument("--market-session", default="unknown")
    parser.add_argument("--attestation", type=Path, required=True)
    parser.add_argument("--attestation-max-age-hours", type=float, default=24.0)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmark-results/tradernet-profile.json"),
    )
    args = parser.parse_args()
    result = asyncio.run(run(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result["results"], indent=2))


if __name__ == "__main__":
    main()
