from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from typing import Any

if __package__:
    from benchmarks.ws_load import execute
else:
    from ws_load import execute


@dataclass(frozen=True, slots=True)
class ProviderAttestation:
    provider: str
    data_mode: str
    gateway_url: str
    deployment_commit_sha: str
    environment: str
    issuer: str
    issued_at: datetime
    licensing_approved_for_publication: bool


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


def load_attestation(
    path: Path,
    *,
    expected_gateway_url: str,
    max_age_hours: float,
    now: datetime | None = None,
) -> ProviderAttestation:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("provider attestation must contain an object")

    required_strings = (
        "provider",
        "data_mode",
        "gateway_url",
        "deployment_commit_sha",
        "environment",
        "issuer",
        "issued_at",
    )
    values: dict[str, str] = {}
    for field in required_strings:
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"provider attestation requires non-empty {field}")
        values[field] = value.strip()

    approved = payload.get("licensing_approved_for_publication")
    if not isinstance(approved, bool):
        raise ValueError(
            "provider attestation requires boolean licensing_approved_for_publication"
        )
    if values["provider"].lower() != "tradernet":
        raise ValueError("provider attestation must identify tradernet")
    if values["data_mode"].lower() not in {"demo", "sid"}:
        raise ValueError("provider attestation data_mode must be demo or sid")
    if values["gateway_url"] != expected_gateway_url:
        raise ValueError("provider attestation gateway_url does not match benchmark target")

    issued_at = datetime.fromisoformat(values["issued_at"].replace("Z", "+00:00"))
    if issued_at.tzinfo is None or issued_at.utcoffset() is None:
        raise ValueError("provider attestation issued_at must be timezone-aware")
    issued_at = issued_at.astimezone(UTC)
    reference_now = (now or datetime.now(UTC)).astimezone(UTC)
    if issued_at > reference_now + timedelta(minutes=5):
        raise ValueError("provider attestation issued_at is in the future")
    if reference_now - issued_at > timedelta(hours=max_age_hours):
        raise ValueError("provider attestation is stale")

    return ProviderAttestation(
        provider="tradernet",
        data_mode=values["data_mode"].lower(),
        gateway_url=values["gateway_url"],
        deployment_commit_sha=values["deployment_commit_sha"],
        environment=values["environment"],
        issuer=values["issuer"],
        issued_at=issued_at,
        licensing_approved_for_publication=approved,
    )


def require_publication_approval(
    profile: str,
    attestation: ProviderAttestation,
) -> None:
    restricted = attestation.data_mode == "sid" or profile == "legacy-673"
    if restricted and not attestation.licensing_approved_for_publication:
        raise PermissionError(
            "SID-backed and legacy-673 provider reports require attested licensing approval"
        )


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
            f"unknown profile {args.profile!r}; available: {', '.join(sorted(profiles))}"
        )
    profile = profiles[args.profile]
    symbols = load_symbols(args)
    required_universe = int(profile.get("required_symbol_universe", 0))
    if required_universe and len(symbols) < required_universe:
        raise ValueError(
            f"profile {args.profile} requires at least {required_universe} real symbols; "
            f"received {len(symbols)}"
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
    parser = argparse.ArgumentParser(description="Run a staged Tradernet gateway benchmark")
    parser.add_argument("--profile", default="smoke")
    parser.add_argument(
        "--profiles",
        type=Path,
        default=Path("benchmarks/tradernet_profiles.json"),
    )
    parser.add_argument("--url", default="ws://localhost:8000/v1/stream")
    parser.add_argument(
        "--symbols",
        default="AAPL.US,MSFT.US,NVDA.US,TSLA.US,AMZN.US,GOOG.US,META.US,AMD.US,INTC.US,ORCL.US",
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
