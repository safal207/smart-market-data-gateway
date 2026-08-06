from __future__ import annotations

import argparse
import asyncio
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys
from time import monotonic
from typing import Literal
from uuid import uuid4

from smart_market_data_gateway.providers.base import MarketDataProvider
from smart_market_data_gateway.providers.coinbase import (
    COINBASE_MARKET_DATA_URL,
    CoinbaseResearchConfig,
    CoinbaseResearchMarketDataProvider,
)
from smart_market_data_gateway.recorder import AtomicJsonlWriter, verify_jsonl_ledger

CompletionReason = Literal["max_records", "max_seconds", "stream_ended"]


@dataclass(frozen=True, slots=True)
class ResearchCaptureResult:
    """Safe-to-share metadata for one local research capture."""

    output: str
    provider: str
    symbols: tuple[str, ...]
    records_written: int
    ledger_records: int
    ledger_head_hash: str
    recorder_session_id: str
    started_at: str
    finished_at: str
    duration_seconds: float
    completion_reason: CompletionReason
    complete: bool
    diagnostic: str | None = None
    verified: bool = True

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["symbols"] = list(self.symbols)
        return payload


def normalize_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()}))
    if not normalized:
        raise ValueError("at least one symbol is required")
    return normalized


def validate_private_output(path: Path, *, append: bool) -> Path:
    """Require local JSONL output under a recordings directory."""

    expanded = path.expanduser()
    if expanded.suffix.lower() != ".jsonl":
        raise ValueError("research capture output must use the .jsonl suffix")
    if "recordings" not in expanded.parts:
        raise ValueError("research capture output must be inside a recordings/ directory")
    if expanded.exists() and expanded.stat().st_size > 0 and not append:
        raise ValueError("output ledger already exists; pass --append to continue its chain")
    return expanded


async def capture_provider_session(
    provider: MarketDataProvider,
    *,
    symbols: Sequence[str],
    output: Path,
    max_records: int,
    max_seconds: float,
    append: bool = False,
    fsync: bool = True,
    connect_timeout: float = 30.0,
    subscribe_timeout: float = 30.0,
) -> ResearchCaptureResult:
    """Capture provider events directly into a verified evidence ledger."""

    normalized_symbols = normalize_symbols(symbols)
    safe_output = validate_private_output(output, append=append)
    if max_records < 0:
        raise ValueError("max_records must not be negative")
    if max_seconds <= 0:
        raise ValueError("max_seconds must be positive")
    if connect_timeout <= 0:
        raise ValueError("connect_timeout must be positive")
    if subscribe_timeout <= 0:
        raise ValueError("subscribe_timeout must be positive")

    before = verify_jsonl_ledger(safe_output, allow_missing=True)
    session_id = str(uuid4())
    started = datetime.now(UTC)
    started_monotonic = monotonic()
    written = 0
    completion_reason: CompletionReason = "stream_ended"

    try:
        try:
            async with asyncio.timeout(connect_timeout):
                await provider.connect()
        except TimeoutError:
            raise RuntimeError(
                f"provider connect exceeded the {connect_timeout:g}s timeout"
            ) from None
        except Exception as exc:
            raise RuntimeError(
                f"provider connect failed:{type(exc).__name__}"
            ) from exc
        try:
            async with asyncio.timeout(subscribe_timeout):
                await provider.subscribe(normalized_symbols)
        except TimeoutError:
            raise RuntimeError(
                f"provider subscribe exceeded the {subscribe_timeout:g}s timeout"
            ) from None
        except Exception as exc:
            raise RuntimeError(
                f"provider subscribe failed:{type(exc).__name__}"
            ) from exc
        with AtomicJsonlWriter(
            safe_output,
            fsync=fsync,
            session_id=session_id,
        ) as writer:

            async def consume() -> None:
                nonlocal completion_reason, written
                async for event in provider.events():
                    record = event.model_dump(mode="json")
                    record.update(
                        {
                            "event_type": "quote",
                            "channel": "quote",
                            "source_message_type": "provider_direct",
                            "stale": False,
                            "degraded_stream": False,
                            "sequence_gap": False,
                            "gap_size": 0,
                        }
                    )
                    writer.write(record)
                    written += 1
                    if max_records > 0 and written >= max_records:
                        completion_reason = "max_records"
                        return

            try:
                async with asyncio.timeout(max_seconds):
                    await consume()
            except TimeoutError:
                completion_reason = "max_seconds"
    finally:
        with suppress(Exception):
            await provider.unsubscribe(normalized_symbols)
        await provider.disconnect()

    if written == 0:
        raise RuntimeError("research capture received no market-data events")

    verification = verify_jsonl_ledger(safe_output)
    expected_records = before.records + written
    if verification.records != expected_records:
        raise RuntimeError(
            "verified ledger record count does not match the completed capture"
        )

    stream_diagnostics = getattr(provider, "diagnostics", None)
    if isinstance(stream_diagnostics, Mapping):
        json.dump(stream_diagnostics, sys.stderr, sort_keys=True)
        sys.stderr.write("\n")

    complete = completion_reason == "max_seconds"
    diagnostic: str | None = None
    if not complete:
        diagnostic = (
            f"capture ended before the planned {max_seconds:g}s window: "
            f"completion_reason={completion_reason}, records_written={written}"
        )

    finished = datetime.now(UTC)
    return ResearchCaptureResult(
        output=str(safe_output),
        provider=provider.name,
        symbols=normalized_symbols,
        records_written=written,
        ledger_records=verification.records,
        ledger_head_hash=verification.head_hash,
        recorder_session_id=session_id,
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        duration_seconds=round(monotonic() - started_monotonic, 6),
        completion_reason=completion_reason,
        complete=complete,
        diagnostic=diagnostic,
    )


async def capture_coinbase_research_session(
    *,
    symbols: Sequence[str],
    output: Path,
    max_records: int,
    max_seconds: float,
    terms_accepted: bool,
    environment: str,
    url: str = COINBASE_MARKET_DATA_URL,
    append: bool = False,
    fsync: bool = True,
    connect_timeout: float = 30.0,
    subscribe_timeout: float = 30.0,
) -> ResearchCaptureResult:
    """Run one explicitly acknowledged Coinbase personal-research capture."""

    config = CoinbaseResearchConfig(
        url=url,
        use_mode="personal_research",
        market_data_terms_accepted=terms_accepted,
        environment=environment,
    )
    config.validate_usage()
    provider = CoinbaseResearchMarketDataProvider(config)
    return await capture_provider_session(
        provider,
        symbols=symbols,
        output=output,
        max_records=max_records,
        max_seconds=max_seconds,
        append=append,
        fsync=fsync,
        connect_timeout=connect_timeout,
        subscribe_timeout=subscribe_timeout,
    )


def default_output() -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return Path("recordings") / f"coinbase-research-{timestamp}.jsonl"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="smdg-research-capture",
        description=(
            "Capture a short, local, tamper-evident Coinbase research session. "
            "No market data is uploaded or committed by this command."
        ),
    )
    parser.add_argument("--symbol", action="append", dest="symbols")
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument(
        "--max-records",
        type=int,
        default=100,
        help="stop after this many records; 0 means no record limit",
    )
    parser.add_argument("--max-seconds", type=float, default=60.0)
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=30.0,
        help="timeout in seconds for the provider connection attempt",
    )
    parser.add_argument(
        "--subscribe-timeout",
        type=float,
        default=30.0,
        help="timeout in seconds for the provider subscription attempt",
    )
    parser.add_argument("--append", action="store_true")
    parser.add_argument("--no-fsync", action="store_true")
    parser.add_argument(
        "--url",
        default=os.getenv("SMDG_COINBASE_WS_URL", COINBASE_MARKET_DATA_URL),
    )
    parser.add_argument(
        "--environment",
        default=os.getenv("SMDG_ENVIRONMENT", "research"),
    )
    parser.add_argument(
        "--accept-current-market-data-terms",
        action="store_true",
        help=(
            "Confirm that you personally reviewed and accept the current Coinbase "
            "Market Data Terms for this local research session."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.symbols:
        raise SystemExit("smdg-research-capture: provide at least one --symbol")
    if not args.accept_current_market_data_terms:
        raise SystemExit(
            "smdg-research-capture: --accept-current-market-data-terms is required"
        )

    try:
        result = asyncio.run(
            capture_coinbase_research_session(
                symbols=args.symbols,
                output=args.output or default_output(),
                max_records=args.max_records,
                max_seconds=args.max_seconds,
                terms_accepted=args.accept_current_market_data_terms,
                environment=args.environment,
                url=args.url,
                append=args.append,
                fsync=not args.no_fsync,
                connect_timeout=args.connect_timeout,
                subscribe_timeout=args.subscribe_timeout,
            )
        )
    except (OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(f"smdg-research-capture: {exc}") from exc

    json.dump(result.as_dict(), sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    if not result.complete:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
