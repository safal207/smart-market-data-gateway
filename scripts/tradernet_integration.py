from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
import json
from pathlib import Path
import time

from smart_market_data_gateway.config import Settings
from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.providers import (
    TradernetMode,
    TradernetProviderAdapter,
    TradernetProviderConfig,
)


@dataclass(frozen=True, slots=True)
class CollectionResult:
    events: int
    symbols: list[str]
    duplicate_event_ids: int
    timestamp_rollbacks: int
    time_to_first_event_ms: float | None
    first_timestamp: str | None
    last_timestamp: str | None


def build_provider(settings: Settings) -> TradernetProviderAdapter:
    return TradernetProviderAdapter(
        TradernetProviderConfig(
            mode=TradernetMode(settings.tradernet_mode),
            websocket_url=settings.tradernet_websocket_url,
            snapshot_base_url=settings.tradernet_snapshot_base_url,
            sid=settings.tradernet_sid,
            user_id=settings.tradernet_user_id,
            api_key=settings.tradernet_api_key,
            api_secret=settings.tradernet_api_secret,
            require_authenticated_sid=settings.tradernet_require_authenticated_sid,
            snapshot_fallback=settings.tradernet_snapshot_fallback,
            connect_timeout_seconds=settings.tradernet_connect_timeout_seconds,
            snapshot_timeout_seconds=settings.tradernet_snapshot_timeout_seconds,
        )
    )


async def collect_events(
    provider: TradernetProviderAdapter,
    *,
    target_events: int,
    timeout_seconds: float,
) -> CollectionResult:
    started = time.perf_counter()
    events: list[QuoteEvent] = []
    try:
        async with asyncio.timeout(timeout_seconds):
            async for event in provider.events():
                events.append(event)
                if len(events) >= target_events:
                    break
    except TimeoutError:
        pass

    ids = Counter(str(event.event_id) for event in events)
    duplicate_ids = sum(count - 1 for count in ids.values() if count > 1)
    rollbacks = 0
    previous_by_symbol: dict[str, datetime] = {}
    for event in events:
        previous = previous_by_symbol.get(event.symbol)
        if previous is not None and event.provider_timestamp < previous:
            rollbacks += 1
        previous_by_symbol[event.symbol] = event.provider_timestamp

    first = events[0] if events else None
    last = events[-1] if events else None
    return CollectionResult(
        events=len(events),
        symbols=sorted({event.symbol for event in events}),
        duplicate_event_ids=duplicate_ids,
        timestamp_rollbacks=rollbacks,
        time_to_first_event_ms=(
            (time.perf_counter() - started) * 1000 if first is not None else None
        ),
        first_timestamp=first.provider_timestamp.isoformat() if first is not None else None,
        last_timestamp=last.provider_timestamp.isoformat() if last is not None else None,
    )


async def execute(args: argparse.Namespace) -> dict[str, object]:
    settings = Settings()
    symbols = [
        symbol.strip().upper()
        for symbol in (args.symbols or settings.tradernet_integration_symbols).split(",")
        if symbol.strip()
    ]
    provider = build_provider(settings)
    await provider.subscribe(symbols)

    await provider.connect()
    before = await collect_events(
        provider,
        target_events=args.events,
        timeout_seconds=args.timeout,
    )
    await provider.disconnect()

    reconnect_started = time.perf_counter()
    await provider.connect()
    reconnect_ms = (time.perf_counter() - reconnect_started) * 1000
    after = await collect_events(
        provider,
        target_events=args.events,
        timeout_seconds=args.timeout,
    )
    await provider.disconnect()

    return {
        "provider": "tradernet",
        "mode": settings.tradernet_mode,
        "websocket_host": settings.tradernet_websocket_url.split("?", 1)[0],
        "requested_symbols": symbols,
        "before_reconnect": asdict(before),
        "after_reconnect": asdict(after),
        "reconnect_ms": reconnect_ms,
        "checks": {
            "received_before_reconnect": before.events > 0,
            "received_after_reconnect": after.events > 0,
            "no_duplicate_ids": before.duplicate_event_ids == 0 and after.duplicate_event_ids == 0,
            "no_timestamp_rollbacks": (
                before.timestamp_rollbacks == 0 and after.timestamp_rollbacks == 0
            ),
        },
        "notes": [
            "No SID, user_id, API key, or secret is written to this report.",
            "Timezone-free Tradernet ltt values use receive time to avoid false latency claims.",
            "Public benchmark publication remains gated by provider and exchange licensing terms.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run opt-in Tradernet quote integration checks")
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--events", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmark-results/tradernet-integration.json"),
    )
    args = parser.parse_args()
    result = asyncio.run(execute(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result["checks"], indent=2))
    if not all(result["checks"].values()):  # type: ignore[union-attr]
        raise SystemExit(1)


if __name__ == "__main__":
    main()
