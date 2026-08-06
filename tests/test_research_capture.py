from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Collection
from datetime import UTC, datetime
from decimal import Decimal
import json
from pathlib import Path

import pytest
from websockets.asyncio.server import ServerConnection, serve

from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.providers.base import (
    MarketDataProvider,
    ProviderHealth,
    ProviderState,
)
from smart_market_data_gateway.providers.coinbase import (
    CoinbaseResearchConfig,
    CoinbaseResearchMarketDataProvider,
)
from smart_market_data_gateway.research_capture import (
    capture_provider_session,
    main,
    validate_private_output,
)
from smart_market_data_gateway.recorder import verify_jsonl_ledger


class FiniteProvider(MarketDataProvider):
    def __init__(self, events: list[QuoteEvent]) -> None:
        self._events = events
        self._state = ProviderState.DISCONNECTED
        self.subscriptions: tuple[str, ...] = ()
        self.connect_delay: float = 0.0
        self.subscribe_delay: float = 0.0
        self.disconnect_calls = 0

    @property
    def name(self) -> str:
        return "finite-provider"

    async def connect(self) -> None:
        if self.connect_delay:
            await asyncio.sleep(self.connect_delay)
        self._state = ProviderState.CONNECTED

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self._state = ProviderState.DISCONNECTED

    async def subscribe(self, symbols: Collection[str]) -> None:
        if self.subscribe_delay:
            await asyncio.sleep(self.subscribe_delay)
        self.subscriptions = tuple(sorted(symbols))

    async def unsubscribe(self, symbols: Collection[str]) -> None:
        self.subscriptions = ()

    async def health(self) -> ProviderHealth:
        return ProviderHealth(state=self._state)

    async def events(self) -> AsyncIterator[QuoteEvent]:
        for event in self._events:
            yield event


class ContinuousProvider(FiniteProvider):
    def __init__(self) -> None:
        super().__init__([])
        self._count = 0

    async def events(self) -> AsyncIterator[QuoteEvent]:
        try:
            while True:
                self._count += 1
                yield quote(self._count)
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            raise


def quote(sequence: int, price: str = "100.00") -> QuoteEvent:
    timestamp = datetime(2026, 8, 4, 15, 0, sequence, tzinfo=UTC)
    return QuoteEvent(
        schema_version="1.0",
        symbol="BTC-USD",
        price=Decimal(price),
        bid=Decimal(price) - Decimal("0.01"),
        ask=Decimal(price) + Decimal("0.01"),
        provider_timestamp=timestamp,
        received_at=timestamp,
        sequence=sequence,
        provider="finite-provider",
    )


@pytest.mark.asyncio
async def test_capture_writes_and_verifies_private_ledger(tmp_path: Path) -> None:
    output = tmp_path / "recordings" / "session.jsonl"

    result = await capture_provider_session(
        FiniteProvider([quote(1), quote(2, "100.10")]),
        symbols=["btc-usd"],
        output=output,
        max_records=2,
        max_seconds=1.0,
    )

    assert result.records_written == 2
    assert result.ledger_records == 2
    assert result.completion_reason == "max_records"
    assert result.symbols == ("BTC-USD",)
    assert result.verified is True
    verification = verify_jsonl_ledger(output)
    assert verification.records == 2
    assert verification.head_hash == result.ledger_head_hash
    assert verification.session_ids == (result.recorder_session_id,)


@pytest.mark.asyncio
async def test_capture_requires_append_for_existing_ledger(tmp_path: Path) -> None:
    output = tmp_path / "recordings" / "session.jsonl"
    await capture_provider_session(
        FiniteProvider([quote(1)]),
        symbols=["BTC-USD"],
        output=output,
        max_records=1,
        max_seconds=1.0,
    )

    with pytest.raises(ValueError, match="--append"):
        await capture_provider_session(
            FiniteProvider([quote(2)]),
            symbols=["BTC-USD"],
            output=output,
            max_records=1,
            max_seconds=1.0,
        )

    second = await capture_provider_session(
        FiniteProvider([quote(2)]),
        symbols=["BTC-USD"],
        output=output,
        max_records=1,
        max_seconds=1.0,
        append=True,
    )

    verification = verify_jsonl_ledger(output)
    assert second.records_written == 1
    assert second.ledger_records == 2
    assert verification.records == 2
    assert len(verification.session_ids) == 2


def test_private_output_must_be_jsonl_under_recordings(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="recordings"):
        validate_private_output(tmp_path / "session.jsonl", append=False)
    with pytest.raises(ValueError, match=".jsonl"):
        validate_private_output(tmp_path / "recordings" / "session.txt", append=False)


def test_cli_requires_explicit_terms_acceptance() -> None:
    with pytest.raises(SystemExit, match="accept-current-market-data-terms"):
        main(["--symbol", "BTC-USD"])


@pytest.mark.asyncio
async def test_capture_fails_when_provider_emits_no_events(tmp_path: Path) -> None:
    provider = FiniteProvider([])
    with pytest.raises(RuntimeError, match="no market-data events"):
        await capture_provider_session(
            provider,
            symbols=["BTC-USD"],
            output=tmp_path / "recordings" / "empty.jsonl",
            max_records=1,
            max_seconds=1.0,
        )
    assert provider.disconnect_calls == 1


@pytest.mark.asyncio
async def test_capture_connect_timeout_raises_and_disconnects(tmp_path: Path) -> None:
    provider = FiniteProvider([quote(1)])
    provider.connect_delay = 5.0

    with pytest.raises(RuntimeError, match="connect exceeded"):
        await capture_provider_session(
            provider,
            symbols=["BTC-USD"],
            output=tmp_path / "recordings" / "timeout.jsonl",
            max_records=1,
            max_seconds=1.0,
            connect_timeout=0.05,
        )
    assert provider.disconnect_calls == 1


@pytest.mark.asyncio
async def test_capture_subscribe_timeout_raises_and_disconnects(tmp_path: Path) -> None:
    provider = FiniteProvider([quote(1)])
    provider.subscribe_delay = 5.0

    with pytest.raises(RuntimeError, match="subscribe exceeded"):
        await capture_provider_session(
            provider,
            symbols=["BTC-USD"],
            output=tmp_path / "recordings" / "timeout.jsonl",
            max_records=1,
            max_seconds=1.0,
            subscribe_timeout=0.05,
        )
    assert provider.disconnect_calls == 1


@pytest.mark.asyncio
async def test_capture_stream_ended_is_incomplete_and_keeps_partial_ledger(
    tmp_path: Path,
) -> None:
    output = tmp_path / "recordings" / "partial.jsonl"

    result = await capture_provider_session(
        FiniteProvider([quote(1), quote(2)]),
        symbols=["BTC-USD"],
        output=output,
        max_records=100,
        max_seconds=60.0,
    )

    assert result.completion_reason == "stream_ended"
    assert result.complete is False
    assert result.records_written == 2
    assert result.diagnostic is not None
    verification = verify_jsonl_ledger(output)
    assert verification.records == 2
    assert verification.head_hash == result.ledger_head_hash


@pytest.mark.asyncio
async def test_capture_max_seconds_is_complete(tmp_path: Path) -> None:
    output = tmp_path / "recordings" / "full.jsonl"

    result = await capture_provider_session(
        ContinuousProvider(),
        symbols=["BTC-USD"],
        output=output,
        max_records=0,
        max_seconds=0.2,
    )

    assert result.completion_reason == "max_seconds"
    assert result.complete is True
    assert result.diagnostic is None
    assert result.records_written > 0
    verification = verify_jsonl_ledger(output)
    assert verification.records == result.records_written


@pytest.mark.asyncio
async def test_capture_max_records_is_incomplete_in_experiment_mode(tmp_path: Path) -> None:
    output = tmp_path / "recordings" / "short.jsonl"

    result = await capture_provider_session(
        ContinuousProvider(),
        symbols=["BTC-USD"],
        output=output,
        max_records=3,
        max_seconds=60.0,
    )

    assert result.completion_reason == "max_records"
    assert result.complete is False
    assert result.records_written == 3
    verification = verify_jsonl_ledger(output)
    assert verification.records == 3


@pytest.mark.asyncio
async def test_capture_zero_max_records_does_not_stop_stream(tmp_path: Path) -> None:
    output = tmp_path / "recordings" / "unlimited.jsonl"

    result = await capture_provider_session(
        ContinuousProvider(),
        symbols=["BTC-USD"],
        output=output,
        max_records=0,
        max_seconds=0.2,
    )

    assert result.completion_reason == "max_seconds"
    assert result.complete is True
    assert result.records_written > 0


def silent_trades_message() -> dict[str, object]:
    return {
        "channel": "market_trades",
        "timestamp": "2026-08-04T15:00:00.250Z",
        "sequence_num": 11,
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "trade_id": "trade-1",
                        "product_id": "BTC-USD",
                        "price": "100.05",
                        "size": "0.30",
                        "side": "SELL",
                        "time": "2026-08-04T15:00:00.100Z",
                    }
                ],
            }
        ],
    }


@pytest.mark.asyncio
async def test_capture_silent_stream_is_incomplete_and_keeps_valid_ledger(
    tmp_path: Path,
) -> None:
    output = tmp_path / "recordings" / "silent.jsonl"

    async def handler(websocket: ServerConnection) -> None:
        for _ in range(3):
            await websocket.recv()
        await websocket.send(json.dumps(silent_trades_message()))
        await asyncio.sleep(1.0)

    async with serve(handler, "127.0.0.1", 0) as server:
        sockets = server.sockets
        assert sockets
        port = sockets[0].getsockname()[1]
        provider = CoinbaseResearchMarketDataProvider(
            CoinbaseResearchConfig(
                url=f"ws://127.0.0.1:{port}",
                use_mode="personal_research",
                market_data_terms_accepted=True,
                environment="research",
                message_idle_timeout_seconds=0.2,
            )
        )
        result = await capture_provider_session(
            provider,
            symbols=["BTC-USD"],
            output=output,
            max_records=0,
            max_seconds=60.0,
        )

    assert result.records_written == 1
    assert result.complete is False
    assert result.completion_reason == "stream_ended"
    assert result.diagnostic is not None
    verification = verify_jsonl_ledger(output)
    assert verification.records == 1
