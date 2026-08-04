from __future__ import annotations

from collections.abc import AsyncIterator, Collection
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.providers.base import (
    MarketDataProvider,
    ProviderHealth,
    ProviderState,
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

    @property
    def name(self) -> str:
        return "finite-provider"

    async def connect(self) -> None:
        self._state = ProviderState.CONNECTED

    async def disconnect(self) -> None:
        self._state = ProviderState.DISCONNECTED

    async def subscribe(self, symbols: Collection[str]) -> None:
        self.subscriptions = tuple(sorted(symbols))

    async def unsubscribe(self, symbols: Collection[str]) -> None:
        self.subscriptions = ()

    async def health(self) -> ProviderHealth:
        return ProviderHealth(state=self._state)

    async def events(self) -> AsyncIterator[QuoteEvent]:
        for event in self._events:
            yield event


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
    with pytest.raises(RuntimeError, match="no market-data events"):
        await capture_provider_session(
            FiniteProvider([]),
            symbols=["BTC-USD"],
            output=tmp_path / "recordings" / "empty.jsonl",
            max_records=1,
            max_seconds=1.0,
        )
