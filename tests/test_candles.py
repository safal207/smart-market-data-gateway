from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from smart_market_data_gateway.candles import CandleBuilder
from smart_market_data_gateway.domain import AcceptedQuoteEvent, DataQualityMetadata, QuoteEvent


def accepted_event(
    *,
    event_id: int,
    timestamp: datetime,
    price: str,
    sequence: int,
    quality: float = 1.0,
    symbol: str = "AAPL",
) -> AcceptedQuoteEvent:
    event = QuoteEvent(
        event_id=UUID(int=event_id),
        symbol=symbol,
        price=Decimal(price),
        bid=Decimal(price) - Decimal("0.01"),
        ask=Decimal(price) + Decimal("0.01"),
        provider_timestamp=timestamp,
        received_at=timestamp + timedelta(milliseconds=10),
        sequence=sequence,
        provider="test-provider",
    )
    return AcceptedQuoteEvent(
        event=event,
        quality=DataQualityMetadata(
            score=quality,
            source_provider=event.provider,
            accepted_at=event.received_at,
        ),
        data_cutoff=event.provider_timestamp,
        source_stream_id=f"{sequence}-0",
    )


def test_candle_builder_finalizes_deterministic_ohlc() -> None:
    start = datetime(2026, 8, 1, 12, 0, 0, 100_000, tzinfo=UTC)
    builder = CandleBuilder((1,), allowed_lateness_seconds=1)

    builder.add(accepted_event(event_id=1, timestamp=start, price="100", sequence=1))
    builder.add(
        accepted_event(
            event_id=2,
            timestamp=start + timedelta(milliseconds=700),
            price="102",
            sequence=2,
            quality=0.8,
        )
    )
    result = builder.add(
        accepted_event(
            event_id=3,
            timestamp=start + timedelta(seconds=3),
            price="101",
            sequence=3,
        )
    )

    assert len(result.finalized) == 1
    candle = result.finalized[0]
    assert candle.bucket_start == datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)
    assert candle.open == Decimal("100")
    assert candle.high == Decimal("102")
    assert candle.low == Decimal("100")
    assert candle.close == Decimal("102")
    assert candle.event_count == 2
    assert candle.quality_score == 0.8


def test_candle_open_and_close_follow_event_time_not_arrival_order() -> None:
    start = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)
    builder = CandleBuilder((10,), allowed_lateness_seconds=20)

    builder.add(
        accepted_event(
            event_id=2,
            timestamp=start + timedelta(seconds=8),
            price="108",
            sequence=2,
        )
    )
    builder.add(
        accepted_event(
            event_id=1,
            timestamp=start + timedelta(seconds=1),
            price="101",
            sequence=1,
        )
    )

    candle = builder.flush()[0]
    assert candle.open == Decimal("101")
    assert candle.close == Decimal("108")
    assert candle.first_event_time == start + timedelta(seconds=1)
    assert candle.last_event_time == start + timedelta(seconds=8)


def test_late_event_does_not_mutate_finalized_candle() -> None:
    start = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)
    builder = CandleBuilder((1,), allowed_lateness_seconds=0)

    builder.add(accepted_event(event_id=1, timestamp=start, price="100", sequence=1))
    finalized = builder.add(
        accepted_event(
            event_id=2,
            timestamp=start + timedelta(seconds=2),
            price="102",
            sequence=2,
        )
    )
    late = builder.add(
        accepted_event(
            event_id=3,
            timestamp=start + timedelta(milliseconds=500),
            price="999",
            sequence=3,
        )
    )

    assert finalized.finalized[0].close == Decimal("100")
    assert late.late_intervals == (1,)
    assert late.finalized == ()


def test_symbol_checkpoint_restores_only_failed_symbol() -> None:
    start = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    builder = CandleBuilder((60,), allowed_lateness_seconds=0)
    builder.add(
        accepted_event(
            event_id=1,
            timestamp=start,
            price="100",
            sequence=1,
            symbol="AAPL",
        )
    )
    builder.add(
        accepted_event(
            event_id=2,
            timestamp=start,
            price="200",
            sequence=2,
            symbol="MSFT",
        )
    )

    checkpoint = builder.checkpoint_symbol("AAPL")
    builder.add(
        accepted_event(
            event_id=3,
            timestamp=start + timedelta(seconds=10),
            price="110",
            sequence=3,
            symbol="AAPL",
        )
    )
    builder.add(
        accepted_event(
            event_id=4,
            timestamp=start + timedelta(seconds=10),
            price="210",
            sequence=4,
            symbol="MSFT",
        )
    )
    builder.restore_symbol(checkpoint)

    candles = {candle.symbol: candle for candle in builder.flush()}
    assert candles["AAPL"].close == Decimal("100")
    assert candles["AAPL"].event_count == 1
    assert candles["MSFT"].close == Decimal("210")
    assert candles["MSFT"].event_count == 2
