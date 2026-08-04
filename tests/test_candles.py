from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from smart_market_data_gateway.candles import (
    BaseMinuteCandle,
    aggregate_base_candles,
)
from smart_market_data_gateway.domain import QuoteEvent


def make_quote(
    *,
    timestamp: datetime,
    price: str,
    event_number: int,
) -> QuoteEvent:
    return QuoteEvent(
        event_id=UUID(int=event_number),
        symbol="AAPL",
        price=Decimal(price),
        provider_timestamp=timestamp,
        received_at=timestamp,
        sequence=event_number,
        provider="candle-test",
    )


def test_minute_candle_uses_event_time_for_open_and_close() -> None:
    minute = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
    candle = BaseMinuteCandle.from_quote(
        make_quote(timestamp=minute + timedelta(seconds=30), price="101", event_number=2)
    )
    candle = candle.with_quote(
        make_quote(timestamp=minute + timedelta(seconds=10), price="99", event_number=1)
    )
    candle = candle.with_quote(
        make_quote(timestamp=minute + timedelta(seconds=50), price="105", event_number=3)
    )

    assert candle.open == Decimal("99")
    assert candle.high == Decimal("105")
    assert candle.low == Decimal("99")
    assert candle.close == Decimal("105")
    assert candle.activity_count == 3


def test_higher_timeframe_aggregates_sparse_observed_minutes() -> None:
    start = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
    first = BaseMinuteCandle.from_quote(
        make_quote(timestamp=start + timedelta(seconds=5), price="100", event_number=1)
    ).with_quote(
        make_quote(timestamp=start + timedelta(seconds=50), price="102", event_number=2)
    )
    third = BaseMinuteCandle.from_quote(
        make_quote(timestamp=start + timedelta(minutes=2, seconds=5), price="101", event_number=3)
    ).with_quote(
        make_quote(timestamp=start + timedelta(minutes=2, seconds=40), price="98", event_number=4)
    )

    series = aggregate_base_candles(
        symbol="AAPL",
        timeframe="5m",
        limit=2,
        end=start + timedelta(minutes=5, seconds=1),
        retention_seconds=86_400,
        base_candles=[third, first],
    )

    assert series.returned_count == 1
    candle = series.data[0]
    assert candle.open == Decimal("100")
    assert candle.high == Decimal("102")
    assert candle.low == Decimal("98")
    assert candle.close == Decimal("98")
    assert candle.activity_count == 4
    assert candle.closed is True
    assert series.source == "observed_quote_aggregation"
    assert "trade volume is unavailable" in series.warnings[0]


def test_current_bucket_is_explicitly_open() -> None:
    start = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
    candle = BaseMinuteCandle.from_quote(
        make_quote(timestamp=start + timedelta(seconds=5), price="100", event_number=1)
    )

    series = aggregate_base_candles(
        symbol="AAPL",
        timeframe="5m",
        limit=1,
        end=start + timedelta(minutes=3),
        retention_seconds=86_400,
        base_candles=[candle],
    )

    assert series.data[0].closed is False
