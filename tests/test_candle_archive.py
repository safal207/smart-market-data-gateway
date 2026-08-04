from datetime import UTC, datetime, timedelta
from decimal import Decimal
import os
from uuid import UUID

from smart_market_data_gateway.candle_archive import (
    CandleArchiveSink,
    HybridCandleHistoryStore,
    PostgresCandleArchive,
)
from smart_market_data_gateway.candles import Candle, CandleSeries
from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.storage import RedisStore


def quote(
    *,
    event_id: str,
    timestamp: datetime,
    price: str,
    sequence: int,
) -> QuoteEvent:
    value = Decimal(price)
    return QuoteEvent(
        event_id=UUID(event_id),
        symbol="AAPL",
        price=value,
        bid=value - Decimal("0.01"),
        ask=value + Decimal("0.01"),
        provider_timestamp=timestamp,
        received_at=timestamp,
        sequence=sequence,
        provider="archive-test",
    )


async def test_archive_is_idempotent_and_preserves_out_of_order_ohlc(test_settings) -> None:
    config = test_settings.model_copy(
        update={
            "database_url": os.environ["TEST_DATABASE_URL"],
            "candle_archive_enabled": True,
        }
    )
    archive = PostgresCandleArchive(config)
    await archive.start()
    assert archive.pool is not None
    async with archive.pool.acquire() as connection:
        await connection.execute("TRUNCATE TABLE candle_archive_events, candle_archive_1m")

    bucket = datetime(2026, 8, 4, 10, 0, tzinfo=UTC)
    later = quote(
        event_id="00000000-0000-0000-0000-000000000003",
        timestamp=bucket + timedelta(seconds=40),
        price="101",
        sequence=3,
    )
    earlier = quote(
        event_id="00000000-0000-0000-0000-000000000002",
        timestamp=bucket + timedelta(seconds=5),
        price="100",
        sequence=1,
    )
    middle = quote(
        event_id="00000000-0000-0000-0000-000000000001",
        timestamp=bucket + timedelta(seconds=20),
        price="105",
        sequence=2,
    )
    next_minute = quote(
        event_id="00000000-0000-0000-0000-000000000004",
        timestamp=bucket + timedelta(minutes=2, seconds=10),
        price="98",
        sequence=4,
    )

    assert await archive.persist_event(later) is True
    assert await archive.persist_event(earlier) is True
    assert await archive.persist_event(middle) is True
    assert await archive.persist_event(middle) is False
    assert await archive.persist_event(next_minute) is True

    one_minute = await archive.get_candles(
        "aapl",
        timeframe="1m",
        limit=5,
        end=bucket + timedelta(minutes=1),
    )
    assert one_minute.returned_count == 1
    candle = one_minute.data[0]
    assert candle.open == Decimal("100")
    assert candle.high == Decimal("105")
    assert candle.low == Decimal("100")
    assert candle.close == Decimal("101")
    assert candle.activity_count == 3

    five_minute = await archive.get_candles(
        "AAPL",
        timeframe="5m",
        limit=1,
        end=bucket + timedelta(minutes=5),
    )
    assert five_minute.returned_count == 1
    aggregate = five_minute.data[0]
    assert aggregate.open == Decimal("100")
    assert aggregate.high == Decimal("105")
    assert aggregate.low == Decimal("98")
    assert aggregate.close == Decimal("98")
    assert aggregate.activity_count == 4

    await archive.close()


async def test_archive_sink_reads_quote_stream_in_its_own_group(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={
            "database_url": os.environ["TEST_DATABASE_URL"],
            "candle_archive_enabled": True,
            "candle_archive_group": "test-candle-archive-group",
        }
    )
    store = RedisStore(redis_client, config)
    event = quote(
        event_id="00000000-0000-0000-0000-000000000010",
        timestamp=datetime(2026, 8, 4, 11, 0, 5, tzinfo=UTC),
        price="123.45",
        sequence=10,
    )
    await store.publish_quote(event)

    sink = CandleArchiveSink(redis_client, config)
    await sink.start()
    assert sink.archive.pool is not None
    async with sink.archive.pool.acquire() as connection:
        await connection.execute("TRUNCATE TABLE candle_archive_events, candle_archive_1m")

    messages = await store.read_group(
        config.quote_stream,
        config.candle_archive_group,
        sink.consumer_name,
        block_ms=10,
    )
    assert len(messages) == 1
    await sink._persist(*messages[0])

    async with sink.archive.pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT symbol, open, close, activity_count FROM candle_archive_1m"
        )
    assert row is not None
    assert row["symbol"] == "AAPL"
    assert row["open"] == Decimal("123.45")
    assert row["close"] == Decimal("123.45")
    assert row["activity_count"] == 1
    await sink.close()


async def test_hybrid_history_overlays_recent_hot_candles(test_settings) -> None:
    now = datetime.now(UTC)
    old_start = now - timedelta(days=40)
    recent_start = now - timedelta(minutes=2)
    archive_series = series(
        candles=[
            candle(old_start, "90"),
            candle(recent_start, "100"),
        ],
        end=now,
        retention_seconds=5 * 365 * 24 * 60 * 60,
    )
    hot_series = series(
        candles=[candle(recent_start, "110")],
        end=now,
        retention_seconds=test_settings.candle_history_retention_seconds,
    )
    history = HybridCandleHistoryStore(
        StaticHistory(hot_series),
        StaticHistory(archive_series),
        test_settings,
    )

    result = await history.get_candles(
        "AAPL",
        timeframe="1m",
        limit=100,
        end=now,
    )

    assert [item.close for item in result.data] == [Decimal("90"), Decimal("110")]
    assert result.retention_seconds == test_settings.candle_archive_retention_seconds
    assert "Complete recent buckets are overlaid from the Redis hot layer." in result.warnings


class StaticHistory:
    def __init__(self, result: CandleSeries) -> None:
        self.result = result

    async def get_candles(self, *args, **kwargs) -> CandleSeries:
        return self.result


def candle(open_time: datetime, price: str) -> Candle:
    value = Decimal(price)
    return Candle(
        symbol="AAPL",
        timeframe="1m",
        open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        open=value,
        high=value,
        low=value,
        close=value,
        activity_count=1,
        first_observation=open_time + timedelta(seconds=1),
        last_observation=open_time + timedelta(seconds=1),
        closed=True,
    )


def series(
    *,
    candles: list[Candle],
    end: datetime,
    retention_seconds: int,
) -> CandleSeries:
    return CandleSeries(
        symbol="AAPL",
        timeframe="1m",
        requested_limit=100,
        returned_count=len(candles),
        retention_seconds=retention_seconds,
        period_start=min(item.open_time for item in candles),
        period_end=end,
        data=candles,
    )
