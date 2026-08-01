from datetime import UTC, datetime, timedelta
from decimal import Decimal
import os
from uuid import UUID

from smart_market_data_gateway.candles import CandleBuilder
from smart_market_data_gateway.domain import AcceptedQuoteEvent, DataQualityMetadata, QuoteEvent
from smart_market_data_gateway.history import HistorySink
from smart_market_data_gateway.storage import RedisStore


def accepted_event(event_id: int, timestamp: datetime, price: str) -> AcceptedQuoteEvent:
    event = QuoteEvent(
        event_id=UUID(int=event_id),
        symbol="AAPL",
        price=Decimal(price),
        provider_timestamp=timestamp,
        received_at=timestamp + timedelta(milliseconds=10),
        sequence=event_id,
        provider="test-provider",
    )
    return AcceptedQuoteEvent(
        event=event,
        quality=DataQualityMetadata(
            score=1.0,
            source_provider=event.provider,
            accepted_at=event.received_at,
        ),
        data_cutoff=event.provider_timestamp,
        source_stream_id=f"{event_id}-0",
    )


async def test_history_sink_persists_events_and_finalized_candles(
    redis_client,
    test_settings,
) -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    config = test_settings.model_copy(
        update={
            "database_url": database_url,
            "candle_intervals_seconds": "1",
            "candle_allowed_lateness_seconds": 0.0,
            "enable_history_retention": False,
        }
    )
    sink = HistorySink(redis_client, config)
    await sink.start()
    assert sink.pool is not None

    async with sink.pool.acquire() as connection:
        await connection.execute("TRUNCATE TABLE late_quote_events, candles, quote_events")
    sink.builder = CandleBuilder((1,), allowed_lateness_seconds=0)

    start = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    store = RedisStore(redis_client, config)
    events = [
        accepted_event(1, start + timedelta(milliseconds=100), "100"),
        accepted_event(2, start + timedelta(milliseconds=700), "102"),
        accepted_event(3, start + timedelta(seconds=2), "101"),
    ]
    for event in events:
        await store.publish_accepted_event(event)

    messages = await store.read_group(
        config.accepted_event_stream,
        config.history_group,
        sink.consumer_name,
        block_ms=10,
    )
    assert len(messages) == 3
    for message in messages:
        await sink._persist(*message)

    async with sink.pool.acquire() as connection:
        event_count = await connection.fetchval("SELECT COUNT(*) FROM quote_events")
        candle = await connection.fetchrow(
            """
            SELECT open, high, low, close, event_count, finalized
            FROM candles
            WHERE symbol = 'AAPL' AND interval_seconds = 1 AND bucket_start = $1
            """,
            start,
        )
    assert event_count == 3
    assert candle is not None
    assert candle["open"] == Decimal("100")
    assert candle["high"] == Decimal("102")
    assert candle["low"] == Decimal("100")
    assert candle["close"] == Decimal("102")
    assert candle["event_count"] == 2
    assert candle["finalized"] is True

    await sink.close()
