import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import os
from uuid import UUID

import pytest

from smart_market_data_gateway.domain import AcceptedQuoteEvent, DataQualityMetadata, QuoteEvent
from smart_market_data_gateway.history import HistorySink, _parse_stream_order
from smart_market_data_gateway.storage import RedisStore


def database_url() -> str:
    value = os.getenv("TEST_DATABASE_URL")
    if not value:
        pytest.skip("TEST_DATABASE_URL is required for history recovery tests")
    return value


def accepted_event(event_id: int) -> AcceptedQuoteEvent:
    timestamp = datetime(2026, 8, 1, 17, 0, tzinfo=UTC) + timedelta(seconds=event_id)
    event = QuoteEvent(
        event_id=UUID(int=event_id),
        symbol="AAPL",
        price=Decimal("100") + event_id,
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
        source_stream_id=f"{event_id}-7",
    )


def test_parse_stream_order_is_strict() -> None:
    assert _parse_stream_order("123-7") == (123, 7)
    assert _parse_stream_order(None) == (None, None)
    assert _parse_stream_order("not-a-stream-id") == (None, None)
    assert _parse_stream_order("-1-0") == (None, None)


async def test_pending_history_message_is_reclaimed_and_persisted(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={
            "database_url": database_url(),
            "candle_intervals_seconds": "60",
            "candle_allowed_lateness_seconds": 0.0,
            "history_pending_idle_seconds": 0.001,
            "enable_history_retention": False,
        }
    )
    sink = HistorySink(redis_client, config)
    await sink.start()
    assert sink.pool is not None
    async with sink.pool.acquire() as connection:
        await connection.execute(
            "TRUNCATE TABLE accepted_event_integrity, late_quote_events, candles, quote_events"
        )
        await connection.execute(
            """
            UPDATE integrity_chain_heads
            SET chain_sequence = 0, record_hash = NULL, updated_at = NOW()
            WHERE chain_name = 'accepted_quotes'
            """
        )

    store = RedisStore(redis_client, config)
    await store.publish_accepted_event(accepted_event(50))
    abandoned = await store.read_group(
        config.accepted_event_stream,
        config.history_group,
        "abandoned-history-consumer",
        count=1,
        block_ms=10,
    )
    assert len(abandoned) == 1
    await asyncio.sleep(0.01)

    reclaimed = await store.claim_stale(
        config.accepted_event_stream,
        config.history_group,
        sink.consumer_name,
        min_idle_ms=1,
        count=1,
    )
    assert reclaimed == abandoned
    await sink._persist(*reclaimed[0])

    async with sink.pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT source_stream_ms, source_stream_sequence
            FROM quote_events
            WHERE event_id = $1
            """,
            UUID(int=50),
        )
        assert row is not None
        assert row["source_stream_ms"] == 50
        assert row["source_stream_sequence"] == 7
        assert await connection.fetchval("SELECT COUNT(*) FROM accepted_event_integrity") == 1

    pending = await redis_client.xpending(
        config.accepted_event_stream,
        config.history_group,
    )
    pending_count = pending["pending"] if isinstance(pending, dict) else pending[0]
    assert int(pending_count) == 0
    await sink.close()
