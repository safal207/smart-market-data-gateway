import asyncio
from datetime import UTC, datetime
from decimal import Decimal
import os
from uuid import UUID

from smart_market_data_gateway.candle_archive_worker import RecoveringCandleArchiveSink
from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.storage import RedisStore


async def test_archive_worker_reclaims_abandoned_pending_delivery(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={
            "database_url": os.environ["TEST_DATABASE_URL"],
            "candle_archive_enabled": True,
            "candle_archive_group": "test-recovering-candle-archive",
            "candle_archive_claim_idle_ms": 1,
        }
    )
    sink = RecoveringCandleArchiveSink(redis_client, config)
    await sink.start()
    assert sink.archive.pool is not None
    async with sink.archive.pool.acquire() as connection:
        await connection.execute("TRUNCATE TABLE candle_archive_events, candle_archive_1m")

    value = Decimal("222.25")
    event = QuoteEvent(
        event_id=UUID("00000000-0000-0000-0000-000000000020"),
        symbol="AAPL",
        price=value,
        bid=value - Decimal("0.01"),
        ask=value + Decimal("0.01"),
        provider_timestamp=datetime(2026, 8, 4, 11, 30, 5, tzinfo=UTC),
        received_at=datetime(2026, 8, 4, 11, 30, 5, tzinfo=UTC),
        sequence=20,
        provider="archive-recovery-test",
    )
    store = RedisStore(redis_client, config)
    stream_id = await store.publish_quote(event)
    abandoned = await store.read_group(
        config.quote_stream,
        config.candle_archive_group,
        "abandoned-consumer",
        block_ms=10,
    )
    assert abandoned[0][0] == stream_id

    await asyncio.sleep(0.01)
    claimed = await sink._claim_stale()
    assert claimed[0][0] == stream_id
    await sink._persist(*claimed[0])

    async with sink.archive.pool.acquire() as connection:
        count = await connection.fetchval(
            "SELECT activity_count FROM candle_archive_1m WHERE symbol = 'AAPL'"
        )
    assert count == 1
    assert await redis_client.xpending_range(
        config.quote_stream,
        config.candle_archive_group,
        min="-",
        max="+",
        count=10,
    ) == []
    await sink.close()
