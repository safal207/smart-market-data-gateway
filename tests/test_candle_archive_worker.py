import asyncio
from datetime import UTC, datetime
from decimal import Decimal
import os
from uuid import UUID

import pytest

from smart_market_data_gateway.candle_archive_worker import RecoveringCandleArchiveSink
from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.storage import RedisStore


def archive_event(event_id: str = "00000000-0000-0000-0000-000000000020") -> QuoteEvent:
    value = Decimal("222.25")
    timestamp = datetime(2026, 8, 4, 11, 30, 5, tzinfo=UTC)
    return QuoteEvent(
        event_id=UUID(event_id),
        symbol="AAPL",
        price=value,
        bid=value - Decimal("0.01"),
        ask=value + Decimal("0.01"),
        provider_timestamp=timestamp,
        received_at=timestamp,
        sequence=20,
        provider="archive-recovery-test",
    )


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

    store = RedisStore(redis_client, config)
    stream_id = await store.publish_quote(archive_event())
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
    assert await pending(redis_client, config) == []
    await sink.close()


async def test_archive_worker_keeps_transient_database_failure_pending(
    redis_client,
    test_settings,
    monkeypatch,
) -> None:
    config = test_settings.model_copy(
        update={
            "database_url": os.environ["TEST_DATABASE_URL"],
            "candle_archive_enabled": True,
            "candle_archive_group": "test-transient-candle-archive",
        }
    )
    sink = RecoveringCandleArchiveSink(redis_client, config)
    await sink.start()
    store = RedisStore(redis_client, config)
    stream_id = await store.publish_quote(
        archive_event("00000000-0000-0000-0000-000000000021")
    )
    messages = await store.read_group(
        config.quote_stream,
        config.candle_archive_group,
        sink.consumer_name,
        block_ms=10,
    )

    async def unavailable(_event: QuoteEvent) -> bool:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(sink.archive, "persist_event", unavailable)
    with pytest.raises(RuntimeError, match="database unavailable"):
        await sink._persist(*messages[0])

    assert [item["message_id"] for item in await pending(redis_client, config)] == [stream_id]
    assert await redis_client.xlen(config.dead_letter_stream) == 0
    retry_key = f"smdg:retry:archive:{config.quote_stream}:{stream_id}"
    assert await redis_client.get(retry_key) is None
    await sink.close()


async def test_archive_worker_dead_letters_only_repeated_invalid_payloads(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={
            "database_url": os.environ["TEST_DATABASE_URL"],
            "candle_archive_enabled": True,
            "candle_archive_group": "test-invalid-candle-archive",
            "retry_limit": 2,
        }
    )
    sink = RecoveringCandleArchiveSink(redis_client, config)
    await sink.start()
    stream_id = str(await redis_client.xadd(config.quote_stream, {"payload": "not-json"}))
    store = RedisStore(redis_client, config)
    messages = await store.read_group(
        config.quote_stream,
        config.candle_archive_group,
        sink.consumer_name,
        block_ms=10,
    )

    with pytest.raises(ValueError):
        await sink._persist(*messages[0])
    assert [item["message_id"] for item in await pending(redis_client, config)] == [stream_id]

    await sink._persist(*messages[0])
    assert await pending(redis_client, config) == []
    assert await redis_client.xlen(config.dead_letter_stream) == 1
    await sink.close()


async def test_archive_worker_advances_the_autoclaim_scan_cursor(test_settings) -> None:
    fake_redis = CursorRedis()
    sink = object.__new__(RecoveringCandleArchiveSink)
    sink.redis = fake_redis
    sink.config = test_settings
    sink.consumer_name = "cursor-test"
    sink._claim_cursor = "0-0"

    assert await sink._claim_stale() == []
    assert sink._claim_cursor == "500-0"
    assert await sink._claim_stale() == []
    assert sink._claim_cursor == "0-0"
    assert fake_redis.start_ids == ["0-0", "500-0"]


class CursorRedis:
    def __init__(self) -> None:
        self.start_ids: list[str] = []
        self.responses = iter((("500-0", []), ("0-0", [])))

    async def xautoclaim(self, *args, **kwargs):
        self.start_ids.append(str(kwargs["start_id"]))
        return next(self.responses)


async def pending(redis_client, config) -> list[dict[str, object]]:
    return await redis_client.xpending_range(
        config.quote_stream,
        config.candle_archive_group,
        min="-",
        max="+",
        count=10,
    )
