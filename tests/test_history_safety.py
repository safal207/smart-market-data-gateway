from datetime import UTC, datetime
from decimal import Decimal
import os
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from smart_market_data_gateway.domain import (
    AcceptedQuoteEvent,
    DataQualityMetadata,
    QuoteEvent,
)
from smart_market_data_gateway.history import (
    HistoryOwnershipLost,
    HistorySink,
)
from smart_market_data_gateway.storage import RedisStore


def history_database_url() -> str:
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for history safety tests")
    return database_url


def history_config(test_settings, **updates):
    return test_settings.model_copy(
        update={
            "database_url": history_database_url(),
            "enable_history_retention": False,
            **updates,
        }
    )


def accepted_event(event_id: UUID) -> AcceptedQuoteEvent:
    timestamp = datetime.now(UTC)
    event = QuoteEvent(
        event_id=event_id,
        symbol="AAPL",
        price=Decimal("101.25"),
        provider_timestamp=timestamp,
        received_at=timestamp,
        sequence=1,
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
        source_stream_id="1-0",
    )


async def test_second_history_writer_is_rejected_until_owner_closes(
    redis_client,
    test_settings,
) -> None:
    config = history_config(test_settings)
    owner = HistorySink(redis_client, config)
    contender = HistorySink(redis_client, config)

    await owner.start()
    try:
        with pytest.raises(RuntimeError, match="another history writer is already active"):
            await contender.start()
        assert contender.pool is None
        assert contender._lock_connection is None
    finally:
        await contender.close()
        await owner.close()

    replacement = HistorySink(redis_client, config)
    await replacement.start()
    await replacement.close()


async def test_retention_fails_closed_without_integrity_checkpoints(
    redis_client,
    test_settings,
) -> None:
    unsafe = HistorySink(
        redis_client,
        history_config(test_settings, enable_history_retention=True),
    )

    with pytest.raises(RuntimeError, match="integrity-preserving checkpoints"):
        await unsafe.start()

    assert unsafe.pool is None
    assert unsafe._lock_connection is None

    safe = HistorySink(redis_client, history_config(test_settings))
    await safe.start()
    await safe.close()


async def test_lock_query_failure_is_cleaned_up_and_start_is_retryable(
    redis_client,
    test_settings,
    monkeypatch,
) -> None:
    config = history_config(test_settings)
    sink = HistorySink(redis_client, config)
    real_connect = asyncpg.connect

    class BrokenLockConnection:
        def __init__(self) -> None:
            self.closed = False

        async def fetchval(self, *_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("lock query failed")

        async def execute(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def is_closed(self) -> bool:
            return self.closed

        async def close(self) -> None:
            self.closed = True

    broken = BrokenLockConnection()

    async def broken_connect(*_args: Any, **_kwargs: Any) -> BrokenLockConnection:
        return broken

    monkeypatch.setattr(
        "smart_market_data_gateway.history.asyncpg.connect",
        broken_connect,
    )
    with pytest.raises(RuntimeError, match="lock query failed"):
        await sink.start()

    assert broken.closed is True
    assert sink.pool is None
    assert sink._lock_connection is None

    monkeypatch.setattr(
        "smart_market_data_gateway.history.asyncpg.connect",
        real_connect,
    )
    await sink.start()
    await sink.close()


async def test_lost_lock_session_blocks_persistence_and_allows_replacement(
    redis_client,
    test_settings,
) -> None:
    config = history_config(test_settings)
    owner = HistorySink(redis_client, config)
    await owner.start()
    assert owner._lock_connection is not None
    assert owner.pool is not None

    event_id = uuid4()
    store = RedisStore(redis_client, config)
    await store.publish_accepted_event(accepted_event(event_id))
    messages = await store.read_group(
        config.accepted_event_stream,
        config.history_group,
        owner.consumer_name,
        count=1,
        block_ms=10,
    )
    assert len(messages) == 1

    await owner._lock_connection.close()
    replacement = HistorySink(redis_client, config)
    await replacement.start()
    try:
        with pytest.raises(HistoryOwnershipLost):
            await owner._persist(*messages[0])

        assert replacement.pool is not None
        async with replacement.pool.acquire() as connection:
            assert await connection.fetchval(
                "SELECT COUNT(*) FROM quote_events WHERE event_id = $1",
                event_id,
            ) == 0

        pending = await redis_client.xpending(
            config.accepted_event_stream,
            config.history_group,
        )
        pending_count = pending["pending"] if isinstance(pending, dict) else pending[0]
        assert int(pending_count) == 1
    finally:
        await replacement.close()
        await owner.close()


async def test_transient_database_failures_remain_pending_without_dlq(
    redis_client,
    test_settings,
    monkeypatch,
) -> None:
    config = history_config(test_settings, retry_limit=2)
    sink = HistorySink(redis_client, config)
    await sink.start()
    assert sink.pool is not None

    event_id = uuid4()
    store = RedisStore(redis_client, config)
    await store.publish_accepted_event(accepted_event(event_id))
    messages = await store.read_group(
        config.accepted_event_stream,
        config.history_group,
        sink.consumer_name,
        count=1,
        block_ms=10,
    )
    assert len(messages) == 1

    async def fail_insert(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("database temporarily unavailable")

    monkeypatch.setattr(sink, "_insert_event", fail_insert)
    try:
        for _attempt in range(config.retry_limit + 2):
            with pytest.raises(RuntimeError, match="temporarily unavailable"):
                await sink._persist(*messages[0])

        assert await redis_client.xlen(config.dead_letter_stream) == 0
        pending = await redis_client.xpending(
            config.accepted_event_stream,
            config.history_group,
        )
        pending_count = pending["pending"] if isinstance(pending, dict) else pending[0]
        assert int(pending_count) == 1

        async with sink.pool.acquire() as connection:
            assert await connection.fetchval(
                "SELECT COUNT(*) FROM quote_events WHERE event_id = $1",
                event_id,
            ) == 0
    finally:
        await sink.close()
