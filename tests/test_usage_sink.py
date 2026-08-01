import os

from smart_market_data_gateway.storage import RedisStore
from smart_market_data_gateway.usage_sink import UsageSink


async def test_usage_sink_persists_idempotently_to_postgres(
    redis_client,
    test_settings,
) -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    config = test_settings.model_copy(update={"database_url": database_url})
    sink = UsageSink(redis_client, config)
    await sink.start()
    assert sink.pool is not None

    async with sink.pool.acquire() as connection:
        await connection.execute("TRUNCATE TABLE usage_records")

    store = RedisStore(redis_client, config)
    assert await store.record_usage(
        idempotency_key="billing-1",
        client_id="client-1",
        event_type="premium_quote",
        quantity=3,
        metadata={"symbols": ["AAPL", "TSLA", "NVDA"]},
    ) is True

    messages = await store.read_group(
        config.usage_stream,
        config.usage_group,
        sink.consumer_name,
        block_ms=10,
    )
    assert len(messages) == 1
    await sink._persist(*messages[0])

    async with sink.pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT client_id, event_type, quantity, metadata FROM usage_records WHERE idempotency_key = $1",
            "billing-1",
        )
    assert row is not None
    assert row["client_id"] == "client-1"
    assert row["event_type"] == "premium_quote"
    assert row["quantity"] == 3
    assert row["metadata"]["symbols"] == ["AAPL", "TSLA", "NVDA"]

    await sink.close()
