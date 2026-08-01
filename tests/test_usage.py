import asyncio

from smart_market_data_gateway.storage import RedisStore
from smart_market_data_gateway.usage import UsageRecorder


async def test_usage_recorder_persists_idempotently_off_request_path(
    redis_client,
    test_settings,
) -> None:
    store = RedisStore(redis_client, test_settings)
    recorder = UsageRecorder(store, max_queue_size=2)
    worker = asyncio.create_task(recorder.run())

    assert recorder.record(
        idempotency_key="usage-1",
        client_id="client-1",
        event_type="quote_read",
        metadata={"symbol": "AAPL"},
    ) is True
    assert recorder.record(
        idempotency_key="usage-1",
        client_id="client-1",
        event_type="quote_read",
        metadata={"symbol": "AAPL"},
    ) is True

    await recorder.close(timeout_seconds=1)
    await worker

    entries = await redis_client.xrange(test_settings.usage_stream)
    assert len(entries) == 1
