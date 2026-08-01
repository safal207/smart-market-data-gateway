import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.pipeline import QuoteProcessor
from smart_market_data_gateway.storage import RedisStore


def quote(
    *,
    symbol: str = "AAPL",
    sequence: int | None = 1,
    provider_timestamp: datetime,
    price: str = "100.00",
) -> QuoteEvent:
    return QuoteEvent(
        event_id=uuid4(),
        symbol=symbol,
        price=Decimal(price),
        provider_timestamp=provider_timestamp,
        received_at=provider_timestamp,
        sequence=sequence,
        provider="pipeline-test",
    )


async def pending_message(
    store: RedisStore,
    test_settings,
    event: QuoteEvent,
) -> tuple[str, dict[str, str]]:
    await store.ensure_groups()
    stream_id = await store.publish_quote(event)
    messages = await store.read_group(
        test_settings.quote_stream,
        test_settings.stream_group,
        "pipeline-test-consumer",
        block_ms=10,
    )
    assert messages == [(stream_id, {"payload": event.model_dump_json()})]
    return messages[0]


async def test_processor_quarantines_out_of_order_without_regressing_latest(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={"quote_timestamp_regression_tolerance_seconds": 0.1}
    )
    store = RedisStore(redis_client, config)
    processor = QuoteProcessor(store, config, GatewayMetrics())
    now = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)

    current = quote(sequence=2, provider_timestamp=now, price="102.00")
    stream_id, fields = await pending_message(store, config, current)
    await processor._process_one(stream_id, fields)

    late = quote(
        sequence=1,
        provider_timestamp=now - timedelta(seconds=5),
        price="99.00",
    )
    stream_id, fields = await pending_message(store, config, late)
    await processor._process_one(stream_id, fields)

    snapshot = await store.get_latest("AAPL")
    assert snapshot is not None
    assert snapshot.quote.event_id == current.event_id
    quarantined = await redis_client.xrange(config.quarantine_stream)
    assert len(quarantined) == 1
    assert quarantined[0][1]["event_id"] == str(late.event_id)
    assert quarantined[0][1]["reason"] == "out_of_order_and_timestamp_regression"


async def test_processor_guards_timestamp_when_provider_has_no_sequence(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={"quote_timestamp_regression_tolerance_seconds": 0.1}
    )
    store = RedisStore(redis_client, config)
    processor = QuoteProcessor(store, config, GatewayMetrics())
    now = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)

    current = quote(symbol="MSFT", sequence=None, provider_timestamp=now, price="202.00")
    stream_id, fields = await pending_message(store, config, current)
    await processor._process_one(stream_id, fields)
    regressed = quote(
        symbol="MSFT",
        sequence=None,
        provider_timestamp=now - timedelta(seconds=2),
        price="198.00",
    )
    stream_id, fields = await pending_message(store, config, regressed)
    await processor._process_one(stream_id, fields)

    snapshot = await store.get_latest("MSFT")
    assert snapshot is not None
    assert snapshot.quote.event_id == current.event_id
    quarantined = await redis_client.xrange(config.quarantine_stream)
    assert quarantined[0][1]["reason"] == "timestamp_regression"


async def test_processing_failure_can_retry_before_dedupe_is_committed(
    redis_client,
    test_settings,
    monkeypatch,
) -> None:
    store = RedisStore(redis_client, test_settings)
    processor = QuoteProcessor(store, test_settings, GatewayMetrics())
    event = quote(
        provider_timestamp=datetime(2026, 8, 1, 12, 0, tzinfo=UTC),
        price="101.00",
    )
    stream_id, fields = await pending_message(store, test_settings, event)
    real_process_quote = store.process_quote
    attempts = 0

    async def fail_once(event_to_process, *, source_stream_id: str = ""):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("injected atomic effect failure")
        return await real_process_quote(
            event_to_process,
            source_stream_id=source_stream_id,
        )

    monkeypatch.setattr(store, "process_quote", fail_once)
    await processor._process_one(stream_id, fields)
    assert await redis_client.get(f"smdg:dedupe:{event.event_id}") is None
    assert await store.pending_count() == 1

    await processor._process_one(stream_id, fields)
    snapshot = await store.get_latest(event.symbol)
    assert snapshot is not None
    assert snapshot.quote.event_id == event.event_id
    assert await redis_client.get(f"smdg:dedupe:{event.event_id}") == "accepted"
    assert await store.pending_count() == 0


async def test_run_reclaims_failed_pending_entry_while_new_quotes_keep_arriving(
    redis_client,
    test_settings,
    monkeypatch,
) -> None:
    store = RedisStore(redis_client, test_settings)
    processor = QuoteProcessor(store, test_settings, GatewayMetrics())
    processor._pending_min_idle_ms = 20
    processor._claim_interval_seconds = 0.01
    target = quote(
        symbol="TARGET",
        provider_timestamp=datetime(2026, 8, 1, 12, 0, tzinfo=UTC),
        price="101.00",
    )
    await store.publish_quote(target)

    real_process_quote = store.process_quote
    target_attempts = 0

    async def fail_target_once(event_to_process, *, source_stream_id: str = ""):
        nonlocal target_attempts
        if event_to_process.event_id == target.event_id:
            target_attempts += 1
            if target_attempts == 1:
                raise RuntimeError("transient target failure")
        return await real_process_quote(
            event_to_process,
            source_stream_id=source_stream_id,
        )

    monkeypatch.setattr(store, "process_quote", fail_target_once)
    stop_producer = asyncio.Event()

    async def publish_continuously() -> None:
        ordinal = 0
        base_timestamp = datetime(2026, 8, 1, 12, 1, tzinfo=UTC)
        while not stop_producer.is_set():
            ordinal += 1
            await store.publish_quote(
                quote(
                    symbol="FEED",
                    sequence=ordinal,
                    provider_timestamp=base_timestamp + timedelta(milliseconds=ordinal),
                    price="102.00",
                )
            )
            await asyncio.sleep(0.005)

    producer_task = asyncio.create_task(publish_continuously())
    processor_task = asyncio.create_task(processor.run())
    try:
        for _ in range(200):
            if await redis_client.get(f"smdg:dedupe:{target.event_id}") == "accepted":
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("failed pending quote was not reclaimed under continuous traffic")
    finally:
        stop_producer.set()
        await processor.close()
        producer_task.cancel()
        processor_task.cancel()
        await asyncio.gather(producer_task, processor_task, return_exceptions=True)

    assert target_attempts == 2
