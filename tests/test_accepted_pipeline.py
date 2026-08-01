from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from smart_market_data_gateway.domain import AcceptedQuoteEvent, QuoteEvent, RejectedQuoteEvent
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.pipeline import QuoteProcessor
from smart_market_data_gateway.storage import RedisStore


def quote(
    *,
    event_id: int,
    sequence: int,
    provider_timestamp: datetime,
    received_at: datetime | None = None,
) -> QuoteEvent:
    return QuoteEvent(
        event_id=UUID(int=event_id),
        symbol="AAPL",
        price=Decimal("100") + Decimal(sequence),
        bid=Decimal("99"),
        ask=Decimal("101") + Decimal(sequence),
        provider_timestamp=provider_timestamp,
        received_at=received_at or provider_timestamp,
        sequence=sequence,
        provider="test-provider",
    )


async def process_all(store: RedisStore, processor: QuoteProcessor) -> None:
    await store.ensure_groups()
    messages = await store.read_group(
        processor.settings.quote_stream,
        processor.settings.stream_group,
        "test-consumer",
        block_ms=1,
    )
    for stream_id, fields in messages:
        await processor._process_one(stream_id, fields)


async def test_only_one_duplicate_enters_accepted_stream(redis_client, test_settings) -> None:
    store = RedisStore(redis_client, test_settings)
    event = quote(
        event_id=1,
        sequence=1,
        provider_timestamp=datetime(2026, 8, 1, 12, 0, tzinfo=UTC),
    )
    await store.publish_quote(event)
    await store.publish_quote(event)

    processor = QuoteProcessor(store, test_settings, GatewayMetrics())
    await process_all(store, processor)

    accepted_entries = await redis_client.xrange(test_settings.accepted_event_stream)
    rejected_entries = await redis_client.xrange(test_settings.rejected_event_stream)
    assert len(accepted_entries) == 1
    assert len(rejected_entries) == 1
    accepted = AcceptedQuoteEvent.model_validate_json(accepted_entries[0][1]["payload"])
    rejected = RejectedQuoteEvent.model_validate_json(rejected_entries[0][1]["payload"])
    assert accepted.event.event_id == event.event_id
    assert rejected.reason.value == "duplicate"


async def test_out_of_order_event_is_audited_but_not_fanned_out(redis_client, test_settings) -> None:
    store = RedisStore(redis_client, test_settings)
    timestamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    newer = quote(event_id=2, sequence=2, provider_timestamp=timestamp + timedelta(seconds=1))
    older = quote(event_id=1, sequence=1, provider_timestamp=timestamp)
    await store.publish_quote(newer)
    await store.publish_quote(older)

    processor = QuoteProcessor(store, test_settings, GatewayMetrics())
    await process_all(store, processor)

    accepted_entries = await redis_client.xrange(test_settings.accepted_event_stream)
    rejected_entries = await redis_client.xrange(test_settings.rejected_event_stream)
    latest = await store.get_latest("AAPL")
    assert len(accepted_entries) == 1
    assert len(rejected_entries) == 1
    assert latest is not None
    assert latest.quote.sequence == 2
    rejected = RejectedQuoteEvent.model_validate_json(rejected_entries[0][1]["payload"])
    assert rejected.reason.value == "out_of_order"


async def test_stale_event_does_not_enter_history_or_latest_cache(redis_client, test_settings) -> None:
    strict_settings = test_settings.model_copy(update={"accepted_event_max_age_seconds": 1.0})
    store = RedisStore(redis_client, strict_settings)
    timestamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    event = quote(
        event_id=3,
        sequence=1,
        provider_timestamp=timestamp,
        received_at=timestamp + timedelta(seconds=5),
    )
    await store.publish_quote(event)

    processor = QuoteProcessor(store, strict_settings, GatewayMetrics())
    await process_all(store, processor)

    assert await redis_client.xlen(strict_settings.accepted_event_stream) == 0
    assert await redis_client.xlen(strict_settings.rejected_event_stream) == 1
    assert await store.get_latest("AAPL") is None
