from datetime import UTC, datetime, timedelta
from decimal import Decimal

from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.storage import RedisStore


def quote(
    symbol: str = "AAPL",
    sequence: int = 1,
    *,
    timestamp: datetime | None = None,
    price: str = "100.01",
) -> QuoteEvent:
    observed_at = timestamp or datetime.now(UTC)
    value = Decimal(price)
    return QuoteEvent(
        symbol=symbol,
        price=value,
        bid=value - Decimal("0.01"),
        ask=value + Decimal("0.01"),
        provider_timestamp=observed_at,
        received_at=observed_at,
        sequence=sequence,
        provider="test-provider",
    )


async def test_latest_cache_dedupe_and_sequence(redis_client, test_settings) -> None:
    store = RedisStore(redis_client, test_settings)
    event = quote()

    await store.cache_quote(event)
    snapshot = await store.get_latest("aapl")
    assert snapshot is not None
    assert snapshot.quote == event
    assert snapshot.stale is False

    assert await store.accept_event_once(event) is True
    assert await store.accept_event_once(event) is False

    assert await store.observe_sequence(event) is None
    gap = await store.observe_sequence(quote(sequence=3))
    assert gap is not None
    assert gap.gap == 1
    assert gap.out_of_order is False

    out_of_order = await store.observe_sequence(quote(sequence=2))
    assert out_of_order is not None
    assert out_of_order.out_of_order is True


async def test_stale_snapshot_and_many(redis_client, test_settings) -> None:
    store = RedisStore(redis_client, test_settings)
    stale = quote().model_copy(
        update={
            "received_at": datetime.now(UTC) - timedelta(seconds=10),
            "provider_timestamp": datetime.now(UTC) - timedelta(seconds=10),
        }
    )
    await store.cache_quote(stale)
    result = await store.get_many(["AAPL", "UNKNOWN"])
    assert result["AAPL"] is not None
    assert result["AAPL"].stale is True
    assert result["UNKNOWN"] is None


async def test_server_side_candle_history(redis_client, test_settings) -> None:
    store = RedisStore(redis_client, test_settings)
    now = datetime.now(UTC)
    current_five_minute = datetime.fromtimestamp(
        int(now.timestamp()) - (int(now.timestamp()) % 300),
        tz=UTC,
    )
    start = current_five_minute - timedelta(minutes=10)
    events = [
        quote(sequence=1, timestamp=start + timedelta(seconds=5), price="100"),
        quote(sequence=2, timestamp=start + timedelta(seconds=40), price="103"),
        quote(sequence=3, timestamp=start + timedelta(minutes=2, seconds=5), price="101"),
        quote(sequence=4, timestamp=start + timedelta(minutes=2, seconds=40), price="98"),
    ]
    for event in events:
        await store.cache_quote(event)

    series = await store.get_candles(
        "aapl",
        timeframe="5m",
        limit=2,
        end=start + timedelta(minutes=5),
    )

    assert series.returned_count == 1
    candle = series.data[0]
    assert candle.open == Decimal("100")
    assert candle.high == Decimal("103")
    assert candle.low == Decimal("98")
    assert candle.close == Decimal("98")
    assert candle.activity_count == 4
    assert candle.closed is True


async def test_bucket_retention_uses_minute_deadline(
    redis_client,
    test_settings,
    monkeypatch,
) -> None:
    retention_settings = test_settings.model_copy(
        update={"candle_history_retention_seconds": 60}
    )
    store = RedisStore(redis_client, retention_settings)
    first_now = datetime(2026, 8, 4, 12, 0, 30, tzinfo=UTC)
    monkeypatch.setattr(
        "smart_market_data_gateway.storage._utc_now",
        lambda: first_now,
    )
    event = quote(
        timestamp=datetime(2026, 8, 4, 11, 59, 45, tzinfo=UTC),
        price="101",
    )

    await store.cache_quote(event)
    readable = await store.get_candles(
        "AAPL",
        timeframe="1m",
        limit=1,
        end=datetime(2026, 8, 4, 12, 0, tzinfo=UTC),
    )
    assert readable.returned_count == 1

    after_deadline = datetime(2026, 8, 4, 12, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(
        "smart_market_data_gateway.storage._utc_now",
        lambda: after_deadline,
    )
    expired = await store.get_candles(
        "AAPL",
        timeframe="1m",
        limit=2,
        end=datetime(2026, 8, 4, 12, 1, tzinfo=UTC),
    )
    bucket_epoch = str(int(datetime(2026, 8, 4, 11, 59, tzinfo=UTC).timestamp()))
    index_key = "smdg:candles:index:v1:1m:AAPL"

    assert expired.returned_count == 0
    assert await redis_client.zscore(index_key, bucket_epoch) is None


async def test_stream_retry_dlq_rate_limit_and_usage(redis_client, test_settings) -> None:
    store = RedisStore(redis_client, test_settings)
    await store.ensure_groups()
    event = quote()
    stream_id = await store.publish_quote(event)
    messages = await store.read_group(
        test_settings.quote_stream,
        test_settings.stream_group,
        "test-consumer",
        block_ms=10,
    )
    assert messages[0][0] == stream_id
    assert "payload" in messages[0][1]

    assert await store.increment_retry(stream_id) == 1
    await store.move_to_dead_letter(
        source_stream=test_settings.quote_stream,
        stream_id=stream_id,
        payload=messages[0][1],
        error="boom",
        retry_count=3,
    )
    assert len(await redis_client.xrange(test_settings.dead_letter_stream)) == 1

    assert await store.rate_limit("client", 2) is True
    assert await store.rate_limit("client", 2) is True
    assert await store.rate_limit("client", 2) is False

    assert await store.record_usage(
        idempotency_key="usage-1",
        client_id="client-1",
        event_type="quote_read",
    ) is True
    assert await store.record_usage(
        idempotency_key="usage-1",
        client_id="client-1",
        event_type="quote_read",
    ) is False
    assert len(await redis_client.xrange(test_settings.usage_stream)) == 1

    await store.ack(test_settings.quote_stream, test_settings.stream_group, stream_id)
