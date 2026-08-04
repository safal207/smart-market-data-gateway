from prometheus_client import generate_latest

from smart_market_data_gateway.archive_observability import (
    CandleArchiveMetrics,
    CandleArchiveMonitor,
    collect_candle_archive_consumer_health,
)


async def test_archive_health_tracks_missing_group_pending_and_undelivered(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={
            "stream_maxlen": 10,
            "candle_archive_group": "test-candle-archive-observability",
        }
    )
    for index in range(3):
        await redis_client.xadd(config.quote_stream, {"payload": str(index)})

    missing = await collect_candle_archive_consumer_health(redis_client, config)
    assert missing.group_present is False
    assert missing.stream_length_entries == 3
    assert missing.pending_entries == 0
    assert missing.undelivered_entries == 3
    assert missing.backlog_entries == 3
    assert missing.backlog_ratio == 0.3
    assert missing.trim_headroom_entries == 7

    await redis_client.xgroup_create(
        config.quote_stream,
        config.candle_archive_group,
        id="0",
    )
    delivered = await redis_client.xreadgroup(
        config.candle_archive_group,
        "archive-worker-1",
        {config.quote_stream: ">"},
        count=1,
        block=10,
    )
    stream_id = str(delivered[0][1][0][0])

    active = await collect_candle_archive_consumer_health(redis_client, config)
    assert active.group_present is True
    assert active.consumer_count == 1
    assert active.pending_entries == 1
    assert active.undelivered_entries == 2
    assert active.backlog_entries == 3
    assert active.backlog_ratio == 0.3
    assert active.oldest_backlog_age_seconds >= 0

    await redis_client.xack(
        config.quote_stream,
        config.candle_archive_group,
        stream_id,
    )
    acknowledged = await collect_candle_archive_consumer_health(redis_client, config)
    assert acknowledged.pending_entries == 0
    assert acknowledged.undelivered_entries == 2
    assert acknowledged.backlog_entries == 2
    assert acknowledged.trim_headroom_entries == 8


async def test_archive_monitor_exports_trim_safety_metrics(
    redis_client,
    test_settings,
) -> None:
    config = test_settings.model_copy(
        update={
            "stream_maxlen": 4,
            "candle_archive_group": "test-candle-archive-metrics",
        }
    )
    await redis_client.xadd(config.quote_stream, {"payload": "one"})
    await redis_client.xadd(config.quote_stream, {"payload": "two"})
    await redis_client.xgroup_create(
        config.quote_stream,
        config.candle_archive_group,
        id="0",
    )

    metrics = CandleArchiveMetrics(config)
    monitor = CandleArchiveMonitor(redis_client, config, metrics)
    snapshot = await monitor.sample()

    assert snapshot is not None
    assert snapshot.backlog_entries == 2
    payload = generate_latest(metrics.registry).decode()
    assert "smdg_candle_archive_monitor_up 1.0" in payload
    assert "smdg_candle_archive_consumer_group_present 1.0" in payload
    assert "smdg_candle_archive_stream_maxlen_entries 4.0" in payload
    assert "smdg_candle_archive_backlog_entries 2.0" in payload
    assert "smdg_candle_archive_backlog_ratio 0.5" in payload
    assert "smdg_candle_archive_trim_headroom_entries 2.0" in payload
