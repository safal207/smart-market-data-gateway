import asyncio

import pytest

from smart_market_data_gateway.collector import CollectorService
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.providers import MockMarketDataProvider, MockProviderConfig
from smart_market_data_gateway.storage import RedisStore


async def test_collector_reconnects_and_restores_active_symbols(
    redis_client,
    test_settings,
) -> None:
    provider = MockMarketDataProvider(
        MockProviderConfig(interval_seconds=0.01, fail_after_events=1)
    )
    collector = CollectorService(
        provider,
        RedisStore(redis_client, test_settings),
        test_settings,
        GatewayMetrics(),
    )
    collector.active_symbols.add("AAPL")

    task = asyncio.create_task(collector._provider_loop())
    await asyncio.sleep(0.65)
    await collector.close()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    reconnects = await redis_client.get("smdg:provider:reconnects:mock-provider")
    assert reconnects is not None
    assert int(reconnects) >= 1
    state = await redis_client.get("smdg:provider:state:mock-provider")
    assert state in {"connected", "disconnected"}


async def test_collector_boot_restores_durable_desired_symbols(
    redis_client,
    test_settings,
) -> None:
    await redis_client.sadd("smdg:sub:upstream-symbols", "AAPL")
    provider = MockMarketDataProvider(MockProviderConfig(interval_seconds=0.01))
    collector = CollectorService(
        provider,
        RedisStore(redis_client, test_settings),
        test_settings,
        GatewayMetrics(),
    )

    task = asyncio.create_task(collector.run())
    for _ in range(20):
        if "AAPL" in provider._symbols:
            break
        await asyncio.sleep(0.01)

    assert collector.active_symbols == {"AAPL"}
    assert provider._symbols == {"AAPL"}
    await collector.close()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_offline_control_updates_provider_desired_state_before_reconnect(
    redis_client,
    test_settings,
) -> None:
    provider = MockMarketDataProvider(MockProviderConfig(interval_seconds=0.01))
    collector = CollectorService(
        provider,
        RedisStore(redis_client, test_settings),
        test_settings,
        GatewayMetrics(),
    )

    await collector._apply_control("subscribe", "AAPL")
    assert collector.active_symbols == {"AAPL"}
    assert provider._symbols == {"AAPL"}

    await provider.connect()
    await provider.disconnect()
    await collector._apply_control("unsubscribe", "AAPL")
    assert collector.active_symbols == set()
    assert provider._symbols == set()

    await provider.connect()
    assert provider._symbols == set()
    await provider.disconnect()


async def test_control_state_changes_only_after_provider_operation_succeeds(
    redis_client,
    test_settings,
    monkeypatch,
) -> None:
    provider = MockMarketDataProvider(MockProviderConfig(interval_seconds=0.01))
    collector = CollectorService(
        provider,
        RedisStore(redis_client, test_settings),
        test_settings,
        GatewayMetrics(),
    )
    real_subscribe = provider.subscribe
    real_unsubscribe = provider.unsubscribe
    subscribe_attempts = 0
    unsubscribe_attempts = 0

    async def fail_first_subscribe(symbols) -> None:
        nonlocal subscribe_attempts
        subscribe_attempts += 1
        if subscribe_attempts == 1:
            raise ConnectionError("transient subscribe failure")
        await real_subscribe(symbols)

    async def fail_first_unsubscribe(symbols) -> None:
        nonlocal unsubscribe_attempts
        unsubscribe_attempts += 1
        if unsubscribe_attempts == 1:
            raise ConnectionError("transient unsubscribe failure")
        await real_unsubscribe(symbols)

    monkeypatch.setattr(provider, "subscribe", fail_first_subscribe)
    with pytest.raises(ConnectionError, match="transient subscribe failure"):
        await collector._apply_control("subscribe", "AAPL")
    assert collector.active_symbols == set()
    assert provider._symbols == set()

    await collector._apply_control("subscribe", "AAPL")
    assert collector.active_symbols == {"AAPL"}
    assert provider._symbols == {"AAPL"}

    monkeypatch.setattr(provider, "unsubscribe", fail_first_unsubscribe)
    with pytest.raises(ConnectionError, match="transient unsubscribe failure"):
        await collector._apply_control("unsubscribe", "AAPL")
    assert collector.active_symbols == {"AAPL"}
    assert provider._symbols == {"AAPL"}

    await collector._apply_control("unsubscribe", "AAPL")
    assert collector.active_symbols == set()
    assert provider._symbols == set()


async def test_control_loop_reclaims_transition_after_transient_provider_failure(
    redis_client,
    test_settings,
    monkeypatch,
) -> None:
    store = RedisStore(redis_client, test_settings)
    await store.ensure_groups()
    provider = MockMarketDataProvider(MockProviderConfig(interval_seconds=0.01))
    collector = CollectorService(provider, store, test_settings, GatewayMetrics())
    collector._pending_min_idle_ms = 20
    collector._claim_interval_seconds = 0.01
    real_subscribe = provider.subscribe
    subscribe_attempts = 0

    async def fail_first_subscribe(symbols) -> None:
        nonlocal subscribe_attempts
        subscribe_attempts += 1
        if subscribe_attempts == 1:
            raise ConnectionError("transient subscribe failure")
        await real_subscribe(symbols)

    monkeypatch.setattr(provider, "subscribe", fail_first_subscribe)
    await redis_client.sadd("smdg:sub:upstream-symbols", "AAPL")
    await store.publish_control("subscribe", "AAPL")
    control_task = asyncio.create_task(collector._control_loop())
    try:
        for _ in range(200):
            if collector.active_symbols == {"AAPL"}:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("failed control transition was not reclaimed")
    finally:
        await collector.close()
        control_task.cancel()
        await asyncio.gather(control_task, return_exceptions=True)

    pending = await redis_client.xpending(test_settings.control_stream, test_settings.control_group)
    assert subscribe_attempts == 2
    assert provider._symbols == {"AAPL"}
    assert pending["pending"] == 0


async def test_reclaimed_subscribe_cannot_supersede_newer_unsubscribe(
    redis_client,
    test_settings,
    monkeypatch,
) -> None:
    store = RedisStore(redis_client, test_settings)
    await store.ensure_groups()
    provider = MockMarketDataProvider(MockProviderConfig(interval_seconds=0.01))
    collector = CollectorService(provider, store, test_settings, GatewayMetrics())
    collector._pending_min_idle_ms = 1
    real_subscribe = provider.subscribe
    subscribe_attempts = 0

    async def fail_first_subscribe(symbols) -> None:
        nonlocal subscribe_attempts
        subscribe_attempts += 1
        if subscribe_attempts == 1:
            raise ConnectionError("transient subscribe failure")
        await real_subscribe(symbols)

    monkeypatch.setattr(provider, "subscribe", fail_first_subscribe)
    await redis_client.sadd("smdg:sub:upstream-symbols", "AAPL")
    subscribe_id = await store.publish_control("subscribe", "AAPL")
    subscribe_messages = await store.read_group(
        test_settings.control_stream,
        test_settings.control_group,
        collector.consumer_name,
        block_ms=10,
    )
    with pytest.raises(ConnectionError, match="transient subscribe failure"):
        await collector._process_control(*subscribe_messages[0])

    await redis_client.srem("smdg:sub:upstream-symbols", "AAPL")
    await store.publish_control("unsubscribe", "AAPL")
    unsubscribe_messages = await store.read_group(
        test_settings.control_stream,
        test_settings.control_group,
        collector.consumer_name,
        block_ms=10,
    )
    await collector._process_control(*unsubscribe_messages[0])
    await asyncio.sleep(0.01)
    reclaimed = await collector._claim_stale_controls()
    assert [stream_id for stream_id, _fields in reclaimed] == [subscribe_id]
    await collector._process_control(*reclaimed[0])

    pending = await redis_client.xpending(test_settings.control_stream, test_settings.control_group)
    assert subscribe_attempts == 1
    assert collector.active_symbols == set()
    assert provider._symbols == set()
    assert pending["pending"] == 0


async def test_reclaimed_unsubscribe_cannot_supersede_newer_subscribe(
    redis_client,
    test_settings,
    monkeypatch,
) -> None:
    store = RedisStore(redis_client, test_settings)
    await store.ensure_groups()
    provider = MockMarketDataProvider(MockProviderConfig(interval_seconds=0.01))
    collector = CollectorService(provider, store, test_settings, GatewayMetrics())
    collector._pending_min_idle_ms = 1
    await collector._apply_control("subscribe", "AAPL")
    real_unsubscribe = provider.unsubscribe
    unsubscribe_attempts = 0

    async def fail_first_unsubscribe(symbols) -> None:
        nonlocal unsubscribe_attempts
        unsubscribe_attempts += 1
        if unsubscribe_attempts == 1:
            raise ConnectionError("transient unsubscribe failure")
        await real_unsubscribe(symbols)

    monkeypatch.setattr(provider, "unsubscribe", fail_first_unsubscribe)
    await redis_client.srem("smdg:sub:upstream-symbols", "AAPL")
    unsubscribe_id = await store.publish_control("unsubscribe", "AAPL")
    unsubscribe_messages = await store.read_group(
        test_settings.control_stream,
        test_settings.control_group,
        collector.consumer_name,
        block_ms=10,
    )
    with pytest.raises(ConnectionError, match="transient unsubscribe failure"):
        await collector._process_control(*unsubscribe_messages[0])

    await redis_client.sadd("smdg:sub:upstream-symbols", "AAPL")
    await store.publish_control("subscribe", "AAPL")
    subscribe_messages = await store.read_group(
        test_settings.control_stream,
        test_settings.control_group,
        collector.consumer_name,
        block_ms=10,
    )
    await collector._process_control(*subscribe_messages[0])
    await asyncio.sleep(0.01)
    reclaimed = await collector._claim_stale_controls()
    assert [stream_id for stream_id, _fields in reclaimed] == [unsubscribe_id]
    await collector._process_control(*reclaimed[0])

    pending = await redis_client.xpending(test_settings.control_stream, test_settings.control_group)
    assert unsubscribe_attempts == 1
    assert collector.active_symbols == {"AAPL"}
    assert provider._symbols == {"AAPL"}
    assert pending["pending"] == 0
