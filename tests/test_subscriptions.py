import asyncio
import time

from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.storage import RedisStore
from smart_market_data_gateway.subscriptions import SubscriptionRegistry


async def test_identical_subscriptions_create_one_upstream_transition(
    redis_client,
    test_settings,
) -> None:
    metrics = GatewayMetrics()
    store = RedisStore(redis_client, test_settings)
    registry = SubscriptionRegistry(redis_client, store, test_settings, metrics)

    assert await registry.subscribe("connection-1", {"AAPL"}) == {"AAPL"}
    assert await registry.subscribe("connection-2", {"AAPL"}) == {"AAPL"}

    controls = await redis_client.xrange(test_settings.control_stream)
    assert [entry[1]["action"] for entry in controls] == ["subscribe"]

    stats = await registry.refresh_metrics()
    assert stats["client_subscriptions"] == 2
    assert stats["unique_upstream_subscriptions"] == 1
    assert stats["aggregation_ratio"] == 2

    assert await registry.disconnect("connection-1") == {"AAPL"}
    await asyncio.sleep(test_settings.subscription_grace_seconds * 2)
    controls = await redis_client.xrange(test_settings.control_stream)
    assert [entry[1]["action"] for entry in controls] == ["subscribe"]

    assert await registry.disconnect("connection-2") == {"AAPL"}
    await asyncio.sleep(test_settings.subscription_grace_seconds * 2)
    controls = await redis_client.xrange(test_settings.control_stream)
    assert [entry[1]["action"] for entry in controls] == ["subscribe", "unsubscribe"]
    await registry.close()


async def test_new_subscriber_cancels_grace_release(redis_client, test_settings) -> None:
    metrics = GatewayMetrics()
    store = RedisStore(redis_client, test_settings)
    registry = SubscriptionRegistry(redis_client, store, test_settings, metrics)

    await registry.subscribe("connection-1", {"TSLA"})
    await registry.disconnect("connection-1")
    await registry.subscribe("connection-2", {"TSLA"})
    await asyncio.sleep(test_settings.subscription_grace_seconds * 2)

    controls = await redis_client.xrange(test_settings.control_stream)
    assert [entry[1]["action"] for entry in controls] == ["subscribe"]
    await registry.close()


async def test_subscription_result_distinguishes_ref_add_from_upstream_transition(
    redis_client,
    test_settings,
) -> None:
    registry = SubscriptionRegistry(
        redis_client,
        RedisStore(redis_client, test_settings),
        test_settings,
        GatewayMetrics(),
    )

    first = await registry.subscribe_with_transitions("connection-1", {"AAPL"})
    second = await registry.subscribe_with_transitions("connection-2", {"AAPL"})

    assert first.added_symbols == {"AAPL"}
    assert first.upstream_transitions == {"AAPL"}
    assert second.added_symbols == {"AAPL"}
    assert second.upstream_transitions == set()
    assert await redis_client.smembers("smdg:sub:upstream-symbols") == {"AAPL"}
    await registry.close()


async def test_heartbeat_recovers_expired_upstream_state_with_ttl(
    redis_client,
    test_settings,
) -> None:
    registry = SubscriptionRegistry(
        redis_client,
        RedisStore(redis_client, test_settings),
        test_settings,
        GatewayMetrics(),
    )
    await registry.subscribe("connection-1", {"AAPL"})
    state_key = "smdg:sub:upstream-state:AAPL"
    assert 0 < await redis_client.ttl(state_key) <= registry._state_ttl_seconds

    await redis_client.delete(state_key)
    await registry.heartbeat("connection-1")

    controls = await redis_client.xrange(test_settings.control_stream)
    assert [entry[1]["action"] for entry in controls] == ["subscribe", "subscribe"]
    assert await redis_client.get(state_key) == "active"
    assert 0 < await redis_client.ttl(state_key) <= registry._state_ttl_seconds
    await registry.close()


async def test_cancelled_grace_task_cannot_remove_its_replacement(
    redis_client,
    test_settings,
) -> None:
    registry = SubscriptionRegistry(
        redis_client,
        RedisStore(redis_client, test_settings),
        test_settings,
        GatewayMetrics(),
    )
    await registry.subscribe("connection-1", {"AAPL"})
    await registry.unsubscribe("connection-1", {"AAPL"})
    first_release = registry._release_tasks["AAPL"]
    await asyncio.sleep(0)

    await registry.subscribe("connection-2", {"AAPL"})
    await registry.unsubscribe("connection-2", {"AAPL"})
    replacement_release = registry._release_tasks["AAPL"]
    assert replacement_release is not first_release

    await asyncio.gather(first_release, return_exceptions=True)
    assert registry._release_tasks.get("AAPL") is replacement_release
    await registry.close()
    assert replacement_release.done()


async def test_cleanup_releases_expired_ref_from_durable_active_set(
    redis_client,
    test_settings,
) -> None:
    registry = SubscriptionRegistry(
        redis_client,
        RedisStore(redis_client, test_settings),
        test_settings,
        GatewayMetrics(),
    )
    await registry.subscribe("connection-1", {"AAPL"})
    await redis_client.zadd("smdg:sub:symbol:AAPL", {"connection-1": time.time() - 1})

    await registry.cleanup_expired()
    await asyncio.sleep(test_settings.subscription_grace_seconds * 2)

    controls = await redis_client.xrange(test_settings.control_stream)
    assert [entry[1]["action"] for entry in controls] == ["subscribe", "unsubscribe"]
    assert await redis_client.smembers("smdg:sub:upstream-symbols") == set()
    assert await redis_client.get("smdg:sub:upstream-state:AAPL") == "inactive"
    await registry.close()
