import asyncio

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
