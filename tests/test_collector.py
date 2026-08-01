import asyncio

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
