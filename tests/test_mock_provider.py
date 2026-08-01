import asyncio

from smart_market_data_gateway.providers import (
    MockMarketDataProvider,
    MockProviderConfig,
    ProviderState,
)


async def test_mock_provider_emits_deterministic_quotes() -> None:
    provider = MockMarketDataProvider(MockProviderConfig(interval_seconds=0.01))
    await provider.subscribe(["aapl"])
    await provider.connect()

    stream = provider.events()
    first = await asyncio.wait_for(anext(stream), timeout=1)
    second = await asyncio.wait_for(anext(stream), timeout=1)

    await provider.disconnect()

    assert first.symbol == "AAPL"
    assert first.sequence == 1
    assert second.sequence == 2
    assert first.event_id != second.event_id
    assert (await provider.health()).state is ProviderState.DISCONNECTED


async def test_mock_provider_can_emit_duplicates() -> None:
    provider = MockMarketDataProvider(
        MockProviderConfig(interval_seconds=0.01, duplicate_every=1)
    )
    await provider.subscribe(["AAPL"])
    await provider.connect()

    stream = provider.events()
    first = await asyncio.wait_for(anext(stream), timeout=1)
    duplicate = await asyncio.wait_for(anext(stream), timeout=1)

    await provider.disconnect()

    assert duplicate == first


async def test_mock_provider_can_simulate_failure() -> None:
    provider = MockMarketDataProvider(
        MockProviderConfig(interval_seconds=0.01, fail_after_events=1)
    )
    await provider.subscribe(["AAPL"])
    await provider.connect()

    stream = provider.events()
    event = await asyncio.wait_for(anext(stream), timeout=1)

    try:
        await asyncio.wait_for(anext(stream), timeout=1)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("event stream must stop after the simulated failure")

    health = await provider.health()
    assert event.sequence == 1
    assert health.state is ProviderState.DEGRADED
    assert health.message == "simulated provider failure"
