from smart_market_data_gateway.connections import ConnectionRegistry


async def test_distributed_connection_limit_and_release(redis_client, test_settings) -> None:
    registry = ConnectionRegistry(redis_client, test_settings)

    assert await registry.acquire(
        client_id="client-1",
        connection_id="connection-1",
        max_connections=2,
    ) == (True, 1)
    assert await registry.acquire(
        client_id="client-1",
        connection_id="connection-2",
        max_connections=2,
    ) == (True, 2)

    allowed, active = await registry.acquire(
        client_id="client-1",
        connection_id="connection-3",
        max_connections=2,
    )
    assert allowed is False
    assert active == 2
    assert await registry.count("client-1") == 2

    await registry.heartbeat("connection-1")
    await registry.release("connection-1")
    assert await registry.count("client-1") == 1
