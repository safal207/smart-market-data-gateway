from datetime import UTC, datetime
from decimal import Decimal

from smart_market_data_gateway.domain import ClientIdentity, QuoteEvent, ServiceTier
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.qos import (
    ClientSession,
    ConnectionHub,
    LatestValueBuffer,
    QoSPolicyService,
)


def event(symbol: str, sequence: int, price: str) -> QuoteEvent:
    return QuoteEvent(
        symbol=symbol,
        price=Decimal(price),
        provider_timestamp=datetime.now(UTC),
        sequence=sequence,
        provider="test",
    )


async def test_latest_value_buffer_coalesces_and_bounds() -> None:
    buffer = LatestValueBuffer(max_items=2)
    first = event("AAPL", 1, "100")
    newer = event("AAPL", 2, "101")
    other = event("TSLA", 1, "200")
    third = event("NVDA", 1, "300")

    assert await buffer.put(first) == (False, False)
    assert await buffer.put(newer) == (True, False)
    assert await buffer.put(other) == (False, False)
    coalesced, dropped = await buffer.put(third)
    assert coalesced is False
    assert dropped is True
    assert buffer.depth == 2
    assert buffer.coalesced == 1
    assert buffer.dropped == 1
    await buffer.close()


async def test_hub_routes_only_subscribed_symbols(test_settings) -> None:
    metrics = GatewayMetrics()
    qos = QoSPolicyService(test_settings)
    hub = ConnectionHub(metrics, qos)
    session = ClientSession(
        connection_id="connection-1",
        identity=ClientIdentity(client_id="client-1", tier=ServiceTier.BASIC),
        buffer=LatestValueBuffer(10),
    )
    await hub.add(session)
    assert await hub.subscribe("connection-1", {"AAPL"}) == {"AAPL"}

    await hub.publish(event("TSLA", 1, "200"))
    assert session.buffer.depth == 0
    await hub.publish(event("AAPL", 1, "100"))
    assert session.buffer.depth == 1

    assert await hub.unsubscribe("connection-1", {"AAPL"}) == {"AAPL"}
    await hub.remove("connection-1")


def test_qos_policy_limits(test_settings) -> None:
    qos = QoSPolicyService(test_settings)
    assert qos.policy_for(ServiceTier.BASIC).max_symbols == 20
    assert qos.delivery_interval(ServiceTier.BASIC) == 1.0
    try:
        qos.validate_symbol_count(ServiceTier.BASIC, 21)
    except ValueError as exc:
        assert "symbol limit exceeded" in str(exc)
    else:
        raise AssertionError("expected symbol limit error")
