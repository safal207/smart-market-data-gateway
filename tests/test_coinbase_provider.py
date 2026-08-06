import asyncio
from datetime import UTC, datetime
from decimal import Decimal
import json

import pytest
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from smart_market_data_gateway.collector import build_provider
from smart_market_data_gateway.config import Settings
from smart_market_data_gateway.domain import (
    EvidenceOrigin,
    MarketEvidenceCapability,
    QuantityUnit,
    VolumeKind,
)
from smart_market_data_gateway.providers.base import ProviderState
from smart_market_data_gateway.providers.coinbase import (
    COINBASE_TRADE_BATCH_WINDOW_MS,
    CoinbaseMessageProjector,
    CoinbaseProtocolError,
    CoinbaseResearchConfig,
    CoinbaseResearchMarketDataProvider,
    CoinbaseUsageError,
    _subscription_message,
)

RECEIVED_AT = datetime(2026, 8, 4, 15, 0, 1, tzinfo=UTC)


def ticker_message() -> dict[str, object]:
    return {
        "channel": "ticker",
        "timestamp": "2026-08-04T15:00:00Z",
        "sequence_num": 10,
        "events": [
            {
                "type": "update",
                "tickers": [
                    {
                        "type": "ticker",
                        "product_id": "BTC-USD",
                        "price": "100.00",
                        "best_bid": "99.90",
                        "best_bid_quantity": "2.50",
                        "best_ask": "100.10",
                        "best_ask_quantity": "3.25",
                    }
                ],
            }
        ],
    }


def market_trades_message() -> dict[str, object]:
    return {
        "channel": "market_trades",
        "timestamp": "2026-08-04T15:00:00.250Z",
        "sequence_num": 11,
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "trade_id": "trade-1",
                        "product_id": "BTC-USD",
                        "price": "100.05",
                        "size": "0.30",
                        "side": "SELL",
                        "time": "2026-08-04T15:00:00.100Z",
                    },
                    {
                        "trade_id": "trade-2",
                        "product_id": "BTC-USD",
                        "price": "100.10",
                        "size": "0.20",
                        "side": "BUY",
                        "time": "2026-08-04T15:00:00.200Z",
                    },
                ],
            }
        ],
    }


def test_projects_real_contract_into_rich_quote_event() -> None:
    projector = CoinbaseMessageProjector()

    assert projector.apply(ticker_message(), received_at=RECEIVED_AT) == ()
    events = projector.apply(market_trades_message(), received_at=RECEIVED_AT)

    assert len(events) == 1
    event = events[0]
    assert event.schema_version == "1.1"
    assert event.symbol == "BTC-USD"
    assert event.price == Decimal("100.10")
    assert event.bid == Decimal("99.90")
    assert event.ask == Decimal("100.10")
    assert event.volume == Decimal("0.50")
    assert event.buy_volume == Decimal("0.30")
    assert event.sell_volume == Decimal("0.20")
    assert event.trade_count == 2
    assert event.bid_depth == Decimal("2.50")
    assert event.ask_depth == Decimal("3.25")
    assert event.received_at == RECEIVED_AT
    assert event.sequence == 1
    assert event.volume_semantics is not None
    assert event.volume_semantics.kind is VolumeKind.INTERVAL
    assert event.volume_semantics.unit is QuantityUnit.BASE_ASSET
    assert event.volume_semantics.aggregation_window_ms == COINBASE_TRADE_BATCH_WINDOW_MS
    assert event.volume_semantics.origin is EvidenceOrigin.GATEWAY_DERIVED
    assert event.depth_semantics is not None
    assert event.depth_semantics.origin is EvidenceOrigin.NATIVE
    assert MarketEvidenceCapability.TOP_OF_BOOK_DEPTH in event.capabilities


def test_projects_trade_evidence_before_ticker_without_inventing_depth() -> None:
    projector = CoinbaseMessageProjector()

    event = projector.apply(market_trades_message(), received_at=RECEIVED_AT)[0]

    assert event.bid is None
    assert event.ask is None
    assert event.bid_depth is None
    assert event.ask_depth is None
    assert event.depth_semantics is None
    assert MarketEvidenceCapability.TOP_OF_BOOK_DEPTH not in event.capabilities
    assert event.volume == Decimal("0.50")


def test_inverts_documented_maker_side_to_aggressor_side() -> None:
    event = CoinbaseMessageProjector().apply(
        market_trades_message(),
        received_at=RECEIVED_AT,
    )[0]

    assert event.buy_volume == Decimal("0.30")
    assert event.sell_volume == Decimal("0.20")


def test_rejects_unknown_trade_side_without_guessing() -> None:
    payload = market_trades_message()
    events = payload["events"]
    assert isinstance(events, list)
    first_event = events[0]
    assert isinstance(first_event, dict)
    trades = first_event["trades"]
    assert isinstance(trades, list)
    first_trade = trades[0]
    assert isinstance(first_trade, dict)
    first_trade["side"] = "UNKNOWN"

    with pytest.raises(CoinbaseProtocolError, match="side must be BUY or SELL"):
        CoinbaseMessageProjector().apply(payload, received_at=RECEIVED_AT)


def test_research_usage_gate_is_disabled_by_default() -> None:
    with pytest.raises(CoinbaseUsageError, match="personal_research"):
        CoinbaseResearchConfig().validate_usage()


def test_research_usage_gate_requires_explicit_terms_acknowledgement() -> None:
    config = CoinbaseResearchConfig(use_mode="personal_research")

    with pytest.raises(CoinbaseUsageError, match="Terms"):
        config.validate_usage()


def test_research_usage_gate_rejects_production() -> None:
    config = CoinbaseResearchConfig(
        use_mode="personal_research",
        market_data_terms_accepted=True,
        environment="production",
    )

    with pytest.raises(CoinbaseUsageError, match="production"):
        config.validate_usage()


def test_collector_fails_fast_when_coinbase_terms_are_not_acknowledged() -> None:
    config = Settings(
        market_data_provider="coinbase",
        coinbase_use_mode="personal_research",
        coinbase_market_data_terms_accepted=False,
    )

    with pytest.raises(CoinbaseUsageError, match="Terms"):
        build_provider(config)


def test_collector_selects_coinbase_only_for_approved_research_mode() -> None:
    config = Settings(
        environment="research",
        market_data_provider="coinbase",
        coinbase_use_mode="personal_research",
        coinbase_market_data_terms_accepted=True,
    )

    assert isinstance(build_provider(config), CoinbaseResearchMarketDataProvider)


def test_subscription_messages_contain_no_credentials() -> None:
    message = json.loads(
        _subscription_message("subscribe", "market_trades", ["BTC-USD"])
    )

    assert message == {
        "channel": "market_trades",
        "product_ids": ["BTC-USD"],
        "type": "subscribe",
    }
    assert "jwt" not in message


@pytest.mark.asyncio
async def test_provider_connects_subscribes_and_emits_rich_event_over_websocket() -> None:
    subscriptions: list[dict[str, object]] = []

    async def handler(websocket: ServerConnection) -> None:
        for _ in range(3):
            subscriptions.append(json.loads(await websocket.recv()))
        await websocket.send(json.dumps(ticker_message()))
        await websocket.send(json.dumps(market_trades_message()))
        await asyncio.sleep(0.05)

    async with serve(handler, "127.0.0.1", 0) as server:
        sockets = server.sockets
        assert sockets
        port = sockets[0].getsockname()[1]
        provider = CoinbaseResearchMarketDataProvider(
            CoinbaseResearchConfig(
                url=f"ws://127.0.0.1:{port}",
                use_mode="personal_research",
                market_data_terms_accepted=True,
                environment="research",
            )
        )
        await provider.connect()
        try:
            await provider.subscribe(["BTC-USD"])
            event = await asyncio.wait_for(anext(provider.events()), timeout=2.0)
        finally:
            await provider.disconnect()

    assert {item["channel"] for item in subscriptions} == {
        "heartbeats",
        "ticker",
        "market_trades",
    }
    assert event.symbol == "BTC-USD"
    assert event.volume == Decimal("0.50")
    assert event.bid_depth == Decimal("2.50")


def subscription_ack_message() -> dict[str, object]:
    return {
        "channel": "subscriptions",
        "timestamp": "2026-08-04T15:00:01Z",
        "sequence_num": 1,
        "events": [
            {
                "type": "subscriptions",
                "subscriptions": [
                    {"name": "heartbeats", "product_ids": []},
                    {"name": "ticker", "product_ids": ["BTC-USD"]},
                    {"name": "market_trades", "product_ids": ["BTC-USD"]},
                ],
            }
        ],
    }


def ticker_batch_message() -> dict[str, object]:
    return {
        "channel": "ticker_batch",
        "timestamp": "2026-08-04T15:00:03Z",
        "sequence_num": 4,
        "events": [{"type": "update", "tickers": []}],
    }


@pytest.mark.asyncio
async def test_provider_diagnostics_count_messages_acks_and_rejections() -> None:
    subscriptions: list[dict[str, object]] = []

    async def handler(websocket: ServerConnection) -> None:
        for _ in range(3):
            subscriptions.append(json.loads(await websocket.recv()))
        await websocket.send(json.dumps(subscription_ack_message()))
        await websocket.send(
            json.dumps(
                {
                    "channel": "heartbeats",
                    "timestamp": "2026-08-04T15:00:02Z",
                    "sequence_num": 2,
                    "events": [],
                }
            )
        )
        await websocket.send(json.dumps(ticker_message()))
        await websocket.send(json.dumps(ticker_batch_message()))
        await websocket.send("not-json")
        await websocket.send(json.dumps(market_trades_message()))
        await asyncio.sleep(0.2)

    async with serve(handler, "127.0.0.1", 0) as server:
        sockets = server.sockets
        assert sockets
        port = sockets[0].getsockname()[1]
        provider = CoinbaseResearchMarketDataProvider(
            CoinbaseResearchConfig(
                url=f"ws://127.0.0.1:{port}",
                use_mode="personal_research",
                market_data_terms_accepted=True,
                environment="research",
            )
        )
        await provider.connect()
        try:
            await provider.subscribe(["BTC-USD"])
            event = await asyncio.wait_for(anext(provider.events()), timeout=2.0)
            await asyncio.sleep(0.5)
        finally:
            await provider.disconnect()

    assert event.symbol == "BTC-USD"
    diagnostics = provider.diagnostics
    assert diagnostics["raw_messages_received_total"] == 6
    assert diagnostics["raw_messages_by_channel"] == {
        "heartbeats": 1,
        "market_trades": 1,
        "subscriptions": 1,
        "ticker": 1,
        "ticker_batch": 1,
        "unparsed": 1,
    }
    assert diagnostics["projected_quote_events_total"] == 1
    assert diagnostics["rejected_messages_total"] == 1
    assert diagnostics["rejected_messages_by_exception_type"] == {
        "JSONDecodeError": 1
    }
    assert diagnostics["subscription_acknowledgements"] == {
        "heartbeats": 1,
        "market_trades": 1,
        "ticker": 1,
    }
    assert diagnostics["reader_final_state"] == "ended"
    assert diagnostics["reader_final_error_type"] is None
    assert diagnostics["provider_state"] == "disconnected"


def heartbeat_message() -> dict[str, object]:
    return {
        "channel": "heartbeats",
        "timestamp": "2026-08-04T15:00:02Z",
        "sequence_num": 2,
        "events": [],
    }


@pytest.mark.asyncio
async def test_provider_stalls_and_marks_degraded_on_silent_connection() -> None:
    subscriptions: list[dict[str, object]] = []

    async def handler(websocket: ServerConnection) -> None:
        for _ in range(3):
            subscriptions.append(json.loads(await websocket.recv()))
        await websocket.send(json.dumps(market_trades_message()))
        await asyncio.sleep(1.0)

    async with serve(handler, "127.0.0.1", 0) as server:
        sockets = server.sockets
        assert sockets
        port = sockets[0].getsockname()[1]
        provider = CoinbaseResearchMarketDataProvider(
            CoinbaseResearchConfig(
                url=f"ws://127.0.0.1:{port}",
                use_mode="personal_research",
                market_data_terms_accepted=True,
                environment="research",
                message_idle_timeout_seconds=0.2,
            )
        )
        await provider.connect()
        try:
            await provider.subscribe(["BTC-USD"])
            event = await asyncio.wait_for(anext(provider.events()), timeout=2.0)
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(anext(provider.events()), timeout=2.0)
            health = await provider.health()
            diagnostics = provider.diagnostics
        finally:
            await provider.disconnect()

    assert event.symbol == "BTC-USD"
    assert health.state is ProviderState.DEGRADED
    assert health.message is not None and "no upstream message" in health.message
    assert diagnostics["reader_final_state"] == "stalled"
    assert diagnostics["reader_final_error_type"] is None
    assert diagnostics["provider_state"] == "degraded"


@pytest.mark.asyncio
async def test_provider_heartbeats_keep_feed_alive_across_trade_pause() -> None:
    subscriptions: list[dict[str, object]] = []

    async def handler(websocket: ServerConnection) -> None:
        for _ in range(3):
            subscriptions.append(json.loads(await websocket.recv()))
        await websocket.send(json.dumps(market_trades_message()))
        try:
            for _ in range(20):
                await websocket.send(json.dumps(heartbeat_message()))
                await asyncio.sleep(0.05)
        except ConnectionClosed:
            pass

    async with serve(handler, "127.0.0.1", 0) as server:
        sockets = server.sockets
        assert sockets
        port = sockets[0].getsockname()[1]
        provider = CoinbaseResearchMarketDataProvider(
            CoinbaseResearchConfig(
                url=f"ws://127.0.0.1:{port}",
                use_mode="personal_research",
                market_data_terms_accepted=True,
                environment="research",
                message_idle_timeout_seconds=0.2,
            )
        )
        await provider.connect()
        try:
            await provider.subscribe(["BTC-USD"])
            event = await asyncio.wait_for(anext(provider.events()), timeout=2.0)
            await asyncio.sleep(0.6)
            health = await provider.health()
            diagnostics = provider.diagnostics
        finally:
            await provider.disconnect()

    assert event.symbol == "BTC-USD"
    assert health.state is ProviderState.CONNECTED
    assert diagnostics["reader_final_state"] == "never_started"
    assert diagnostics["provider_state"] == "connected"
    assert diagnostics["raw_messages_by_channel"]["heartbeats"] >= 10
