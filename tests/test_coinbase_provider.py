from datetime import UTC, datetime
from decimal import Decimal
import json

import pytest

from smart_market_data_gateway.domain import (
    EvidenceOrigin,
    MarketEvidenceCapability,
    QuantityUnit,
    VolumeKind,
)
from smart_market_data_gateway.providers.coinbase import (
    COINBASE_TRADE_BATCH_WINDOW_MS,
    CoinbaseMessageProjector,
    CoinbaseProtocolError,
    CoinbaseResearchConfig,
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
    trades = events[0]["trades"]
    assert isinstance(trades, list)
    trades[0]["side"] = "UNKNOWN"

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
