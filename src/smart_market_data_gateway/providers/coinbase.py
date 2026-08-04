from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator, Collection, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
import json
import logging
from typing import Any, cast
from uuid import NAMESPACE_URL, uuid5

from websockets.asyncio.client import ClientConnection, connect

from smart_market_data_gateway.domain import (
    DepthSemantics,
    EvidenceOrigin,
    MarketEvidenceCapability,
    QuantityUnit,
    QuoteEvent,
    VolumeKind,
    VolumeSemantics,
)
from smart_market_data_gateway.providers.base import (
    MarketDataProvider,
    ProviderHealth,
    ProviderState,
)

logger = logging.getLogger(__name__)

COINBASE_MARKET_DATA_URL = "wss://advanced-trade-ws.coinbase.com"
COINBASE_PROVIDER_NAME = "coinbase-advanced-trade"
COINBASE_TRADE_BATCH_WINDOW_MS = 250
_ALLOWED_USE_MODE = "personal_research"


class CoinbaseUsageError(PermissionError):
    """Raised when Coinbase market data is enabled outside the approved local mode."""


class CoinbaseProtocolError(ValueError):
    """Raised when an upstream message cannot be normalized without guessing."""


@dataclass(frozen=True, slots=True)
class CoinbaseResearchConfig:
    """Fail-closed configuration for the public Coinbase market-data feed."""

    url: str = COINBASE_MARKET_DATA_URL
    use_mode: str = "disabled"
    market_data_terms_accepted: bool = False
    environment: str = "development"
    queue_size: int = 10_000

    def __post_init__(self) -> None:
        if not self.url.startswith(("wss://", "ws://")):
            raise ValueError("Coinbase WebSocket URL must use ws:// or wss://")
        if self.queue_size <= 0:
            raise ValueError("queue_size must be positive")

    def validate_usage(self) -> None:
        if self.use_mode != _ALLOWED_USE_MODE:
            raise CoinbaseUsageError(
                "Coinbase provider requires use_mode=personal_research"
            )
        if not self.market_data_terms_accepted:
            raise CoinbaseUsageError(
                "Coinbase Market Data Terms must be acknowledged explicitly"
            )
        if self.environment.strip().lower() == "production":
            raise CoinbaseUsageError(
                "Coinbase public market data integration is disabled in production"
            )


@dataclass(frozen=True, slots=True)
class _TickerState:
    price: Decimal
    bid: Decimal | None
    ask: Decimal | None
    bid_quantity: Decimal | None
    ask_quantity: Decimal | None
    provider_timestamp: datetime


class CoinbaseMessageProjector:
    """Project Coinbase public WebSocket messages into provider-neutral events."""

    def __init__(self) -> None:
        self._ticker_by_symbol: dict[str, _TickerState] = {}
        self._sequence_by_symbol: dict[str, int] = defaultdict(int)

    def clear_connection_state(self) -> None:
        """Drop connection-local book state while preserving local sequence continuity."""

        self._ticker_by_symbol.clear()

    def apply(
        self,
        payload: Mapping[str, Any],
        *,
        received_at: datetime | None = None,
    ) -> tuple[QuoteEvent, ...]:
        channel = str(payload.get("channel", ""))
        observed_at = received_at or datetime.now(UTC)
        if channel == "ticker":
            self._apply_ticker(payload)
            return ()
        if channel == "market_trades":
            return self._project_trades(payload, received_at=observed_at)
        if channel in {"heartbeats", "subscriptions"}:
            return ()
        return ()

    def _apply_ticker(self, payload: Mapping[str, Any]) -> None:
        message_timestamp = _timestamp(payload.get("timestamp"), "timestamp")
        for event in _mapping_sequence(payload.get("events"), "events"):
            for ticker in _mapping_sequence(event.get("tickers"), "tickers"):
                symbol = _symbol(ticker.get("product_id"))
                self._ticker_by_symbol[symbol] = _TickerState(
                    price=_positive_decimal(ticker.get("price"), "price"),
                    bid=_optional_positive_decimal(ticker.get("best_bid"), "best_bid"),
                    ask=_optional_positive_decimal(ticker.get("best_ask"), "best_ask"),
                    bid_quantity=_optional_non_negative_decimal(
                        ticker.get("best_bid_quantity"),
                        "best_bid_quantity",
                    ),
                    ask_quantity=_optional_non_negative_decimal(
                        ticker.get("best_ask_quantity"),
                        "best_ask_quantity",
                    ),
                    provider_timestamp=message_timestamp,
                )

    def _project_trades(
        self,
        payload: Mapping[str, Any],
        *,
        received_at: datetime,
    ) -> tuple[QuoteEvent, ...]:
        message_timestamp = _timestamp(payload.get("timestamp"), "timestamp")
        trades_by_symbol: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for event in _mapping_sequence(payload.get("events"), "events"):
            for trade in _mapping_sequence(event.get("trades"), "trades"):
                trades_by_symbol[_symbol(trade.get("product_id"))].append(trade)

        events: list[QuoteEvent] = []
        for symbol, trades in sorted(trades_by_symbol.items()):
            if trades:
                events.append(
                    self._build_quote_event(
                        symbol,
                        trades,
                        message_timestamp=message_timestamp,
                        received_at=received_at,
                    )
                )
        return tuple(events)

    def _build_quote_event(
        self,
        symbol: str,
        trades: Sequence[Mapping[str, Any]],
        *,
        message_timestamp: datetime,
        received_at: datetime,
    ) -> QuoteEvent:
        total_volume = Decimal("0")
        aggressive_buy_volume = Decimal("0")
        aggressive_sell_volume = Decimal("0")
        trade_ids: list[str] = []
        latest_price: Decimal | None = None
        latest_timestamp = message_timestamp

        for trade in trades:
            size = _non_negative_decimal(trade.get("size"), "size")
            price = _positive_decimal(trade.get("price"), "price")
            maker_side = str(trade.get("side", "")).strip().upper()
            if maker_side == "SELL":
                aggressive_buy_volume += size
            elif maker_side == "BUY":
                aggressive_sell_volume += size
            else:
                raise CoinbaseProtocolError("trade side must be BUY or SELL")

            trade_id = str(trade.get("trade_id", "")).strip()
            if not trade_id:
                raise CoinbaseProtocolError("trade_id must not be empty")
            trade_ids.append(trade_id)
            total_volume += size
            latest_price = price

            trade_time = trade.get("time")
            if trade_time is not None:
                parsed_trade_time = _timestamp(trade_time, "trade.time")
                if parsed_trade_time > latest_timestamp:
                    latest_timestamp = parsed_trade_time

        if latest_price is None:
            raise CoinbaseProtocolError("market_trades event contains no trades")

        ticker = self._ticker_by_symbol.get(symbol)
        bid = ticker.bid if ticker is not None else None
        ask = ticker.ask if ticker is not None else None
        bid_depth = ticker.bid_quantity if ticker is not None else None
        ask_depth = ticker.ask_quantity if ticker is not None else None

        capabilities = {
            MarketEvidenceCapability.LEVEL1_QUOTE,
            MarketEvidenceCapability.VOLUME,
            MarketEvidenceCapability.AGGRESSOR_FLOW,
            MarketEvidenceCapability.TRADE_COUNT,
        }
        depth_semantics: DepthSemantics | None = None
        if bid_depth is not None and ask_depth is not None:
            capabilities.add(MarketEvidenceCapability.TOP_OF_BOOK_DEPTH)
            depth_semantics = DepthSemantics(
                unit=QuantityUnit.BASE_ASSET,
                levels=1,
                origin=EvidenceOrigin.NATIVE,
            )

        self._sequence_by_symbol[symbol] += 1
        local_sequence = self._sequence_by_symbol[symbol]
        event_identity = ":".join(
            (
                COINBASE_PROVIDER_NAME,
                symbol,
                ",".join(trade_ids),
            )
        )

        return QuoteEvent(
            schema_version="1.1",
            event_id=uuid5(NAMESPACE_URL, event_identity),
            symbol=symbol,
            price=latest_price,
            bid=bid,
            ask=ask,
            provider_timestamp=latest_timestamp,
            received_at=received_at,
            sequence=local_sequence,
            provider=COINBASE_PROVIDER_NAME,
            capabilities=frozenset(capabilities),
            volume=total_volume,
            buy_volume=aggressive_buy_volume,
            sell_volume=aggressive_sell_volume,
            trade_count=len(trades),
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            volume_semantics=VolumeSemantics(
                kind=VolumeKind.INTERVAL,
                unit=QuantityUnit.BASE_ASSET,
                aggregation_window_ms=COINBASE_TRADE_BATCH_WINDOW_MS,
                origin=EvidenceOrigin.GATEWAY_DERIVED,
            ),
            depth_semantics=depth_semantics,
        )


class CoinbaseResearchMarketDataProvider(MarketDataProvider):
    """Research-only adapter for Coinbase Advanced Trade public market data."""

    def __init__(self, config: CoinbaseResearchConfig | None = None) -> None:
        self._config = config or CoinbaseResearchConfig()
        self._state = ProviderState.DISCONNECTED
        self._message: str | None = None
        self._symbols: set[str] = set()
        self._queue: asyncio.Queue[QuoteEvent] = asyncio.Queue(maxsize=self._config.queue_size)
        self._projector = CoinbaseMessageProjector()
        self._connection: ClientConnection | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._heartbeats_subscribed = False

    @property
    def name(self) -> str:
        return COINBASE_PROVIDER_NAME

    @property
    def capabilities(self) -> frozenset[MarketEvidenceCapability]:
        return frozenset(
            {
                MarketEvidenceCapability.LEVEL1_QUOTE,
                MarketEvidenceCapability.VOLUME,
                MarketEvidenceCapability.AGGRESSOR_FLOW,
                MarketEvidenceCapability.TRADE_COUNT,
                MarketEvidenceCapability.TOP_OF_BOOK_DEPTH,
            }
        )

    async def connect(self) -> None:
        self._config.validate_usage()
        async with self._lock:
            if self._reader_task is not None and not self._reader_task.done():
                return
            self._state = ProviderState.CONNECTING
            self._message = None
            self._projector.clear_connection_state()
            try:
                self._connection = await connect(
                    self._config.url,
                    max_queue=self._config.queue_size,
                    ping_interval=20,
                    ping_timeout=20,
                )
            except Exception as exc:
                self._state = ProviderState.DEGRADED
                self._message = str(exc)
                raise
            self._reader_task = asyncio.create_task(
                self._reader_loop(),
                name="coinbase-market-data-reader",
            )
            self._state = ProviderState.CONNECTED
            self._heartbeats_subscribed = False

    async def disconnect(self) -> None:
        async with self._lock:
            task = self._reader_task
            connection = self._connection
            self._reader_task = None
            self._connection = None
            self._symbols.clear()
            self._state = ProviderState.DISCONNECTED
            self._message = None
            self._heartbeats_subscribed = False

        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        if connection is not None:
            await connection.close()

    async def subscribe(self, symbols: Collection[str]) -> None:
        normalized = {_symbol(symbol) for symbol in symbols}
        new_symbols = sorted(normalized.difference(self._symbols))
        self._symbols.update(normalized)
        if not new_symbols:
            return
        connection = self._require_connection()
        if not self._heartbeats_subscribed:
            await connection.send(_subscription_message("subscribe", "heartbeats", ()))
            self._heartbeats_subscribed = True
        for channel in ("ticker", "market_trades"):
            await connection.send(_subscription_message("subscribe", channel, new_symbols))

    async def unsubscribe(self, symbols: Collection[str]) -> None:
        normalized = {_symbol(symbol) for symbol in symbols}
        active = sorted(normalized.intersection(self._symbols))
        self._symbols.difference_update(normalized)
        if not active:
            return
        connection = self._require_connection()
        for channel in ("ticker", "market_trades"):
            await connection.send(_subscription_message("unsubscribe", channel, active))

    async def health(self) -> ProviderHealth:
        return ProviderHealth(state=self._state, message=self._message)

    async def events(self) -> AsyncIterator[QuoteEvent]:
        while True:
            task = self._reader_task
            if (task is None or task.done()) and self._queue.empty():
                return
            try:
                yield await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except TimeoutError:
                continue

    async def _reader_loop(self) -> None:
        connection = self._require_connection()
        try:
            async for raw_message in connection:
                try:
                    parsed = json.loads(raw_message)
                    if not isinstance(parsed, Mapping):
                        raise CoinbaseProtocolError("WebSocket message must be a JSON object")
                    payload = cast(Mapping[str, Any], parsed)
                    for event in self._projector.apply(payload):
                        if event.symbol in self._symbols:
                            await self._queue.put(event)
                except (TypeError, ValueError):
                    logger.warning(
                        "Coinbase market-data message rejected",
                        extra={"event": "coinbase_message_rejected"},
                        exc_info=True,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._state = ProviderState.DEGRADED
            self._message = str(exc)

    def _require_connection(self) -> ClientConnection:
        if self._connection is None:
            raise ConnectionError("Coinbase provider is not connected")
        return self._connection


def _subscription_message(action: str, channel: str, symbols: Sequence[str]) -> str:
    payload: dict[str, object] = {"type": action, "channel": channel}
    if symbols:
        payload["product_ids"] = list(symbols)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _mapping_sequence(value: object, field: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        raise CoinbaseProtocolError(f"{field} must be an array")
    result: list[Mapping[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise CoinbaseProtocolError(f"{field} items must be objects")
        result.append(cast(Mapping[str, Any], item))
    return tuple(result)


def _symbol(value: object) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        raise CoinbaseProtocolError("product_id must not be empty")
    if any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-" for character in symbol):
        raise CoinbaseProtocolError("product_id contains unsupported characters")
    return symbol


def _timestamp(value: object, field: str) -> datetime:
    if value is None:
        raise CoinbaseProtocolError(f"{field} is required")
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise CoinbaseProtocolError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CoinbaseProtocolError(f"{field} must include timezone information")
    return parsed


def _positive_decimal(value: object, field: str) -> Decimal:
    number = _decimal(value, field)
    if number <= 0:
        raise CoinbaseProtocolError(f"{field} must be greater than zero")
    return number


def _non_negative_decimal(value: object, field: str) -> Decimal:
    number = _decimal(value, field)
    if number < 0:
        raise CoinbaseProtocolError(f"{field} must not be negative")
    return number


def _optional_positive_decimal(value: object, field: str) -> Decimal | None:
    if value is None or value == "":
        return None
    return _positive_decimal(value, field)


def _optional_non_negative_decimal(value: object, field: str) -> Decimal | None:
    if value is None or value == "":
        return None
    return _non_negative_decimal(value, field)


def _decimal(value: object, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise CoinbaseProtocolError(f"{field} must be numeric") from exc
