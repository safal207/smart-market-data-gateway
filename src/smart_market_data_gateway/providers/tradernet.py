from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import json
import logging
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import NAMESPACE_URL, uuid5

import httpx
import websockets

from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.providers.base import (
    MarketDataProvider,
    ProviderHealth,
    ProviderState,
)

logger = logging.getLogger(__name__)


class TradernetMode(StrEnum):
    PUBLIC_DEMO = "public_demo"
    SID_SESSION = "sid_session"
    API_KEY = "api_key"


class TradernetError(RuntimeError):
    pass


class TradernetAuthenticationError(TradernetError):
    pass


class TradernetAPIError(TradernetError):
    pass


@dataclass(frozen=True, slots=True)
class TradernetProviderConfig:
    mode: TradernetMode = TradernetMode.PUBLIC_DEMO
    websocket_url: str = "wss://wss.tradernet.com/"
    snapshot_base_url: str = "https://tradernet.com"
    sid: str | None = None
    user_id: str | None = None
    api_key: str | None = None
    api_secret: str | None = None
    require_authenticated_sid: bool = True
    snapshot_fallback: bool = True
    connect_timeout_seconds: float = 10.0
    snapshot_timeout_seconds: float = 10.0


WebSocketFactory = Callable[..., Awaitable[Any]]


class TradernetProviderAdapter(MarketDataProvider):
    """Tradernet/Freedom quote adapter using the documented JSON WebSocket protocol.

    The provider sends and receives ``[event, data]`` frames. Quote subscriptions are
    replaced atomically by sending ``["quotes", [symbols...]]`` and updates arrive as
    the ``q`` event. SID mode is read-only here; order and portfolio methods are not
    implemented by this market-data adapter.
    """

    def __init__(
        self,
        config: TradernetProviderConfig,
        *,
        websocket_factory: WebSocketFactory | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self._websocket_factory = websocket_factory or websockets.connect
        self._http_client = http_client
        self._websocket: Any | None = None
        self._state = ProviderState.DISCONNECTED
        self._message: str | None = None
        self._active_symbols: set[str] = set()
        self._session_info: dict[str, Any] = {}

    @property
    def name(self) -> str:
        return "tradernet"

    @property
    def active_symbols(self) -> frozenset[str]:
        return frozenset(self._active_symbols)

    @property
    def session_info(self) -> Mapping[str, Any]:
        return dict(self._session_info)

    async def connect(self) -> None:
        if self.config.mode is TradernetMode.API_KEY:
            raise NotImplementedError(
                "Tradernet API-key WebSocket authentication is gated until the current "
                "HMAC canonical-string contract is verified against official documentation"
            )
        if self.config.mode is TradernetMode.SID_SESSION and not self.config.sid:
            raise TradernetAuthenticationError("SID mode requires SMDG_TRADERNET_SID")

        await self.disconnect()
        self._state = ProviderState.CONNECTING
        self._message = None
        try:
            self._websocket = await self._websocket_factory(
                self._connection_url(),
                open_timeout=self.config.connect_timeout_seconds,
                close_timeout=5,
                ping_interval=20,
                ping_timeout=20,
                max_queue=1024,
            )
            self._state = ProviderState.CONNECTED
            if self._active_symbols:
                await self._send_subscription()
        except Exception as exc:
            self._state = ProviderState.DISCONNECTED
            self._message = str(exc)
            raise

    async def disconnect(self) -> None:
        websocket = self._websocket
        self._websocket = None
        self._state = ProviderState.DISCONNECTED
        if websocket is not None:
            try:
                await websocket.close()
            except Exception:
                logger.debug("Tradernet websocket close failed", exc_info=True)

    async def subscribe(self, symbols: Collection[str]) -> None:
        self._active_symbols.update(self._normalize_symbols(symbols))
        if self._state is ProviderState.CONNECTED:
            await self._send_subscription()

    async def unsubscribe(self, symbols: Collection[str]) -> None:
        self._active_symbols.difference_update(self._normalize_symbols(symbols))
        if self._state is ProviderState.CONNECTED:
            await self._send_subscription()

    async def health(self) -> ProviderHealth:
        return ProviderHealth(self._state, self._message)

    async def events(self) -> AsyncIterator[QuoteEvent]:
        websocket = self._websocket
        if websocket is None or self._state is not ProviderState.CONNECTED:
            raise TradernetError("Tradernet provider is not connected")

        while self._websocket is websocket:
            try:
                raw = await websocket.recv()
            except Exception as exc:
                self._state = ProviderState.DEGRADED
                self._message = f"websocket receive failed: {exc}"
                if self.config.snapshot_fallback and self._active_symbols:
                    try:
                        for snapshot in await self.fetch_snapshots(self._active_symbols):
                            yield snapshot
                    except Exception:
                        logger.warning(
                            "Tradernet HTTP snapshot fallback failed",
                            extra={"event": "tradernet_snapshot_fallback_failed"},
                            exc_info=True,
                        )
                raise

            for event in self.parse_message(raw):
                yield event

    def parse_message(self, raw: str | bytes) -> list[QuoteEvent]:
        try:
            text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            frame = json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
            logger.warning(
                "Ignoring malformed Tradernet frame",
                extra={"event": "tradernet_malformed_frame"},
            )
            return []

        if not isinstance(frame, list) or not frame or not isinstance(frame[0], str):
            logger.warning(
                "Ignoring invalid Tradernet frame shape",
                extra={"event": "tradernet_invalid_frame_shape"},
            )
            return []

        event_name = frame[0]
        payload = frame[1] if len(frame) > 1 else None
        if event_name == "userData":
            self._handle_user_data(payload)
            return []
        if event_name not in {"q", "quotes"}:
            logger.debug(
                "Ignoring unsupported Tradernet event",
                extra={"event": "tradernet_unknown_event", "provider_event": event_name},
            )
            return []

        rows = self._quote_rows(payload)
        normalized: list[QuoteEvent] = []
        for row in rows:
            quote_event = self._normalize_quote(row)
            if quote_event is not None:
                normalized.append(quote_event)
        return normalized

    async def fetch_snapshots(self, symbols: Collection[str]) -> list[QuoteEvent]:
        normalized = self._normalize_symbols(symbols)
        if not normalized:
            return []
        url = self._snapshot_url(normalized)
        owns_client = self._http_client is None
        client = self._http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.snapshot_timeout_seconds)
        )
        try:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise TradernetAPIError(f"Tradernet snapshot request failed: {exc}") from exc
        finally:
            if owns_client:
                await client.aclose()

        self._raise_for_api_error(payload)
        events: list[QuoteEvent] = []
        for row in self._extract_snapshot_rows(payload):
            event = self._normalize_quote(row)
            if event is not None:
                events.append(event)
        return events

    def _connection_url(self) -> str:
        split = urlsplit(self.config.websocket_url)
        query = dict(parse_qsl(split.query, keep_blank_values=True))
        if self.config.sid:
            query["SID"] = self.config.sid
        if self.config.user_id:
            query["user_id"] = self.config.user_id
        return urlunsplit((split.scheme, split.netloc, split.path or "/", urlencode(query), split.fragment))

    def _snapshot_url(self, symbols: Collection[str]) -> str:
        encoded = "+".join(quote(symbol, safe="./:_-") for symbol in sorted(symbols))
        return f"{self.config.snapshot_base_url.rstrip('/')}/securities/export?tickers={encoded}"

    async def _send_subscription(self) -> None:
        websocket = self._websocket
        if websocket is None:
            raise TradernetError("Tradernet provider is not connected")
        await websocket.send(json.dumps(["quotes", sorted(self._active_symbols)]))

    def _handle_user_data(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        self._session_info = dict(payload)
        is_demo = bool(payload.get("isDemo")) or str(payload.get("mode", "")).lower() == "demo"
        if (
            self.config.mode is TradernetMode.SID_SESSION
            and self.config.require_authenticated_sid
            and is_demo
        ):
            self._state = ProviderState.DEGRADED
            self._message = "SID was rejected or expired; Tradernet returned demo mode"
            raise TradernetAuthenticationError(self._message)

    @staticmethod
    def _normalize_symbols(symbols: Collection[str]) -> set[str]:
        return {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}

    @staticmethod
    def _quote_rows(payload: Any) -> Iterable[Mapping[str, Any]]:
        if isinstance(payload, dict):
            if "c" in payload:
                return [payload]
            nested = payload.get("q") or payload.get("quotes") or payload.get("data")
            return TradernetProviderAdapter._quote_rows(nested)
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

    @staticmethod
    def _raise_for_api_error(payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        code = payload.get("code")
        error = payload.get("error") or payload.get("errMsg")
        if code not in {None, 0, "0", "ok", "OK"} or error:
            raise TradernetAPIError(f"Tradernet API error code={code!r}: {error or 'unknown error'}")

    @classmethod
    def _extract_snapshot_rows(cls, payload: Any) -> Iterable[Mapping[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if not isinstance(payload, dict):
            return []
        result = payload.get("result", payload)
        if isinstance(result, dict):
            for key in ("q", "quotes", "data", "securities"):
                if key in result:
                    rows = result[key]
                    if isinstance(rows, dict):
                        return [item for item in rows.values() if isinstance(item, dict)]
                    return cls._quote_rows(rows)
        return cls._quote_rows(result)

    @classmethod
    def _normalize_quote(cls, row: Mapping[str, Any]) -> QuoteEvent | None:
        symbol = str(row.get("c", "")).strip().upper()
        if not symbol:
            return None

        bid = cls._positive_decimal(row.get("bbp"))
        ask = cls._positive_decimal(row.get("bap"))
        price = cls._positive_decimal(row.get("ltp"))
        if price is None and bid is not None and ask is not None:
            price = (bid + ask) / Decimal(2)
        price = price or bid or ask
        if price is None:
            return None

        received_at = datetime.now(UTC)
        provider_timestamp = cls._provider_timestamp(row.get("ltt"), received_at)
        fingerprint = json.dumps(
            {
                "symbol": symbol,
                "price": str(price),
                "bid": str(bid) if bid is not None else None,
                "ask": str(ask) if ask is not None else None,
                "ltt": row.get("ltt"),
                "lts": row.get("lts"),
                "trades": row.get("trades"),
                "vol": row.get("vol"),
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return QuoteEvent(
            event_id=uuid5(NAMESPACE_URL, f"tradernet:{fingerprint}"),
            symbol=symbol,
            price=price,
            bid=bid,
            ask=ask,
            provider_timestamp=provider_timestamp,
            received_at=received_at,
            provider="tradernet",
        )

    @staticmethod
    def _positive_decimal(value: Any) -> Decimal | None:
        if value in {None, "", "-"}:
            return None
        try:
            parsed = Decimal(str(value).replace(" ", "").replace(",", "."))
        except (InvalidOperation, ValueError):
            return None
        return parsed if parsed > 0 else None

    @staticmethod
    def _provider_timestamp(value: Any, received_at: datetime) -> datetime:
        if isinstance(value, (int, float)):
            seconds = float(value)
            if seconds > 10_000_000_000:
                seconds /= 1000
            try:
                return datetime.fromtimestamp(seconds, tz=UTC)
            except (OverflowError, OSError, ValueError):
                return received_at
        if isinstance(value, str) and value.strip():
            try:
                parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            except ValueError:
                return received_at
            if parsed.tzinfo is not None and parsed.utcoffset() is not None:
                return parsed.astimezone(UTC)
            # Tradernet examples contain timezone-free local timestamps. Using receive time
            # avoids publishing false exchange-latency measurements based on an unknown zone.
            return received_at
        return received_at
