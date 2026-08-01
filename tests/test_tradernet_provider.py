import asyncio
import json
from typing import Any

import httpx
import pytest

from smart_market_data_gateway.collector import CollectorService
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.providers import (
    ProviderState,
    TradernetAPIError,
    TradernetAuthenticationError,
    TradernetMode,
    TradernetProviderAdapter,
    TradernetProviderConfig,
)
from smart_market_data_gateway.storage import RedisStore


class FakeWebSocket:
    def __init__(self, messages: list[str] | None = None) -> None:
        self.sent: list[str] = []
        self.messages = list(messages or [])
        self.closed = False

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def recv(self) -> str:
        if not self.messages:
            raise ConnectionError("stream ended")
        return self.messages.pop(0)

    async def close(self) -> None:
        self.closed = True


class FakeWebSocketFactory:
    def __init__(self, sockets: list[FakeWebSocket]) -> None:
        self.sockets = sockets
        self.urls: list[str] = []
        self.options: list[dict[str, Any]] = []

    async def __call__(self, url: str, **kwargs: Any) -> FakeWebSocket:
        self.urls.append(url)
        self.options.append(kwargs)
        return self.sockets.pop(0)


class ControlledWebSocket(FakeWebSocket):
    def __init__(self) -> None:
        super().__init__()
        self.recv_started = asyncio.Event()
        self.recv_result: asyncio.Future[str] = asyncio.get_running_loop().create_future()

    async def recv(self) -> str:
        self.recv_started.set()
        return await self.recv_result


def quote_payload() -> dict[str, str]:
    return {
        "c": "aapl.us",
        "ltp": "147,42",
        "bbp": "147,39",
        "bap": "147,45",
        "ltt": "2026-08-01T12:00:00+00:00",
        "lts": "10,5",
        "trades": "100,0",
        "vol": "1 000,25",
    }


def test_parse_quote_event_and_decimal_commas() -> None:
    provider = TradernetProviderAdapter(TradernetProviderConfig())
    events = provider.parse_message(json.dumps(["q", [quote_payload()]]))

    assert len(events) == 1
    event = events[0]
    assert event.symbol == "AAPL.US"
    assert str(event.price) == "147.42"
    assert str(event.bid) == "147.39"
    assert str(event.ask) == "147.45"
    assert str(event.last_size) == "10.5"
    assert str(event.cumulative_volume) == "1000.25"
    assert event.trade_count == 100
    assert event.provider == "tradernet"


def test_duplicate_payload_gets_deterministic_event_id() -> None:
    provider = TradernetProviderAdapter(TradernetProviderConfig())
    frame = json.dumps(["q", quote_payload()])

    first = provider.parse_message(frame)[0]
    second = provider.parse_message(frame)[0]

    assert first.event_id == second.event_id


def test_malformed_and_unknown_frames_are_ignored() -> None:
    provider = TradernetProviderAdapter(TradernetProviderConfig())

    assert provider.parse_message("not-json") == []
    assert provider.parse_message(json.dumps({"event": "q"})) == []
    assert provider.parse_message(json.dumps(["portfolio", {}])) == []


async def test_subscription_is_replaced_and_restored_after_reconnect() -> None:
    first = FakeWebSocket()
    second = FakeWebSocket()
    factory = FakeWebSocketFactory([first, second])
    provider = TradernetProviderAdapter(
        TradernetProviderConfig(
            mode=TradernetMode.SID_SESSION,
            sid="test-session",
            user_id="123",
        ),
        websocket_factory=factory,
    )

    await provider.subscribe(["AAPL.US", "MSFT.US"])
    await provider.connect()
    assert json.loads(first.sent[-1]) == ["quotes", ["AAPL.US", "MSFT.US"]]
    assert "SID=test-session" in factory.urls[0]
    assert "user_id=123" in factory.urls[0]

    await provider.unsubscribe(["MSFT.US"])
    assert json.loads(first.sent[-1]) == ["quotes", ["AAPL.US"]]

    await provider.disconnect()
    await provider.connect()
    assert json.loads(second.sent[-1]) == ["quotes", ["AAPL.US"]]
    assert len(second.sent) == 1

    # CollectorService reapplies its desired symbols after connect. The adapter already
    # restored the list, so an idempotent subscribe must not send the full watch-list twice.
    await provider.subscribe(["AAPL.US"])
    assert len(second.sent) == 1
    assert (await provider.health()).state is ProviderState.CONNECTED


async def test_collector_offline_unsubscribe_is_not_restored_as_zombie(
    redis_client,
    test_settings,
) -> None:
    first = FakeWebSocket()
    second = FakeWebSocket()
    provider = TradernetProviderAdapter(
        TradernetProviderConfig(snapshot_fallback=False),
        websocket_factory=FakeWebSocketFactory([first, second]),
    )
    collector = CollectorService(
        provider,
        RedisStore(redis_client, test_settings),
        test_settings,
        GatewayMetrics(),
    )

    await collector._apply_control("subscribe", "AAPL.US")
    await provider.connect()
    assert json.loads(first.sent[-1]) == ["quotes", ["AAPL.US"]]

    await provider.disconnect()
    await collector._apply_control("unsubscribe", "AAPL.US")
    await provider.connect()

    assert provider.active_symbols == frozenset()
    assert second.sent == []
    await provider.disconnect()


async def test_old_connection_frame_is_fenced_after_reconnect() -> None:
    first = ControlledWebSocket()
    second = FakeWebSocket()
    provider = TradernetProviderAdapter(
        TradernetProviderConfig(snapshot_fallback=False),
        websocket_factory=FakeWebSocketFactory([first, second]),
    )
    await provider.subscribe(["AAPL.US"])
    await provider.connect()
    old_generation = provider.connection_generation

    next_event = asyncio.create_task(anext(provider.events()))
    await first.recv_started.wait()
    await provider.connect()
    assert provider.connection_generation > old_generation

    first.recv_result.set_result(json.dumps(["q", [quote_payload()]]))
    with pytest.raises(StopAsyncIteration):
        await next_event
    assert (await provider.health()).state is ProviderState.CONNECTED


async def test_old_snapshot_fallback_cannot_emit_or_degrade_new_connection() -> None:
    fallback_started = asyncio.Event()
    release_fallback = asyncio.Event()

    async def handler(_request: httpx.Request) -> httpx.Response:
        fallback_started.set()
        await release_fallback.wait()
        return httpx.Response(200, json={"result": {"q": {"0": quote_payload()}}})

    first = FakeWebSocket()
    second = FakeWebSocket()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = TradernetProviderAdapter(
            TradernetProviderConfig(snapshot_base_url="https://example.test"),
            websocket_factory=FakeWebSocketFactory([first, second]),
            http_client=client,
        )
        await provider.subscribe(["AAPL.US"])
        await provider.connect()

        old_fallback = asyncio.create_task(anext(provider.events()))
        await fallback_started.wait()
        await provider.connect()
        release_fallback.set()

        with pytest.raises(StopAsyncIteration):
            await old_fallback
        assert (await provider.health()).state is ProviderState.CONNECTED


async def test_sid_expiry_is_detected_from_demo_user_data() -> None:
    provider = TradernetProviderAdapter(
        TradernetProviderConfig(
            mode=TradernetMode.SID_SESSION,
            sid="expired-session",
            require_authenticated_sid=True,
        )
    )

    with pytest.raises(TradernetAuthenticationError, match="rejected or expired"):
        provider.parse_message(json.dumps(["userData", {"isDemo": True, "mode": "demo"}]))

    assert (await provider.health()).state is ProviderState.DEGRADED


async def test_public_demo_accepts_demo_user_data() -> None:
    provider = TradernetProviderAdapter(
        TradernetProviderConfig(mode=TradernetMode.PUBLIC_DEMO)
    )

    assert provider.parse_message(json.dumps(["userData", {"mode": "demo"}])) == []
    assert provider.session_info["mode"] == "demo"


async def test_snapshot_preserves_literal_plus_separator() -> None:
    observed_urls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        observed_urls.append(str(request.url))
        return httpx.Response(
            200,
            json={"result": {"q": {"0": quote_payload(), "1": {**quote_payload(), "c": "MSFT.US"}}}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = TradernetProviderAdapter(
            TradernetProviderConfig(snapshot_base_url="https://example.test"),
            http_client=client,
        )
        snapshots = await provider.fetch_snapshots(["AAPL.US", "MSFT.US"])

    assert len(snapshots) == 2
    assert "/securities/export?tickers=AAPL.US+MSFT.US" in observed_urls[0]
    assert "%2B" not in observed_urls[0]


async def test_http_200_with_internal_error_is_rejected() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 12, "error": "Invalid credentials"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = TradernetProviderAdapter(
            TradernetProviderConfig(snapshot_base_url="https://example.test"),
            http_client=client,
        )
        with pytest.raises(TradernetAPIError, match="code=12"):
            await provider.fetch_snapshots(["AAPL.US"])


async def test_api_key_mode_is_explicitly_gated() -> None:
    provider = TradernetProviderAdapter(
        TradernetProviderConfig(
            mode=TradernetMode.API_KEY,
            api_key="public-placeholder",
            api_secret="private-placeholder",
        )
    )

    with pytest.raises(NotImplementedError, match="HMAC"):
        await provider.connect()
