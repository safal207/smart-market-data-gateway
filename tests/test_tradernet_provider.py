import json
from typing import Any

import httpx
import pytest

from smart_market_data_gateway.providers import (
    ProviderState,
    TradernetAPIError,
    TradernetAuthenticationError,
    TradernetMode,
    TradernetProviderAdapter,
    TradernetProviderConfig,
)


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


def quote_payload() -> dict[str, str]:
    return {
        "c": "aapl.us",
        "ltp": "147,42",
        "bbp": "147,39",
        "bap": "147,45",
        "ltt": "2026-08-01T12:00:00+00:00",
        "lts": "10",
        "trades": "100",
        "vol": "1000",
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
