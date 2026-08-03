import json
from typing import Any

import pytest

from smart_market_data_gateway.resilience_client import (
    connect_and_subscribe,
    receive_quote_after_marker,
)


class FakeSocket:
    def __init__(
        self,
        messages: list[dict[str, Any]] | None = None,
        *,
        send_error: BaseException | None = None,
    ) -> None:
        self.messages = [json.dumps(message) for message in messages or []]
        self.sent: list[dict[str, Any]] = []
        self.closed = False
        self.send_error = send_error

    async def recv(self) -> str:
        if not self.messages:
            raise TimeoutError("no more messages")
        return self.messages.pop(0)

    async def send(self, payload: str) -> None:
        if self.send_error is not None:
            raise self.send_error
        message = json.loads(payload)
        assert isinstance(message, dict)
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True


async def test_connection_setup_failure_closes_benchmark_socket() -> None:
    socket = FakeSocket(
        [{"type": "connected", "data": {}}],
        send_error=ConnectionError("subscription failed"),
    )

    async def factory(_url: str, **_kwargs: Any) -> FakeSocket:
        return socket

    with pytest.raises(ConnectionError, match="subscription failed"):
        await connect_and_subscribe(
            1,
            url="ws://localhost:8000/v1/stream",
            symbols=["AAPL.US"],
            socket_factory=factory,
        )

    assert socket.closed is True


async def test_pong_marker_discards_backlog_and_wrong_ack() -> None:
    socket = FakeSocket()

    async def send(payload: str) -> None:
        message = json.loads(payload)
        assert isinstance(message, dict)
        socket.sent.append(message)
        request_id = str(message["request_id"])
        socket.messages.extend(
            [
                json.dumps(
                    {
                        "type": "quote",
                        "data": {"quote": {"price": "old"}},
                    }
                ),
                json.dumps(
                    {
                        "type": "ack",
                        "request_id": "wrong-marker",
                        "data": {"action": "pong"},
                    }
                ),
                json.dumps(
                    {
                        "type": "ack",
                        "request_id": request_id,
                        "data": {"action": "pong"},
                    }
                ),
                json.dumps(
                    {
                        "type": "quote",
                        "data": {"quote": {"price": "fresh"}},
                    }
                ),
            ]
        )

    socket.send = send  # type: ignore[method-assign]
    quote = await receive_quote_after_marker(socket, timeout_seconds=1)

    assert quote == {"price": "fresh"}
    assert socket.sent[0]["action"] == "ping"


async def test_missing_matching_pong_fails_closed() -> None:
    socket = FakeSocket(
        [
            {"type": "quote", "data": {"quote": {"price": "old"}}},
            {
                "type": "ack",
                "request_id": "unrelated",
                "data": {"action": "pong"},
            },
        ]
    )

    with pytest.raises(TimeoutError):
        await receive_quote_after_marker(socket, timeout_seconds=0.01)
