from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import json
from typing import Any, Protocol, cast
from uuid import uuid4

import websockets


class ResilienceSocket(Protocol):
    async def send(self, payload: str) -> None: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


SocketFactory = Callable[..., Awaitable[ResilienceSocket]]


def websocket_url(base: str, token: str) -> str:
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}token={token}"


async def connect_and_subscribe(
    client_id: int,
    *,
    url: str,
    symbols: list[str],
    socket_factory: SocketFactory | None = None,
    setup_timeout_seconds: float = 10.0,
) -> ResilienceSocket:
    factory = socket_factory or cast(SocketFactory, websockets.connect)
    socket = await factory(
        websocket_url(url, f"dev-pro:resilience-{client_id}"),
        open_timeout=10,
        close_timeout=3,
        ping_interval=20,
        ping_timeout=20,
    )
    try:
        await asyncio.wait_for(socket.recv(), timeout=setup_timeout_seconds)
        await socket.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "symbols": symbols,
                    "channels": ["quote"],
                    "request_id": f"resilience-{client_id}",
                },
                separators=(",", ":"),
            )
        )
        return socket
    except BaseException:
        await socket.close()
        raise


def decode_message(raw: str | bytes) -> dict[str, Any]:
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("WebSocket message must be an object")
    return payload


def is_matching_pong(payload: dict[str, Any], request_id: str) -> bool:
    data = payload.get("data")
    return (
        payload.get("type") == "ack"
        and payload.get("request_id") == request_id
        and isinstance(data, dict)
        and data.get("action") == "pong"
    )


async def receive_quote(
    socket: ResilienceSocket,
    timeout_seconds: float,
) -> dict[str, Any]:
    async with asyncio.timeout(timeout_seconds):
        while True:
            payload = decode_message(await socket.recv())
            if payload.get("type") == "quote":
                data = payload.get("data")
                if isinstance(data, dict) and isinstance(data.get("quote"), dict):
                    return dict(data["quote"])


async def receive_quote_after_marker(
    socket: ResilienceSocket,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Return the first quote ordered after a matching gateway pong marker.

    The benchmark and gateway can run on hosts with different clocks. A matching pong
    on the same WebSocket is therefore the boundary: all messages before it are backlog
    and discarded, while the first quote after it proves the stream advanced past the
    marker without relying on cross-host timestamps.
    """

    request_id = f"resilience-marker-{uuid4()}"
    await socket.send(
        json.dumps(
            {"action": "ping", "request_id": request_id},
            separators=(",", ":"),
        )
    )
    marker_seen = False
    async with asyncio.timeout(timeout_seconds):
        while True:
            payload = decode_message(await socket.recv())
            if not marker_seen:
                marker_seen = is_matching_pong(payload, request_id)
                continue
            if payload.get("type") == "quote":
                data = payload.get("data")
                if isinstance(data, dict) and isinstance(data.get("quote"), dict):
                    return dict(data["quote"])
