from __future__ import annotations

import asyncio
import json
import socket
from contextlib import suppress
from pathlib import Path

import pytest
from redis.asyncio import Redis
from uvicorn import Config, Server

from smart_market_data_gateway.app import create_app
from smart_market_data_gateway.collector import build_mock_collector
from smart_market_data_gateway.config import Settings
from smart_market_data_gateway.recorder import record_websocket, verify_jsonl_ledger


def _unused_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


async def _wait_until_started(server: Server, timeout_seconds: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while not server.started:
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("test gateway did not start")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_records_real_gateway_websocket_session_to_verified_rich_ledger(
    tmp_path: Path,
    redis_client: Redis,
    test_settings: Settings,
) -> None:
    settings = test_settings.model_copy(
        update={
            "mock_interval_seconds": 0.01,
            "heartbeat_seconds": 0.25,
            "client_idle_timeout_seconds": 5.0,
            "shutdown_timeout_seconds": 2.0,
        }
    )
    port = _unused_tcp_port()
    output = tmp_path / "live-session.jsonl"

    collector = build_mock_collector(redis_client, settings)
    collector_task = asyncio.create_task(collector.run(), name="e2e-mock-collector")

    server = Server(
        Config(
            create_app(settings),
            host="127.0.0.1",
            port=port,
            log_level="warning",
            access_log=False,
            lifespan="on",
        )
    )
    server_task = asyncio.create_task(server.serve(), name="e2e-gateway-server")

    try:
        await _wait_until_started(server)
        counters = await asyncio.wait_for(
            record_websocket(
                url=f"ws://127.0.0.1:{port}/v1/stream",
                token="dev-pro:e2e-recorder",
                symbols=["AAPL", "TSLA"],
                output=output,
                max_records=4,
                max_reconnects=1,
                fsync=False,
            ),
            timeout=10.0,
        )
    finally:
        await collector.close()
        collector_task.cancel()
        with suppress(asyncio.CancelledError):
            await collector_task
        server.should_exit = True
        await asyncio.wait_for(server_task, timeout=5.0)

    verification = verify_jsonl_ledger(output)
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]

    assert counters.written == 4
    assert counters.reconnects == 0
    assert verification.records == 4
    assert verification.head_hash == rows[-1]["record_hash"]
    assert {row["symbol"] for row in rows} == {"AAPL", "TSLA"}
    assert all(row["source_message_type"] == "quote" for row in rows)
    assert all(row["provenance_transport"] == "websocket" for row in rows)

    assert all(row["schema_version"] == "1.1" for row in rows)
    assert all("volume" in row and float(row["volume"]) >= 0 for row in rows)
    assert all("buy_volume" in row and "sell_volume" in row for row in rows)
    assert all("bid_depth" in row and "ask_depth" in row for row in rows)
    assert all(row["trade_count"] >= 0 for row in rows)
    assert all(row["volume_semantics"]["unit"] == "base_asset" for row in rows)
    assert all(row["volume_semantics"]["aggregation_window_ms"] == 10 for row in rows)
    assert all(row["depth_semantics"]["levels"] == 1 for row in rows)
    assert all(
        {
            "level1_quote",
            "volume",
            "aggressor_flow",
            "trade_count",
            "top_of_book_depth",
        }.issubset(set(row["capabilities"]))
        for row in rows
    )
