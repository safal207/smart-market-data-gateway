from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient
from redis import Redis

from smart_market_data_gateway.app import create_app
from smart_market_data_gateway.candles import BaseMinuteCandle
from smart_market_data_gateway.domain import QuoteEvent


def make_quote(
    symbol: str = "AAPL",
    sequence: int = 1,
    *,
    timestamp: datetime | None = None,
    price: str = "215.42",
) -> QuoteEvent:
    observed_at = timestamp or datetime.now(UTC)
    value = Decimal(price)
    return QuoteEvent(
        symbol=symbol,
        price=value,
        bid=value - Decimal("0.02"),
        ask=value + Decimal("0.02"),
        provider_timestamp=observed_at,
        received_at=observed_at,
        sequence=sequence,
        provider="api-test",
    )


def test_rest_latest_and_multi_quote(test_settings) -> None:
    redis = Redis.from_url(test_settings.redis_url, decode_responses=True)
    redis.flushdb()
    quote = make_quote()
    redis.set("smdg:latest:AAPL", quote.model_dump_json())

    with TestClient(create_app(test_settings)) as client:
        assert client.get("/health/live").json() == {"status": "ok"}
        response = client.get("/v1/quotes/AAPL")
        assert response.status_code == 200
        assert response.json()["quote"]["symbol"] == "AAPL"

        response = client.get("/v1/quotes", params={"symbols": "AAPL,UNKNOWN"})
        assert response.status_code == 200
        assert response.json()["data"][0]["quote"]["symbol"] == "AAPL"
        assert response.json()["errors"] == [{"symbol": "UNKNOWN", "code": "not_found"}]

        assert client.get("/v1/quotes/UNKNOWN").status_code == 404
        assert client.get("/metrics").status_code == 200

    redis.flushdb()
    redis.close()


def test_candle_history_requires_entitlement_and_returns_server_series(test_settings) -> None:
    redis = Redis.from_url(test_settings.redis_url, decode_responses=True)
    redis.flushdb()
    start = datetime.now(UTC).replace(second=0, microsecond=0)
    candle = BaseMinuteCandle.from_quote(
        make_quote(timestamp=start + timedelta(seconds=5), price="100")
    ).with_quote(
        make_quote(sequence=2, timestamp=start + timedelta(seconds=45), price="103")
    )
    bucket_epoch = str(int(start.timestamp()))
    redis.set(
        f"smdg:candles:data:v1:1m:AAPL:{bucket_epoch}",
        candle.model_dump_json(),
    )
    redis.zadd("smdg:candles:index:v1:1m:AAPL", {bucket_epoch: start.timestamp()})

    with TestClient(create_app(test_settings)) as client:
        basic = client.get("/v1/candles/AAPL")
        assert basic.status_code == 403
        assert "basic tier" in basic.json()["detail"]

        response = client.get(
            "/v1/candles/aapl",
            params={
                "timeframe": "5m",
                "limit": 10,
                "end": (start + timedelta(minutes=5, seconds=1)).isoformat(),
            },
            headers={"Authorization": "Bearer dev-pro:bob"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["symbol"] == "AAPL"
        assert payload["source"] == "observed_quote_aggregation"
        assert payload["returned_count"] == 1
        assert payload["data"][0]["open"] == "100"
        assert payload["data"][0]["high"] == "103"
        assert payload["data"][0]["activity_count"] == 2
        assert payload["data"][0]["closed"] is True

        invalid_end = client.get(
            "/v1/candles/AAPL",
            params={"end": "2026-08-04T12:00:00"},
            headers={"Authorization": "Bearer dev-pro:bob"},
        )
        assert invalid_end.status_code == 422

    redis.flushdb()
    redis.close()


def test_websocket_snapshot_subscribe_live_event_and_unsubscribe(test_settings) -> None:
    redis = Redis.from_url(test_settings.redis_url, decode_responses=True)
    redis.flushdb()
    initial = make_quote(sequence=1)
    redis.set("smdg:latest:AAPL", initial.model_dump_json())

    with TestClient(create_app(test_settings)) as client:
        with client.websocket_connect("/v1/stream?token=dev-basic:test-client") as socket:
            connected = socket.receive_json()
            assert connected["type"] == "connected"
            assert connected["data"]["tier"] == "basic"

            socket.send_json(
                {
                    "action": "subscribe",
                    "symbols": ["aapl"],
                    "channels": ["quote"],
                    "request_id": "sub-1",
                }
            )
            snapshot = socket.receive_json()
            ack = socket.receive_json()
            assert snapshot["type"] == "snapshot"
            assert snapshot["data"]["quote"]["symbol"] == "AAPL"
            assert ack["type"] == "ack"
            assert ack["data"]["symbols"] == ["AAPL"]

            live = make_quote(sequence=2)
            redis.publish(test_settings.quote_pubsub_channel, live.model_dump_json())
            quote_message = socket.receive_json()
            assert quote_message["type"] == "quote"
            assert quote_message["data"]["quote"]["sequence"] == 2

            socket.send_json(
                {
                    "action": "unsubscribe",
                    "symbols": ["AAPL"],
                    "request_id": "unsub-1",
                }
            )
            unsubscribe_ack = socket.receive_json()
            assert unsubscribe_ack["type"] == "ack"
            assert unsubscribe_ack["data"]["action"] == "unsubscribe"

    redis.flushdb()
    redis.close()
