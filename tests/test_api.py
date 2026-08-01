from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient
from redis import Redis

from smart_market_data_gateway.app import create_app
from smart_market_data_gateway.domain import QuoteEvent


def make_quote(symbol: str = "AAPL", sequence: int = 1) -> QuoteEvent:
    return QuoteEvent(
        symbol=symbol,
        price=Decimal("215.42"),
        bid=Decimal("215.40"),
        ask=Decimal("215.44"),
        provider_timestamp=datetime.now(UTC),
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
            subscription_messages = []
            while len(subscription_messages) < 2:
                message = socket.receive_json()
                if message["type"] != "heartbeat":
                    subscription_messages.append(message)
            snapshot, ack = subscription_messages
            assert snapshot["type"] == "snapshot"
            assert snapshot["data"]["quote"]["symbol"] == "AAPL"
            assert ack["type"] == "ack"
            assert ack["data"]["symbols"] == ["AAPL"]
            assert ack["data"]["upstream_transitions"] == ["AAPL"]

            live = make_quote(sequence=2)
            redis.publish(test_settings.quote_pubsub_channel, live.model_dump_json())
            quote_message = socket.receive_json()
            while quote_message["type"] == "heartbeat":
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
            while unsubscribe_ack["type"] == "heartbeat":
                unsubscribe_ack = socket.receive_json()
            assert unsubscribe_ack["type"] == "ack"
            assert unsubscribe_ack["data"]["action"] == "unsubscribe"

    redis.flushdb()
    redis.close()
