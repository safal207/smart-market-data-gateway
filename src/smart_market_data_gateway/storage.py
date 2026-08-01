from collections.abc import AsyncIterator
from datetime import UTC, datetime
import json
import logging
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from redis.exceptions import ResponseError

from smart_market_data_gateway.config import Settings
from smart_market_data_gateway.domain import (
    AcceptedQuoteEvent,
    GapObservation,
    QuoteEvent,
    QuoteSnapshot,
    RejectedQuoteEvent,
)

logger = logging.getLogger(__name__)


class RedisStore:
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self.redis = redis
        self.settings = settings

    async def ensure_group(self, stream: str, group: str) -> None:
        try:
            await self.redis.xgroup_create(stream, group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def ensure_groups(self) -> None:
        for stream, group in (
            (self.settings.quote_stream, self.settings.stream_group),
            (self.settings.control_stream, self.settings.control_group),
        ):
            await self.ensure_group(stream, group)

    async def publish_quote(self, event: QuoteEvent) -> str:
        stream_id = await self.redis.xadd(
            self.settings.quote_stream,
            {"payload": event.model_dump_json()},
            maxlen=self.settings.stream_maxlen,
            approximate=True,
        )
        return str(stream_id)

    async def publish_accepted_event(self, accepted: AcceptedQuoteEvent) -> str:
        stream_id = await self.redis.xadd(
            self.settings.accepted_event_stream,
            {
                "payload": accepted.model_dump_json(),
                "event_id": str(accepted.event.event_id),
                "symbol": accepted.event.symbol,
                "event_time": accepted.event.provider_timestamp.isoformat(),
                "accepted_at": accepted.quality.accepted_at.isoformat(),
                "quality_score": str(accepted.quality.score),
            },
            maxlen=self.settings.accepted_stream_maxlen,
            approximate=True,
        )
        return str(stream_id)

    async def publish_rejected_event(self, rejected: RejectedQuoteEvent) -> str:
        stream_id = await self.redis.xadd(
            self.settings.rejected_event_stream,
            {
                "payload": rejected.model_dump_json(),
                "event_id": str(rejected.event.event_id),
                "symbol": rejected.event.symbol,
                "reason": rejected.reason.value,
                "rejected_at": rejected.rejected_at.isoformat(),
            },
            maxlen=self.settings.rejected_stream_maxlen,
            approximate=True,
        )
        return str(stream_id)

    async def publish_control(self, action: str, symbol: str) -> str:
        stream_id = await self.redis.xadd(
            self.settings.control_stream,
            {
                "action": action,
                "symbol": symbol,
                "created_at": datetime.now(UTC).isoformat(),
            },
            maxlen=10_000,
            approximate=True,
        )
        return str(stream_id)

    async def read_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int = 100,
        block_ms: int = 1000,
    ) -> list[tuple[str, dict[str, str]]]:
        result = await self.redis.xreadgroup(
            group,
            consumer,
            {stream: ">"},
            count=count,
            block=block_ms,
        )
        messages: list[tuple[str, dict[str, str]]] = []
        for _stream_name, entries in result:
            for stream_id, fields in entries:
                messages.append((str(stream_id), {str(k): str(v) for k, v in fields.items()}))
        return messages

    async def ack(self, stream: str, group: str, *stream_ids: str) -> None:
        if stream_ids:
            await self.redis.xack(stream, group, *stream_ids)

    async def cache_quote(self, event: QuoteEvent) -> None:
        await self.redis.set(f"smdg:latest:{event.symbol}", event.model_dump_json())

    async def get_latest(self, symbol: str) -> QuoteSnapshot | None:
        payload = await self.redis.get(f"smdg:latest:{symbol.upper()}")
        if payload is None:
            return None
        quote = QuoteEvent.model_validate_json(payload)
        age_seconds = max(0.0, (datetime.now(UTC) - quote.received_at).total_seconds())
        return QuoteSnapshot(
            quote=quote,
            stale=age_seconds > self.settings.quote_freshness_seconds,
            age_ms=int(age_seconds * 1000),
        )

    async def get_many(self, symbols: list[str]) -> dict[str, QuoteSnapshot | None]:
        normalized = [symbol.upper() for symbol in symbols]
        payloads = await self.redis.mget([f"smdg:latest:{symbol}" for symbol in normalized])
        result: dict[str, QuoteSnapshot | None] = {}
        now = datetime.now(UTC)
        for symbol, payload in zip(normalized, payloads, strict=True):
            if payload is None:
                result[symbol] = None
                continue
            quote = QuoteEvent.model_validate_json(payload)
            age_seconds = max(0.0, (now - quote.received_at).total_seconds())
            result[symbol] = QuoteSnapshot(
                quote=quote,
                stale=age_seconds > self.settings.quote_freshness_seconds,
                age_ms=int(age_seconds * 1000),
            )
        return result

    async def accept_event_once(self, event: QuoteEvent) -> bool:
        accepted = await self.redis.set(
            f"smdg:dedupe:{event.event_id}",
            "1",
            ex=self.settings.dedupe_ttl_seconds,
            nx=True,
        )
        return bool(accepted)

    async def observe_sequence(self, event: QuoteEvent) -> GapObservation | None:
        if event.sequence is None:
            return None
        key = f"smdg:sequence:{event.provider}:{event.symbol}"
        previous_raw = await self.redis.get(key)
        previous = int(previous_raw) if previous_raw is not None else None
        if previous is None or event.sequence > previous:
            await self.redis.set(key, event.sequence, ex=86_400)
        if previous is None or event.sequence == previous + 1:
            return None
        return GapObservation(
            symbol=event.symbol,
            previous_sequence=previous,
            current_sequence=event.sequence,
            gap=max(0, event.sequence - previous - 1),
            out_of_order=event.sequence <= previous,
        )

    async def publish_fanout(self, event: QuoteEvent) -> None:
        await self.redis.publish(self.settings.quote_pubsub_channel, event.model_dump_json())

    async def fanout_messages(self) -> AsyncIterator[QuoteEvent]:
        pubsub: PubSub = self.redis.pubsub(ignore_subscribe_messages=True)
        await pubsub.subscribe(self.settings.quote_pubsub_channel)
        try:
            while True:
                message = await pubsub.get_message(timeout=1.0)
                if not message:
                    continue
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode()
                if isinstance(data, str):
                    yield QuoteEvent.model_validate_json(data)
        finally:
            await pubsub.unsubscribe(self.settings.quote_pubsub_channel)
            await pubsub.aclose()

    async def move_to_dead_letter(
        self,
        *,
        source_stream: str,
        stream_id: str,
        payload: dict[str, Any],
        error: str,
        retry_count: int,
    ) -> None:
        await self.redis.xadd(
            self.settings.dead_letter_stream,
            {
                "source_stream": source_stream,
                "source_id": stream_id,
                "payload": json.dumps(payload, default=str),
                "error": error,
                "retry_count": retry_count,
                "failed_at": datetime.now(UTC).isoformat(),
            },
            maxlen=10_000,
            approximate=True,
        )

    async def increment_retry(self, stream_id: str) -> int:
        key = f"smdg:retry:{stream_id}"
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, 3600)
            result = await pipe.execute()
        return int(result[0])

    async def rate_limit(self, key: str, limit: int, window_seconds: int = 60) -> bool:
        redis_key = f"smdg:rate:{key}"
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.incr(redis_key)
            pipe.expire(redis_key, window_seconds, nx=True)
            result = await pipe.execute()
        return int(result[0]) <= limit

    async def record_usage(
        self,
        *,
        idempotency_key: str,
        client_id: str,
        event_type: str,
        quantity: int = 1,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        accepted = await self.redis.set(
            f"smdg:usage:idempotency:{idempotency_key}",
            "1",
            ex=86_400,
            nx=True,
        )
        if not accepted:
            return False
        await self.redis.xadd(
            self.settings.usage_stream,
            {
                "idempotency_key": idempotency_key,
                "client_id": client_id,
                "event_type": event_type,
                "quantity": quantity,
                "metadata": json.dumps(metadata or {}, default=str),
                "created_at": datetime.now(UTC).isoformat(),
            },
            maxlen=100_000,
            approximate=True,
        )
        return True

    async def pending_count(self) -> int:
        try:
            summary = await self.redis.xpending(
                self.settings.quote_stream,
                self.settings.stream_group,
            )
        except ResponseError:
            return 0
        if isinstance(summary, dict):
            return int(summary.get("pending", 0))
        if isinstance(summary, (list, tuple)) and summary:
            return int(summary[0])
        return 0
