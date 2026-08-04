from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
import json
import logging
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from redis.exceptions import ResponseError, WatchError

from smart_market_data_gateway.candles import (
    BaseMinuteCandle,
    CandleSeries,
    CandleTimeframe,
    aggregate_base_candles,
    floor_time,
    history_window,
)
from smart_market_data_gateway.config import Settings
from smart_market_data_gateway.domain import GapObservation, QuoteEvent, QuoteSnapshot

logger = logging.getLogger(__name__)


class RedisStore:
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self.redis = redis
        self.settings = settings

    async def ensure_groups(self) -> None:
        for stream, group in (
            (self.settings.quote_stream, self.settings.stream_group),
            (self.settings.control_stream, self.settings.control_group),
        ):
            try:
                await self.redis.xgroup_create(stream, group, id="0", mkstream=True)
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise

    async def publish_quote(self, event: QuoteEvent) -> str:
        stream_id = await self.redis.xadd(
            self.settings.quote_stream,
            {"payload": event.model_dump_json()},
            maxlen=self.settings.stream_maxlen,
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

    @staticmethod
    def _candle_index_key(symbol: str) -> str:
        return f"smdg:candles:index:v1:1m:{symbol}"

    @staticmethod
    def _candle_key(symbol: str, bucket_epoch: str) -> str:
        return f"smdg:candles:data:v1:1m:{symbol}:{bucket_epoch}"

    async def cache_quote(self, event: QuoteEvent) -> None:
        latest_key = f"smdg:latest:{event.symbol}"
        retention_seconds = self.settings.candle_history_retention_seconds
        now = datetime.now(UTC)
        cutoff = now - timedelta(seconds=retention_seconds)

        if event.provider_timestamp.astimezone(UTC) < cutoff:
            await self.redis.set(latest_key, event.model_dump_json())
            return

        bucket = floor_time(event.provider_timestamp, 60)
        bucket_epoch = str(int(bucket.timestamp()))
        candle_key = self._candle_key(event.symbol, bucket_epoch)
        index_key = self._candle_index_key(event.symbol)
        ttl_seconds = retention_seconds + 120

        for _attempt in range(self.settings.candle_update_retry_limit):
            async with self.redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(candle_key)
                    payload = await pipe.get(candle_key)
                    if payload is None:
                        candle = BaseMinuteCandle.from_quote(event)
                    else:
                        candle = BaseMinuteCandle.model_validate_json(payload).with_quote(event)

                    pipe.multi()
                    pipe.set(latest_key, event.model_dump_json())
                    pipe.set(candle_key, candle.model_dump_json(), ex=ttl_seconds)
                    pipe.zadd(index_key, {bucket_epoch: bucket.timestamp()})
                    pipe.zremrangebyscore(index_key, "-inf", cutoff.timestamp())
                    await pipe.execute()
                    return
                except WatchError:
                    continue

        raise RuntimeError(
            f"failed to update candle for {event.symbol} after "
            f"{self.settings.candle_update_retry_limit} optimistic retries"
        )

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

    async def get_candles(
        self,
        symbol: str,
        *,
        timeframe: CandleTimeframe,
        limit: int,
        end: datetime,
    ) -> CandleSeries:
        normalized = symbol.upper()
        start, effective_end = history_window(timeframe=timeframe, limit=limit, end=end)
        index_key = self._candle_index_key(normalized)
        bucket_epochs = await self.redis.zrangebyscore(
            index_key,
            start.timestamp(),
            effective_end.timestamp(),
        )
        keys = [self._candle_key(normalized, str(bucket_epoch)) for bucket_epoch in bucket_epochs]
        payloads = await self.redis.mget(keys) if keys else []

        base_candles: list[BaseMinuteCandle] = []
        missing_index_members: list[str] = []
        for bucket_epoch, payload in zip(bucket_epochs, payloads, strict=True):
            if payload is None:
                missing_index_members.append(str(bucket_epoch))
                continue
            base_candles.append(BaseMinuteCandle.model_validate_json(payload))

        if missing_index_members:
            await self.redis.zrem(index_key, *missing_index_members)

        return aggregate_base_candles(
            symbol=normalized,
            timeframe=timeframe,
            limit=limit,
            end=effective_end,
            retention_seconds=self.settings.candle_history_retention_seconds,
            base_candles=base_candles,
        )

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
