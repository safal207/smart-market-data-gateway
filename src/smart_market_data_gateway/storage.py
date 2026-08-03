from collections.abc import AsyncIterator
from datetime import UTC, datetime
import json
import logging
from typing import Any, Literal, cast

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

EventClaimStatus = Literal["claimed", "processed", "busy"]
StreamMessage = tuple[str, dict[str, str]]

_CLAIM_EVENT_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 'processed'
end
local claimed = redis.call('SET', KEYS[2], ARGV[1], 'NX', 'EX', ARGV[2])
if claimed then
  return 'claimed'
end
return 'busy'
"""

_MARK_EVENT_PROCESSED_SCRIPT = """
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
redis.call('DEL', KEYS[2])
return 1
"""


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
        )
        await self._compact_accepted_stream()
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
    ) -> list[StreamMessage]:
        result = await self.redis.xreadgroup(
            group,
            consumer,
            {stream: ">"},
            count=count,
            block=block_ms,
        )
        messages: list[StreamMessage] = []
        for _stream_name, entries in result:
            messages.extend(self._normalize_entries(entries))
        return messages

    async def claim_stale(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        min_idle_ms: int,
        count: int = 100,
    ) -> list[StreamMessage]:
        claimed = await self.redis.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=count,
        )
        entries = claimed[1] if isinstance(claimed, (list, tuple)) and len(claimed) > 1 else []
        return self._normalize_entries(entries)

    async def ack(self, stream: str, group: str, *stream_ids: str) -> None:
        if not stream_ids:
            return
        await self.redis.xack(stream, group, *stream_ids)
        if (
            stream == self.settings.accepted_event_stream
            and group == self.settings.history_group
        ):
            await self._compact_accepted_stream()

    async def _compact_accepted_stream(self) -> None:
        """Trim only history entries proven safe by consumer-group progress."""
        stream = self.settings.accepted_event_stream
        target = self.settings.accepted_stream_maxlen
        if target <= 0:
            return
        try:
            if int(await self.redis.xlen(stream)) <= target:
                return
            groups = await self.redis.xinfo_groups(stream)
        except ResponseError:
            return

        history_group: Any | None = None
        for group in groups:
            name = self._mapping_value(group, "name")
            if self._as_text(name) == self.settings.history_group:
                history_group = group
                break
        if history_group is None:
            return

        try:
            pending = await self.redis.xpending(stream, self.settings.history_group)
        except ResponseError:
            return
        pending_count, oldest_pending = self._pending_summary(pending)

        threshold: str | None = None
        if pending_count > 0:
            threshold = self._as_text(oldest_pending)
        else:
            last_delivered = self._as_text(
                self._mapping_value(history_group, "last-delivered-id")
            )
            if not last_delivered or last_delivered == "0-0":
                return
            newer = await self.redis.xrange(
                stream,
                min=f"({last_delivered}",
                max="+",
                count=1,
            )
            if newer:
                threshold = self._as_text(newer[0][0])
            else:
                threshold = self._next_stream_id(last_delivered)

        if not threshold:
            return
        await self.redis.execute_command("XTRIM", stream, "MINID", threshold)

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

    async def claim_event(self, event: QuoteEvent, source_stream_id: str) -> EventClaimStatus:
        result = await self.redis.execute_command(
            "EVAL",
            _CLAIM_EVENT_SCRIPT,
            "2",
            self._processed_key(event),
            self._claim_key(event),
            source_stream_id,
            str(self.settings.event_claim_ttl_seconds),
        )
        if isinstance(result, bytes):
            status = result.decode()
        else:
            status = str(result)
        if status not in {"claimed", "processed", "busy"}:
            raise RuntimeError(f"unexpected event claim status: {status}")
        return cast(EventClaimStatus, status)

    async def mark_event_processed(self, event: QuoteEvent) -> None:
        await self.redis.execute_command(
            "EVAL",
            _MARK_EVENT_PROCESSED_SCRIPT,
            "2",
            self._processed_key(event),
            self._claim_key(event),
            str(self.settings.dedupe_ttl_seconds),
        )

    async def release_event_claim(self, event: QuoteEvent) -> None:
        await self.redis.delete(self._claim_key(event))

    async def accept_event_once(self, event: QuoteEvent) -> bool:
        """Compatibility helper for callers that only need a simple dedupe gate."""
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

    @staticmethod
    def _mapping_value(mapping: Any, name: str) -> Any:
        if not isinstance(mapping, dict):
            return None
        if name in mapping:
            return mapping[name]
        encoded = name.encode()
        return mapping.get(encoded)

    @classmethod
    def _pending_summary(cls, summary: Any) -> tuple[int, Any | None]:
        if isinstance(summary, dict):
            return (
                int(cls._mapping_value(summary, "pending") or 0),
                cls._mapping_value(summary, "min"),
            )
        if isinstance(summary, (list, tuple)) and summary:
            oldest = summary[1] if len(summary) > 1 else None
            return int(summary[0]), oldest
        return 0, None

    @staticmethod
    def _as_text(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode()
        return str(value)

    @staticmethod
    def _next_stream_id(stream_id: str) -> str:
        milliseconds, separator, sequence = stream_id.partition("-")
        if not separator:
            raise ValueError(f"invalid Redis Stream ID: {stream_id}")
        return f"{int(milliseconds)}-{int(sequence) + 1}"

    @staticmethod
    def _normalize_entries(entries: Any) -> list[StreamMessage]:
        messages: list[StreamMessage] = []
        for stream_id, fields in entries:
            if stream_id is None or fields is None:
                continue
            messages.append(
                (
                    str(stream_id),
                    {str(key): str(value) for key, value in fields.items()},
                )
            )
        return messages

    @staticmethod
    def _processed_key(event: QuoteEvent) -> str:
        return f"smdg:event:processed:{event.event_id}"

    @staticmethod
    def _claim_key(event: QuoteEvent) -> str:
        return f"smdg:event:claim:{event.event_id}"
