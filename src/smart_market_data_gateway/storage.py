from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import logging
from typing import Any, Literal, cast

from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from redis.exceptions import ResponseError

from smart_market_data_gateway.config import Settings
from smart_market_data_gateway.domain import GapObservation, QuoteEvent, QuoteSnapshot

logger = logging.getLogger(__name__)

_PROCESS_QUOTE_SCRIPT = """
local dedupe_ttl = tonumber(ARGV[1])
local current_sequence = nil
if ARGV[2] ~= '' then
  current_sequence = tonumber(ARGV[2])
end
local current_timestamp = tonumber(ARGV[3])
local timestamp_tolerance = tonumber(ARGV[4])
local payload = ARGV[5]
local event_id = ARGV[6]
local provider = ARGV[7]
local symbol = ARGV[8]
local observed_at = ARGV[9]
local source_stream_id = ARGV[10]

local previous_sequence = nil
local previous_sequence_raw = redis.call('GET', KEYS[2])
if previous_sequence_raw then
  previous_sequence = tonumber(previous_sequence_raw)
end
local previous_timestamp = nil
local previous_timestamp_raw = redis.call('GET', KEYS[3])
if previous_timestamp_raw then
  previous_timestamp = tonumber(previous_timestamp_raw)
end

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {
    'duplicate', previous_sequence or -1, current_sequence or -1, 0, 0, 0
  }
end

local out_of_order = 0
if current_sequence and previous_sequence and current_sequence <= previous_sequence then
  out_of_order = 1
end
local timestamp_regressed = 0
if previous_timestamp and current_timestamp + timestamp_tolerance < previous_timestamp then
  timestamp_regressed = 1
end
local gap = 0
if current_sequence and previous_sequence and current_sequence > previous_sequence + 1 then
  gap = current_sequence - previous_sequence - 1
end

local kind = ''
if out_of_order == 1 and timestamp_regressed == 1 then
  kind = 'out_of_order_and_timestamp_regression'
elseif out_of_order == 1 then
  kind = 'out_of_order'
elseif timestamp_regressed == 1 then
  kind = 'timestamp_regression'
elseif gap > 0 then
  kind = 'gap'
end

if out_of_order == 1 or timestamp_regressed == 1 then
  local diagnostic = cjson.encode({
    status = 'degraded',
    kind = kind,
    previous_sequence = previous_sequence or -1,
    current_sequence = current_sequence or -1,
    previous_provider_timestamp_ms = previous_timestamp or -1,
    current_provider_timestamp_ms = current_timestamp,
    observed_at = observed_at
  })
  redis.call('XADD', KEYS[6], 'MAXLEN', '~', 10000, '*',
    'event_id', event_id,
    'provider', provider,
    'symbol', symbol,
    'reason', kind,
    'source_stream_id', source_stream_id,
    'payload', payload,
    'diagnostic', diagnostic,
    'quarantined_at', observed_at
  )
  redis.call('SET', KEYS[7], diagnostic, 'EX', 300)
  redis.call('SET', KEYS[1], 'rejected', 'EX', dedupe_ttl)
  return {
    'rejected', previous_sequence or -1, current_sequence or -1,
    gap, out_of_order, timestamp_regressed
  }
end

redis.call('SET', KEYS[4], payload)
redis.call('PUBLISH', KEYS[5], payload)
if current_sequence and (not previous_sequence or current_sequence > previous_sequence) then
  redis.call('SET', KEYS[2], current_sequence, 'EX', 86400)
end
if not previous_timestamp or current_timestamp > previous_timestamp then
  redis.call('SET', KEYS[3], current_timestamp, 'EX', 86400)
end
redis.call('SET', KEYS[1], 'accepted', 'EX', dedupe_ttl)

if gap > 0 then
  local diagnostic = cjson.encode({
    status = 'degraded',
    kind = kind,
    previous_sequence = previous_sequence or -1,
    current_sequence = current_sequence or -1,
    gap = gap,
    previous_provider_timestamp_ms = previous_timestamp or -1,
    current_provider_timestamp_ms = current_timestamp,
    observed_at = observed_at
  })
  redis.call('SET', KEYS[7], diagnostic, 'EX', 300)
end

return {
  'accepted', previous_sequence or -1, current_sequence or -1,
  gap, out_of_order, timestamp_regressed
}
"""


@dataclass(frozen=True, slots=True)
class QuoteProcessingResult:
    status: Literal["accepted", "duplicate", "rejected"]
    previous_sequence: int | None
    current_sequence: int | None
    gap: int = 0
    out_of_order: bool = False
    timestamp_regressed: bool = False

    @property
    def accepted(self) -> bool:
        return self.status == "accepted"

    @property
    def duplicate(self) -> bool:
        return self.status == "duplicate"

    @property
    def rejected(self) -> bool:
        return self.status == "rejected"


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

    async def get_active_upstream_symbols(self) -> set[str]:
        request = cast(
            Awaitable[set[Any]],
            self.redis.smembers("smdg:sub:upstream-symbols"),
        )
        return {str(symbol).strip().upper() for symbol in await request if str(symbol).strip()}

    async def is_active_upstream_symbol(self, symbol: str) -> bool:
        request = cast(
            Awaitable[Any],
            self.redis.sismember(
                "smdg:sub:upstream-symbols",
                symbol.strip().upper(),
            ),
        )
        return bool(await request)

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

    async def process_quote(
        self,
        event: QuoteEvent,
        *,
        source_stream_id: str = "",
    ) -> QuoteProcessingResult:
        """Atomically deduplicate, guard temporal order, cache, and fan out one quote."""

        provider_timestamp_ms = int(event.provider_timestamp.timestamp() * 1000)
        tolerance_ms = int(self.settings.quote_timestamp_regression_tolerance_seconds * 1000)
        evaluation = cast(
            Awaitable[Any],
            self.redis.eval(
                _PROCESS_QUOTE_SCRIPT,
                7,
                f"smdg:dedupe:{event.event_id}",
                f"smdg:sequence:{event.provider}:{event.symbol}",
                f"smdg:provider-timestamp:{event.provider}:{event.symbol}",
                f"smdg:latest:{event.symbol}",
                self.settings.quote_pubsub_channel,
                self.settings.quarantine_stream,
                f"smdg:stream-health:{event.provider}:{event.symbol}",
                str(self.settings.dedupe_ttl_seconds),
                "" if event.sequence is None else str(event.sequence),
                str(provider_timestamp_ms),
                str(tolerance_ms),
                event.model_dump_json(),
                str(event.event_id),
                event.provider,
                event.symbol,
                datetime.now(UTC).isoformat(),
                source_stream_id,
            ),
        )
        raw = cast(list[Any], await evaluation)
        status = str(raw[0])
        if status not in {"accepted", "duplicate", "rejected"}:
            raise RuntimeError(f"unexpected quote processing status: {status}")

        previous_sequence_raw = int(raw[1])
        current_sequence_raw = int(raw[2])
        return QuoteProcessingResult(
            status=cast(Literal["accepted", "duplicate", "rejected"], status),
            previous_sequence=None if previous_sequence_raw < 0 else previous_sequence_raw,
            current_sequence=None if current_sequence_raw < 0 else current_sequence_raw,
            gap=int(raw[3]),
            out_of_order=bool(int(raw[4])),
            timestamp_regressed=bool(int(raw[5])),
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
