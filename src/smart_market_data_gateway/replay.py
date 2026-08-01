import argparse
import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import TextIO

import asyncpg
from redis.asyncio import Redis

from smart_market_data_gateway.config import settings
from smart_market_data_gateway.domain import AcceptedQuoteEvent

Emitter = Callable[[AcceptedQuoteEvent], Awaitable[None]]
Sleeper = Callable[[float], Awaitable[None]]


class ReplayService:
    """Re-emits accepted events in deterministic point-in-time order."""

    def __init__(
        self,
        source: AsyncIterator[AcceptedQuoteEvent],
        emit: Emitter,
        *,
        sleep: Sleeper = asyncio.sleep,
    ) -> None:
        self.source = source
        self.emit = emit
        self.sleep = sleep

    async def run(self, *, speed: float | None = None) -> int:
        if speed is not None and speed <= 0:
            raise ValueError("speed must be positive or None for max speed")
        previous_timestamp: datetime | None = None
        emitted = 0
        async for accepted in self.source:
            current_timestamp = accepted.event.provider_timestamp
            if speed is not None and previous_timestamp is not None:
                delay = max(0.0, (current_timestamp - previous_timestamp).total_seconds()) / speed
                if delay > 0:
                    await self.sleep(delay)
            await self.emit(accepted)
            previous_timestamp = current_timestamp
            emitted += 1
        return emitted


async def postgres_events(
    pool: asyncpg.Pool,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    symbols: tuple[str, ...] = (),
) -> AsyncIterator[AcceptedQuoteEvent]:
    normalized_symbols = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
    query = """
        SELECT payload::text
        FROM quote_events
        WHERE ($1::timestamptz IS NULL OR provider_timestamp >= $1)
          AND ($2::timestamptz IS NULL OR provider_timestamp < $2)
          AND ($3::text[] IS NULL OR symbol = ANY($3))
        ORDER BY provider_timestamp, received_at, event_id
    """
    async with pool.acquire() as connection:
        async with connection.transaction():
            cursor = connection.cursor(
                query,
                start,
                end,
                normalized_symbols or None,
                prefetch=500,
            )
            async for row in cursor:
                yield AcceptedQuoteEvent.model_validate_json(row["payload"])


def parse_speed(value: str) -> float | None:
    normalized = value.strip().lower()
    if normalized == "max":
        return None
    speed = float(normalized.removesuffix("x"))
    if speed <= 0:
        raise argparse.ArgumentTypeError("speed must be max or a positive number")
    return speed


def parse_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamps must include an explicit timezone")
    return parsed


class JsonlEmitter:
    def __init__(self, output: TextIO) -> None:
        self.output = output

    async def __call__(self, accepted: AcceptedQuoteEvent) -> None:
        self.output.write(accepted.model_dump_json() + "\n")
        self.output.flush()


class RedisStreamEmitter:
    def __init__(self, redis: Redis, stream: str, *, maxlen: int) -> None:
        self.redis = redis
        self.stream = stream
        self.maxlen = maxlen

    async def __call__(self, accepted: AcceptedQuoteEvent) -> None:
        await self.redis.xadd(
            self.stream,
            {
                "payload": accepted.model_dump_json(),
                "event_id": str(accepted.event.event_id),
                "symbol": accepted.event.symbol,
                "event_time": accepted.event.provider_timestamp.isoformat(),
                "replay": "true",
            },
            maxlen=self.maxlen,
            approximate=True,
        )


async def _run(args: argparse.Namespace) -> int:
    if not settings.database_url:
        raise ValueError("SMDG_DATABASE_URL is required for replay")
    pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=1,
        max_size=2,
        command_timeout=settings.history_command_timeout_seconds,
    )
    output_handle: TextIO | None = None
    redis: Redis | None = None
    try:
        source = postgres_events(
            pool,
            start=parse_timestamp(args.start),
            end=parse_timestamp(args.end),
            symbols=tuple(args.symbol),
        )
        if args.output_stream:
            redis = Redis.from_url(settings.redis_url, decode_responses=True)
            emitter: Emitter = RedisStreamEmitter(
                redis,
                args.output_stream,
                maxlen=settings.accepted_stream_maxlen,
            )
        else:
            if args.output:
                output_handle = Path(args.output).open("w", encoding="utf-8")
            else:
                import sys

                output_handle = sys.stdout
            emitter = JsonlEmitter(output_handle)
        service = ReplayService(source, emitter)
        return await service.run(speed=parse_speed(args.speed))
    finally:
        if output_handle is not None and args.output:
            output_handle.close()
        if redis is not None:
            await redis.aclose()
        await pool.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay accepted quote events deterministically")
    parser.add_argument("--from", dest="start", help="inclusive ISO-8601 timestamp")
    parser.add_argument("--to", dest="end", help="exclusive ISO-8601 timestamp")
    parser.add_argument("--symbol", action="append", default=[], help="symbol filter; repeatable")
    parser.add_argument("--speed", default="max", help="max, 1, 10, or another positive multiplier")
    parser.add_argument("--output", help="JSONL output path; defaults to stdout")
    parser.add_argument(
        "--output-stream",
        help="optional Redis stream for replay envelopes; use a dedicated replay stream",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    emitted = asyncio.run(_run(args))
    print(f"replayed_events={emitted}")


if __name__ == "__main__":
    main()
