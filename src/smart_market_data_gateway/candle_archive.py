from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import logging
from uuid import uuid4

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from smart_market_data_gateway.candles import (
    Candle,
    CandleSeries,
    CandleTimeframe,
    floor_time,
    history_window,
    timeframe_seconds,
)
from smart_market_data_gateway.config import Settings, settings
from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.logging import configure_logging
from smart_market_data_gateway.storage import RedisStore

logger = logging.getLogger(__name__)

_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS candle_archive_events (
    event_id UUID PRIMARY KEY,
    provider_timestamp TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS candle_archive_events_archived_idx
    ON candle_archive_events (archived_at DESC);

CREATE TABLE IF NOT EXISTS candle_archive_1m (
    symbol TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    open NUMERIC NOT NULL CHECK (open > 0),
    high NUMERIC NOT NULL CHECK (high > 0),
    low NUMERIC NOT NULL CHECK (low > 0),
    close NUMERIC NOT NULL CHECK (close > 0),
    activity_count BIGINT NOT NULL CHECK (activity_count >= 1),
    first_provider_timestamp TIMESTAMPTZ NOT NULL,
    last_provider_timestamp TIMESTAMPTZ NOT NULL,
    first_event_id UUID NOT NULL,
    last_event_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, open_time),
    CHECK (low <= high),
    CHECK (first_provider_timestamp <= last_provider_timestamp)
);
CREATE INDEX IF NOT EXISTS candle_archive_1m_symbol_time_idx
    ON candle_archive_1m (symbol, open_time DESC);
"""

_UPSERT_EVENT = """
WITH accepted AS (
    INSERT INTO candle_archive_events (event_id, provider_timestamp)
    VALUES ($1, $2)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id
)
INSERT INTO candle_archive_1m (
    symbol,
    open_time,
    open,
    high,
    low,
    close,
    activity_count,
    first_provider_timestamp,
    last_provider_timestamp,
    first_event_id,
    last_event_id
)
SELECT
    $3,
    $4,
    $5,
    $5,
    $5,
    $5,
    1,
    $2,
    $2,
    $1,
    $1
FROM accepted
ON CONFLICT (symbol, open_time) DO UPDATE SET
    open = CASE
        WHEN (EXCLUDED.first_provider_timestamp, EXCLUDED.first_event_id)
             < (candle_archive_1m.first_provider_timestamp, candle_archive_1m.first_event_id)
        THEN EXCLUDED.open
        ELSE candle_archive_1m.open
    END,
    high = GREATEST(candle_archive_1m.high, EXCLUDED.high),
    low = LEAST(candle_archive_1m.low, EXCLUDED.low),
    close = CASE
        WHEN (EXCLUDED.last_provider_timestamp, EXCLUDED.last_event_id)
             > (candle_archive_1m.last_provider_timestamp, candle_archive_1m.last_event_id)
        THEN EXCLUDED.close
        ELSE candle_archive_1m.close
    END,
    activity_count = candle_archive_1m.activity_count + 1,
    first_provider_timestamp = LEAST(
        candle_archive_1m.first_provider_timestamp,
        EXCLUDED.first_provider_timestamp
    ),
    last_provider_timestamp = GREATEST(
        candle_archive_1m.last_provider_timestamp,
        EXCLUDED.last_provider_timestamp
    ),
    first_event_id = CASE
        WHEN (EXCLUDED.first_provider_timestamp, EXCLUDED.first_event_id)
             < (candle_archive_1m.first_provider_timestamp, candle_archive_1m.first_event_id)
        THEN EXCLUDED.first_event_id
        ELSE candle_archive_1m.first_event_id
    END,
    last_event_id = CASE
        WHEN (EXCLUDED.last_provider_timestamp, EXCLUDED.last_event_id)
             > (candle_archive_1m.last_provider_timestamp, candle_archive_1m.last_event_id)
        THEN EXCLUDED.last_event_id
        ELSE candle_archive_1m.last_event_id
    END,
    updated_at = NOW()
RETURNING activity_count
"""

_SELECT_CANDLES = """
WITH bucketed AS (
    SELECT
        open_time,
        open,
        high,
        low,
        close,
        activity_count,
        first_provider_timestamp,
        last_provider_timestamp,
        to_timestamp(
            floor(extract(epoch FROM open_time) / $4::double precision)
            * $4::double precision
        ) AS bucket_start
    FROM candle_archive_1m
    WHERE symbol = $1
      AND open_time >= $2
      AND open_time < $3
),
aggregated AS (
    SELECT
        bucket_start,
        bucket_start + ($4::double precision * INTERVAL '1 second') AS bucket_end,
        (array_agg(open ORDER BY open_time ASC))[1] AS open,
        max(high) AS high,
        min(low) AS low,
        (array_agg(close ORDER BY open_time DESC))[1] AS close,
        sum(activity_count) AS activity_count,
        min(first_provider_timestamp) AS first_observation,
        max(last_provider_timestamp) AS last_observation
    FROM bucketed
    GROUP BY bucket_start
)
SELECT
    bucket_start,
    bucket_end,
    open,
    high,
    low,
    close,
    activity_count,
    first_observation,
    last_observation
FROM aggregated
WHERE bucket_end <= $3
ORDER BY bucket_start DESC
LIMIT $5
"""


class PostgresCandleArchive:
    """Idempotent PostgreSQL/Timescale candle archive and aggregate reader."""

    def __init__(self, config: Settings) -> None:
        if not config.database_url:
            raise ValueError("database_url is required for the candle archive")
        self.config = config
        self.pool: asyncpg.Pool | None = None
        self.timescale_enabled = False

    @property
    def available(self) -> bool:
        return self.pool is not None

    async def start(self) -> None:
        if self.pool is not None:
            return
        self.pool = await asyncpg.create_pool(
            dsn=self.config.database_url,
            min_size=1,
            max_size=self.config.candle_archive_pool_max_size,
            command_timeout=15,
        )
        async with self.pool.acquire() as connection:
            await connection.execute(_CREATE_TABLES)
            self.timescale_enabled = await self._enable_timescale(connection)

    async def _enable_timescale(self, connection: asyncpg.Connection) -> bool:
        available = await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb')"
        )
        if not available:
            logger.info(
                "TimescaleDB extension is unavailable; using PostgreSQL archive tables",
                extra={"event": "candle_archive_postgres_fallback"},
            )
            return False
        try:
            await connection.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            await connection.execute(
                """
                SELECT create_hypertable(
                    'candle_archive_1m',
                    'open_time',
                    if_not_exists => TRUE,
                    migrate_data => TRUE
                )
                """
            )
            await connection.execute(
                """
                SELECT add_retention_policy(
                    'candle_archive_1m',
                    drop_after => $1::interval,
                    if_not_exists => TRUE
                )
                """,
                timedelta(seconds=self.config.candle_archive_retention_seconds),
            )
        except asyncpg.PostgresError:
            logger.warning(
                "TimescaleDB initialization failed; regular PostgreSQL archive remains available",
                extra={"event": "candle_archive_timescale_fallback"},
                exc_info=True,
            )
            return False
        return True

    async def persist_event(self, event: QuoteEvent) -> bool:
        if self.pool is None:
            raise RuntimeError("candle archive is not started")
        bucket = floor_time(event.provider_timestamp, 60)
        async with self.pool.acquire() as connection:
            activity_count = await connection.fetchval(
                _UPSERT_EVENT,
                event.event_id,
                event.provider_timestamp.astimezone(UTC),
                event.symbol,
                bucket,
                event.price,
            )
        return activity_count is not None

    async def get_candles(
        self,
        symbol: str,
        *,
        timeframe: CandleTimeframe,
        limit: int,
        end: datetime,
    ) -> CandleSeries:
        if self.pool is None:
            raise RuntimeError("candle archive is not started")
        normalized = symbol.upper()
        start, effective_end = history_window(timeframe=timeframe, limit=limit, end=end)
        interval_seconds = timeframe_seconds(timeframe)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                _SELECT_CANDLES,
                normalized,
                start,
                effective_end,
                interval_seconds,
                limit,
            )
        data = [
            Candle(
                symbol=normalized,
                timeframe=timeframe,
                open_time=row["bucket_start"],
                close_time=row["bucket_end"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                activity_count=int(row["activity_count"]),
                first_observation=row["first_observation"],
                last_observation=row["last_observation"],
                closed=True,
            )
            for row in reversed(rows)
        ]
        return CandleSeries(
            symbol=normalized,
            timeframe=timeframe,
            requested_limit=limit,
            returned_count=len(data),
            retention_seconds=self.config.candle_archive_retention_seconds,
            period_start=start,
            period_end=effective_end,
            data=data,
            warnings=[
                "Candles are aggregated from observed quote prices; trade volume is unavailable.",
                "Intervals with no accepted quote observations are omitted.",
                "Durable history is stored as canonical one-minute candles and aggregated on read.",
            ],
        )

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None


class HybridCandleHistoryStore:
    """Reads long history from PostgreSQL and overlays complete Redis hot buckets."""

    def __init__(
        self,
        hot_store: RedisStore,
        archive: PostgresCandleArchive | None,
        config: Settings,
    ) -> None:
        self.hot_store = hot_store
        self.archive = archive
        self.config = config

    async def get_candles(
        self,
        symbol: str,
        *,
        timeframe: CandleTimeframe,
        limit: int,
        end: datetime,
    ) -> CandleSeries:
        if self.archive is None:
            return await self.hot_store.get_candles(
                symbol,
                timeframe=timeframe,
                limit=limit,
                end=end,
            )

        hot_task = asyncio.create_task(
            self.hot_store.get_candles(
                symbol,
                timeframe=timeframe,
                limit=limit,
                end=end,
            )
        )
        archive_task = asyncio.create_task(
            self.archive.get_candles(
                symbol,
                timeframe=timeframe,
                limit=limit,
                end=end,
            )
        )

        hot_series: CandleSeries | None = None
        archive_series: CandleSeries | None = None
        hot_error: Exception | None = None
        archive_error: Exception | None = None
        try:
            hot_series = await hot_task
        except Exception as exc:
            hot_error = exc
            logger.warning(
                "Redis hot candle read failed",
                extra={"event": "candle_hot_read_failed", "symbol": symbol},
                exc_info=True,
            )
        try:
            archive_series = await archive_task
        except Exception as exc:
            archive_error = exc
            logger.warning(
                "Candle archive read failed; retaining Redis hot-layer availability",
                extra={"event": "candle_archive_read_failed", "symbol": symbol},
                exc_info=True,
            )

        if hot_series is None and archive_series is None:
            raise hot_error or archive_error or RuntimeError("all candle history stores failed")
        if archive_series is None:
            assert hot_series is not None
            return hot_series.model_copy(
                update={
                    "warnings": _unique_warnings(
                        hot_series.warnings,
                        ["Durable archive was unavailable; this response contains Redis hot history only."],
                    )
                }
            )
        if hot_series is None:
            return archive_series.model_copy(
                update={
                    "warnings": _unique_warnings(
                        archive_series.warnings,
                        ["Redis hot history was unavailable; this response contains durable archive data only."],
                    )
                }
            )

        authoritative_hot_start = _ceil_timeframe(
            datetime.now(UTC) - timedelta(seconds=self.config.candle_history_retention_seconds),
            timeframe,
        )
        merged = {candle.open_time: candle for candle in archive_series.data}
        for candle in hot_series.data:
            if candle.open_time >= authoritative_hot_start or candle.open_time not in merged:
                merged[candle.open_time] = candle
        data = [merged[key] for key in sorted(merged)][-limit:]
        return CandleSeries(
            symbol=archive_series.symbol,
            timeframe=timeframe,
            requested_limit=limit,
            returned_count=len(data),
            retention_seconds=self.config.candle_archive_retention_seconds,
            period_start=archive_series.period_start,
            period_end=archive_series.period_end,
            data=data,
            warnings=_unique_warnings(
                archive_series.warnings,
                hot_series.warnings,
                ["Complete recent buckets are overlaid from the Redis hot layer."],
            ),
        )


class CandleArchiveSink:
    """Consumes an independent Redis event stream and archives quotes without blocking delivery."""

    def __init__(self, redis: Redis, config: Settings) -> None:
        if not config.candle_archive_enabled:
            raise ValueError("candle archive is disabled")
        self.redis = redis
        self.config = config
        self.store = RedisStore(redis, config)
        self.archive = PostgresCandleArchive(config)
        self.consumer_name = f"candle-archive-{uuid4()}"
        self._closed = False

    async def start(self) -> None:
        await self.archive.start()
        try:
            await self.redis.xgroup_create(
                self.config.candle_archive_stream,
                self.config.candle_archive_group,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def run(self) -> None:
        if not self.archive.available:
            await self.start()
        while not self._closed:
            try:
                messages = await self.store.read_group(
                    self.config.candle_archive_stream,
                    self.config.candle_archive_group,
                    self.consumer_name,
                    count=200,
                    block_ms=1000,
                )
                for stream_id, fields in messages:
                    await self._persist(stream_id, fields)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "candle archive sink loop failed",
                    extra={"event": "candle_archive_sink_failed"},
                )
                await asyncio.sleep(1)

    async def _persist(self, stream_id: str, fields: dict[str, str]) -> None:
        try:
            event = QuoteEvent.model_validate_json(fields["payload"])
            await self.archive.persist_event(event)
            await self.store.ack(
                self.config.candle_archive_stream,
                self.config.candle_archive_group,
                stream_id,
            )
        except Exception as exc:
            retry_key = f"archive:{self.config.candle_archive_stream}:{stream_id}"
            retry_count = await self.store.increment_retry(retry_key)
            if retry_count >= self.config.retry_limit:
                await self.store.move_to_dead_letter(
                    source_stream=self.config.candle_archive_stream,
                    stream_id=stream_id,
                    payload=fields,
                    error=str(exc),
                    retry_count=retry_count,
                )
                await self.store.ack(
                    self.config.candle_archive_stream,
                    self.config.candle_archive_group,
                    stream_id,
                )
            else:
                raise

    async def close(self) -> None:
        self._closed = True
        await self.archive.close()


def _ceil_timeframe(value: datetime, timeframe: CandleTimeframe) -> datetime:
    interval_seconds = timeframe_seconds(timeframe)
    bucket = floor_time(value, interval_seconds)
    if bucket < value.astimezone(UTC):
        return bucket + timedelta(seconds=interval_seconds)
    return bucket


def _unique_warnings(*groups: list[str]) -> list[str]:
    return list(dict.fromkeys(warning for group in groups for warning in group))


async def _run() -> None:
    configure_logging(settings.log_level)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    sink = CandleArchiveSink(redis, settings)
    try:
        await sink.run()
    finally:
        await sink.close()
        await redis.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
