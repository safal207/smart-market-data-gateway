import asyncio
from copy import deepcopy
from datetime import timedelta
import logging
from uuid import UUID, uuid4

import asyncpg
from redis.asyncio import Redis

from smart_market_data_gateway.candles import CandleBuilder
from smart_market_data_gateway.config import Settings, settings
from smart_market_data_gateway.domain import AcceptedQuoteEvent, Candle
from smart_market_data_gateway.integrity import (
    ACCEPTED_EVENT_CHAIN_NAME,
    build_integrity_record,
)
from smart_market_data_gateway.logging import configure_logging
from smart_market_data_gateway.storage import RedisStore

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS quote_events (
    event_id UUID NOT NULL,
    provider_timestamp TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL,
    data_cutoff TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    provider TEXT NOT NULL,
    sequence BIGINT,
    price NUMERIC NOT NULL CHECK (price > 0),
    bid NUMERIC CHECK (bid > 0),
    ask NUMERIC CHECK (ask > 0),
    quality_score DOUBLE PRECISION NOT NULL CHECK (quality_score BETWEEN 0 AND 1),
    gap_detected BOOLEAN NOT NULL,
    normalization_version TEXT NOT NULL,
    source_stream_id TEXT,
    payload JSONB NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, provider_timestamp)
);
CREATE INDEX IF NOT EXISTS quote_events_symbol_time_idx
    ON quote_events (symbol, provider_timestamp DESC);
CREATE INDEX IF NOT EXISTS quote_events_provider_time_idx
    ON quote_events (provider, provider_timestamp DESC);

CREATE TABLE IF NOT EXISTS accepted_event_integrity (
    chain_name TEXT NOT NULL,
    chain_sequence BIGINT NOT NULL CHECK (chain_sequence > 0),
    profile TEXT NOT NULL,
    event_id UUID NOT NULL,
    provider_timestamp TIMESTAMPTZ NOT NULL,
    source_stream_id TEXT,
    payload_digest TEXT NOT NULL CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    previous_record_hash TEXT CHECK (
        previous_record_hash IS NULL OR previous_record_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    record_hash TEXT NOT NULL CHECK (record_hash ~ '^sha256:[0-9a-f]{64}$'),
    appended_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_name, chain_sequence),
    UNIQUE (chain_name, event_id, provider_timestamp),
    UNIQUE (chain_name, record_hash)
);
CREATE INDEX IF NOT EXISTS accepted_event_integrity_event_idx
    ON accepted_event_integrity (event_id, provider_timestamp);

CREATE TABLE IF NOT EXISTS integrity_chain_heads (
    chain_name TEXT PRIMARY KEY,
    chain_sequence BIGINT NOT NULL DEFAULT 0 CHECK (chain_sequence >= 0),
    record_hash TEXT CHECK (
        record_hash IS NULL OR record_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (chain_sequence = 0 AND record_hash IS NULL)
        OR (chain_sequence > 0 AND record_hash IS NOT NULL)
    )
);
INSERT INTO integrity_chain_heads (chain_name)
VALUES ('accepted_quotes')
ON CONFLICT (chain_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds > 0),
    bucket_start TIMESTAMPTZ NOT NULL,
    open NUMERIC NOT NULL CHECK (open > 0),
    high NUMERIC NOT NULL CHECK (high > 0),
    low NUMERIC NOT NULL CHECK (low > 0),
    close NUMERIC NOT NULL CHECK (close > 0),
    event_count BIGINT NOT NULL CHECK (event_count > 0),
    first_event_time TIMESTAMPTZ NOT NULL,
    last_event_time TIMESTAMPTZ NOT NULL,
    quality_score DOUBLE PRECISION NOT NULL CHECK (quality_score BETWEEN 0 AND 1),
    finalized BOOLEAN NOT NULL DEFAULT TRUE,
    schema_version TEXT NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, interval_seconds, bucket_start)
);
CREATE INDEX IF NOT EXISTS candles_symbol_interval_time_idx
    ON candles (symbol, interval_seconds, bucket_start DESC);

CREATE TABLE IF NOT EXISTS late_quote_events (
    event_id UUID NOT NULL,
    provider_timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL,
    payload JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, provider_timestamp, interval_seconds)
);

CREATE TABLE IF NOT EXISTS data_quality_intervals (
    provider TEXT NOT NULL,
    symbol TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL CHECK (score BETWEEN 0 AND 1),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (provider, symbol, started_at)
);

CREATE TABLE IF NOT EXISTS market_sessions (
    market TEXT NOT NULL,
    session_date DATE NOT NULL,
    opens_at TIMESTAMPTZ NOT NULL,
    closes_at TIMESTAMPTZ NOT NULL,
    timezone TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'scheduled',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (market, session_date),
    CHECK (closes_at > opens_at)
);
"""

_TIMESCALE_SQL = (
    "SELECT create_hypertable('quote_events', 'provider_timestamp', if_not_exists => TRUE, migrate_data => TRUE)",
    "SELECT create_hypertable('candles', 'bucket_start', if_not_exists => TRUE, migrate_data => TRUE)",
    "SELECT create_hypertable('late_quote_events', 'provider_timestamp', if_not_exists => TRUE, migrate_data => TRUE)",
)

_INSERT_EVENT = """
INSERT INTO quote_events (
    event_id,
    provider_timestamp,
    received_at,
    accepted_at,
    data_cutoff,
    symbol,
    provider,
    sequence,
    price,
    bid,
    ask,
    quality_score,
    gap_detected,
    normalization_version,
    source_stream_id,
    payload
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14, $15, $16::jsonb
)
ON CONFLICT (event_id, provider_timestamp) DO NOTHING
RETURNING event_id
"""

_LOCK_INTEGRITY_HEAD = """
SELECT chain_sequence, record_hash
FROM integrity_chain_heads
WHERE chain_name = $1
FOR UPDATE
"""

_INITIALIZE_INTEGRITY_HEAD = """
INSERT INTO integrity_chain_heads (chain_name)
VALUES ($1)
ON CONFLICT (chain_name) DO NOTHING
"""

_INSERT_INTEGRITY_RECORD = """
INSERT INTO accepted_event_integrity (
    chain_name,
    chain_sequence,
    profile,
    event_id,
    provider_timestamp,
    source_stream_id,
    payload_digest,
    previous_record_hash,
    record_hash
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
"""

_UPDATE_INTEGRITY_HEAD = """
UPDATE integrity_chain_heads
SET chain_sequence = $2, record_hash = $3, updated_at = NOW()
WHERE chain_name = $1
"""

_UPSERT_CANDLE = """
INSERT INTO candles (
    symbol,
    interval_seconds,
    bucket_start,
    open,
    high,
    low,
    close,
    event_count,
    first_event_time,
    last_event_time,
    quality_score,
    finalized,
    schema_version
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (symbol, interval_seconds, bucket_start) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    event_count = EXCLUDED.event_count,
    first_event_time = EXCLUDED.first_event_time,
    last_event_time = EXCLUDED.last_event_time,
    quality_score = EXCLUDED.quality_score,
    finalized = EXCLUDED.finalized,
    schema_version = EXCLUDED.schema_version,
    persisted_at = NOW()
"""

_INSERT_LATE_EVENT = """
INSERT INTO late_quote_events (
    event_id,
    provider_timestamp,
    symbol,
    interval_seconds,
    payload
)
VALUES ($1, $2, $3, $4, $5::jsonb)
ON CONFLICT (event_id, provider_timestamp, interval_seconds) DO NOTHING
"""


class HistorySink:
    """Persists accepted quotes and deterministic candles outside the gateway hot path."""

    def __init__(self, redis: Redis, config: Settings) -> None:
        if not config.database_url:
            raise ValueError("database_url is required for the history sink")
        self.redis = redis
        self.config = config
        self.store = RedisStore(redis, config)
        self.consumer_name = f"history-sink-{uuid4()}"
        self.pool: asyncpg.Pool | None = None
        self.builder = CandleBuilder(
            config.candle_intervals,
            allowed_lateness_seconds=config.candle_allowed_lateness_seconds,
        )
        self._closed = False

    async def start(self) -> None:
        self.pool = await asyncpg.create_pool(
            dsn=self.config.database_url,
            min_size=1,
            max_size=4,
            command_timeout=self.config.history_command_timeout_seconds,
        )
        async with self.pool.acquire() as connection:
            await self._prepare_schema(connection)
            await self._hydrate_builder(connection)
        await self.store.ensure_group(
            self.config.accepted_event_stream,
            self.config.history_group,
        )

    async def _prepare_schema(self, connection: asyncpg.Connection) -> None:
        try:
            await connection.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        except asyncpg.PostgresError:
            logger.warning(
                "TimescaleDB extension is unavailable; using PostgreSQL-compatible tables",
                extra={"event": "timescale_extension_unavailable"},
            )
        await connection.execute(_SCHEMA_SQL)
        has_timescale = bool(
            await connection.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')"
            )
        )
        if has_timescale:
            for statement in _TIMESCALE_SQL:
                await connection.execute(statement)
            await self._configure_retention(connection)

    async def _configure_retention(self, connection: asyncpg.Connection) -> None:
        if not self.config.enable_history_retention:
            return
        await connection.execute(
            "SELECT add_retention_policy('quote_events', make_interval(days => $1), if_not_exists => TRUE)",
            self.config.quote_event_retention_days,
        )
        await connection.execute(
            "SELECT add_retention_policy('candles', make_interval(days => $1), if_not_exists => TRUE)",
            self.config.candle_retention_days,
        )

    async def _hydrate_builder(self, connection: asyncpg.Connection) -> None:
        has_events = await connection.fetchval("SELECT EXISTS (SELECT 1 FROM quote_events)")
        if not has_events:
            return
        lookback = timedelta(
            seconds=max(self.config.candle_intervals)
            + self.config.candle_allowed_lateness_seconds
        )
        rows = await connection.fetch(
            """
            WITH latest_by_symbol AS (
                SELECT symbol, MAX(provider_timestamp) AS latest_timestamp
                FROM quote_events
                GROUP BY symbol
            )
            SELECT quote_events.payload::text
            FROM quote_events
            JOIN latest_by_symbol USING (symbol)
            WHERE quote_events.provider_timestamp >= latest_timestamp - $1::interval
            ORDER BY
              CASE
                WHEN source_stream_id ~ '^[0-9]+-[0-9]+$'
                THEN split_part(source_stream_id, '-', 1)::bigint
              END NULLS LAST,
              CASE
                WHEN source_stream_id ~ '^[0-9]+-[0-9]+$'
                THEN split_part(source_stream_id, '-', 2)::bigint
              END NULLS LAST,
              accepted_at,
              event_id
            """,
            lookback,
        )
        for row in rows:
            self.builder.add(AcceptedQuoteEvent.model_validate_json(row["payload"]))

    async def run(self) -> None:
        if self.pool is None:
            await self.start()
        while not self._closed:
            try:
                messages = await self.store.read_group(
                    self.config.accepted_event_stream,
                    self.config.history_group,
                    self.consumer_name,
                    count=self.config.history_batch_size,
                    block_ms=1000,
                )
                for stream_id, fields in messages:
                    await self._persist(stream_id, fields)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "history sink loop failed",
                    extra={"event": "history_sink_failed"},
                )
                await asyncio.sleep(1)

    async def _persist(self, stream_id: str, fields: dict[str, str]) -> None:
        if self.pool is None:
            raise RuntimeError("history sink is not started")
        builder_before = deepcopy(self.builder)
        try:
            accepted = AcceptedQuoteEvent.model_validate_json(fields["payload"])
            result = self.builder.add(accepted)
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    inserted = await self._insert_event(connection, accepted)
                    if inserted is None:
                        self.builder = builder_before
                    else:
                        for interval in result.late_intervals:
                            await connection.execute(
                                _INSERT_LATE_EVENT,
                                accepted.event.event_id,
                                accepted.event.provider_timestamp,
                                accepted.event.symbol,
                                interval,
                                accepted.model_dump_json(),
                            )
                        for candle in result.finalized:
                            await self._upsert_candle(connection, candle)
            await self.store.ack(
                self.config.accepted_event_stream,
                self.config.history_group,
                stream_id,
            )
        except Exception as exc:
            self.builder = builder_before
            retry_count = await self.store.increment_retry(
                f"history:{self.config.accepted_event_stream}:{stream_id}"
            )
            if retry_count >= self.config.retry_limit:
                await self.store.move_to_dead_letter(
                    source_stream=self.config.accepted_event_stream,
                    stream_id=stream_id,
                    payload=fields,
                    error=str(exc),
                    retry_count=retry_count,
                )
                await self.store.ack(
                    self.config.accepted_event_stream,
                    self.config.history_group,
                    stream_id,
                )
            else:
                raise

    async def _persist_integrity_record(
        self,
        connection: asyncpg.Connection,
        accepted: AcceptedQuoteEvent,
    ) -> None:
        await connection.execute(_INITIALIZE_INTEGRITY_HEAD, ACCEPTED_EVENT_CHAIN_NAME)
        head = await connection.fetchrow(
            _LOCK_INTEGRITY_HEAD,
            ACCEPTED_EVENT_CHAIN_NAME,
        )
        if head is None:
            raise RuntimeError("accepted-event integrity chain head is unavailable")
        previous_record_hash = head["record_hash"]
        if previous_record_hash is not None and not isinstance(previous_record_hash, str):
            raise RuntimeError("accepted-event integrity chain head is invalid")
        sequence = int(head["chain_sequence"]) + 1
        record = build_integrity_record(
            sequence,
            accepted,
            previous_record_hash,
        )
        await connection.execute(
            _INSERT_INTEGRITY_RECORD,
            record.chain_name,
            record.sequence,
            record.profile,
            record.event_id,
            record.provider_timestamp,
            record.source_stream_id,
            record.payload_digest,
            record.previous_record_hash,
            record.record_hash,
        )
        await connection.execute(
            _UPDATE_INTEGRITY_HEAD,
            record.chain_name,
            record.sequence,
            record.record_hash,
        )

    async def _insert_event(
        self,
        connection: asyncpg.Connection,
        accepted: AcceptedQuoteEvent,
    ) -> UUID | None:
        event = accepted.event
        payload = accepted.model_dump_json()
        value = await connection.fetchval(
            _INSERT_EVENT,
            event.event_id,
            event.provider_timestamp,
            event.received_at,
            accepted.quality.accepted_at,
            accepted.data_cutoff,
            event.symbol,
            event.provider,
            event.sequence,
            event.price,
            event.bid,
            event.ask,
            accepted.quality.score,
            accepted.quality.gap_detected,
            accepted.quality.normalization_version,
            accepted.source_stream_id,
            payload,
        )
        if not isinstance(value, UUID):
            return None
        await self._persist_integrity_record(connection, accepted)
        return value

    async def _upsert_candle(
        self,
        connection: asyncpg.Connection,
        candle: Candle,
    ) -> None:
        await connection.execute(
            _UPSERT_CANDLE,
            candle.symbol,
            candle.interval_seconds,
            candle.bucket_start,
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.event_count,
            candle.first_event_time,
            candle.last_event_time,
            candle.quality_score,
            candle.finalized,
            candle.schema_version,
        )

    async def close(self) -> None:
        self._closed = True
        if self.pool is not None:
            await self.pool.close()
            self.pool = None


async def _run() -> None:
    configure_logging(settings.log_level)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    sink = HistorySink(redis, settings)
    try:
        await sink.run()
    finally:
        await sink.close()
        await redis.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
