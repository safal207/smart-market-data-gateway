import asyncio
from datetime import timedelta
import logging
from uuid import UUID, uuid4

import asyncpg
from redis.asyncio import Redis

from smart_market_data_gateway.candles import (
    CandleBuilder,
    CandleSymbolCheckpoint,
)
from smart_market_data_gateway.config import Settings, settings
from smart_market_data_gateway.domain import AcceptedQuoteEvent, Candle
from smart_market_data_gateway.durability import (
    HistoryFailpoint,
    crash_if,
    parse_history_failpoint,
)
from smart_market_data_gateway.integrity import (
    ACCEPTED_EVENT_CHAIN_NAME,
    build_integrity_record,
)
from smart_market_data_gateway.logging import configure_logging
from smart_market_data_gateway.storage import RedisStore

logger = logging.getLogger(__name__)

REQUIRED_MIGRATION = "001_temporal_market_foundation.sql"
REQUIRED_TABLES = (
    "quote_events",
    "accepted_event_integrity",
    "integrity_chain_heads",
    "candles",
    "late_quote_events",
)
HISTORY_WRITER_ADVISORY_LOCK = 0x534D444748495354

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
    source_stream_ms,
    source_stream_sequence,
    payload
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14, $15, $16, $17, $18::jsonb
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

_HYDRATE_QUERY = """
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
    source_stream_ms NULLS LAST,
    source_stream_sequence NULLS LAST,
    accepted_at,
    event_id
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
        self._lock_connection: asyncpg.Connection | None = None
        self.builder = CandleBuilder(
            config.candle_intervals,
            allowed_lateness_seconds=config.candle_allowed_lateness_seconds,
        )
        self.failpoint = parse_history_failpoint(config.history_failpoint)
        self._closed = False

    async def start(self) -> None:
        if self.pool is not None or self._lock_connection is not None:
            raise RuntimeError("history sink is already started")
        self._closed = False
        self._lock_connection = await asyncpg.connect(
            dsn=self.config.database_url,
            command_timeout=self.config.history_command_timeout_seconds,
        )
        locked = bool(
            await self._lock_connection.fetchval(
                "SELECT pg_try_advisory_lock($1::bigint)",
                HISTORY_WRITER_ADVISORY_LOCK,
            )
        )
        if not locked:
            await self._release_writer_lock()
            raise RuntimeError(
                "another history writer is already active; candle state requires one owner"
            )
        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.config.database_url,
                min_size=1,
                max_size=4,
                command_timeout=self.config.history_command_timeout_seconds,
            )
            async with self.pool.acquire() as connection:
                await self._verify_schema(connection)
                await self._configure_retention(connection)
                await self._hydrate_builder(connection)
            await self.store.ensure_group(
                self.config.accepted_event_stream,
                self.config.history_group,
            )
        except Exception:
            if self.pool is not None:
                await self.pool.close()
                self.pool = None
            await self._release_writer_lock()
            raise

    async def _verify_schema(self, connection: asyncpg.Connection) -> None:
        migration_table = await connection.fetchval(
            "SELECT to_regclass('public.schema_migrations')"
        )
        if migration_table is None:
            raise RuntimeError(
                "database schema is not migrated; run smdg-migrate before history-writer"
            )
        applied = await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)",
            REQUIRED_MIGRATION,
        )
        if not applied:
            raise RuntimeError(
                f"required migration {REQUIRED_MIGRATION} is missing; run smdg-migrate"
            )
        for table in REQUIRED_TABLES:
            exists = await connection.fetchval(
                "SELECT to_regclass($1)",
                f"public.{table}",
            )
            if exists is None:
                raise RuntimeError(
                    f"required table {table} is missing after {REQUIRED_MIGRATION}"
                )

    async def _configure_retention(self, connection: asyncpg.Connection) -> None:
        has_timescale = bool(
            await connection.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')"
            )
        )
        if not has_timescale:
            if self.config.enable_history_retention:
                raise RuntimeError(
                    "history retention is unavailable without integrity-preserving checkpoints"
                )
            return

        await connection.execute(
            "SELECT remove_retention_policy('quote_events', if_exists => TRUE)"
        )
        await connection.execute(
            "SELECT remove_retention_policy('candles', if_exists => TRUE)"
        )
        if self.config.enable_history_retention:
            raise RuntimeError(
                "history retention is unavailable without integrity-preserving checkpoints"
            )

    async def _hydrate_builder(self, connection: asyncpg.Connection) -> None:
        has_events = await connection.fetchval("SELECT EXISTS (SELECT 1 FROM quote_events)")
        if not has_events:
            return
        lookback = timedelta(
            seconds=max(self.config.candle_intervals)
            + self.config.candle_allowed_lateness_seconds
        )
        async with connection.transaction():
            cursor = connection.cursor(_HYDRATE_QUERY, lookback, prefetch=500)
            async for row in cursor:
                self.builder.add(AcceptedQuoteEvent.model_validate_json(row["payload"]))

    async def run(self) -> None:
        if self.pool is None:
            await self.start()
        while not self._closed:
            try:
                messages = await self.store.claim_stale(
                    self.config.accepted_event_stream,
                    self.config.history_group,
                    self.consumer_name,
                    min_idle_ms=max(
                        1,
                        int(self.config.history_pending_idle_seconds * 1000),
                    ),
                    count=self.config.history_batch_size,
                )
                if not messages:
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
        accepted = AcceptedQuoteEvent.model_validate_json(fields["payload"])
        checkpoint: CandleSymbolCheckpoint | None = None
        committed = False
        try:
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    inserted = await self._insert_event(connection, accepted)
                    if inserted is not None:
                        crash_if(
                            self.failpoint,
                            HistoryFailpoint.AFTER_LEDGER_APPEND_BEFORE_COMMIT,
                        )
                        checkpoint = self.builder.checkpoint_symbol(
                            accepted.event.symbol
                        )
                        result = self.builder.add(accepted)
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
            committed = True
            crash_if(
                self.failpoint,
                HistoryFailpoint.AFTER_DB_COMMIT_BEFORE_ACK,
            )
            await self.store.ack(
                self.config.accepted_event_stream,
                self.config.history_group,
                stream_id,
            )
        except Exception as exc:
            if checkpoint is not None and not committed:
                self.builder.restore_symbol(checkpoint)
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
        stream_ms, stream_sequence = _parse_stream_order(accepted.source_stream_id)
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
            stream_ms,
            stream_sequence,
            accepted.model_dump_json(),
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

    async def _release_writer_lock(self) -> None:
        connection = self._lock_connection
        self._lock_connection = None
        if connection is None:
            return
        try:
            if not connection.is_closed():
                await connection.execute(
                    "SELECT pg_advisory_unlock($1::bigint)",
                    HISTORY_WRITER_ADVISORY_LOCK,
                )
        finally:
            await connection.close()

    async def close(self) -> None:
        self._closed = True
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
        await self._release_writer_lock()


def _parse_stream_order(source_stream_id: str | None) -> tuple[int | None, int | None]:
    if not source_stream_id:
        return None, None
    milliseconds, separator, sequence = source_stream_id.partition("-")
    if not separator:
        return None, None
    try:
        parsed = (int(milliseconds), int(sequence))
    except ValueError:
        return None, None
    if parsed[0] < 0 or parsed[1] < 0:
        return None, None
    return parsed


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
