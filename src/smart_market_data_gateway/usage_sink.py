import asyncio
from datetime import UTC, datetime
import json
import logging
from uuid import uuid4

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from smart_market_data_gateway.config import Settings, settings
from smart_market_data_gateway.logging import configure_logging
from smart_market_data_gateway.storage import RedisStore

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS usage_records (
    idempotency_key TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    quantity BIGINT NOT NULL CHECK (quantity >= 0),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS usage_records_client_created_idx
    ON usage_records (client_id, created_at DESC);
CREATE INDEX IF NOT EXISTS usage_records_event_created_idx
    ON usage_records (event_type, created_at DESC);
"""

_INSERT_USAGE = """
INSERT INTO usage_records (
    idempotency_key,
    client_id,
    event_type,
    quantity,
    metadata,
    created_at
)
VALUES ($1, $2, $3, $4, $5::jsonb, $6)
ON CONFLICT (idempotency_key) DO NOTHING
"""


class UsageSink:
    """Persists Redis usage events to PostgreSQL without touching the quote delivery path."""

    def __init__(self, redis: Redis, config: Settings) -> None:
        if not config.database_url:
            raise ValueError("database_url is required for the usage sink")
        self.redis = redis
        self.config = config
        self.store = RedisStore(redis, config)
        self.consumer_name = f"usage-sink-{uuid4()}"
        self.pool: asyncpg.Pool | None = None
        self._closed = False

    async def start(self) -> None:
        self.pool = await asyncpg.create_pool(
            dsn=self.config.database_url,
            min_size=1,
            max_size=4,
            command_timeout=10,
        )
        async with self.pool.acquire() as connection:
            await connection.execute(_CREATE_TABLE)
        try:
            await self.redis.xgroup_create(
                self.config.usage_stream,
                self.config.usage_group,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def run(self) -> None:
        if self.pool is None:
            await self.start()
        while not self._closed:
            try:
                messages = await self.store.read_group(
                    self.config.usage_stream,
                    self.config.usage_group,
                    self.consumer_name,
                    count=100,
                    block_ms=1000,
                )
                for stream_id, fields in messages:
                    await self._persist(stream_id, fields)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "usage sink loop failed",
                    extra={"event": "usage_sink_failed"},
                )
                await asyncio.sleep(1)

    async def _persist(self, stream_id: str, fields: dict[str, str]) -> None:
        if self.pool is None:
            raise RuntimeError("usage sink is not started")
        try:
            created_at_raw = fields.get("created_at") or datetime.now(UTC).isoformat()
            created_at = datetime.fromisoformat(created_at_raw)
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)
            metadata = json.loads(fields.get("metadata", "{}"))
            async with self.pool.acquire() as connection:
                await connection.execute(
                    _INSERT_USAGE,
                    fields["idempotency_key"],
                    fields["client_id"],
                    fields["event_type"],
                    int(fields.get("quantity", "1")),
                    json.dumps(metadata, separators=(",", ":")),
                    created_at,
                )
            await self.store.ack(
                self.config.usage_stream,
                self.config.usage_group,
                stream_id,
            )
        except Exception as exc:
            retry_count = await self.store.increment_retry(
                f"usage:{self.config.usage_stream}:{stream_id}"
            )
            if retry_count >= self.config.retry_limit:
                await self.store.move_to_dead_letter(
                    source_stream=self.config.usage_stream,
                    stream_id=stream_id,
                    payload=fields,
                    error=str(exc),
                    retry_count=retry_count,
                )
                await self.store.ack(
                    self.config.usage_stream,
                    self.config.usage_group,
                    stream_id,
                )
            else:
                raise

    async def close(self) -> None:
        self._closed = True
        if self.pool is not None:
            await self.pool.close()
            self.pool = None


async def _run() -> None:
    configure_logging(settings.log_level)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    sink = UsageSink(redis, settings)
    try:
        await sink.run()
    finally:
        await sink.close()
        await redis.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
