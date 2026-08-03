import asyncio
import os
from pathlib import Path

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from smart_market_data_gateway.config import Settings
from smart_market_data_gateway.migrations import migrate


@pytest.fixture(scope="session", autouse=True)
def migrated_test_database() -> None:
    database_url = os.getenv("TEST_DATABASE_URL")
    if database_url:
        migrations_dir = Path(__file__).resolve().parents[1] / "migrations"
        asyncio.run(migrate(database_url, migrations_dir))


@pytest.fixture
def test_settings() -> Settings:
    return Settings(
        environment="test",
        redis_url=os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15"),
        allow_anonymous_dev=True,
        allow_dev_tokens=True,
        subscription_grace_seconds=0.01,
        subscription_ttl_seconds=2,
        heartbeat_seconds=0.05,
        quote_freshness_seconds=5,
        websocket_queue_size=32,
    )


@pytest_asyncio.fixture
async def redis_client(test_settings: Settings):
    redis = Redis.from_url(test_settings.redis_url, decode_responses=True)
    await redis.flushdb()
    yield redis
    try:
        await redis.flushdb()
    finally:
        await redis.aclose()
