import asyncio
import logging
import time
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any, cast

from redis.asyncio import Redis

from smart_market_data_gateway.config import Settings
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.storage import RedisStore

logger = logging.getLogger(__name__)

_SUBSCRIBE_SCRIPT = """
local now = tonumber(ARGV[1])
local expires_at = tonumber(ARGV[2])
local connection_id = ARGV[3]
local symbol = ARGV[4]
local ttl = tonumber(ARGV[5])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local before = redis.call('ZCARD', KEYS[1])
local added = redis.call('ZADD', KEYS[1], expires_at, connection_id)
redis.call('SADD', KEYS[2], symbol)
redis.call('EXPIRE', KEYS[2], ttl)
redis.call('EXPIRE', KEYS[1], ttl * 2)
local after = redis.call('ZCARD', KEYS[1])
return {before, after, added}
"""

_UNSUBSCRIBE_SCRIPT = """
local now = tonumber(ARGV[1])
local connection_id = ARGV[2]
local symbol = ARGV[3]
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local removed = redis.call('ZREM', KEYS[1], connection_id)
redis.call('SREM', KEYS[2], symbol)
local after = redis.call('ZCARD', KEYS[1])
return {removed, after}
"""

_RELEASE_SCRIPT = """
local now = tonumber(ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZCARD', KEYS[1]) ~= 0 then
  return 0
end
if redis.call('GET', KEYS[2]) == 'active' then
  redis.call('DEL', KEYS[2])
  return 1
end
return 0
"""


class SubscriptionRegistry:
    """Redis-backed registry with TTL, grace release, and global first/last transitions."""

    def __init__(
        self,
        redis: Redis,
        store: RedisStore,
        settings: Settings,
        metrics: GatewayMetrics,
    ) -> None:
        self.redis = redis
        self.store = store
        self.settings = settings
        self.metrics = metrics
        self._release_tasks: dict[str, asyncio.Task[None]] = {}
        self._closed = False

    @staticmethod
    def _symbol_key(symbol: str) -> str:
        return f"smdg:sub:symbol:{symbol}"

    @staticmethod
    def _connection_key(connection_id: str) -> str:
        return f"smdg:sub:connection:{connection_id}"

    @staticmethod
    def _state_key(symbol: str) -> str:
        return f"smdg:sub:upstream-state:{symbol}"

    async def subscribe(self, connection_id: str, symbols: set[str]) -> set[str]:
        added_symbols: set[str] = set()
        now = time.time()
        expires_at = now + self.settings.subscription_ttl_seconds
        for symbol in sorted(symbols):
            symbol = symbol.upper()
            evaluation = cast(
                Awaitable[Any],
                self.redis.eval(
                    _SUBSCRIBE_SCRIPT,
                    2,
                    self._symbol_key(symbol),
                    self._connection_key(connection_id),
                    str(now),
                    str(expires_at),
                    connection_id,
                    symbol,
                    str(self.settings.subscription_ttl_seconds),
                ),
            )
            result = cast(list[Any], await evaluation)
            _before, after, added = (int(result[0]), int(result[1]), int(result[2]))
            if added:
                added_symbols.add(symbol)
            release_task = self._release_tasks.pop(symbol, None)
            if release_task is not None:
                release_task.cancel()
            if after == 1:
                activated = await self.redis.set(self._state_key(symbol), "active", nx=True)
                if activated:
                    await self.store.publish_control("subscribe", symbol)
                    logger.info(
                        "activated upstream subscription",
                        extra={"event": "upstream_subscribe", "symbol": symbol},
                    )
        await self.refresh_metrics()
        return added_symbols

    async def unsubscribe(self, connection_id: str, symbols: set[str]) -> set[str]:
        removed_symbols: set[str] = set()
        now = time.time()
        for symbol in sorted(symbols):
            symbol = symbol.upper()
            evaluation = cast(
                Awaitable[Any],
                self.redis.eval(
                    _UNSUBSCRIBE_SCRIPT,
                    2,
                    self._symbol_key(symbol),
                    self._connection_key(connection_id),
                    str(now),
                    connection_id,
                    symbol,
                ),
            )
            result = cast(list[Any], await evaluation)
            removed, after = int(result[0]), int(result[1])
            if removed:
                removed_symbols.add(symbol)
            if after == 0:
                self._schedule_release(symbol)
        await self.refresh_metrics()
        return removed_symbols

    async def _connection_symbols(self, connection_key: str) -> set[str]:
        request = cast(Awaitable[set[Any]], self.redis.smembers(connection_key))
        return {str(symbol) for symbol in await request}

    async def heartbeat(self, connection_id: str) -> None:
        connection_key = self._connection_key(connection_id)
        symbols = await self._connection_symbols(connection_key)
        if not symbols:
            return
        expires_at = time.time() + self.settings.subscription_ttl_seconds
        async with self.redis.pipeline(transaction=False) as pipe:
            for symbol in symbols:
                pipe.zadd(self._symbol_key(symbol), {connection_id: expires_at})
                pipe.expire(self._symbol_key(symbol), self.settings.subscription_ttl_seconds * 2)
            pipe.expire(connection_key, self.settings.subscription_ttl_seconds)
            await pipe.execute()

    async def disconnect(self, connection_id: str) -> set[str]:
        connection_key = self._connection_key(connection_id)
        symbols = await self._connection_symbols(connection_key)
        removed = await self.unsubscribe(connection_id, symbols) if symbols else set()
        await self.redis.delete(connection_key)
        return removed

    def _schedule_release(self, symbol: str) -> None:
        existing = self._release_tasks.get(symbol)
        if existing is not None and not existing.done():
            return
        self._release_tasks[symbol] = asyncio.create_task(
            self._release_after_grace(symbol),
            name=f"release-{symbol}",
        )

    async def _release_after_grace(self, symbol: str) -> None:
        try:
            await asyncio.sleep(self.settings.subscription_grace_seconds)
            evaluation = cast(
                Awaitable[Any],
                self.redis.eval(
                    _RELEASE_SCRIPT,
                    2,
                    self._symbol_key(symbol),
                    self._state_key(symbol),
                    str(time.time()),
                ),
            )
            released = await evaluation
            if int(released):
                await self.store.publish_control("unsubscribe", symbol)
                logger.info(
                    "released upstream subscription",
                    extra={"event": "upstream_unsubscribe", "symbol": symbol},
                )
        except asyncio.CancelledError:
            raise
        finally:
            self._release_tasks.pop(symbol, None)
            await self.refresh_metrics()

    async def cleanup_expired(self) -> None:
        cursor = 0
        now = time.time()
        while True:
            cursor, keys = await self.redis.scan(
                cursor=cursor,
                match="smdg:sub:symbol:*",
                count=100,
            )
            for raw_key in keys:
                key = str(raw_key)
                symbol = key.rsplit(":", 1)[-1]
                before = await self.redis.zcard(key)
                await self.redis.zremrangebyscore(key, "-inf", now)
                after = await self.redis.zcard(key)
                if before and not after:
                    self._schedule_release(symbol)
            if cursor == 0:
                break
        await self.refresh_metrics()

    async def run_cleanup_loop(self) -> None:
        while not self._closed:
            try:
                await self.cleanup_expired()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("subscription cleanup failed", extra={"event": "cleanup_failed"})
            await asyncio.sleep(max(1.0, self.settings.subscription_ttl_seconds / 3))

    async def refresh_metrics(self) -> dict[str, int | float]:
        cursor = 0
        client_subscriptions = 0
        unique_symbols = 0
        now = time.time()
        while True:
            cursor, keys = await self.redis.scan(
                cursor=cursor,
                match="smdg:sub:symbol:*",
                count=100,
            )
            for key in keys:
                await self.redis.zremrangebyscore(key, "-inf", now)
                count = int(await self.redis.zcard(key))
                client_subscriptions += count
                if count:
                    unique_symbols += 1
            if cursor == 0:
                break
        self.metrics.update_subscription_metrics(client_subscriptions, unique_symbols)
        return {
            "client_subscriptions": client_subscriptions,
            "unique_upstream_subscriptions": unique_symbols,
            "aggregation_ratio": client_subscriptions / unique_symbols if unique_symbols else 0.0,
            "measured_at": datetime.now(UTC).timestamp(),
        }

    async def close(self) -> None:
        self._closed = True
        tasks = list(self._release_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
