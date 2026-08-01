import asyncio
from dataclasses import dataclass
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
local state_ttl = tonumber(ARGV[6])
local created_at = ARGV[7]
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local added = redis.call('ZADD', KEYS[1], expires_at, connection_id)
redis.call('SADD', KEYS[2], symbol)
redis.call('EXPIRE', KEYS[2], ttl)
redis.call('EXPIRE', KEYS[1], ttl * 2)
local after = redis.call('ZCARD', KEYS[1])
local transitioned = 0
if redis.call('GET', KEYS[3]) ~= 'active' or redis.call('SISMEMBER', KEYS[5], symbol) == 0 then
  redis.call('SET', KEYS[3], 'active', 'EX', state_ttl)
  redis.call('SADD', KEYS[5], symbol)
  redis.call(
    'XADD', KEYS[4], 'MAXLEN', '~', 10000, '*',
    'action', 'subscribe', 'symbol', symbol, 'created_at', created_at
  )
  transitioned = 1
else
  redis.call('EXPIRE', KEYS[3], state_ttl)
end
return {added, transitioned, after}
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
local symbol = ARGV[2]
local state_ttl = tonumber(ARGV[3])
local created_at = ARGV[4]
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZCARD', KEYS[1]) ~= 0 then
  redis.call('SET', KEYS[2], 'active', 'EX', state_ttl)
  redis.call('SADD', KEYS[4], symbol)
  return 0
end
local state = redis.call('GET', KEYS[2])
local was_active = redis.call('SISMEMBER', KEYS[4], symbol)
redis.call('SET', KEYS[2], 'inactive', 'EX', state_ttl)
redis.call('SREM', KEYS[4], symbol)
if state == 'active' or was_active == 1 then
  redis.call(
    'XADD', KEYS[3], 'MAXLEN', '~', 10000, '*',
    'action', 'unsubscribe', 'symbol', symbol, 'created_at', created_at
  )
  return 1
end
return 0
"""

_HEARTBEAT_SCRIPT = """
local now = tonumber(ARGV[1])
local expires_at = tonumber(ARGV[2])
local connection_id = ARGV[3]
local symbol = ARGV[4]
local ttl = tonumber(ARGV[5])
local state_ttl = tonumber(ARGV[6])
local created_at = ARGV[7]
if redis.call('SISMEMBER', KEYS[2], symbol) == 0 then
  return 0
end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
redis.call('ZADD', KEYS[1], expires_at, connection_id)
redis.call('EXPIRE', KEYS[1], ttl * 2)
redis.call('EXPIRE', KEYS[2], ttl)
if redis.call('GET', KEYS[3]) ~= 'active' or redis.call('SISMEMBER', KEYS[5], symbol) == 0 then
  redis.call('SET', KEYS[3], 'active', 'EX', state_ttl)
  redis.call('SADD', KEYS[5], symbol)
  redis.call(
    'XADD', KEYS[4], 'MAXLEN', '~', 10000, '*',
    'action', 'subscribe', 'symbol', symbol, 'created_at', created_at
  )
  return 1
end
redis.call('EXPIRE', KEYS[3], state_ttl)
return 0
"""


@dataclass(slots=True)
class SubscriptionResult:
    added_symbols: set[str]
    upstream_transitions: set[str]


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

    @staticmethod
    def _active_symbols_key() -> str:
        return "smdg:sub:upstream-symbols"

    @property
    def _state_ttl_seconds(self) -> int:
        grace_window = int(self.settings.subscription_grace_seconds) + 1
        return max(3, self.settings.subscription_ttl_seconds * 3, grace_window * 2)

    async def subscribe(self, connection_id: str, symbols: set[str]) -> set[str]:
        result = await self.subscribe_with_transitions(connection_id, symbols)
        return result.added_symbols

    async def subscribe_with_transitions(
        self,
        connection_id: str,
        symbols: set[str],
    ) -> SubscriptionResult:
        added_symbols: set[str] = set()
        upstream_transitions: set[str] = set()
        now = time.time()
        expires_at = now + self.settings.subscription_ttl_seconds
        for symbol in sorted(symbols):
            symbol = symbol.upper()
            evaluation = cast(
                Awaitable[Any],
                self.redis.eval(
                    _SUBSCRIBE_SCRIPT,
                    5,
                    self._symbol_key(symbol),
                    self._connection_key(connection_id),
                    self._state_key(symbol),
                    self.settings.control_stream,
                    self._active_symbols_key(),
                    str(now),
                    str(expires_at),
                    connection_id,
                    symbol,
                    str(self.settings.subscription_ttl_seconds),
                    str(self._state_ttl_seconds),
                    datetime.now(UTC).isoformat(),
                ),
            )
            result = cast(list[Any], await evaluation)
            added, transitioned, _after = (int(result[0]), int(result[1]), int(result[2]))
            if added:
                added_symbols.add(symbol)
            release_task = self._release_tasks.pop(symbol, None)
            if release_task is not None:
                release_task.cancel()
            if transitioned:
                upstream_transitions.add(symbol)
                logger.info(
                    "activated upstream subscription",
                    extra={"event": "upstream_subscribe", "symbol": symbol},
                )
        await self.refresh_metrics()
        return SubscriptionResult(added_symbols, upstream_transitions)

    async def unsubscribe(self, connection_id: str, symbols: set[str]) -> set[str]:
        removed_symbols: set[str] = set()
        release_symbols: set[str] = set()
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
                release_symbols.add(symbol)
        await self.refresh_metrics()
        for symbol in release_symbols:
            self._schedule_release(symbol)
        return removed_symbols

    async def _connection_symbols(self, connection_key: str) -> set[str]:
        request = cast(Awaitable[set[Any]], self.redis.smembers(connection_key))
        return {str(symbol) for symbol in await request}

    async def heartbeat(self, connection_id: str) -> None:
        connection_key = self._connection_key(connection_id)
        symbols = await self._connection_symbols(connection_key)
        if not symbols:
            return
        now = time.time()
        expires_at = now + self.settings.subscription_ttl_seconds
        for symbol in sorted(symbols):
            evaluation = cast(
                Awaitable[Any],
                self.redis.eval(
                    _HEARTBEAT_SCRIPT,
                    5,
                    self._symbol_key(symbol),
                    connection_key,
                    self._state_key(symbol),
                    self.settings.control_stream,
                    self._active_symbols_key(),
                    str(now),
                    str(expires_at),
                    connection_id,
                    symbol,
                    str(self.settings.subscription_ttl_seconds),
                    str(self._state_ttl_seconds),
                    datetime.now(UTC).isoformat(),
                ),
            )
            transitioned = int(await evaluation)
            if transitioned:
                logger.info(
                    "recovered upstream subscription state",
                    extra={"event": "upstream_subscribe_recovered", "symbol": symbol},
                )

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
        release_task = asyncio.current_task()
        try:
            await asyncio.sleep(self.settings.subscription_grace_seconds)
            evaluation = cast(
                Awaitable[Any],
                self.redis.eval(
                    _RELEASE_SCRIPT,
                    4,
                    self._symbol_key(symbol),
                    self._state_key(symbol),
                    self.settings.control_stream,
                    self._active_symbols_key(),
                    str(time.time()),
                    symbol,
                    str(self._state_ttl_seconds),
                    datetime.now(UTC).isoformat(),
                ),
            )
            released = await evaluation
            if int(released):
                logger.info(
                    "released upstream subscription",
                    extra={"event": "upstream_unsubscribe", "symbol": symbol},
                )
        except asyncio.CancelledError:
            raise
        finally:
            if self._release_tasks.get(symbol) is release_task:
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
        active_symbols_request = cast(Awaitable[set[Any]], self.redis.smembers(self._active_symbols_key()))
        for raw_symbol in await active_symbols_request:
            symbol = str(raw_symbol)
            key = self._symbol_key(symbol)
            await self.redis.zremrangebyscore(key, "-inf", now)
            if not await self.redis.zcard(key):
                self._schedule_release(symbol)
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
