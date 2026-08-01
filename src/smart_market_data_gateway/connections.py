import time
from typing import Any

from redis.asyncio import Redis

from smart_market_data_gateway.config import Settings

_ACQUIRE_SCRIPT = """
local now = tonumber(ARGV[1])
local expires_at = tonumber(ARGV[2])
local connection_id = ARGV[3]
local max_connections = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
redis.call('ZADD', KEYS[1], expires_at, connection_id)
local count = redis.call('ZCARD', KEYS[1])
if count > max_connections then
  redis.call('ZREM', KEYS[1], connection_id)
  return {0, count - 1}
end
redis.call('SET', KEYS[2], ARGV[6], 'EX', ttl)
redis.call('EXPIRE', KEYS[1], ttl * 2)
return {1, count}
"""


class ConnectionRegistry:
    """Distributed concurrent-connection limit with TTL crash recovery."""

    def __init__(self, redis: Redis, settings: Settings) -> None:
        self.redis = redis
        self.settings = settings

    @staticmethod
    def _client_key(client_id: str) -> str:
        return f"smdg:connections:client:{client_id}"

    @staticmethod
    def _owner_key(connection_id: str) -> str:
        return f"smdg:connections:owner:{connection_id}"

    async def acquire(
        self,
        *,
        client_id: str,
        connection_id: str,
        max_connections: int,
    ) -> tuple[bool, int]:
        now = time.time()
        result: list[Any] = await self.redis.eval(
            _ACQUIRE_SCRIPT,
            2,
            self._client_key(client_id),
            self._owner_key(connection_id),
            now,
            now + self.settings.subscription_ttl_seconds,
            connection_id,
            max_connections,
            self.settings.subscription_ttl_seconds,
            client_id,
        )
        return bool(int(result[0])), int(result[1])

    async def heartbeat(self, connection_id: str) -> None:
        owner_key = self._owner_key(connection_id)
        client_id = await self.redis.get(owner_key)
        if client_id is None:
            return
        expires_at = time.time() + self.settings.subscription_ttl_seconds
        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.zadd(self._client_key(str(client_id)), {connection_id: expires_at})
            pipe.expire(
                self._client_key(str(client_id)),
                self.settings.subscription_ttl_seconds * 2,
            )
            pipe.expire(owner_key, self.settings.subscription_ttl_seconds)
            await pipe.execute()

    async def release(self, connection_id: str) -> None:
        owner_key = self._owner_key(connection_id)
        client_id = await self.redis.get(owner_key)
        if client_id is None:
            return
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self._client_key(str(client_id)), connection_id)
            pipe.delete(owner_key)
            await pipe.execute()

    async def count(self, client_id: str) -> int:
        key = self._client_key(client_id)
        await self.redis.zremrangebyscore(key, "-inf", time.time())
        return int(await self.redis.zcard(key))
