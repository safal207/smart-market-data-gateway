from collections.abc import Awaitable
import math
from typing import Any, cast

from redis.asyncio import Redis

_TOKEN_BUCKET_SCRIPT = """
local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) + tonumber(now_parts[2]) / 1000000
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at')
local tokens = tonumber(state[1])
local updated_at = tonumber(state[2])
if tokens == nil then
  tokens = capacity
  updated_at = now
else
  tokens = math.min(capacity, tokens + math.max(0, now - updated_at) * refill_rate)
end
local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated_at', now)
redis.call('EXPIRE', KEYS[1], ttl)
return {allowed, tostring(tokens)}
"""


class RedisTokenBucket:
    """Distributed token bucket supporting both short bursts and sustained limits."""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def allow(
        self,
        key: str,
        *,
        requests_per_minute: int,
        cost: int = 1,
        burst_capacity: int | None = None,
    ) -> tuple[bool, float]:
        if requests_per_minute <= 0:
            return False, 0.0
        capacity = burst_capacity or max(5, math.ceil(requests_per_minute / 6))
        refill_rate = requests_per_minute / 60.0
        ttl = max(60, math.ceil(capacity / refill_rate * 2))
        evaluation = cast(
            Awaitable[Any],
            self.redis.eval(
                _TOKEN_BUCKET_SCRIPT,
                1,
                f"smdg:token-bucket:{key}",
                str(capacity),
                str(refill_rate),
                str(cost),
                str(ttl),
            ),
        )
        result = cast(list[Any], await evaluation)
        return bool(int(result[0])), float(result[1])
