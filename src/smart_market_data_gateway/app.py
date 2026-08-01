from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Response, status
from redis.asyncio import Redis
from redis.exceptions import RedisError

from smart_market_data_gateway.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    redis_client: Redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.redis = redis_client
    try:
        yield
    finally:
        await redis_client.aclose()


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health/live", tags=["health"])
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready", tags=["health"])
async def readiness(response: Response) -> dict[str, Any]:
    redis_client: Redis = app.state.redis
    try:
        await redis_client.ping()
    except RedisError as exc:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "degraded", "redis": "unavailable", "detail": str(exc)}
    return {"status": "ok", "redis": "available"}
