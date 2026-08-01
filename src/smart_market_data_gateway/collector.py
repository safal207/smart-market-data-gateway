import asyncio
from contextlib import suppress
import logging
import random
from uuid import uuid4

from redis.asyncio import Redis

from smart_market_data_gateway.config import Settings, settings
from smart_market_data_gateway.logging import configure_logging
from smart_market_data_gateway.providers import (
    MarketDataProvider,
    MockMarketDataProvider,
    MockProviderConfig,
    ProviderState,
)
from smart_market_data_gateway.storage import RedisStore

logger = logging.getLogger(__name__)


class CollectorService:
    """Owns the provider connection and translates global control events into subscriptions."""

    def __init__(self, provider: MarketDataProvider, store: RedisStore, config: Settings) -> None:
        self.provider = provider
        self.store = store
        self.config = config
        self.consumer_name = f"collector-{uuid4()}"
        self.active_symbols: set[str] = set()
        self._closed = False
        self._provider_lock = asyncio.Lock()

    async def run(self) -> None:
        await self.store.ensure_groups()
        control_task = asyncio.create_task(self._control_loop(), name="collector-control")
        provider_task = asyncio.create_task(self._provider_loop(), name="collector-provider")
        try:
            await asyncio.gather(control_task, provider_task)
        finally:
            control_task.cancel()
            provider_task.cancel()
            await asyncio.gather(control_task, provider_task, return_exceptions=True)
            await self.provider.disconnect()

    async def _control_loop(self) -> None:
        while not self._closed:
            try:
                messages = await self.store.read_group(
                    self.config.control_stream,
                    self.config.control_group,
                    self.consumer_name,
                    count=100,
                )
                for stream_id, fields in messages:
                    action = fields.get("action")
                    symbol = fields.get("symbol", "").upper()
                    if action not in {"subscribe", "unsubscribe"} or not symbol:
                        await self.store.move_to_dead_letter(
                            source_stream=self.config.control_stream,
                            stream_id=stream_id,
                            payload=fields,
                            error="invalid control message",
                            retry_count=1,
                        )
                        await self.store.ack(
                            self.config.control_stream,
                            self.config.control_group,
                            stream_id,
                        )
                        continue
                    await self._apply_control(action, symbol)
                    await self.store.ack(
                        self.config.control_stream,
                        self.config.control_group,
                        stream_id,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("control loop failed", extra={"event": "control_loop_failed"})
                await asyncio.sleep(0.5)

    async def _apply_control(self, action: str, symbol: str) -> None:
        async with self._provider_lock:
            health = await self.provider.health()
            connected = health.state is ProviderState.CONNECTED
            if action == "subscribe":
                was_new = symbol not in self.active_symbols
                self.active_symbols.add(symbol)
                if was_new and connected:
                    await self.provider.subscribe([symbol])
            else:
                was_active = symbol in self.active_symbols
                self.active_symbols.discard(symbol)
                if was_active and connected:
                    await self.provider.unsubscribe([symbol])
        logger.info(
            "collector control applied",
            extra={
                "event": f"collector_{action}",
                "symbol": symbol,
                "provider": self.provider.name,
            },
        )

    async def _provider_loop(self) -> None:
        backoff = 0.5
        while not self._closed:
            try:
                async with self._provider_lock:
                    await self.provider.connect()
                    if self.active_symbols:
                        await self.provider.subscribe(sorted(self.active_symbols))
                logger.info(
                    "provider connected",
                    extra={"event": "provider_connected", "provider": self.provider.name},
                )
                backoff = 0.5
                async for event in self.provider.events():
                    if self._closed:
                        break
                    await self.store.publish_quote(event)
                if self._closed:
                    break
                health = await self.provider.health()
                raise ConnectionError(health.message or "provider event stream ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "provider disconnected; reconnect scheduled",
                    extra={"event": "provider_reconnect", "provider": self.provider.name},
                )
                logger.debug("provider reconnect reason: %s", exc)
                await self.store.redis.incr(f"smdg:provider:reconnects:{self.provider.name}")
                with suppress(Exception):
                    await self.provider.disconnect()
                jitter = random.uniform(0.0, backoff * 0.25)
                await asyncio.sleep(backoff + jitter)
                backoff = min(30.0, backoff * 2)

    async def close(self) -> None:
        self._closed = True


def build_mock_collector(redis: Redis, config: Settings) -> CollectorService:
    provider = MockMarketDataProvider(
        MockProviderConfig(
            interval_seconds=config.mock_interval_seconds,
            duplicate_every=config.mock_duplicate_every,
            fail_after_events=config.mock_fail_after_events,
        )
    )
    return CollectorService(provider, RedisStore(redis, config), config)


async def _run() -> None:
    configure_logging(settings.log_level)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    collector = build_mock_collector(redis, settings)
    try:
        await collector.run()
    finally:
        await collector.close()
        await redis.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
