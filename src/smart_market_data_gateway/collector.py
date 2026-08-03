import asyncio
from contextlib import suppress
import logging
import random
import time
from uuid import uuid4

from prometheus_client import start_http_server
from redis.asyncio import Redis

from smart_market_data_gateway.config import Settings, settings
from smart_market_data_gateway.logging import configure_logging
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.providers import (
    MarketDataProvider,
    MockMarketDataProvider,
    MockProviderConfig,
    TradernetMode,
    TradernetProviderAdapter,
    TradernetProviderConfig,
)
from smart_market_data_gateway.storage import RedisStore

logger = logging.getLogger(__name__)


class CollectorService:
    """Owns the provider connection and translates global control events into subscriptions."""

    def __init__(
        self,
        provider: MarketDataProvider,
        store: RedisStore,
        config: Settings,
        metrics: GatewayMetrics,
    ) -> None:
        self.provider = provider
        self.store = store
        self.config = config
        self.metrics = metrics
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
            self.metrics.provider_connected.labels(self.provider.name).set(0)
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
            if action == "subscribe":
                was_new = symbol not in self.active_symbols
                self.active_symbols.add(symbol)
                if was_new:
                    await self.provider.subscribe([symbol])
            else:
                was_active = symbol in self.active_symbols
                self.active_symbols.discard(symbol)
                if was_active:
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
        outage_started: float | None = None
        while not self._closed:
            try:
                async with self._provider_lock:
                    await self.provider.connect()
                    if self.active_symbols:
                        await self.provider.subscribe(sorted(self.active_symbols))
                self.metrics.provider_connected.labels(self.provider.name).set(1)
                await self.store.redis.set(
                    f"smdg:provider:state:{self.provider.name}",
                    "connected",
                    ex=60,
                )
                if outage_started is not None:
                    self.metrics.provider_outage_duration.labels(self.provider.name).observe(
                        time.monotonic() - outage_started
                    )
                    outage_started = None
                logger.info(
                    "provider connected",
                    extra={"event": "provider_connected", "provider": self.provider.name},
                )
                backoff = 0.5
                async for event in self.provider.events():
                    if self._closed:
                        break
                    await self.store.publish_quote(event)
                    await self.store.redis.expire(
                        f"smdg:provider:state:{self.provider.name}",
                        60,
                    )
                if self._closed:
                    break
                health = await self.provider.health()
                raise ConnectionError(health.message or "provider event stream ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.metrics.provider_connected.labels(self.provider.name).set(0)
                self.metrics.provider_reconnects.labels(self.provider.name).inc()
                if outage_started is None:
                    outage_started = time.monotonic()
                await self.store.redis.set(
                    f"smdg:provider:state:{self.provider.name}",
                    "disconnected",
                    ex=60,
                )
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


def build_provider(config: Settings) -> MarketDataProvider:
    provider_name = config.provider.strip().lower()
    if provider_name == "mock":
        return MockMarketDataProvider(
            MockProviderConfig(
                interval_seconds=config.mock_interval_seconds,
                duplicate_every=config.mock_duplicate_every,
                fail_after_events=config.mock_fail_after_events,
            )
        )
    if provider_name in {"tradernet", "freedom", "freedom24"}:
        try:
            mode = TradernetMode(config.tradernet_mode)
        except ValueError as exc:
            raise ValueError(
                "SMDG_TRADERNET_MODE must be public_demo or sid_session"
            ) from exc
        if mode is TradernetMode.API_KEY:
            raise ValueError(
                "SMDG_TRADERNET_MODE=api_key is disabled until the Tradernet HMAC contract is verified"
            )
        return TradernetProviderAdapter(
            TradernetProviderConfig(
                mode=mode,
                websocket_url=config.tradernet_websocket_url,
                snapshot_base_url=config.tradernet_snapshot_base_url,
                sid=config.tradernet_sid,
                user_id=config.tradernet_user_id,
                api_key=config.tradernet_api_key,
                api_secret=config.tradernet_api_secret,
                require_authenticated_sid=config.tradernet_require_authenticated_sid,
                snapshot_fallback=config.tradernet_snapshot_fallback,
                connect_timeout_seconds=config.tradernet_connect_timeout_seconds,
                snapshot_timeout_seconds=config.tradernet_snapshot_timeout_seconds,
            )
        )
    raise ValueError(f"Unsupported market-data provider: {config.provider}")


def build_collector(
    redis: Redis,
    config: Settings,
    metrics: GatewayMetrics | None = None,
) -> CollectorService:
    return CollectorService(
        build_provider(config),
        RedisStore(redis, config),
        config,
        metrics or GatewayMetrics(),
    )


def build_mock_collector(
    redis: Redis,
    config: Settings,
    metrics: GatewayMetrics | None = None,
) -> CollectorService:
    mock_config = config.model_copy(update={"provider": "mock"})
    return build_collector(redis, mock_config, metrics)


async def _run() -> None:
    configure_logging(settings.log_level)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    metrics = GatewayMetrics()
    start_http_server(settings.collector_metrics_port, registry=metrics.registry)
    collector = build_collector(redis, settings, metrics)
    try:
        await collector.run()
    finally:
        await collector.close()
        await redis.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
