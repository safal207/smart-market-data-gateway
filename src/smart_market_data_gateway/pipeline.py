import asyncio
import logging
import time
from typing import Any
from uuid import uuid4

from pydantic import ValidationError

from smart_market_data_gateway.config import Settings
from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.qos import ConnectionHub
from smart_market_data_gateway.storage import RedisStore

logger = logging.getLogger(__name__)


class QuoteProcessor:
    """Durable stream processor: validate, dedupe, detect gaps, cache, and fan out."""

    def __init__(self, store: RedisStore, settings: Settings, metrics: GatewayMetrics) -> None:
        self.store = store
        self.settings = settings
        self.metrics = metrics
        self.consumer_name = f"processor-{uuid4()}"
        self._closed = False
        self._pending_min_idle_ms = max(5_000, int(self.settings.heartbeat_seconds * 2_000))
        self._claim_interval_seconds = 1.0

    async def run(self) -> None:
        await self.store.ensure_groups()
        next_claim_at = 0.0
        while not self._closed:
            try:
                now = time.monotonic()
                if now >= next_claim_at:
                    claimed = await self._claim_stale()
                    self.metrics.redis_pending_entries.set(await self.store.pending_count())
                    next_claim_at = now + self._claim_interval_seconds
                    for stream_id, fields in claimed:
                        await self._process_one(stream_id, fields)

                messages = await self.store.read_group(
                    self.settings.quote_stream,
                    self.settings.stream_group,
                    self.consumer_name,
                )
                for stream_id, fields in messages:
                    await self._process_one(stream_id, fields)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("quote processor loop failed", extra={"event": "processor_failed"})
                await asyncio.sleep(0.5)

    async def _claim_stale(self) -> list[tuple[str, dict[str, str]]]:
        claimed = await self.store.redis.xautoclaim(
            self.settings.quote_stream,
            self.settings.stream_group,
            self.consumer_name,
            min_idle_time=self._pending_min_idle_ms,
            start_id="0-0",
            count=100,
        )
        entries = claimed[1] if isinstance(claimed, (list, tuple)) and len(claimed) > 1 else []
        result: list[tuple[str, dict[str, str]]] = []
        for stream_id, fields in entries:
            result.append((str(stream_id), {str(key): str(value) for key, value in fields.items()}))
        return result

    async def _process_one(self, stream_id: str, fields: dict[str, str]) -> None:
        try:
            payload = fields["payload"]
            event = QuoteEvent.model_validate_json(payload)
            result = await self.store.process_quote(event, source_stream_id=stream_id)
            if result.duplicate:
                self.metrics.deduplicated_events.inc()
                await self.store.ack(
                    self.settings.quote_stream,
                    self.settings.stream_group,
                    stream_id,
                )
                return

            if result.out_of_order or result.timestamp_regressed or result.gap:
                if result.out_of_order and result.timestamp_regressed:
                    kind = "out_of_order_and_timestamp_regression"
                elif result.out_of_order:
                    kind = "out_of_order"
                elif result.timestamp_regressed:
                    kind = "timestamp_regression"
                else:
                    kind = "gap"
                self.metrics.gap_events.labels(kind).inc()
                logger.warning(
                    "temporal quote anomaly",
                    extra={
                        "event": "temporal_quote_anomaly",
                        "symbol": event.symbol,
                        "provider": event.provider,
                        "stream_id": stream_id,
                    },
                )

            if result.accepted:
                self.metrics.provider_events.labels(event.provider).inc()
            await self.store.ack(
                self.settings.quote_stream,
                self.settings.stream_group,
                stream_id,
            )
        except (KeyError, ValidationError, ValueError) as exc:
            await self._handle_failure(stream_id, fields, exc)
        except Exception as exc:
            await self._handle_failure(stream_id, fields, exc)

    async def _handle_failure(
        self,
        stream_id: str,
        fields: dict[str, Any],
        exc: Exception,
    ) -> None:
        retry_count = await self.store.increment_retry(stream_id)
        logger.error(
            "quote processing failed",
            extra={
                "event": "quote_processing_failed",
                "stream_id": stream_id,
                "retry_count": retry_count,
            },
            exc_info=exc,
        )
        if retry_count >= self.settings.retry_limit:
            await self.store.move_to_dead_letter(
                source_stream=self.settings.quote_stream,
                stream_id=stream_id,
                payload=fields,
                error=str(exc),
                retry_count=retry_count,
            )
            await self.store.ack(
                self.settings.quote_stream,
                self.settings.stream_group,
                stream_id,
            )

    async def close(self) -> None:
        self._closed = True


class FanoutListener:
    def __init__(self, store: RedisStore, hub: ConnectionHub) -> None:
        self.store = store
        self.hub = hub
        self._closed = False

    async def run(self) -> None:
        while not self._closed:
            try:
                async for event in self.store.fanout_messages():
                    if self._closed:
                        break
                    await self.hub.publish(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("fanout listener failed", extra={"event": "fanout_failed"})
                await asyncio.sleep(0.5)

    async def close(self) -> None:
        self._closed = True
