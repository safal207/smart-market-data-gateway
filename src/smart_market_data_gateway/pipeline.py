import asyncio
import logging
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

    async def run(self) -> None:
        await self.store.ensure_groups()
        while not self._closed:
            try:
                messages = await self.store.read_group(
                    self.settings.quote_stream,
                    self.settings.stream_group,
                    self.consumer_name,
                )
                if not messages:
                    self.metrics.redis_pending_entries.set(await self.store.pending_count())
                    continue
                for stream_id, fields in messages:
                    await self._process_one(stream_id, fields)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("quote processor loop failed", extra={"event": "processor_failed"})
                await asyncio.sleep(0.5)

    async def _process_one(self, stream_id: str, fields: dict[str, str]) -> None:
        try:
            payload = fields["payload"]
            event = QuoteEvent.model_validate_json(payload)
            if not await self.store.accept_event_once(event):
                self.metrics.deduplicated_events.inc()
                await self.store.ack(
                    self.settings.quote_stream,
                    self.settings.stream_group,
                    stream_id,
                )
                return

            observation = await self.store.observe_sequence(event)
            if observation is not None:
                kind = "out_of_order" if observation.out_of_order else "gap"
                self.metrics.gap_events.labels(kind).inc()
                logger.warning(
                    "sequence anomaly",
                    extra={
                        "event": "sequence_anomaly",
                        "symbol": event.symbol,
                        "provider": event.provider,
                        "stream_id": stream_id,
                    },
                )

            await self.store.cache_quote(event)
            await self.store.publish_fanout(event)
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
