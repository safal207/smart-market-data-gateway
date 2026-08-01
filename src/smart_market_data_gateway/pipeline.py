import asyncio
from datetime import UTC, datetime
import json
import logging
from typing import Any
from uuid import uuid4

from pydantic import ValidationError

from smart_market_data_gateway.config import Settings
from smart_market_data_gateway.domain import (
    AcceptedQuoteEvent,
    DataQualityMetadata,
    QuoteEvent,
    QuoteRejectionReason,
    RejectedQuoteEvent,
)
from smart_market_data_gateway.metrics import GatewayMetrics
from smart_market_data_gateway.qos import ConnectionHub
from smart_market_data_gateway.storage import RedisStore

logger = logging.getLogger(__name__)


class QuoteProcessor:
    """Durable stream processor: validate, gate, audit, cache, and fan out."""

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
                    messages = await self._claim_stale()
                    self.metrics.redis_pending_entries.set(await self.store.pending_count())
                    if not messages:
                        continue
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
            min_idle_time=max(5_000, int(self.settings.heartbeat_seconds * 2000)),
            start_id="0-0",
            count=100,
        )
        entries = claimed[1] if isinstance(claimed, (list, tuple)) and len(claimed) > 1 else []
        result: list[tuple[str, dict[str, str]]] = []
        for stream_id, fields in entries:
            result.append((str(stream_id), {str(key): str(value) for key, value in fields.items()}))
        return result

    async def _process_one(self, stream_id: str, fields: dict[str, str]) -> None:
        event: QuoteEvent | None = None
        claimed = False
        try:
            payload = fields["payload"]
            event = QuoteEvent.model_validate_json(payload)
            claim_status = await self.store.claim_event(event, stream_id)
            if claim_status == "busy":
                logger.debug(
                    "quote event is already being processed",
                    extra={"event": "quote_event_busy", "stream_id": stream_id},
                )
                return
            if claim_status == "processed":
                self.metrics.deduplicated_events.inc()
                await self._reject(event, stream_id, QuoteRejectionReason.DUPLICATE)
                await self._ack(stream_id)
                return
            claimed = True

            accepted_at = datetime.now(UTC)
            feed_age_seconds = max(
                0.0,
                (accepted_at - event.provider_timestamp).total_seconds(),
            )
            if feed_age_seconds > self.settings.accepted_event_max_age_seconds:
                await self._reject(
                    event,
                    stream_id,
                    QuoteRejectionReason.STALE,
                    detail=f"feed age {feed_age_seconds:.3f}s exceeds accepted limit",
                )
                await self.store.mark_event_processed(event)
                await self._ack(stream_id)
                return

            observation = await self.store.observe_sequence(event)
            if observation is not None:
                kind = "out_of_order" if observation.out_of_order else "gap"
                self.metrics.gap_events.labels(kind).inc()
                await self.store.redis.set(
                    f"smdg:stream-health:{event.provider}:{event.symbol}",
                    json.dumps(
                        {
                            "status": "degraded",
                            "kind": kind,
                            "previous_sequence": observation.previous_sequence,
                            "current_sequence": observation.current_sequence,
                            "gap": observation.gap,
                            "observed_at": accepted_at.isoformat(),
                        }
                    ),
                    ex=300,
                )
                logger.warning(
                    "sequence anomaly",
                    extra={
                        "event": "sequence_anomaly",
                        "symbol": event.symbol,
                        "provider": event.provider,
                        "stream_id": stream_id,
                    },
                )
                if observation.out_of_order:
                    await self._reject(
                        event,
                        stream_id,
                        QuoteRejectionReason.OUT_OF_ORDER,
                        detail=(
                            f"sequence {observation.current_sequence} is not newer than "
                            f"{observation.previous_sequence}"
                        ),
                    )
                    await self.store.mark_event_processed(event)
                    await self._ack(stream_id)
                    return

            quality_score = 0.8 if observation is not None else 1.0
            if event.sequence is None:
                quality_score = min(quality_score, 0.9)
            accepted = AcceptedQuoteEvent(
                event=event,
                quality=DataQualityMetadata(
                    score=quality_score,
                    gap_detected=observation is not None,
                    out_of_order=False,
                    stale=False,
                    source_provider=event.provider,
                    normalization_version=self.settings.normalization_version,
                    accepted_at=accepted_at,
                ),
                data_cutoff=event.provider_timestamp,
                source_stream_id=stream_id,
            )

            await self.store.publish_accepted_event(accepted)
            await self.store.cache_quote(event)
            await self.store.publish_fanout(event)
            await self.store.mark_event_processed(event)
            self.metrics.provider_events.labels(event.provider).inc()
            await self._ack(stream_id)
        except (KeyError, ValidationError, ValueError) as exc:
            if claimed and event is not None:
                await self.store.release_event_claim(event)
            await self._handle_failure(stream_id, fields, exc)
        except Exception as exc:
            if claimed and event is not None:
                await self.store.release_event_claim(event)
            await self._handle_failure(stream_id, fields, exc)

    async def _reject(
        self,
        event: QuoteEvent,
        stream_id: str,
        reason: QuoteRejectionReason,
        *,
        detail: str | None = None,
    ) -> None:
        await self.store.publish_rejected_event(
            RejectedQuoteEvent(
                event=event,
                reason=reason,
                rejected_at=datetime.now(UTC),
                source_stream_id=stream_id,
                detail=detail,
            )
        )

    async def _ack(self, stream_id: str) -> None:
        await self.store.ack(
            self.settings.quote_stream,
            self.settings.stream_group,
            stream_id,
        )

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
            await self._ack(stream_id)

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
