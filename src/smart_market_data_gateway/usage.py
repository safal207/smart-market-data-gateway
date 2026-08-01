import asyncio
from dataclasses import dataclass, field
import logging
from typing import Any

from smart_market_data_gateway.storage import RedisStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class UsageEvent:
    idempotency_key: str
    client_id: str
    event_type: str
    quantity: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)


class UsageRecorder:
    """Moves usage persistence away from REST and WebSocket delivery paths."""

    def __init__(self, store: RedisStore, max_queue_size: int = 10_000) -> None:
        self.store = store
        self._queue: asyncio.Queue[UsageEvent] = asyncio.Queue(maxsize=max_queue_size)
        self._closed = False
        self.dropped = 0

    def record(
        self,
        *,
        idempotency_key: str,
        client_id: str,
        event_type: str,
        quantity: int = 1,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        event = UsageEvent(
            idempotency_key=idempotency_key,
            client_id=client_id,
            event_type=event_type,
            quantity=quantity,
            metadata=metadata or {},
        )
        try:
            self._queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            self.dropped += 1
            logger.warning(
                "usage queue full",
                extra={"event": "usage_queue_full", "client_id": client_id},
            )
            return False

    async def run(self) -> None:
        while not self._closed or not self._queue.empty():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except TimeoutError:
                continue
            try:
                await self.store.record_usage(
                    idempotency_key=event.idempotency_key,
                    client_id=event.client_id,
                    event_type=event.event_type,
                    quantity=event.quantity,
                    metadata=event.metadata,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "usage persistence failed",
                    extra={
                        "event": "usage_persistence_failed",
                        "client_id": event.client_id,
                    },
                )
            finally:
                self._queue.task_done()

    async def close(self, timeout_seconds: float = 5.0) -> None:
        self._closed = True
        try:
            await asyncio.wait_for(self._queue.join(), timeout=timeout_seconds)
        except TimeoutError:
            logger.warning(
                "usage queue did not drain before shutdown",
                extra={"event": "usage_shutdown_timeout"},
            )
