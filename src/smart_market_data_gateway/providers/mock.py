import asyncio
from collections.abc import AsyncIterator, Collection
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from smart_market_data_gateway.domain import QuoteEvent
from smart_market_data_gateway.providers.base import (
    MarketDataProvider,
    ProviderHealth,
    ProviderState,
)


@dataclass(frozen=True, slots=True)
class MockProviderConfig:
    interval_seconds: float = 0.25
    duplicate_every: int | None = None
    fail_after_events: int | None = None

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.duplicate_every is not None and self.duplicate_every <= 0:
            raise ValueError("duplicate_every must be positive")
        if self.fail_after_events is not None and self.fail_after_events <= 0:
            raise ValueError("fail_after_events must be positive")


class MockMarketDataProvider(MarketDataProvider):
    """Deterministic provider for local development, tests, and demonstrations."""

    def __init__(self, config: MockProviderConfig | None = None) -> None:
        self._config = config or MockProviderConfig()
        self._state = ProviderState.DISCONNECTED
        self._message: str | None = None
        self._symbols: set[str] = set()
        self._sequence_by_symbol: dict[str, int] = {}
        self._events_emitted = 0
        self._queue: asyncio.Queue[QuoteEvent | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    @property
    def name(self) -> str:
        return "mock-provider"

    async def connect(self) -> None:
        async with self._lock:
            if self._task is not None and not self._task.done():
                return
            self._queue = asyncio.Queue()
            self._state = ProviderState.CONNECTING
            self._message = None
            self._task = asyncio.create_task(self._run(), name="mock-market-data-provider")
            self._state = ProviderState.CONNECTED

    async def disconnect(self) -> None:
        async with self._lock:
            task = self._task
            self._task = None
            self._state = ProviderState.DISCONNECTED
            self._message = None

        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await self._queue.put(None)

    async def subscribe(self, symbols: Collection[str]) -> None:
        normalized = {self._normalize_symbol(symbol) for symbol in symbols}
        async with self._lock:
            self._symbols.update(normalized)
            for symbol in normalized:
                self._sequence_by_symbol.setdefault(symbol, 0)

    async def unsubscribe(self, symbols: Collection[str]) -> None:
        normalized = {self._normalize_symbol(symbol) for symbol in symbols}
        async with self._lock:
            self._symbols.difference_update(normalized)

    async def health(self) -> ProviderHealth:
        return ProviderHealth(state=self._state, message=self._message)

    async def events(self) -> AsyncIterator[QuoteEvent]:
        while True:
            event = await self._queue.get()
            if event is None:
                return
            yield event

    async def _run(self) -> None:
        try:
            while True:
                async with self._lock:
                    symbols = sorted(self._symbols)

                for symbol in symbols:
                    event = self._next_event(symbol)
                    await self._queue.put(event)
                    self._events_emitted += 1

                    duplicate_every = self._config.duplicate_every
                    if duplicate_every and self._events_emitted % duplicate_every == 0:
                        await self._queue.put(event)

                    fail_after = self._config.fail_after_events
                    if fail_after and self._events_emitted >= fail_after:
                        raise ConnectionError("simulated provider failure")

                await asyncio.sleep(self._config.interval_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._state = ProviderState.DEGRADED
            self._message = str(exc)
            await self._queue.put(None)

    def _next_event(self, symbol: str) -> QuoteEvent:
        sequence = self._sequence_by_symbol[symbol] + 1
        self._sequence_by_symbol[symbol] = sequence

        base = Decimal(50 + sum(symbol.encode("utf-8")) % 200)
        price = base + Decimal(sequence % 20) / Decimal("100")
        now = datetime.now(UTC)
        event_id = uuid5(NAMESPACE_URL, f"{self.name}:{symbol}:{sequence}")

        return QuoteEvent(
            event_id=event_id,
            symbol=symbol,
            price=price,
            bid=price - Decimal("0.01"),
            ask=price + Decimal("0.01"),
            provider_timestamp=now,
            received_at=now,
            sequence=sequence,
            provider=self.name,
        )

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        normalized = symbol.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized
