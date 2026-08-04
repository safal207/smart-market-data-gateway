import asyncio
from collections.abc import AsyncIterator, Collection
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from smart_market_data_gateway.domain import (
    DepthSemantics,
    EvidenceOrigin,
    MarketEvidenceCapability,
    QuantityUnit,
    QuoteEvent,
    VolumeKind,
    VolumeSemantics,
)
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
    gap_every: int | None = None
    out_of_order_every: int | None = None

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        for name, value in (
            ("duplicate_every", self.duplicate_every),
            ("fail_after_events", self.fail_after_events),
            ("gap_every", self.gap_every),
            ("out_of_order_every", self.out_of_order_every),
        ):
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive")


class MockMarketDataProvider(MarketDataProvider):
    """Deterministic provider for local development, tests, and demonstrations."""

    def __init__(self, config: MockProviderConfig | None = None) -> None:
        self._config = config or MockProviderConfig()
        self._state = ProviderState.DISCONNECTED
        self._message: str | None = None
        self._symbols: set[str] = set()
        self._sequence_by_symbol: dict[str, int] = {}
        self._events_emitted = 0
        self._queue: asyncio.Queue[QuoteEvent] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    @property
    def name(self) -> str:
        return "mock-provider"

    @property
    def capabilities(self) -> frozenset[MarketEvidenceCapability]:
        return frozenset(
            {
                MarketEvidenceCapability.LEVEL1_QUOTE,
                MarketEvidenceCapability.VOLUME,
                MarketEvidenceCapability.AGGRESSOR_FLOW,
                MarketEvidenceCapability.TRADE_COUNT,
                MarketEvidenceCapability.TOP_OF_BOOK_DEPTH,
            }
        )

    async def connect(self) -> None:
        async with self._lock:
            if self._task is not None and not self._task.done():
                return
            self._state = ProviderState.CONNECTING
            self._message = None
            self._events_emitted = 0
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
        timeout_seconds = max(self._config.interval_seconds * 2, 0.05)
        while True:
            task = self._task
            if (task is None or task.done()) and self._queue.empty():
                return
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=timeout_seconds)
            except TimeoutError:
                continue
            yield event

    async def _run(self) -> None:
        try:
            while True:
                async with self._lock:
                    symbols = sorted(self._symbols)

                for symbol in symbols:
                    self._events_emitted += 1
                    event = self._next_event(symbol, self._events_emitted)
                    await self._queue.put(event)

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

    def _next_event(self, symbol: str, emission_number: int) -> QuoteEvent:
        sequence = self._sequence_by_symbol[symbol] + 1
        if self._config.gap_every and emission_number % self._config.gap_every == 0:
            sequence += 1
        self._sequence_by_symbol[symbol] = sequence

        emitted_sequence = sequence
        if (
            self._config.out_of_order_every
            and emission_number % self._config.out_of_order_every == 0
            and sequence > 1
        ):
            emitted_sequence = sequence - 1

        symbol_seed = sum(symbol.encode("utf-8"))
        base = Decimal(50 + symbol_seed % 200)
        price = base + Decimal(sequence % 20) / Decimal("100")
        now = datetime.now(UTC)
        event_id = uuid5(
            NAMESPACE_URL,
            f"{self.name}:{symbol}:{emitted_sequence}:{emission_number}",
        )

        volume = Decimal(1_000 + symbol_seed % 250 + emission_number * 5)
        buy_share = Decimal("0.45") + Decimal(sequence % 4) * Decimal("0.05")
        buy_volume = (volume * buy_share).quantize(Decimal("0.01"))
        sell_volume = volume - buy_volume
        bid_depth = Decimal(500 + symbol_seed % 100 + sequence * 3)
        ask_depth = Decimal(480 + symbol_seed % 90 + sequence * 2)
        interval_ms = max(1, round(self._config.interval_seconds * 1_000))

        return QuoteEvent(
            schema_version="1.1",
            event_id=event_id,
            symbol=symbol,
            price=price,
            bid=price - Decimal("0.01"),
            ask=price + Decimal("0.01"),
            provider_timestamp=now,
            received_at=now,
            sequence=emitted_sequence,
            provider=self.name,
            capabilities=self.capabilities,
            volume=volume,
            buy_volume=buy_volume,
            sell_volume=sell_volume,
            trade_count=50 + emission_number % 25,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            volume_semantics=VolumeSemantics(
                kind=VolumeKind.INTERVAL,
                unit=QuantityUnit.BASE_ASSET,
                aggregation_window_ms=interval_ms,
                origin=EvidenceOrigin.PROVIDER_AGGREGATED,
            ),
            depth_semantics=DepthSemantics(
                unit=QuantityUnit.BASE_ASSET,
                levels=1,
                origin=EvidenceOrigin.NATIVE,
            ),
        )

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        normalized = symbol.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized
