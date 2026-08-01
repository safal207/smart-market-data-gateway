from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from smart_market_data_gateway.domain import AcceptedQuoteEvent, Candle


@dataclass(frozen=True, slots=True)
class CandleBuildResult:
    finalized: tuple[Candle, ...]
    late_intervals: tuple[int, ...]


@dataclass(slots=True)
class _MutableCandle:
    symbol: str
    interval_seconds: int
    bucket_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    event_count: int
    first_event_time: datetime
    last_event_time: datetime
    first_order_key: tuple[datetime, str]
    last_order_key: tuple[datetime, str]
    quality_score: float

    @classmethod
    def from_event(cls, accepted: AcceptedQuoteEvent, interval_seconds: int) -> "_MutableCandle":
        event = accepted.event
        order_key = (event.provider_timestamp, str(event.event_id))
        return cls(
            symbol=event.symbol,
            interval_seconds=interval_seconds,
            bucket_start=_bucket_start(event.provider_timestamp, interval_seconds),
            open=event.price,
            high=event.price,
            low=event.price,
            close=event.price,
            event_count=1,
            first_event_time=event.provider_timestamp,
            last_event_time=event.provider_timestamp,
            first_order_key=order_key,
            last_order_key=order_key,
            quality_score=accepted.quality.score,
        )

    def add(self, accepted: AcceptedQuoteEvent) -> None:
        event = accepted.event
        order_key = (event.provider_timestamp, str(event.event_id))
        self.high = max(self.high, event.price)
        self.low = min(self.low, event.price)
        self.event_count += 1
        self.quality_score = min(self.quality_score, accepted.quality.score)
        if order_key < self.first_order_key:
            self.first_order_key = order_key
            self.first_event_time = event.provider_timestamp
            self.open = event.price
        if order_key > self.last_order_key:
            self.last_order_key = order_key
            self.last_event_time = event.provider_timestamp
            self.close = event.price

    def finalize(self) -> Candle:
        return Candle(
            symbol=self.symbol,
            interval_seconds=self.interval_seconds,
            bucket_start=self.bucket_start,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            event_count=self.event_count,
            first_event_time=self.first_event_time,
            last_event_time=self.last_event_time,
            quality_score=self.quality_score,
            finalized=True,
        )


class CandleBuilder:
    """Builds deterministic quote-derived candles with an explicit lateness watermark."""

    def __init__(
        self,
        intervals_seconds: tuple[int, ...] = (1, 10, 60, 300),
        *,
        allowed_lateness_seconds: float = 2.0,
    ) -> None:
        intervals = tuple(sorted(set(intervals_seconds)))
        if not intervals or any(interval <= 0 for interval in intervals):
            raise ValueError("intervals_seconds must contain positive integers")
        if allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds must be non-negative")
        self.intervals_seconds = intervals
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self._active: dict[tuple[str, int, datetime], _MutableCandle] = {}
        self._watermark_by_symbol: dict[str, datetime] = {}
        self._finalized_through: dict[tuple[str, int], datetime] = {}

    def add(self, accepted: AcceptedQuoteEvent) -> CandleBuildResult:
        event = accepted.event
        previous_watermark = self._watermark_by_symbol.get(event.symbol)
        watermark = max(previous_watermark, event.provider_timestamp) if previous_watermark else event.provider_timestamp
        self._watermark_by_symbol[event.symbol] = watermark

        late_intervals: list[int] = []
        for interval in self.intervals_seconds:
            bucket_start = _bucket_start(event.provider_timestamp, interval)
            finalized_through = self._finalized_through.get((event.symbol, interval))
            if finalized_through is not None and bucket_start <= finalized_through:
                late_intervals.append(interval)
                continue
            key = (event.symbol, interval, bucket_start)
            candle = self._active.get(key)
            if candle is None:
                self._active[key] = _MutableCandle.from_event(accepted, interval)
            else:
                candle.add(accepted)

        finalized = self._finalize_ready(event.symbol, watermark)
        return CandleBuildResult(
            finalized=tuple(finalized),
            late_intervals=tuple(late_intervals),
        )

    def flush(self) -> tuple[Candle, ...]:
        finalized = [candle.finalize() for candle in self._active.values()]
        finalized.sort(key=lambda item: (item.symbol, item.interval_seconds, item.bucket_start))
        self._active.clear()
        for candle in finalized:
            self._finalized_through[(candle.symbol, candle.interval_seconds)] = candle.bucket_start
        return tuple(finalized)

    def _finalize_ready(self, symbol: str, watermark: datetime) -> list[Candle]:
        ready_keys: list[tuple[str, int, datetime]] = []
        for key, candle in self._active.items():
            if candle.symbol != symbol:
                continue
            bucket_end = candle.bucket_start + timedelta(seconds=candle.interval_seconds)
            if bucket_end + self.allowed_lateness <= watermark:
                ready_keys.append(key)

        ready_keys.sort(key=lambda item: (item[1], item[2]))
        finalized: list[Candle] = []
        for key in ready_keys:
            candle = self._active.pop(key).finalize()
            finalized.append(candle)
            self._finalized_through[(candle.symbol, candle.interval_seconds)] = candle.bucket_start
        return finalized


def _bucket_start(timestamp: datetime, interval_seconds: int) -> datetime:
    utc_timestamp = timestamp.astimezone(UTC)
    epoch_seconds = int(utc_timestamp.timestamp())
    bucket_epoch = epoch_seconds - epoch_seconds % interval_seconds
    return datetime.fromtimestamp(bucket_epoch, tz=UTC)
