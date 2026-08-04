from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from smart_market_data_gateway.domain import QuoteEvent

CandleTimeframe = Literal["1m", "5m", "15m", "1h", "4h", "1d"]
TIMEFRAME_SECONDS: dict[CandleTimeframe, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3_600,
    "4h": 14_400,
    "1d": 86_400,
}

Symbol = Annotated[str, Field(min_length=1, max_length=32, pattern=r"^[A-Z0-9._:-]+$")]
PositiveDecimal = Annotated[Decimal, Field(gt=0)]


def floor_time(value: datetime, interval_seconds: int) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include timezone information")
    utc_value = value.astimezone(UTC)
    epoch = int(utc_value.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % interval_seconds), tz=UTC)


def timeframe_seconds(timeframe: CandleTimeframe) -> int:
    return TIMEFRAME_SECONDS[timeframe]


def history_window(
    *,
    timeframe: CandleTimeframe,
    limit: int,
    end: datetime,
) -> tuple[datetime, datetime]:
    if limit < 1:
        raise ValueError("limit must be positive")
    if end.tzinfo is None or end.utcoffset() is None:
        raise ValueError("end must include timezone information")
    interval_seconds = timeframe_seconds(timeframe)
    effective_end = end.astimezone(UTC)
    last_closed_bucket = floor_time(effective_end, interval_seconds) - timedelta(
        seconds=interval_seconds
    )
    start = last_closed_bucket - timedelta(seconds=interval_seconds * (limit - 1))
    return start, effective_end


class BaseMinuteCandle(BaseModel):
    """Canonical one-minute candle aggregated from accepted quote events."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: Symbol
    open_time: datetime
    open: PositiveDecimal
    high: PositiveDecimal
    low: PositiveDecimal
    close: PositiveDecimal
    activity_count: Annotated[int, Field(ge=1)]
    first_provider_timestamp: datetime
    last_provider_timestamp: datetime
    first_event_id: UUID
    last_event_id: UUID

    @field_validator("open_time", "first_provider_timestamp", "last_provider_timestamp")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include timezone information")
        return value.astimezone(UTC)

    @classmethod
    def from_quote(cls, event: QuoteEvent) -> "BaseMinuteCandle":
        event_timestamp = event.provider_timestamp.astimezone(UTC)
        return cls(
            symbol=event.symbol,
            open_time=floor_time(event_timestamp, 60),
            open=event.price,
            high=event.price,
            low=event.price,
            close=event.price,
            activity_count=1,
            first_provider_timestamp=event_timestamp,
            last_provider_timestamp=event_timestamp,
            first_event_id=event.event_id,
            last_event_id=event.event_id,
        )

    def with_quote(self, event: QuoteEvent) -> "BaseMinuteCandle":
        if event.symbol != self.symbol:
            raise ValueError("quote symbol does not match candle symbol")
        event_timestamp = event.provider_timestamp.astimezone(UTC)
        if floor_time(event_timestamp, 60) != self.open_time:
            raise ValueError("quote does not belong to this minute candle")

        first_key = (self.first_provider_timestamp, str(self.first_event_id))
        last_key = (self.last_provider_timestamp, str(self.last_event_id))
        event_key = (event_timestamp, str(event.event_id))

        update: dict[str, object] = {
            "high": max(self.high, event.price),
            "low": min(self.low, event.price),
            "activity_count": self.activity_count + 1,
        }
        if event_key < first_key:
            update.update(
                open=event.price,
                first_provider_timestamp=event_timestamp,
                first_event_id=event.event_id,
            )
        if event_key > last_key:
            update.update(
                close=event.price,
                last_provider_timestamp=event_timestamp,
                last_event_id=event.event_id,
            )
        return self.model_copy(update=update)


class Candle(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: Symbol
    timeframe: CandleTimeframe
    open_time: datetime
    close_time: datetime
    open: PositiveDecimal
    high: PositiveDecimal
    low: PositiveDecimal
    close: PositiveDecimal
    activity_count: Annotated[int, Field(ge=1)]
    first_observation: datetime
    last_observation: datetime
    closed: bool

    @field_validator("open_time", "close_time", "first_observation", "last_observation")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include timezone information")
        return value.astimezone(UTC)


class CandleSeries(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "1.0"
    symbol: Symbol
    timeframe: CandleTimeframe
    source: Literal["observed_quote_aggregation"] = "observed_quote_aggregation"
    requested_limit: Annotated[int, Field(ge=1, le=1_000)]
    returned_count: Annotated[int, Field(ge=0)]
    retention_seconds: Annotated[int, Field(gt=0)]
    period_start: datetime
    period_end: datetime
    data: list[Candle]
    warnings: list[str] = Field(
        default_factory=lambda: [
            "Candles are aggregated from observed quote prices; trade volume is unavailable.",
            "Intervals with no accepted quote observations are omitted.",
            "Only fully closed intervals at or before period_end are returned.",
        ]
    )

    @field_validator("period_start", "period_end")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include timezone information")
        return value.astimezone(UTC)


def aggregate_base_candles(
    *,
    symbol: str,
    timeframe: CandleTimeframe,
    limit: int,
    end: datetime,
    retention_seconds: int,
    base_candles: Iterable[BaseMinuteCandle],
) -> CandleSeries:
    start, effective_end = history_window(timeframe=timeframe, limit=limit, end=end)
    interval_seconds = timeframe_seconds(timeframe)
    grouped: dict[datetime, list[BaseMinuteCandle]] = {}

    for candle in sorted(base_candles, key=lambda item: item.open_time):
        bucket = floor_time(candle.open_time, interval_seconds)
        bucket_end = bucket + timedelta(seconds=interval_seconds)
        if bucket < start or bucket_end > effective_end:
            continue
        grouped.setdefault(bucket, []).append(candle)

    data: list[Candle] = []
    for bucket in sorted(grouped):
        observations = grouped[bucket]
        bucket_end = bucket + timedelta(seconds=interval_seconds)
        last_observation = max(item.last_provider_timestamp for item in observations)
        if last_observation > effective_end:
            continue
        data.append(
            Candle(
                symbol=symbol,
                timeframe=timeframe,
                open_time=bucket,
                close_time=bucket_end,
                open=observations[0].open,
                high=max(item.high for item in observations),
                low=min(item.low for item in observations),
                close=observations[-1].close,
                activity_count=sum(item.activity_count for item in observations),
                first_observation=min(item.first_provider_timestamp for item in observations),
                last_observation=last_observation,
                closed=True,
            )
        )

    result = data[-limit:]
    return CandleSeries(
        symbol=symbol,
        timeframe=timeframe,
        requested_limit=limit,
        returned_count=len(result),
        retention_seconds=retention_seconds,
        period_start=start,
        period_end=effective_end,
        data=result,
    )
