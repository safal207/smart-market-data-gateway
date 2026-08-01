from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Symbol = Annotated[str, Field(min_length=1, max_length=32, pattern=r"^[A-Z0-9._:-]+$")]
PositiveDecimal = Annotated[Decimal, Field(gt=0)]
NonNegativeInteger = Annotated[int, Field(ge=0)]


class ServiceTier(StrEnum):
    BASIC = "basic"
    PRO = "pro"
    PREMIUM = "premium"


class QuoteRejectionReason(StrEnum):
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"
    STALE = "stale"
    INVALID = "invalid"


class QuoteEvent(BaseModel):
    """Provider-independent market quote event, schema version 1."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = Field(default="1.0", pattern=r"^1\.[0-9]+$")
    event_id: UUID = Field(default_factory=uuid4)
    symbol: Symbol
    price: PositiveDecimal
    bid: PositiveDecimal | None = None
    ask: PositiveDecimal | None = None
    provider_timestamp: datetime
    received_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    sequence: NonNegativeInteger | None = None
    provider: str = Field(min_length=1, max_length=64)

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().upper()
        return value

    @field_validator("provider_timestamp", "received_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include timezone information")
        return value

    @model_validator(mode="after")
    def validate_market(self) -> "QuoteEvent":
        if self.bid is not None and self.ask is not None and self.bid > self.ask:
            raise ValueError("bid must be less than or equal to ask")
        return self


class DataQualityMetadata(BaseModel):
    """Point-in-time quality facts attached when a quote enters the accepted stream."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    score: float = Field(ge=0.0, le=1.0)
    gap_detected: bool = False
    out_of_order: bool = False
    stale: bool = False
    source_provider: str = Field(min_length=1, max_length=64)
    normalization_version: str = Field(default="1.0", min_length=1, max_length=32)
    accepted_at: datetime

    @field_validator("accepted_at")
    @classmethod
    def require_accepted_at_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("accepted_at must include timezone information")
        return value


class AcceptedQuoteEvent(BaseModel):
    """Immutable envelope consumed by history, features, and prediction workers."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = Field(default="1.0", pattern=r"^1\.[0-9]+$")
    event: QuoteEvent
    quality: DataQualityMetadata
    data_cutoff: datetime
    source_stream_id: str | None = Field(default=None, max_length=64)

    @field_validator("data_cutoff")
    @classmethod
    def require_data_cutoff_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("data_cutoff must include timezone information")
        return value


class RejectedQuoteEvent(BaseModel):
    """Audit envelope for quotes that did not pass accepted-stream gates."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event: QuoteEvent
    reason: QuoteRejectionReason
    rejected_at: datetime
    source_stream_id: str | None = Field(default=None, max_length=64)
    detail: str | None = Field(default=None, max_length=512)

    @field_validator("rejected_at")
    @classmethod
    def require_rejected_at_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("rejected_at must include timezone information")
        return value


class Candle(BaseModel):
    """Deterministic quote-derived OHLC candle.

    QuoteEvent currently has no traded-volume field, so event_count is explicit and
    must not be presented as exchange volume.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = Field(default="1.0", pattern=r"^1\.[0-9]+$")
    symbol: Symbol
    interval_seconds: int = Field(gt=0)
    bucket_start: datetime
    open: PositiveDecimal
    high: PositiveDecimal
    low: PositiveDecimal
    close: PositiveDecimal
    event_count: int = Field(gt=0)
    first_event_time: datetime
    last_event_time: datetime
    quality_score: float = Field(ge=0.0, le=1.0)
    finalized: bool = True

    @field_validator("bucket_start", "first_event_time", "last_event_time")
    @classmethod
    def require_candle_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("candle timestamps must include timezone information")
        return value

    @model_validator(mode="after")
    def validate_ohlc(self) -> "Candle":
        if self.high < max(self.open, self.close, self.low):
            raise ValueError("high must be greater than or equal to open, close, and low")
        if self.low > min(self.open, self.close, self.high):
            raise ValueError("low must be less than or equal to open, close, and high")
        if self.last_event_time < self.first_event_time:
            raise ValueError("last_event_time must not be before first_event_time")
        return self


class QuoteSnapshot(BaseModel):
    quote: QuoteEvent
    stale: bool
    age_ms: int


class ClientIdentity(BaseModel):
    client_id: str = Field(min_length=1, max_length=128)
    tier: ServiceTier = ServiceTier.BASIC
    allowed_symbols: set[str] | None = None
    allowed_channels: set[str] = Field(default_factory=lambda: {"quote"})
    organization_id: str | None = None
    claims: dict[str, Any] = Field(default_factory=dict)

    @field_validator("allowed_symbols", mode="before")
    @classmethod
    def normalize_allowed_symbols(cls, value: Any) -> Any:
        if value is None:
            return None
        return {str(symbol).strip().upper() for symbol in value if str(symbol).strip()}


class SubscriptionCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["subscribe", "unsubscribe", "ping"]
    symbols: list[Symbol] = Field(default_factory=list, max_length=500)
    channels: list[str] = Field(default_factory=lambda: ["quote"], max_length=10)
    request_id: str | None = Field(default=None, max_length=128)

    @field_validator("symbols", mode="before")
    @classmethod
    def normalize_symbols(cls, value: Any) -> Any:
        if value is None:
            return []
        return list(dict.fromkeys(str(symbol).strip().upper() for symbol in value if str(symbol).strip()))

    @model_validator(mode="after")
    def validate_action_payload(self) -> "SubscriptionCommand":
        if self.action in {"subscribe", "unsubscribe"} and not self.symbols:
            raise ValueError("symbols are required for subscribe and unsubscribe")
        return self


class StreamMessage(BaseModel):
    type: Literal[
        "connected",
        "ack",
        "snapshot",
        "quote",
        "heartbeat",
        "warning",
        "error",
    ]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    request_id: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)


class GapObservation(BaseModel):
    symbol: Symbol
    previous_sequence: int | None
    current_sequence: int
    gap: int
    out_of_order: bool = False
