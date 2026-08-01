from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Symbol = Annotated[str, Field(min_length=1, max_length=32, pattern=r"^[A-Z0-9._:-]+$")]
PositiveDecimal = Annotated[Decimal, Field(gt=0)]
NonNegativeDecimal = Annotated[Decimal, Field(ge=0)]
NonNegativeInteger = Annotated[int, Field(ge=0)]


class ServiceTier(StrEnum):
    BASIC = "basic"
    PRO = "pro"
    PREMIUM = "premium"


class QuoteEvent(BaseModel):
    """Provider-independent market quote event, schema version 1."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = Field(default="1.0", pattern=r"^1\.[0-9]+$")
    event_id: UUID = Field(default_factory=uuid4)
    symbol: Symbol
    price: PositiveDecimal
    bid: PositiveDecimal | None = None
    ask: PositiveDecimal | None = None
    last_size: NonNegativeDecimal | None = None
    cumulative_volume: NonNegativeDecimal | None = None
    trade_count: NonNegativeInteger | None = None
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
