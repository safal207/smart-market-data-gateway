from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Annotated
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


Symbol = Annotated[str, Field(min_length=1, max_length=32, pattern=r"^[A-Z0-9._:-]+$")]
PositiveDecimal = Annotated[Decimal, Field(gt=0)]
NonNegativeInteger = Annotated[int, Field(ge=0)]


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
