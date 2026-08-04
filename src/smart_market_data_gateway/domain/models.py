from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    field_serializer,
    field_validator,
    model_serializer,
    model_validator,
)

Symbol = Annotated[str, Field(min_length=1, max_length=32, pattern=r"^[A-Z0-9._:-]+$")]
PositiveDecimal = Annotated[Decimal, Field(gt=0)]
NonNegativeDecimal = Annotated[Decimal, Field(ge=0)]
NonNegativeInteger = Annotated[int, Field(ge=0)]
PositiveInteger = Annotated[int, Field(gt=0)]
CurrencyCode = Annotated[str, Field(min_length=2, max_length=12, pattern=r"^[A-Z0-9]+$")]


class ServiceTier(StrEnum):
    BASIC = "basic"
    PRO = "pro"
    PREMIUM = "premium"


class MarketEvidenceCapability(StrEnum):
    """Provider/session capabilities that may be represented on a quote event."""

    LEVEL1_QUOTE = "level1_quote"
    VOLUME = "volume"
    AGGRESSOR_FLOW = "aggressor_flow"
    TRADE_COUNT = "trade_count"
    TOP_OF_BOOK_DEPTH = "top_of_book_depth"


class EvidenceOrigin(StrEnum):
    """How a market evidence value was produced before normalization."""

    NATIVE = "native"
    PROVIDER_AGGREGATED = "provider_aggregated"
    GATEWAY_DERIVED = "gateway_derived"


class QuantityUnit(StrEnum):
    """Unambiguous quantity unit for volume and depth observations."""

    BASE_ASSET = "base_asset"
    QUOTE_NOTIONAL = "quote_notional"
    CONTRACTS = "contracts"


class VolumeKind(StrEnum):
    """Temporal interpretation of volume-like evidence."""

    INTERVAL = "interval"
    CUMULATIVE = "cumulative"


class VolumeSemantics(BaseModel):
    """Units, aggregation window, and provenance for volume-like evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: VolumeKind
    unit: QuantityUnit
    aggregation_window_ms: PositiveInteger | None = None
    currency: CurrencyCode | None = None
    origin: EvidenceOrigin

    @field_validator("currency", mode="before")
    @classmethod
    def normalize_currency(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().upper()
        return value

    @model_validator(mode="after")
    def validate_semantics(self) -> "VolumeSemantics":
        if self.kind is VolumeKind.INTERVAL and self.aggregation_window_ms is None:
            raise ValueError("interval volume requires aggregation_window_ms")
        if self.kind is VolumeKind.CUMULATIVE and self.aggregation_window_ms is not None:
            raise ValueError("cumulative volume must not declare aggregation_window_ms")
        if self.unit is QuantityUnit.QUOTE_NOTIONAL and self.currency is None:
            raise ValueError("quote_notional volume requires currency")
        if self.unit is not QuantityUnit.QUOTE_NOTIONAL and self.currency is not None:
            raise ValueError("currency is only valid for quote_notional volume")
        return self


class DepthSemantics(BaseModel):
    """Units and provenance for one top-of-book depth snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    unit: QuantityUnit
    levels: Annotated[int, Field(ge=1, le=20)] = 1
    currency: CurrencyCode | None = None
    origin: EvidenceOrigin

    @field_validator("currency", mode="before")
    @classmethod
    def normalize_currency(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().upper()
        return value

    @model_validator(mode="after")
    def validate_semantics(self) -> "DepthSemantics":
        if self.unit is QuantityUnit.QUOTE_NOTIONAL and self.currency is None:
            raise ValueError("quote_notional depth requires currency")
        if self.unit is not QuantityUnit.QUOTE_NOTIONAL and self.currency is not None:
            raise ValueError("currency is only valid for quote_notional depth")
        return self


class QuoteEvent(BaseModel):
    """Provider-independent market quote event.

    Schema 1.0 contains Level-1 price, bid, and ask evidence only. Schema 1.1 adds
    optional volume, aggressor-flow, trade-count, and top-of-book depth evidence.
    Missing evidence remains absent; zero is reserved for an observed zero.
    """

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

    capabilities: frozenset[MarketEvidenceCapability] = Field(
        default_factory=lambda: frozenset({MarketEvidenceCapability.LEVEL1_QUOTE})
    )
    volume: NonNegativeDecimal | None = None
    buy_volume: NonNegativeDecimal | None = None
    sell_volume: NonNegativeDecimal | None = None
    trade_count: NonNegativeInteger | None = None
    bid_depth: NonNegativeDecimal | None = None
    ask_depth: NonNegativeDecimal | None = None
    volume_semantics: VolumeSemantics | None = None
    depth_semantics: DepthSemantics | None = None

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

    @field_serializer("capabilities")
    def serialize_capabilities(
        self,
        capabilities: frozenset[MarketEvidenceCapability],
    ) -> list[str]:
        return sorted(capability.value for capability in capabilities)

    @model_serializer(mode="wrap")
    def serialize_available_evidence(
        self,
        handler: SerializerFunctionWrapHandler,
    ) -> dict[str, Any]:
        payload = handler(self)
        return {key: value for key, value in payload.items() if value is not None}

    @model_validator(mode="after")
    def validate_market(self) -> "QuoteEvent":
        if self.bid is not None and self.ask is not None and self.bid > self.ask:
            raise ValueError("bid must be less than or equal to ask")
        if MarketEvidenceCapability.LEVEL1_QUOTE not in self.capabilities:
            raise ValueError("level1_quote capability is required")

        rich_values_present = any(
            value is not None
            for value in (
                self.volume,
                self.buy_volume,
                self.sell_volume,
                self.trade_count,
                self.bid_depth,
                self.ask_depth,
                self.volume_semantics,
                self.depth_semantics,
            )
        )
        schema_minor = int(self.schema_version.split(".", 1)[1])
        if rich_values_present and schema_minor < 1:
            raise ValueError("rich market evidence requires schema_version 1.1 or newer")

        volume_values_present = any(
            value is not None
            for value in (self.volume, self.buy_volume, self.sell_volume, self.trade_count)
        )
        if volume_values_present and self.volume_semantics is None:
            raise ValueError("volume-like evidence requires volume_semantics")

        if self.volume is not None and MarketEvidenceCapability.VOLUME not in self.capabilities:
            raise ValueError("volume evidence requires volume capability")

        flow_present = self.buy_volume is not None or self.sell_volume is not None
        if flow_present:
            if self.buy_volume is None or self.sell_volume is None:
                raise ValueError("buy_volume and sell_volume must be provided together")
            if MarketEvidenceCapability.AGGRESSOR_FLOW not in self.capabilities:
                raise ValueError("aggressor flow requires aggressor_flow capability")
            if self.volume is not None and self.buy_volume + self.sell_volume > self.volume:
                raise ValueError("classified buy and sell volume must not exceed total volume")

        if (
            self.trade_count is not None
            and MarketEvidenceCapability.TRADE_COUNT not in self.capabilities
        ):
            raise ValueError("trade_count evidence requires trade_count capability")

        depth_present = self.bid_depth is not None or self.ask_depth is not None
        if depth_present:
            if self.bid_depth is None or self.ask_depth is None:
                raise ValueError("bid_depth and ask_depth must be provided together")
            if self.depth_semantics is None:
                raise ValueError("depth evidence requires depth_semantics")
            if self.depth_semantics.levels != 1:
                raise ValueError("bid_depth and ask_depth represent exactly one top-of-book level")
            if MarketEvidenceCapability.TOP_OF_BOOK_DEPTH not in self.capabilities:
                raise ValueError("depth evidence requires top_of_book_depth capability")

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
