from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from smart_market_data_gateway.domain.models import Symbol

Confidence = Annotated[Decimal, Field(ge=0, le=1)]
Sha256Hex = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
NonNegativeInteger = Annotated[int, Field(ge=0)]
MetricValue = Decimal | int | str | bool


class ObservationKind(StrEnum):
    """Evidence class represented by one temporal market observation."""

    QUOTE = "quote"
    VOLUME = "volume"
    AGGRESSOR_FLOW = "aggressor_flow"
    TOP_OF_BOOK_DEPTH = "top_of_book_depth"
    EVENT = "event"
    DERIVED = "derived"


class HypothesisState(StrEnum):
    """Small, explicit state machine for a testable market hypothesis."""

    NO_SIGNAL = "no_signal"
    WATCH = "watch"
    CONFIRMED = "confirmed"
    INVALIDATED = "invalidated"
    EXPIRED = "expired"


class EvidenceRef(BaseModel):
    """Stable pointer from an analytical claim to source and knowledge time."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provenance_system: str = Field(min_length=1, max_length=128)
    provenance_component: str = Field(min_length=1, max_length=128)
    locator: str = Field(min_length=1, max_length=512)
    observed_at: datetime
    received_at: datetime
    record_hash: Sha256Hex | None = None
    ledger_index: NonNegativeInteger | None = None

    @field_validator("observed_at", "received_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("evidence timestamps must include timezone information")
        return value

    @model_validator(mode="after")
    def require_hash_for_ledger_index(self) -> Self:
        if self.ledger_index is not None and self.record_hash is None:
            raise ValueError("ledger_index requires record_hash")
        return self


class MarketObservation(BaseModel):
    """One immutable fact known to the system at a precise knowledge time.

    `observed_at` is source/event time. `received_at` is when this system first
    learned the fact. Point-in-time analysis must filter on `received_at` to
    prevent look-ahead leakage.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    observation_id: UUID = Field(default_factory=uuid4)
    symbol: Symbol
    kind: ObservationKind
    observed_at: datetime
    received_at: datetime
    source: str = Field(min_length=1, max_length=128)
    fact: str = Field(min_length=1, max_length=1_000)
    metrics: dict[str, MetricValue] = Field(default_factory=dict)
    confidence: Confidence = Decimal("1")
    evidence: tuple[EvidenceRef, ...] = Field(min_length=1)
    expires_at: datetime | None = None
    generation: NonNegativeInteger | None = None

    @field_validator("observed_at", "received_at", "expires_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("timestamps must include timezone information")
        return value

    @model_validator(mode="after")
    def validate_temporal_bounds(self) -> Self:
        if self.expires_at is not None and self.expires_at <= self.observed_at:
            raise ValueError("expires_at must be later than observed_at")
        if any(reference.received_at > self.received_at for reference in self.evidence):
            raise ValueError("observation cannot cite evidence learned after received_at")
        return self


class Hypothesis(BaseModel):
    """A bounded, falsifiable market hypothesis with explicit evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hypothesis_id: UUID = Field(default_factory=uuid4)
    symbol: Symbol
    statement: str = Field(min_length=1, max_length=2_000)
    expected_event: str = Field(min_length=1, max_length=1_000)
    created_at: datetime
    deadline: datetime
    confidence: Confidence
    supporting_evidence: tuple[EvidenceRef, ...] = Field(min_length=1)
    counter_evidence: tuple[EvidenceRef, ...] = ()

    @field_validator("created_at", "deadline")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include timezone information")
        return value

    @model_validator(mode="after")
    def validate_deadline_and_knowledge(self) -> Self:
        if self.deadline <= self.created_at:
            raise ValueError("deadline must be later than created_at")
        evidence = self.supporting_evidence + self.counter_evidence
        if any(reference.received_at > self.created_at for reference in evidence):
            raise ValueError("hypothesis evidence was not known at created_at")
        return self


class LedgerEntry(BaseModel):
    """One append-only state transition in a prediction ledger hash chain."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ledger_index: NonNegativeInteger
    hypothesis_id: UUID
    from_state: HypothesisState
    to_state: HypothesisState
    occurred_at: datetime
    reason: str = Field(min_length=1, max_length=2_000)
    evidence: tuple[EvidenceRef, ...] = ()
    hypothesis: Hypothesis | None = None
    previous_record_hash: Sha256Hex
    record_hash: Sha256Hex

    @field_validator("occurred_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include timezone information")
        return value


class HypothesisSnapshot(BaseModel):
    """Current materialized view of a hypothesis and its latest transition."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hypothesis: Hypothesis
    state: HypothesisState
    latest_transition: LedgerEntry
