from collections.abc import Iterable
from datetime import datetime
from uuid import UUID

from smart_market_data_gateway.intelligence.models import MarketObservation, ObservationKind


class TemporalMarketMemory:
    """In-memory point-in-time observation store with idempotent ingestion.

    The memory is intentionally knowledge-time aware. Queries include only facts
    whose `received_at` is not later than the requested `knowledge_time`, even if
    their source/event timestamp is earlier. This prevents late-arriving facts
    from leaking into historical analysis.
    """

    def __init__(self) -> None:
        self._observations: dict[UUID, MarketObservation] = {}

    def __len__(self) -> int:
        return len(self._observations)

    def append(self, observation: MarketObservation) -> bool:
        """Append one observation.

        Returns `True` when a new observation is stored and `False` for an exact
        idempotent replay. Reusing an observation ID with different content is a
        hard integrity error.
        """

        existing = self._observations.get(observation.observation_id)
        if existing is None:
            self._observations[observation.observation_id] = observation
            return True
        if existing == observation:
            return False
        raise ValueError("observation_id already exists with different content")

    def append_many(self, observations: Iterable[MarketObservation]) -> int:
        """Append a batch and return the number of newly stored observations."""

        return sum(1 for observation in observations if self.append(observation))

    def get(self, observation_id: UUID) -> MarketObservation | None:
        return self._observations.get(observation_id)

    def as_of(
        self,
        symbol: str,
        *,
        knowledge_time: datetime,
        valid_at: datetime | None = None,
        kinds: set[ObservationKind] | None = None,
    ) -> tuple[MarketObservation, ...]:
        """Return the deterministic evidence timeline available at a point in time.

        `knowledge_time` controls what the system was allowed to know. `valid_at`
        controls TTL validity and defaults to the knowledge time.
        """

        self._require_timezone(knowledge_time, "knowledge_time")
        evaluation_time = valid_at or knowledge_time
        self._require_timezone(evaluation_time, "valid_at")
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol must not be blank")

        observations = [
            observation
            for observation in self._observations.values()
            if observation.symbol == normalized_symbol
            and observation.received_at <= knowledge_time
            and (kinds is None or observation.kind in kinds)
            and (
                observation.expires_at is None
                or observation.expires_at > evaluation_time
            )
        ]
        observations.sort(
            key=lambda observation: (
                observation.observed_at,
                observation.received_at,
                str(observation.observation_id),
            )
        )
        return tuple(observations)

    def latest(
        self,
        symbol: str,
        *,
        knowledge_time: datetime,
        valid_at: datetime | None = None,
        kinds: set[ObservationKind] | None = None,
    ) -> MarketObservation | None:
        timeline = self.as_of(
            symbol,
            knowledge_time=knowledge_time,
            valid_at=valid_at,
            kinds=kinds,
        )
        return timeline[-1] if timeline else None

    @staticmethod
    def _require_timezone(value: datetime, field_name: str) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field_name} must include timezone information")
