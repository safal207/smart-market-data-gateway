from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Collection
from dataclasses import dataclass
from enum import StrEnum

from smart_market_data_gateway.domain import MarketEvidenceCapability, QuoteEvent


class ProviderState(StrEnum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DEGRADED = "degraded"


@dataclass(frozen=True, slots=True)
class ProviderHealth:
    state: ProviderState
    message: str | None = None


class MarketDataProvider(ABC):
    """Vendor-neutral contract for streaming or polling market-data adapters."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable provider identifier used in events and metrics."""

    @property
    def capabilities(self) -> frozenset[MarketEvidenceCapability]:
        """Evidence classes this provider can emit for the current adapter mode."""

        return frozenset({MarketEvidenceCapability.LEVEL1_QUOTE})

    @abstractmethod
    async def connect(self) -> None:
        """Establish the provider connection and prepare event delivery."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Stop event delivery and release provider resources."""

    @abstractmethod
    async def subscribe(self, symbols: Collection[str]) -> None:
        """Activate upstream delivery for the supplied symbols."""

    @abstractmethod
    async def unsubscribe(self, symbols: Collection[str]) -> None:
        """Deactivate upstream delivery for the supplied symbols."""

    @abstractmethod
    async def health(self) -> ProviderHealth:
        """Return current adapter health without performing network I/O."""

    @abstractmethod
    def events(self) -> AsyncIterator[QuoteEvent]:
        """Yield normalized quote events until disconnected."""
