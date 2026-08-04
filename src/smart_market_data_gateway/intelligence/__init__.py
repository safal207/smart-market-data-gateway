"""Auditable temporal market-intelligence primitives.

This package intentionally provides evidence, memory, and hypothesis-state
contracts only. It does not claim causality, predict prices, or place trades.
"""

from smart_market_data_gateway.intelligence.adapters import quote_event_to_observations
from smart_market_data_gateway.intelligence.ledger import (
    GENESIS_HASH,
    PredictionLedger,
    verify_ledger_entries,
)
from smart_market_data_gateway.intelligence.memory import TemporalMarketMemory
from smart_market_data_gateway.intelligence.models import (
    EvidenceRef,
    Hypothesis,
    HypothesisSnapshot,
    HypothesisState,
    LedgerEntry,
    MarketObservation,
    ObservationKind,
)

__all__ = [
    "GENESIS_HASH",
    "EvidenceRef",
    "Hypothesis",
    "HypothesisSnapshot",
    "HypothesisState",
    "LedgerEntry",
    "MarketObservation",
    "ObservationKind",
    "PredictionLedger",
    "TemporalMarketMemory",
    "quote_event_to_observations",
    "verify_ledger_entries",
]
