# Temporal Market Intelligence Foundation

This module adapts the useful interaction pattern behind continuously aware assistants to a narrower, auditable market-intelligence domain. It does not copy an external product, claim general intelligence, infer causality from correlation, predict prices by itself, or place trades.

## Objective

Create a small foundation that can answer four precise questions:

1. What market fact was observed?
2. When did the source say it happened?
3. When did this system first know it?
4. Did a bounded hypothesis become confirmed, invalidated, or expire?

The gateway already records normalized `QuoteEvent` evidence and can bind it to a tamper-evident recorder hash. The intelligence layer consumes that evidence without weakening its provenance.

## Components

### Typed observations

`quote_event_to_observations()` converts only evidence that exists on a validated `QuoteEvent`:

- Level-1 quote;
- volume and trade count;
- aggressor flow;
- top-of-book depth.

Unsupported evidence stays absent. Provider units, aggregation windows, origin, sequence, stale state, and recorder-ledger references remain explicit.

### Point-in-time temporal memory

Every `MarketObservation` stores two different timestamps:

- `observed_at`: source or event time;
- `received_at`: when this system first learned the fact.

Historical queries filter by `received_at`, not merely by event time. A report published at 12:10 about an event at 12:00 therefore cannot appear in a 12:05 replay. This is the primary look-ahead prevention invariant.

The memory accepts exact idempotent replay but rejects reuse of an observation ID with different content.

### Prediction ledger

A `Hypothesis` must declare:

- a falsifiable statement;
- a concrete expected event;
- a deadline;
- confidence;
- supporting evidence;
- optional counter-evidence.

The state machine is deliberately small:

```text
NO_SIGNAL -> WATCH -> CONFIRMED
                   -> INVALIDATED
                   -> EXPIRED
```

`CONFIRMED`, `INVALIDATED`, and `EXPIRED` are terminal. Confirmation and invalidation require explicit evidence. Expiry is generated only after the declared deadline.

Every transition is linked to the previous transition with canonical SHA-256 hashing. Editing a reason, timestamp, state, evidence reference, ordering, or chain link invalidates verification.

## Example

```python
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from smart_market_data_gateway.intelligence import (
    Hypothesis,
    HypothesisState,
    PredictionLedger,
    quote_event_to_observations,
)

observations = quote_event_to_observations(
    quote_event,
    record_hash=recorder_record_hash,
    ledger_index=recorder_ledger_index,
)

hypothesis = Hypothesis(
    symbol=quote_event.symbol,
    statement="aggressive buy flow may precede a bounded breakout",
    expected_event="price trades above 65050 before the deadline",
    created_at=datetime.now(UTC),
    deadline=datetime.now(UTC) + timedelta(minutes=10),
    confidence=Decimal("0.64"),
    supporting_evidence=tuple(ref for item in observations for ref in item.evidence),
)

ledger = PredictionLedger()
ledger.register(hypothesis)
ledger.transition(
    hypothesis.hypothesis_id,
    to_state=HypothesisState.CONFIRMED,
    occurred_at=datetime.now(UTC),
    reason="declared price and flow thresholds were observed",
    evidence=observations[0].evidence,
)
ledger.verify()
```

## Non-goals and residual risks

- No news ingestion, entity resolution, semantic retrieval, or agent orchestration is included yet.
- No statistical rule establishes causality; the ledger records claims and outcomes.
- The in-memory temporal store is not durable and is intended as a domain foundation.
- The SHA-256 prediction chain is tamper-evident, not externally signed or anchored.
- Confidence is supplied by an upstream evaluator and is not calibrated here.
- Recorder hashes preserve evidence integrity only under the recorder's documented threat model.
- Provider storage, derived-analytics, training, and redistribution rights remain external release constraints.

## Next bounded increments

1. Persist observations and prediction transitions in the existing archive layer.
2. Add event/news observations with source licensing and publication-time provenance.
3. Implement deterministic replay evaluation with explicit `NO_SIGNAL` output.
4. Add skeptic and verifier roles as isolated evaluators, not autonomous trading agents.
5. Expose read-only event, evidence, and realization endpoints after the storage contract is stable.
