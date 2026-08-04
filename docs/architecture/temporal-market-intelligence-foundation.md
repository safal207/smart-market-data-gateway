# Temporal Market Intelligence Foundation

This module adapts the useful interaction pattern behind continuously aware assistants to a narrower, auditable market-intelligence domain. It does not copy an external product, claim general intelligence, infer causality from correlation, predict prices by itself, or place trades.

## Objective

Create a small foundation that can answer five precise questions:

1. What market fact was observed?
2. When did the source say it happened?
3. When did this system first know it?
4. What exactly did the system predict before the outcome?
5. Did that bounded hypothesis become confirmed, invalidated, or expire?

The gateway already records normalized `QuoteEvent` evidence and can bind it to a tamper-evident recorder hash. The intelligence layer consumes that evidence without weakening its provenance.

## Components

### Typed observations

`quote_event_to_observations()` converts only evidence that exists on a validated `QuoteEvent`:

- Level-1 quote;
- volume and trade count;
- aggressor flow;
- top-of-book depth.

Unsupported evidence stays absent. Provider identity, units, aggregation windows, origin, sequence, stale state, receipt time, and recorder-ledger references remain explicit.

Every `EvidenceRef` contains both source/event time and system receipt time. An observation cannot cite evidence that was learned after the observation's own `received_at`.

Observation IDs are deterministically derived from the complete adapter input: the normalized quote event, evidence-ledger reference, evidence kind, stale/age context, expiry, and generation. Repeating the exact same recorder replay therefore produces the same IDs and is idempotent in temporal memory. A changed adaptation context receives a different ID rather than silently overwriting the earlier observation.

`MarketObservation` is deeply immutable. Its metrics are copied into a read-only mapping during validation, rather than leaving a mutable dictionary inside an otherwise frozen model. The memory revalidates each supplied observation at its public ingestion boundary so unsafe Pydantic construction helpers cannot bypass temporal or immutability constraints.

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

Every cited evidence reference must have been received no later than `created_at`. The full hypothesis object—not only its ID—is embedded in the registration entry and covered by the first ledger hash. Changing the statement, expected event, deadline, confidence, or original evidence after the outcome therefore invalidates verification.

The state machine is deliberately small:

```text
NO_SIGNAL -> WATCH -> CONFIRMED
                   -> INVALIDATED
                   -> EXPIRED
```

`CONFIRMED`, `INVALIDATED`, and `EXPIRED` are terminal. Confirmation and invalidation require explicit evidence that was already known at the transition time, must include at least one evidence item received strictly after hypothesis creation, and must occur strictly before the declared deadline. Original supporting evidence may remain contextual evidence, but it cannot by itself be reused as proof that the predicted outcome happened. At the deadline, an unresolved hypothesis becomes `EXPIRED` rather than being retroactively confirmed.

Every transition is linked to the previous appended transition with canonical SHA-256 hashing. Independent hypotheses may interleave in append order, while time may not regress within one hypothesis. Editing a hypothesis, reason, timestamp, state, evidence reference, ordering, or chain link invalidates verification.

The ledger revalidates hypotheses, transition evidence, and externally supplied ledger entries at its public boundaries. This closes unsafe `model_copy` or `model_construct` bypasses before data is trusted or verified.

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

created_at = datetime.now(UTC)
hypothesis = Hypothesis(
    symbol=quote_event.symbol,
    statement="aggressive buy flow may precede a bounded breakout",
    expected_event="price trades above 65050 before the deadline",
    created_at=created_at,
    deadline=created_at + timedelta(minutes=10),
    confidence=Decimal("0.64"),
    supporting_evidence=tuple(ref for item in observations for ref in item.evidence),
)

ledger = PredictionLedger()
ledger.register(hypothesis)
ledger.transition(
    hypothesis.hypothesis_id,
    to_state=HypothesisState.CONFIRMED,
    occurred_at=created_at + timedelta(minutes=5),
    reason="declared price and flow thresholds were observed",
    evidence=outcome_observations[0].evidence,
)
ledger.verify()
```

## Non-goals and residual risks

- No news ingestion, entity resolution, semantic retrieval, or agent orchestration is included yet.
- No statistical rule establishes causality; the ledger records claims and outcomes.
- The in-memory temporal store is not durable and is intended as a domain foundation.
- The SHA-256 prediction chain is tamper-evident, not externally signed or anchored.
- Evidence receipt timestamps depend on the integrity of the ingesting component's clock and provenance.
- Evidence references identify their source through provenance and locator fields; richer first-class cross-asset relationship semantics remain future work.
- Confidence is supplied by an upstream evaluator and is not calibrated here.
- Recorder hashes preserve evidence integrity only under the recorder's documented threat model.
- Provider storage, derived-analytics, training, and redistribution rights remain external release constraints.

## Next bounded increments

1. Persist observations and prediction transitions in the existing archive layer.
2. Add event/news observations with source licensing and publication-time provenance.
3. Implement deterministic replay evaluation with explicit `NO_SIGNAL` output.
4. Add skeptic and verifier roles as isolated evaluators, not autonomous trading agents.
5. Expose read-only event, evidence, and realization endpoints after the storage contract is stable.
