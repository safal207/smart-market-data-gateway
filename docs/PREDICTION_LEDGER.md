# Prediction Outcome Ledger

Status: design contract; not implemented in the current foundation.

## Purpose

The ledger will preserve every prediction and its eventual outcome without rewriting history. A model correction creates a new prediction version; it does not mutate the original record.

## Minimum prediction record

```text
prediction_id
created_at
instrument
event_type
horizon_seconds
probability
base_rate
lift
p10
p50
p90
valid_until
model_version
calibration_version
feature_version
graph_snapshot_id
data_cutoff
quality_score
status
```

## Lifecycle

```text
CREATED
ACTIVE
WEAKENED
INVALIDATED
EXPIRED
RESOLVED_TRUE
RESOLVED_FALSE
UNRESOLVED
```

## Outcome rules

- Target definitions are versioned and machine executable.
- Outcome resolution uses only the declared future window.
- `FALSE`, `UNRESOLVED`, `INVALIDATED`, and `ABSTAIN` are separate states.
- Re-running the resolver is idempotent.
- Negative results and degraded-feed periods are retained.

## Audit requirement

Opening a historical prediction must reveal the exact accepted events, feature values, graph snapshot, model, calibration artifact, supporting evidence, opposing evidence, and target rule known at creation time.
