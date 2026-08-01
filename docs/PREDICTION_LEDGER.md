# Prediction Outcome Ledger

Status: design contract; not implemented in the current foundation.

## Purpose

The ledger will preserve every prediction decision and its eventual outcome without rewriting history. Corrections append a new immutable record linked to the prior version.

## Identity and correction lineage

`prediction_id` is the stable identity of one logical forecast decision. Every stored revision also has a unique `prediction_record_id` and an increasing `prediction_version`.

A correction must:

- retain the original `prediction_id`;
- increment `prediction_version` by one;
- set `supersedes_record_id` to the immediately previous record;
- preserve the original record unchanged;
- state a machine-readable `revision_reason`.

The first record has `prediction_version = 1` and `supersedes_record_id = null`.

## Minimum prediction record

```text
prediction_record_id
prediction_id
prediction_version
supersedes_record_id
revision_reason
created_at
instrument
event_type
horizon_seconds
decision                 # SIGNAL | ABSTAIN
abstain_reason
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
lifecycle_status
outcome_status
```

## Decision contract

### `SIGNAL`

- `probability`, `base_rate`, and `lift` are required;
- quantiles are required only when the relevant quantile model is calibrated;
- the record proceeds through the signal lifecycle and may receive a resolved outcome.

### `ABSTAIN`

`ABSTAIN` is a first-class decision, not a low-probability signal.

- `abstain_reason` is required and machine-readable;
- `probability`, `base_rate`, `lift`, `p10`, `p50`, and `p90` are `null`;
- `valid_until` may express when the abstention should be reconsidered;
- `lifecycle_status` is `ABSTAINED`;
- `outcome_status` is `NOT_APPLICABLE` and never becomes `RESOLVED_TRUE` or `RESOLVED_FALSE`;
- the abstention and its data-quality context remain permanently auditable.

## Signal lifecycle

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

`ABSTAINED` is terminal for that immutable record. A later signal is a new prediction record, not a mutation of the abstention.

## Outcome rules

- Target definitions are versioned and machine-executable.
- Outcome resolution uses only the declared future window.
- `RESOLVED_FALSE`, `UNRESOLVED`, `INVALIDATED`, and `ABSTAINED` remain distinct.
- Re-running the resolver is idempotent.
- Negative results, abstentions, and degraded-feed periods are retained.

## Audit requirement

Opening a historical prediction must reveal the complete version lineage and the exact accepted events, feature values, graph snapshot, model, calibration artifact, supporting evidence, opposing evidence, target rule, and data-quality state known at creation time.
