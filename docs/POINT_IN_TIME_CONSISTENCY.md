# Point-in-Time Consistency

## Invariant

For a prediction created at `prediction_time`, every event, edge, feature, calibration artifact, and model input must satisfy:

```text
known_at <= prediction_time
```

A historical value that was corrected later must not replace the value that was actually known at prediction time.

## Current timestamps

| Field | Meaning |
|---|---|
| `provider_timestamp` | Source time assigned by the provider |
| `received_at` | Time the collector received the normalized quote |
| `accepted_at` | Time the quote passed gateway quality gates |
| `data_cutoff` | Latest source timestamp represented by an accepted envelope |
| `persisted_at` | Database write time; never a substitute for `known_at` |

The current accepted-event foundation uses `accepted_at` as the first explicit platform knowledge timestamp.

## Required future fields

Every temporal edge and feature row must contain:

```text
valid_from
valid_to
known_at
data_cutoff
method_version
data_version
```

Predictions must store:

```text
prediction_time
feature_version
graph_snapshot_id
model_version
calibration_version
data_cutoff
```

## Replay ordering

Historical replay uses this deterministic order:

```text
provider_timestamp ASC,
received_at ASC,
event_id ASC
```

The event ID is a stable tie-breaker when two events share timestamps.

## Candle policy

- Event-time buckets are UTC based.
- Open and close use event time, not arrival order.
- A bounded watermark accepts configured lateness.
- Once finalized, a candle is not silently rewritten.
- Later events are persisted separately for audit and possible versioned correction workflows.

## Leakage test requirements

Future feature and model code must include automated failures for:

- future candle joins;
- future graph edges;
- full-dataset normalization;
- revised provider history used before its publication time;
- incorrect timezone or session boundaries;
- graph construction performed outside each training window;
- target values leaking through rolling windows;
- survivorship-biased instrument universes.

## Operational rule

When knowledge time cannot be proven, the value is excluded. Missing information may produce `ABSTAIN`; it must never be repaired with future data during evaluation.
