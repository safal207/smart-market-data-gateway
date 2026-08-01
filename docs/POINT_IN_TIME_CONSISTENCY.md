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
| `source_stream_id` | Original accepted Redis Stream position |
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

Historical replay reproduces accepted-event order using the numeric Redis Stream ID components:

```text
source_stream_milliseconds ASC,
source_stream_sequence ASC,
accepted_at ASC,
event_id ASC
```

Rows without a valid source stream ID sort after stream-addressable rows and use `accepted_at` plus `event_id` as deterministic fallbacks. Replay delays use `accepted_at`, because that is when the platform could first consume the event. Market-time filtering still uses `provider_timestamp`.

Candle construction remains event-time based. Therefore replay order and candle order intentionally answer different questions:

- replay order: what the system knew, and in which sequence;
- candle order: where an event belongs in market time.

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
