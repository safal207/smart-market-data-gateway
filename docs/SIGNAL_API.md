# Explainable Signal API

Status: proposed contract; no prediction endpoint is implemented yet.

## Planned resources

```http
GET /v1/signals
GET /v1/signals/{prediction_id}
GET /v1/instruments/{symbol}/signals
```

## Signal response

```json
{
  "prediction_record_id": "uuid",
  "prediction_id": "uuid",
  "prediction_version": 1,
  "supersedes_record_id": null,
  "decision": "SIGNAL",
  "lifecycle_status": "ACTIVE",
  "outcome_status": "UNRESOLVED",
  "instrument": "AAPL",
  "event": {
    "type": "breakout_5m_high",
    "horizon_seconds": 180
  },
  "abstain_reason": null,
  "probability": 0.64,
  "base_rate": 0.41,
  "lift": 1.56,
  "quantiles": {
    "p10": -0.0008,
    "p50": 0.0007,
    "p90": 0.0021
  },
  "valid_until": "2026-08-01T12:31:20Z",
  "market_regime": "uptrend_medium_volatility",
  "data_quality": 0.97,
  "supports": [],
  "opposes": [],
  "invalidation_conditions": [],
  "calibration": {
    "bucket": "0.60-0.66",
    "observed_rate": 0.62,
    "sample_size": 1842
  }
}
```

## `ABSTAIN` response

```json
{
  "prediction_record_id": "uuid",
  "prediction_id": "uuid",
  "prediction_version": 1,
  "supersedes_record_id": null,
  "decision": "ABSTAIN",
  "lifecycle_status": "ABSTAINED",
  "outcome_status": "NOT_APPLICABLE",
  "instrument": "AAPL",
  "event": {
    "type": "breakout_5m_high",
    "horizon_seconds": 180
  },
  "abstain_reason": "STALE_DATA",
  "probability": null,
  "base_rate": null,
  "lift": null,
  "quantiles": {
    "p10": null,
    "p50": null,
    "p90": null
  },
  "valid_until": "2026-08-01T12:31:20Z",
  "market_regime": "UNKNOWN",
  "data_quality": 0.42,
  "supports": [],
  "opposes": [],
  "invalidation_conditions": [],
  "calibration": null
}
```

Allowed `abstain_reason` values are versioned and include:

```text
STALE_DATA
DEGRADED_FEED
MISSING_FEATURES
LOW_CONFIDENCE
UNKNOWN_REGIME
GRAPH_EXPIRED
INTEGRITY_NOT_VERIFIED
```

## Safety and honesty rules

- The API describes observed evidence and probabilistic events, not guaranteed returns.
- `LEADS` is labeled statistical, not causal.
- Expired signals are never returned as active.
- Supporting and opposing evidence are both exposed.
- Every explanation links to feature provenance.
- Stale, incomplete, low-confidence, or unverified data produces `ABSTAIN` rather than a forced probability.
- `ABSTAIN` always uses `null` probability, lift, calibration, and quantile values; clients must not interpret it as zero probability.
