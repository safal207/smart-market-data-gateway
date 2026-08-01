# Explainable Signal API

Status: proposed contract; no prediction endpoint is implemented yet.

## Planned resources

```http
GET /v1/signals
GET /v1/signals/{prediction_id}
GET /v1/instruments/{symbol}/signals
```

## Minimum response

```json
{
  "prediction_id": "uuid",
  "instrument": "AAPL",
  "event": {
    "type": "breakout_5m_high",
    "horizon_seconds": 180
  },
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

## Safety and honesty rules

- The API describes observed evidence and probabilistic events, not guaranteed returns.
- `LEADS` is labeled statistical, not causal.
- Expired signals are never returned as active.
- Supporting and opposing evidence are both exposed.
- Every explanation links to feature provenance.
- Stale, incomplete, or low-confidence data produces `ABSTAIN` rather than a forced probability.
