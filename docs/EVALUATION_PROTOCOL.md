# Evaluation Protocol

Status: experimental contract; model training is not implemented yet.

## Models compared

```text
A. Statistical base rate
B. Price, spread, activity, volatility, and market regime
C. The same features plus temporal graph features
```

C is useful as a predictive layer only when it repeatedly improves B.

## Primary metrics

- Brier score;
- log loss;
- calibration error and reliability curves;
- interval coverage for P10/P50/P90;
- abstain rate;
- results after spread, latency, fees, and slippage.

Accuracy alone is not an acceptance metric for probabilistic signals.

## Validation

- Purged walk-forward windows.
- Embargo between train and evaluation periods.
- Graph reconstruction inside each training window.
- `known_at <= prediction_time` for every input.
- Survivorship-free instrument universe.
- Separate calibration window.
- Stress scenarios at 1x, 1.5x, and 2x estimated costs.
- Results split by instrument, session, and market regime.

## Shadow requirement

Before user-facing probability claims:

- at least 30 trading days;
- at least 1,000 resolved predictions;
- multiple instruments and regimes;
- automated stale and degraded-feed abstention;
- live calibration compared with backtest calibration.

## GO

Proceed when graph features improve the non-graph model across multiple walk-forward windows and the improvement survives live shadow evaluation and estimated costs.

## NO-GO for graph alpha

Do not call graph features a source of alpha when improvement exists only in backtest, depends on one instrument, disappears after costs, degrades calibration, or is explained by leakage.
