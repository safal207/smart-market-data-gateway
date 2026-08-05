# Smart Market Data Gateway — Product Boundary

## Role

Smart Market Data Gateway is the **market-evidence data plane** for the Temporal Market Intelligence product.

It owns:

- market-data provider access and provider-specific safety gates;
- normalized `QuoteEvent` contracts and explicit evidence semantics;
- source time, receive time, sequence, stale/gap, capability, and unit metadata;
- Redis delivery, PostgreSQL/Timescale archival, REST/WebSocket serving, and observability;
- append-only JSONL capture and SHA-256 evidence-ledger verification;
- provenance, licensing warnings, redaction, replayability, and transport reliability.

It does **not** own:

- event or news hypotheses;
- prediction state machines;
- point-in-time analytical memory;
- causal claims;
- scoring, verdicts, attribution, backtesting decisions, or trade actions.

Those analytical responsibilities belong to [`safal207/temporal-market-intelligence`](https://github.com/safal207/temporal-market-intelligence).

## Canonical product flow

```text
Exchange / provider
        │
        ▼
Smart Market Data Gateway
  normalize → validate → timestamp → preserve provenance
        │
        ▼
Verified QuoteEvent 1.1 JSONL evidence ledger
        │
        ▼
Temporal Market Intelligence
  preregister → anchor → replay → score → verdict → receipt
```

## Cross-repository contract

The integration boundary is the gateway recorder output:

1. Every accepted market row is a validated normalized quote event.
2. Event time and system receive time remain distinct.
3. Optional volume, aggressor flow, trade count, depth, spread, units, and aggregation windows are explicit rather than inferred.
4. Every retained field is covered by the canonical linked SHA-256 ledger.
5. TMI verifies the full ledger before evaluating market realization.
6. Missing evidence remains missing and must never be converted into fabricated zeroes.

## Product rule

A change belongs in this repository only when it improves the reliability, integrity, provenance, availability, or delivery of market evidence.

A change belongs in TMI when it interprets evidence, registers a hypothesis, prevents analytical look-ahead, computes a feature, assigns a verdict, or produces an experiment receipt.

Do not introduce a second analytical truth source inside the gateway.

## Current release path

The first product release uses:

- Coinbase Advanced Trade as an explicitly enabled personal/internal research provider;
- `QuoteEvent` 1.1 rich evidence where the provider supplies it;
- verified JSONL evidence capture;
- Temporal Market Intelligence dual-anchor preregistration and replay;
- a private reproducible experiment receipt as the completion artifact.

Tradernet/Freedom support is intentionally outside the first release until a fresh implementation is based on current `main` and explicit provider licensing is available.

## Non-claims

This repository is not a trading bot, does not provide investment advice, does not establish causality or alpha, and grants no market-data rights beyond the operator's actual provider agreement.
