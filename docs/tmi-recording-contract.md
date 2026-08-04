# Temporal Market Intelligence recording contract

This document defines the append-only JSONL boundary used to replay normalized Smart Market Data Gateway events in Temporal Market Intelligence (TMI).

## Purpose

The gateway remains responsible for reliable market-data delivery. TMI remains responsible for event realization analysis. A recorder may persist already-normalized WebSocket quote events without adding analytical logic to the gateway.

## JSONL event

Each line is one UTF-8 JSON object. Existing normalized quote fields remain valid:

```json
{
  "event_id": "0191c414-85ad-7fb8-a645-f32769ae7f27",
  "symbol": "BTC/USDT",
  "price": 65000.0,
  "bid": 64999.0,
  "ask": 65001.0,
  "provider_timestamp": "2026-08-01T12:00:00.000Z",
  "received_at": "2026-08-01T12:00:00.035Z",
  "sequence": 1847291,
  "provider": "demo-provider"
}
```

Optional evidence fields may be included when licensed and available:

```json
{
  "volume": 125.0,
  "buy_volume": 55.0,
  "sell_volume": 70.0,
  "bid_depth": 480.0,
  "ask_depth": 520.0,
  "spread_bps": 0.31
}
```

## Recorder invariants

- Preserve provider timestamps and timezone information.
- Write one complete JSON object per line.
- Never rewrite prior lines.
- Preserve symbol, provider, sequence, and event identifiers when present.
- Do not derive predictions, labels, or TMI verdicts inside the gateway.
- Apply the same sensitive-value redaction policy used by gateway logs.
- Record gaps, degraded-stream state, and reconnect boundaries in metadata or companion events.
- Respect provider rights for historical storage, non-display use, and redistribution.

## TMI behavior

TMI reads this recording, selects quotes nearest to pre-registered event times, derives midpoint/spread when needed, calculates comparable baseline volume, and rejects stale or malformed evidence.

Reference implementation: `safal207/temporal-market-intelligence`, draft PR #1.
