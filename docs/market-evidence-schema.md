# Market Evidence Schema 1.1

`QuoteEvent` schema 1.1 extends the existing Level-1 quote contract with optional market-realization evidence. The extension is backward compatible: schema 1.0 events remain valid and contain price, bid, ask, timestamps, sequence, and provider only.

The central rule is:

> An absent field means unavailable evidence. A numeric zero means an observed zero.

The gateway never fills unsupported evidence with zero and never infers provider capabilities from missing values.

## Capabilities

Every event declares the evidence classes supported by its provider/adapter mode:

| Capability | Meaning |
|---|---|
| `level1_quote` | Price and optional best bid/ask |
| `volume` | Total observed volume for the declared semantics |
| `aggressor_flow` | Both buy- and sell-aggressor volume |
| `trade_count` | Observed trade count for the declared volume window |
| `top_of_book_depth` | Best-bid and best-ask depth snapshot |

`level1_quote` is always required. A provider may declare a capability while omitting a value on a particular event, but a value may never appear without its matching capability.

Provider adapters inherit a conservative Level-1-only default. Rich adapters must override their capability set explicitly.

## Volume-like fields

Optional fields:

- `volume`
- `buy_volume`
- `sell_volume`
- `trade_count`

When any of these fields is present, `volume_semantics` is required.

```json
{
  "kind": "interval",
  "unit": "base_asset",
  "aggregation_window_ms": 1000,
  "currency": null,
  "origin": "provider_aggregated"
}
```

### `kind`

- `interval`: value covers the declared `aggregation_window_ms` ending at `provider_timestamp`;
- `cumulative`: value is cumulative within the provider-defined session and must not declare an interval window.

### `unit`

- `base_asset`: shares, coins, tokens, or other native instrument units;
- `quote_notional`: monetary notional in the declared `currency`;
- `contracts`: derivative contract count.

`quote_notional` requires an uppercase `currency`. Other units must not declare one.

### `origin`

- `native`: supplied directly by the upstream venue/provider;
- `provider_aggregated`: aggregated by the upstream provider before gateway receipt;
- `gateway_derived`: calculated by the gateway from explicitly documented raw evidence.

Aggressor flow requires both `buy_volume` and `sell_volume`. If total `volume` is present, classified buy plus sell volume may not exceed it; an unclassified remainder is allowed.

## Top-of-book depth

Optional fields:

- `bid_depth`
- `ask_depth`

Both must be present together and require `depth_semantics`:

```json
{
  "unit": "base_asset",
  "levels": 1,
  "currency": null,
  "origin": "native"
}
```

The current fields represent exactly one best-price level. Multi-level depth requires a future explicitly versioned structure rather than overloading these two numbers.

## Versioning

- `1.0`: Level-1 quote only;
- `1.1`: optional volume, aggressor flow, trade count, and top-of-book depth.

Any rich field or rich semantics object on a `1.0` event is rejected. Future minor versions must remain backward compatible within major version 1.

## Recorder and ledger

The WebSocket recorder persists all normalized event fields before adding ledger metadata. Canonical SHA-256 hashing therefore covers:

- capability declarations;
- evidence values;
- units and currency;
- aggregation windows;
- evidence origin;
- timestamps and provider identity.

Editing any rich evidence field after recording invalidates `record_hash` and the remaining chain.

## Mock provider

The deterministic mock provider emits schema 1.1 evidence for tests and demos:

- interval base-asset volume;
- buy/sell aggressor split;
- trade count;
- top-of-book bid/ask depth.

This data is synthetic test evidence. It must not be represented as exchange-derived or used to claim market alpha.

## Provider implementation checklist

Before a real adapter enables rich fields, document and test:

1. exact upstream field mapping;
2. quantity unit and currency;
3. aggregation or cumulative window;
4. session reset behavior;
5. timestamp meaning;
6. native, provider-aggregated, or gateway-derived origin;
7. missing-data behavior;
8. licensing rights for storage, non-display use, derived analytics, model training, benchmark publication, and redistribution.

Ambiguous units, undocumented windows, negative values, one-sided depth, one-sided aggressor flow, or unsupported capabilities must fail closed.
