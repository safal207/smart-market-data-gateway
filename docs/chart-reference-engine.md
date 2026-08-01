# Market Chart Reference Engine

## Purpose

The chart reference app is a correctness-first vertical slice for the Smart Market Data Gateway. It demonstrates how one browser workspace can share physical market-data subscriptions, reject stale or duplicated events, build deterministic candles, and render an accessible financial chart without coupling the data model to a charting library.

The default source is a synthetic deterministic replay. No Tradernet credentials or stored real-market history are required.

## Architecture

```text
Replay fixture or gateway WebSocket
                 |
        MarketDataSource adapter
                 |
      SmartSubscriptionManager
       - reference counting
       - reconnect generation fence
       - desired-subscription replay
                 |
         TemporalIntegrityGuard
       - event-id duplicate guard
       - sequence/revision ordering
       - timestamp regression policy
                 |
           MarketDataStore
       - bounded accepted events
       - quarantine diagnostics
       - deterministic rebuilds
                 |
           CandleAggregator
                 |
        ChartRenderer interface
                 |
     LightweightChartsRenderer 5.2
       - candlesticks
       - volume/update-count pane
       - crosshair and viewport
```

The renderer is deliberately the last layer. Replacing Lightweight Charts must not change subscription, temporal-integrity, aggregation, or workspace-persistence behavior.

## Data-source modes

### Replay

Replay is the safe default and the source used by tests. Events and timing are deterministic, so candle output and diagnostics are reproducible.

### Gateway

Gateway mode connects to `/v1/stream`, subscribes to the existing quote channel, accepts a cached snapshot before the acknowledgement, sends client pings, and reconnects with a bounded backoff. The browser token is held only in memory. Query-string tokens are intended for local development; a production deployment should use a same-origin session or short-lived WebSocket ticket.

The Vite development server proxies `/v1/stream` to `ws://localhost:8000`. In production, serve the static app behind a reverse proxy that routes the same path to the FastAPI gateway.

## Integrity rules

An incoming event is applied only when all relevant checks pass:

1. Its connection generation is current.
2. Its event ID has not already been applied.
3. Its sequence/revision does not move the stream backwards.
4. Its event time does not materially regress behind the accepted watermark.
5. Its values and timestamps are finite and valid.

Duplicate and quarantined events remain visible in bounded diagnostics with a reason such as `duplicate_event`, `stale_generation`, `sequence_regression`, or `timestamp_rollback`. Invalid wire frames are surfaced as source warnings. None of them alter OHLC values or increment the lower pane twice. The UI labels observed delivery discontinuities as **Sequence jumps** because entitlement-tier coalescing can legitimately skip provider sequence values; distinguishing those from upstream loss requires future per-delivery coalescing metadata.

## Candle and volume semantics

Candles are aggregated in event-time order. A candle is keyed by symbol, timeframe, and floored opening timestamp. Timeframe changes rebuild from the accepted bounded event set, producing the same result regardless of UI lifecycle.

For synthetic replay, the lower pane contains a deterministic quote-update count. Gateway quotes can provide cumulative volume and last size when the provider supplies trustworthy fields; the client converts cumulative values to non-negative deltas. If those fields are absent, the pane is explicitly labelled as update count rather than pretending it is exchange volume.

No real Tradernet history is persisted. Historical storage, redistribution, and derived-data publication remain subject to the provider and exchange licensing checklist.

## Run locally

Start the reference app in replay mode:

```bash
cd apps/chart-reference
pnpm install --frozen-lockfile
pnpm dev
```

To exercise gateway mode, start the existing gateway stack separately and use a local development token such as `dev-basic:alice` in the in-app connection panel.

Quality checks:

```bash
pnpm lint
pnpm typecheck
pnpm test:coverage
pnpm build
pnpm test:e2e
```

The browser suite builds and serves the production bundle through Vite preview using only deterministic replay data. It verifies phone, tablet, and desktop layouts, keyboard operation, accessible names, viewport-specific chart sizing, and absence of horizontal overflow. Development and preview servers both proxy `/v1/stream` for optional local gateway checks.

## Deliberate MVP boundaries

Included:

- one active instrument with persisted symbol and timeframe;
- shared quote subscriptions and reconnect fencing;
- deterministic replay and live gateway adapters;
- client-side candles, crosshair OHLC, and diagnostics;
- precision-safe price formatting;
- responsive, keyboard-accessible layout and a textual data summary.

Not included:

- order placement or account actions;
- order-book reconstruction;
- unlicensed storage of real provider history;
- technical indicators or drawing tools;
- a multi-pane workspace beyond price and the volume/update-count study;
- claims of exchange-grade tick or candle completeness.

## Production follow-ups

Before this reference becomes a production chart service:

1. Confirm caching, historical-storage, redistribution, and derived-data rights.
2. Add a licensed history API with a history-before-live merge contract.
3. Publish instrument metadata for tick size, currency, and market timezone.
4. Replace query-string bearer tokens with an ephemeral WebSocket ticket.
5. Define Redis Cluster hash-slot keys before running the multi-key atomic scripts on a clustered deployment.
6. Add a frozen-but-open provider watchdog and define provider-specific cumulative-volume reset rules and market-session calendars.
