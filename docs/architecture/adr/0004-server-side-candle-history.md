# ADR 0004 — Server-side candle history from accepted quote observations

**Status:** Accepted

## Context

The chart reference application can aggregate candles inside one browser session, but those candles disappear on reload and differ between clients depending on when each client connected. The gateway needs one server-side historical representation that can bootstrap charts consistently and can be governed by the existing `historical_data` tier entitlement.

Provider-native exchange candles are not yet available because the real-provider licensing and redistribution gate remains unresolved. Calling browser-observed quotes "exchange candles" or interpreting quote update counts as traded volume would be misleading.

## Decision

The quote processor maintains canonical one-minute candles in Redis after an event has passed schema validation and event-id deduplication.

- Each symbol and UTC minute has one JSON candle value and one sorted-set index entry.
- Updates use optimistic Redis transactions with `WATCH` so concurrent processors cannot silently overwrite another accepted observation.
- Open and close are selected by provider event time, with event ID as a deterministic tie-breaker.
- High and low retain decimal values without converting prices through binary floating-point arithmetic.
- Higher timeframes (`5m`, `15m`, `1h`, `4h`, `1d`) are aggregated from the canonical `1m` series at read time.
- Historical responses contain only fully closed intervals whose interval end is at or before the requested `end`.
- Retention preserves a whole UTC minute until `bucket_end + retention`; value TTL, index pruning, index expiry, and read filtering use that same deadline.
- Retention defaults to 30 days and is configurable with `SMDG_CANDLE_HISTORY_RETENTION_SECONDS`.
- The REST endpoint is available only when the caller's tier policy has `historical_data=true`.

The public source label is `observed_quote_aggregation`. Responses explicitly state that trade volume is unavailable, partial intervals are omitted, and intervals without accepted observations are omitted. `activity_count` means accepted quote observations, not executed trades.

## API

`GET /v1/candles/{symbol}` accepts:

- `timeframe`: `1m`, `5m`, `15m`, `1h`, `4h`, or `1d`;
- `limit`: 1–1000 closed candle slots in the requested lookback window;
- `end`: optional timezone-aware timestamp that must not be in the future.

The endpoint returns an empty `data` list rather than a fabricated flat or partial candle when no fully closed observations exist. Because only minute OHLC state is retained, the service cannot reconstruct an as-of partial minute; it omits that interval instead.

## Failure and recovery

A candle update is committed in the same optimistic Redis transaction as the latest-quote cache update. Transaction conflicts retry up to `SMDG_CANDLE_UPDATE_RETRY_LIMIT`; exhausting retries fails quote processing so the existing stream retry and dead-letter behavior remains visible. Non-positive retention or retry settings fail application configuration validation before startup.

Each candle value expires at its bucket-aligned retention deadline. Writes prune expired sorted-set members and refresh an index TTL; reads also prune expired or missing members, so inactive indexes disappear and stale members cannot make expired candles readable.

## Consequences

### Positive

- all clients receive the same server-derived history;
- out-of-order quotes produce deterministic OHLC values;
- historical boundaries never expose observations later than `period_end`;
- storage grows by minutes rather than raw quote frequency;
- tier entitlements can monetize historical access;
- the response does not overclaim trade volume or provider-native exchange history.

### Limitations

- history starts warming only after this feature is deployed;
- in-progress and partial historical buckets are omitted and must be completed from the live stream by clients;
- gaps remain gaps and are not forward-filled;
- quote-derived OHLC can differ from trade-derived or exchange-official candles;
- retention is Redis-backed and is not yet a long-term archival store;
- provider licensing must be approved before replacing this source with licensed native historical candles.
