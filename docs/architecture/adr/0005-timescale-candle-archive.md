# ADR 0005: TimescaleDB candle archive behind the Redis hot layer

- Status: accepted
- Date: 2026-08-04

## Context

The first server-side candle implementation keeps canonical one-minute candles in Redis with bucket-aligned expiry. That is appropriate for low-latency recent history, but it cannot provide multi-year replay or survive intentional Redis retention.

The gateway already operates PostgreSQL for usage records and already depends on `asyncpg`. Adding ClickHouse at this stage would introduce another query language, driver, deployment, backup, and observability surface before the candle workload requires it.

## Decision

Use TimescaleDB 2.28.3 on PostgreSQL 17 as the durable candle archive while retaining Redis as the hot layer.

1. The collector continues to publish each quote once to the existing Redis quote stream.
2. The live quote processor and the candle archive worker use independent consumer groups on that stream.
3. The archive worker inserts each `event_id` into an idempotency table and updates the canonical one-minute candle in the same PostgreSQL statement.
4. Open and close ordering use `(provider_timestamp, event_id)`, matching the Redis canonical candle rule for out-of-order and equal-timestamp events.
5. The durable one-minute table becomes a Timescale hypertable when the extension is available. Plain PostgreSQL remains a supported fallback for tests and degraded deployments.
6. Larger timeframes are aggregated in SQL at read time. Empty intervals remain omitted and no synthetic forward-fill is introduced.
7. The API queries Redis and the archive concurrently. Archive data supplies the long window; Redis overwrites only buckets whose full timeframe is conservatively inside the hot-retention window.
8. Archive startup or query failure must not block quote ingestion, WebSocket delivery, or recent Redis history.
9. The local Docker stack pins `timescale/timescaledb:2.28.3-pg17` and runs a dedicated `candle-archive` worker.

## Invariants

- A repeated Redis delivery of the same `event_id` does not increment `activity_count`.
- Event arrival order does not change the final OHLC values.
- Redis expiry cannot delete durable archive rows.
- A database outage cannot stop the live quote processor.
- A Redis hot-layer outage may degrade recent history, but archived history remains queryable when PostgreSQL is available.
- `activity_count` remains accepted quote observations, not exchange trade volume.

## Retention

Redis hot retention defaults to 30 days. The archive defaults to five years and installs a Timescale retention policy when the extension is available. Operators must size the Redis stream so the archive consumer cannot be trimmed while lagging; pending-entry monitoring and alerting are required before production rollout.

## Consequences

### Positive

- Multi-year history without changing the public candle contract.
- Reuses PostgreSQL operations, credentials, backups, and the existing Python driver.
- Keeps database latency and outages off the live delivery path.
- Supports deterministic replay and later continuous aggregates.

### Negative

- The archive is eventually consistent with the quote stream.
- The event-id idempotency table grows with accepted observations.
- PostgreSQL aggregation is sufficient for this stage but may eventually require continuous aggregates or a ClickHouse export for very large cross-symbol research workloads.
- Updating an existing Timescale retention duration requires an explicit migration; `if_not_exists` does not rewrite an existing policy.

## Follow-up

- Add archive consumer-lag metrics and alerts.
- Add Timescale continuous aggregates for heavily requested timeframes after query evidence justifies them.
- Add backup/restore verification for the archive volume.
- Re-evaluate ClickHouse only when cross-symbol analytical scans exceed the operationally acceptable PostgreSQL envelope.
