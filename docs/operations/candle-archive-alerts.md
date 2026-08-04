# Candle archive alert runbook

This runbook covers the Prometheus alerts emitted for the Redis-to-Timescale candle archive path. The objective is to preserve accepted quote observations before approximate Redis Stream trimming removes entries that the archive group has not persisted.

## First checks

1. Open the **Smart Market Data Gateway** Grafana dashboard.
2. Inspect **Archive backlog**, **Archive trim risk**, **Oldest archive backlog**, and **Archive trim headroom**.
3. Confirm the worker target in Prometheus: `up{job="smart-market-data-candle-archive"}`.
4. Inspect the Redis group without changing ownership:

```bash
redis-cli XINFO GROUPS smdg:quotes:v1
redis-cli XPENDING smdg:quotes:v1 smdg:candle-archive-writers:v1
redis-cli XLEN smdg:quotes:v1
```

Do not acknowledge, delete, or claim entries manually until the database and worker failure mode is understood. The worker already uses `XAUTOCLAIM` for abandoned pending deliveries.

## CandleArchiveExporterDown

**Meaning:** Prometheus cannot reach the worker metrics endpoint on port `9102`.

Check the worker process/container, restart count, health logs, network policy, and metrics-port binding. Confirm that the archive worker is still consuming and that PostgreSQL is reachable. A metrics bind failure is non-fatal to persistence, so distinguish exporter failure from worker failure before restarting.

## CandleArchiveMonitorUnavailable

**Meaning:** The worker is reachable but cannot obtain a trustworthy Redis consumer-group sample.

Check Redis connectivity and permissions for `XLEN`, `XINFO GROUPS`, `XPENDING`, and `XRANGE`. Confirm the deployed Redis version reports group `lag`. Unknown lag intentionally fails closed as `monitor_up=0`; it is never converted to a false zero backlog.

## CandleArchiveConsumerGroupMissing

**Meaning:** The configured archive group does not exist. Every retained quote-stream entry is conservatively treated as unarchived.

Verify `SMDG_CANDLE_ARCHIVE_GROUP` and the quote-stream name. Start the archive worker so its idempotent startup creates the group. Do not create the group at `$`; the archive contract starts at `0` so retained observations can be archived.

## CandleArchiveNoConsumers

**Meaning:** Backlog exists, but Redis reports no registered archive consumers.

Start or recover the `candle-archive` worker. Inspect crash loops, PostgreSQL credentials, Timescale extension initialization, and resource limits. Once a worker reconnects, stale pending entries are recovered automatically.

## CandleArchiveBacklogStale

**Meaning:** The oldest pending or undelivered observation has remained unarchived for more than five minutes.

Inspect PostgreSQL latency, connection-pool saturation, locks, disk pressure, and worker error logs. Compare backlog growth with provider event rate. Valid entries remain pending during database/network outages; malformed entries alone follow the dead-letter retry policy.

## CandleArchiveTrimRisk

**Meaning:** Archive backlog exceeds 50% of configured stream capacity.

Treat this as a capacity warning. Restore archive throughput, reduce database latency, or increase `SMDG_STREAM_MAXLEN` based on measured event rate and expected recovery time. Because Redis uses approximate trimming, the ratio is a guardrail rather than an exact countdown.

## CandleArchiveTrimImminent

**Meaning:** Archive backlog is at least 80% of configured stream capacity.

Treat this as a durability incident. Prioritize restoring the archive worker and PostgreSQL. If the worker is healthy but cannot catch up, temporarily increase stream capacity and scale archive throughput. Preserve the Redis volume and avoid destructive stream operations.

## Recovery confirmation

Recovery is complete when all of the following hold for a sustained period:

- `smdg_candle_archive_monitor_up == 1`
- `smdg_candle_archive_consumer_group_present == 1`
- at least one archive consumer is registered
- backlog and oldest-backlog age are decreasing
- trim headroom is increasing
- no new archive persistence errors appear
- historical API reads contain both durable and recent hot-layer candles

After the incident, record peak provider rate, maximum backlog, catch-up rate, database outage duration, and the minimum observed trim headroom. Use those measurements to calibrate stream capacity and alert thresholds.
