# ADR 0001 — Redis Streams for durability, Pub/Sub for local fan-out

**Status:** Accepted

## Context

Quote ingestion must survive processor restarts and support explicit acknowledgement. WebSocket delivery must also reach every gateway instance that owns client connections.

## Decision

- The collector writes normalized events to Redis Streams.
- A consumer group validates, deduplicates, detects sequence gaps, updates the latest-value cache, and acknowledges each event.
- Successfully processed events are published to a Redis Pub/Sub channel.
- Every gateway instance subscribes to Pub/Sub and fans events out only to its local WebSocket sessions.
- Poison messages move to a dead-letter stream after bounded retries.

## Consequences

Streams provide recovery and observable pending entries. Pub/Sub avoids the incorrect design where a shared consumer group would deliver a quote to only one gateway instance. Pub/Sub delivery itself is ephemeral, so reconnecting clients always receive a cached snapshot before live events.
