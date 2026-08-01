# ADR 0003 — Versioned contracts and latest-value quote backpressure

**Status:** Accepted

## Public contracts

REST and WebSocket endpoints use the `/v1` namespace. Pydantic models are the source of truth for runtime validation and OpenAPI. WebSocket messages are documented in `docs/asyncapi.yaml`. Additive fields are non-breaking; removing, renaming, or changing the meaning of a field requires a new major API version and a deprecation window.

## Slow consumers

Quote channels use a bounded latest-value buffer. When a client cannot keep up, a newer quote replaces an older unsent quote for the same symbol. This prevents unbounded memory growth and avoids delivering stale queues. Coalescing and queue depth are observable.

Transactional messages such as order statuses, trade confirmations, risk events, and critical alerts are outside the quote policy and must use a separate durable channel without replacement semantics.

## Provider abstraction

All vendor-specific authentication, payload formats, polling, and streaming behavior remain behind `MarketDataProvider`. The rest of the gateway consumes only normalized `QuoteEvent` objects.
