# Implementation plan and backlog coverage

This document maps the product backlog to implementation units. The generic gateway is implemented without binding the repository to a commercial provider. A real provider adapter and provider-backed benchmark remain gated by provider selection and licensing review.

## Implemented in the full-backlog branch

### Architecture

- C4 System Context and Container diagrams.
- ADRs for Redis Streams/Pub/Sub, at-least-once processing, idempotency, API versioning, provider abstraction, and latest-value backpressure.
- End-to-end quote and failure flow.

### Provider and event pipeline

- `MarketDataProvider` abstraction.
- Deterministic mock provider with configurable duplicates, failures, gaps, and out-of-order events.
- Dedicated collector process.
- Redis control stream for first/last global subscription transitions.
- Reconnect with capped exponential backoff and jitter.
- Normalized `QuoteEvent` schema.
- Redis quote stream, explicit acknowledgements, dedupe, latest cache, sequence observation, retries, dead-letter stream, and Pub/Sub fan-out.

### Smart subscriptions and APIs

- Redis-backed connection/symbol registry with TTL and grace release.
- Aggregation ratio and upstream-subscription metrics.
- Single and multi-symbol REST reads with stale metadata and partial errors.
- WebSocket connect, subscribe, unsubscribe, snapshot, quote, heartbeat, acknowledgement, and structured error messages.
- Shared Pydantic contracts, OpenAPI, and AsyncAPI.

### QoS, reliability, security, and monetization foundation

- Configuration-driven Basic, Pro, and Premium policies.
- Adaptive quote frequency by tier.
- Symbol and operation limits.
- Bounded latest-value buffers for slow consumers.
- JWT validation, development tokens, optional JWKS, optional Client Profile integration, entitlement cache/fallback, and symbol/channel authorization.
- Redis-backed rate limiting.
- Idempotent usage records in an auditable Redis stream.
- Graceful application shutdown.

### Observability and quality

- Structured JSON logging with correlation and connection IDs.
- Sensitive-field redaction.
- Prometheus metrics for connections, subscriptions, aggregation, input/delivery rates, latency, dedupe, gaps, queues, stale reads, authentication failures, and Redis pending entries.
- Provisioned Grafana dashboard and Prometheus alerts.
- Unit, Redis integration, REST, WebSocket, contract, and benchmark tests.
- CI linting, formatting, typing, coverage threshold, dependency audit, secret-pattern scanning, Docker Compose validation, OpenAPI/AsyncAPI validation, and benchmark artifacts.

### Benchmark framework

- Deterministic baseline versus smart-subscription benchmark.
- Deployed WebSocket load runner using real client connections.
- Documented staged path to 10,000 clients.
- Explicit separation between measured values and extrapolated node/cost estimates.

## Gated next phase: real provider

1. Compare provider capabilities, market coverage, real-time/delayed status, trial access, rate limits, reliability, and total cost.
2. Complete `provider-licensing-checklist.md` using the exact contract/terms version.
3. Select a provider that explicitly permits the intended cache, display, redistribution, non-display, historical, and benchmark uses.
4. Implement the adapter behind the existing provider contract without changing gateway clients.
5. Add provider-specific contract tests, rate-limit tests, reconnect tests, and payload fixtures that are legally safe to store.
6. Run the deployed benchmark with the mock source first, then with the real test source under identical infrastructure limits.
7. Publish raw measurements, environment, commit SHA, assumptions, limitations, and confidence interval.

## Benchmark release criteria

A result may be called a real system benchmark only when it includes real WebSocket connections, real Redis operations, fixed container or host limits, repeated runs, raw artifacts, error rate, throughput, P50/P95/P99 latency, CPU, memory, bandwidth, pending stream entries, and the exact provider mode. Production capacity and cost claims require an additional saturation study and cannot be inferred from the deterministic routing benchmark alone.
