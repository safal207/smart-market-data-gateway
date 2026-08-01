# Smart Market Data Gateway — Product Backlog

## Product vision

Build a unified REST and WebSocket gateway for market data that reduces infrastructure cost by aggregating identical subscriptions, applies tier-based Quality of Service (QoS), and exposes one consistent API for web, mobile, and external clients.

The gateway sits between one or more market-data providers and client applications:

`Market-data provider → Collector → Redis Streams → Smart Subscription Service → Unified REST/WebSocket API → Web/Mobile/API clients`

## Business outcomes

- Reduce duplicated upstream subscriptions and outbound events.
- Reduce CPU, memory, bandwidth, and the number of required application nodes.
- Provide predictable service levels for Basic, Pro, and Premium clients.
- Create a monetization layer for higher API limits, update frequency, market depth, and historical data.
- Keep critical trading events equally reliable for all users.
- Provide measurable before/after efficiency benchmarks.

## MVP success metrics

| Metric | MVP target |
|---|---:|
| Identical user subscriptions aggregated into one logical upstream subscription | ≥ 95% |
| Reduction of delivered quote events versus a non-adaptive baseline | ≥ 35% |
| P95 gateway delivery latency under target load | ≤ 250 ms |
| Duplicate events processed more than once | 0 |
| Slow consumers affecting healthy clients | 0 |
| Reconnection after temporary provider failure | Automatic |
| REST and WebSocket API availability in local Docker environment | 100% |

> Targets are hypotheses until validated by load tests and provider-specific constraints.

## Priority model

- **P0** — required for the MVP.
- **P1** — required for a production-ready beta.
- **P2** — post-MVP capability or optimization.

Estimates use story points: `1, 2, 3, 5, 8, 13`.

---

# Epic 1 — Architecture and product boundaries

## ARCH-1 — C4 System Context diagram

**Priority:** P0  
**Estimate:** 2 SP  
**Dependencies:** none

**User story**  
As an engineer or stakeholder, I want to see the system boundary and external actors so that the product scope is unambiguous.

**Acceptance criteria**

- Shows web, mobile, and external API clients.
- Shows market-data providers and Client Profile/Billing systems.
- Shows Smart Market Data Gateway as one bounded system.
- Names the major protocols: HTTPS, WebSocket, REST, provider stream.

## ARCH-2 — C4 Container diagram

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** ARCH-1

**Acceptance criteria**

- Contains API Gateway, WebSocket Gateway, Collector, Subscription Service, QoS Policy Service, Redis, PostgreSQL, and observability components.
- Shows synchronous and asynchronous interactions separately.
- Defines ownership of subscriptions, cached quotes, policies, and connection state.

## ARCH-3 — Architecture Decision Records

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** ARCH-2

Create ADRs for:

- Redis Streams vs Redis Pub/Sub.
- At-most-once vs at-least-once event processing.
- Latest-value delivery for slow quote consumers.
- REST/WebSocket versioning strategy.
- Provider adapter abstraction.

## ARCH-4 — End-to-end quote flow

**Priority:** P0  
**Estimate:** 2 SP  
**Dependencies:** ARCH-2

**Acceptance criteria**

- Documents the complete flow from provider event to client delivery.
- Includes event normalization, cache update, deduplication, QoS filtering, and WebSocket delivery.
- Includes failure paths and reconnection behavior.

---

# Epic 2 — Market-data provider integration

## PROVIDER-1 — Provider adapter interface

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** ARCH-3

**Acceptance criteria**

- Defines connect, disconnect, subscribe, unsubscribe, health, and event callbacks.
- Hides vendor-specific formats from the rest of the system.
- Supports both streaming and polling providers.

## PROVIDER-2 — Mock market-data provider

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** PROVIDER-1

**Acceptance criteria**

- Generates deterministic quotes for a configurable list of symbols.
- Supports configurable event frequency and simulated failures.
- Can emit duplicates, gaps, malformed messages, and out-of-order events.
- Runs locally without external credentials.

## PROVIDER-3 — First real provider adapter

**Priority:** P1  
**Estimate:** 8 SP  
**Dependencies:** PROVIDER-1, SEC-5

**Acceptance criteria**

- Connects to one licensed/test provider.
- Maps provider messages to the normalized event schema.
- Handles provider rate limits and authentication.
- Does not expose provider credentials in logs or responses.

## PROVIDER-4 — Reconnect and exponential backoff

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** PROVIDER-1

**Acceptance criteria**

- Automatically reconnects after temporary disconnects.
- Uses capped exponential backoff with jitter.
- Restores active upstream subscriptions after reconnect.
- Exposes reconnect count and outage duration metrics.

## PROVIDER-5 — Normalized quote event schema

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** PROVIDER-1

```json
{
  "event_id": "0191c414-85ad-7fb8-a645-f32769ae7f27",
  "symbol": "AAPL",
  "price": 215.42,
  "bid": 215.40,
  "ask": 215.44,
  "provider_timestamp": "2026-08-01T12:00:00.000Z",
  "received_at": "2026-08-01T12:00:00.035Z",
  "sequence": 1847291,
  "provider": "demo-provider"
}
```

**Acceptance criteria**

- Schema is versioned.
- Mandatory and optional fields are documented.
- Validation rejects malformed events without crashing the stream.

---

# Epic 3 — Smart subscription aggregation

## SUB-1 — Active subscription registry

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** REDIS-1

**Acceptance criteria**

- Tracks clients by connection, symbol, channel, and service tier.
- Supports subscribe, unsubscribe, disconnect cleanup, and expiry.
- Registry state is observable through internal metrics.

## SUB-2 — Aggregate identical subscriptions

**Priority:** P0  
**Estimate:** 8 SP  
**Dependencies:** SUB-1, PROVIDER-1

**User story**  
As the platform, I want 100 clients watching `AAPL` to create one logical upstream subscription so that duplicated provider and server work is removed.

**Acceptance criteria**

- First subscriber activates one upstream subscription.
- Additional subscribers reuse the existing upstream stream.
- Subscriber count is accurate during concurrent connect/disconnect operations.
- No duplicate upstream subscriptions appear under race conditions.

## SUB-3 — Unsubscribe upstream after the last client

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** SUB-2

**Acceptance criteria**

- The final unsubscribe schedules upstream removal.
- A configurable grace period avoids subscribe/unsubscribe thrashing.
- A new subscriber during the grace period cancels removal.

## SUB-4 — Inactive subscription TTL

**Priority:** P1  
**Estimate:** 3 SP  
**Dependencies:** SUB-1

**Acceptance criteria**

- Orphaned subscriptions expire automatically.
- TTL cleanup does not remove active subscriptions.
- Cleanup actions are logged and counted.

## SUB-5 — Event deduplication

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** PROVIDER-5, REDIS-2

**Acceptance criteria**

- Repeated `event_id` is processed once.
- Provider events without IDs use a documented deterministic fingerprint.
- Deduplication retention is configurable.
- Metric records dropped duplicates.

## SUB-6 — Sequence and gap detection

**Priority:** P1  
**Estimate:** 5 SP  
**Dependencies:** PROVIDER-5

**Acceptance criteria**

- Detects missing and out-of-order sequence numbers.
- Emits an internal alert/event on a detected gap.
- Policy can request a snapshot or mark the stream as degraded.

---

# Epic 4 — Redis event backbone and cache

## REDIS-1 — Local Redis environment

**Priority:** P0  
**Estimate:** 2 SP  
**Dependencies:** none

**Acceptance criteria**

- Redis starts through Docker Compose.
- Health check is exposed.
- Local development requires no manual Redis configuration.

## REDIS-2 — Latest quote cache

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** REDIS-1, PROVIDER-5

**Acceptance criteria**

- Stores the latest normalized quote per symbol.
- New subscribers receive the current snapshot before live updates.
- Cached payload includes source and timestamps.
- Stale quotes are marked according to configurable freshness rules.

## REDIS-3 — Redis Streams event delivery

**Priority:** P0  
**Estimate:** 8 SP  
**Dependencies:** REDIS-1, PROVIDER-5

**Acceptance criteria**

- Collector publishes normalized events to a stream.
- Consumer processes events with explicit acknowledgement.
- Stream retention and maximum length are configurable.
- Consumer restart does not silently lose pending events.

## REDIS-4 — Consumer groups

**Priority:** P1  
**Estimate:** 5 SP  
**Dependencies:** REDIS-3

**Acceptance criteria**

- Multiple gateway instances divide processing safely.
- Pending entries can be inspected and reclaimed.
- Consumer identity is visible in metrics.

## REDIS-5 — Retry and dead-letter stream

**Priority:** P1  
**Estimate:** 5 SP  
**Dependencies:** REDIS-3

**Acceptance criteria**

- Transient failures retry with bounded attempts.
- Poison messages move to a dead-letter stream.
- Dead-letter events preserve payload and failure reason.

---

# Epic 5 — Unified REST and WebSocket API

## API-1 — Get latest quote

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** REDIS-2

`GET /v1/quotes/{symbol}`

**Acceptance criteria**

- Returns the latest normalized quote.
- Returns clear not-found and stale-data responses.
- Includes provider and freshness metadata.

## API-2 — Get multiple latest quotes

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** API-1

`GET /v1/quotes?symbols=AAPL,TSLA,NVDA`

**Acceptance criteria**

- Validates symbol count against user tier.
- Supports partial results with per-symbol errors.
- Response ordering is deterministic.

## API-3 — WebSocket market-data stream

**Priority:** P0  
**Estimate:** 8 SP  
**Dependencies:** SUB-2, REL-1

`GET /v1/stream`

Client command:

```json
{
  "action": "subscribe",
  "symbols": ["AAPL", "TSLA"],
  "channels": ["quote"]
}
```

**Acceptance criteria**

- Supports authenticated connect, subscribe, unsubscribe, and heartbeat.
- Subscription changes do not require reconnecting.
- Sends a snapshot before live events.
- Produces structured error responses.

## API-4 — Shared contracts for web and mobile

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** API-1, API-3

**Acceptance criteria**

- REST and WebSocket schemas are documented from one source of truth.
- No client-specific business rules exist in gateway endpoints.
- Mobile and web use identical symbol, error, and entitlement models.

## API-5 — OpenAPI and AsyncAPI documentation

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** API-1, API-3

**Acceptance criteria**

- REST endpoints appear in Swagger/OpenAPI.
- WebSocket commands and events are described in AsyncAPI or equivalent documentation.
- Example requests and responses are executable or validated in CI.

## API-6 — API versioning and compatibility policy

**Priority:** P1  
**Estimate:** 2 SP  
**Dependencies:** API-5

**Acceptance criteria**

- Breaking and non-breaking changes are defined.
- Deprecation headers/messages are documented.
- Schema compatibility tests run in CI.

---

# Epic 6 — Client tiers, QoS, and monetization

## QOS-1 — Tier policy model

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** ARCH-3

Initial hypothesis:

| Tier | Quote frequency | Max symbols | Market depth | Historical data |
|---|---:|---:|---:|---|
| Basic | 1 update/s | 20 | top of book | no |
| Pro | up to 5 updates/s | 100 | limited | limited |
| Premium | up to 10 updates/s | 500 | extended | yes |

**Acceptance criteria**

- Policies are configuration-driven.
- Trading confirmations, order statuses, risk events, and critical alerts are excluded from quote throttling.
- A policy change can be rolled out without client code changes.

## QOS-2 — Client Profile API integration

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** SEC-1, QOS-1

**Acceptance criteria**

- Resolves client ID, subscription tier, and optional portfolio segment.
- Applies timeout, retry, and fallback rules.
- Does not expose portfolio values to market-data clients.
- Caches short-lived entitlement decisions safely.

## QOS-3 — Adaptive quote frequency

**Priority:** P0  
**Estimate:** 8 SP  
**Dependencies:** QOS-1, SUB-2

**Acceptance criteria**

- Basic, Pro, and Premium clients receive configured maximum update rates.
- Latest-value semantics prevent queues of stale quotes.
- Same upstream event can feed multiple QoS schedules.
- Metrics show input events versus delivered events per tier.

## QOS-4 — Symbol and connection limits

**Priority:** P1  
**Estimate:** 3 SP  
**Dependencies:** QOS-1, API-3

**Acceptance criteria**

- Enforces max symbols, connections, and subscription operations.
- Returns machine-readable limit errors.
- Prevents rapid subscription churn abuse.

## QOS-5 — REST and WebSocket rate limiting

**Priority:** P1  
**Estimate:** 5 SP  
**Dependencies:** SEC-1, QOS-1

**Acceptance criteria**

- Limits are tier-specific.
- Supports burst and sustained rates.
- Limit state works across multiple application nodes.

## QOS-6 — Billable usage records

**Priority:** P2  
**Estimate:** 8 SP  
**Dependencies:** QOS-3, OBS-2

**Acceptance criteria**

- Records billable requests, connections, symbols, and premium channels.
- Usage records are idempotent and auditable.
- Does not put billing writes on the critical quote delivery path.

---

# Epic 7 — Reliability and slow-consumer protection

## REL-1 — Per-client bounded delivery queue

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** API-3

**Acceptance criteria**

- Every connection has a bounded queue.
- One slow client cannot increase memory indefinitely.
- Queue size and drops are observable.

## REL-2 — Latest-value backpressure policy

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** REL-1

**Acceptance criteria**

- For quote channels, a newer event replaces an unsent older event for the same symbol.
- Critical transactional events are never replaced by quote policy.
- Client receives a degradation indicator if updates are coalesced heavily.

## REL-3 — Heartbeat and stale connection cleanup

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** API-3

**Acceptance criteria**

- Server sends ping or protocol heartbeat.
- Dead connections close and their subscriptions are removed.
- Timeouts are configurable.

## REL-4 — Graceful shutdown

**Priority:** P1  
**Estimate:** 3 SP  
**Dependencies:** API-3, REDIS-3

**Acceptance criteria**

- Stops accepting new connections.
- Finishes or safely reassigns in-flight work.
- Closes provider and Redis connections cleanly.

## REL-5 — Idempotent event processing

**Priority:** P1  
**Estimate:** 5 SP  
**Dependencies:** SUB-5, REDIS-3

**Acceptance criteria**

- Replayed events do not create duplicated downstream effects.
- Processing key and retention policy are documented.
- Recovery scenario is covered by integration tests.

---

# Epic 8 — Security and provider licensing

## SEC-1 — JWT authentication

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** API-3

**Acceptance criteria**

- Validates issuer, audience, signature, and expiry.
- Rejects invalid tokens before subscription creation.
- Authentication failures do not leak sensitive details.

## SEC-2 — Tier and channel authorization

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** SEC-1, QOS-2

**Acceptance criteria**

- Client can access only entitled symbols/channels/features.
- Authorization applies consistently to REST and WebSocket.
- Permission changes are reflected within a defined time window.

## SEC-3 — Subscription abuse protection

**Priority:** P1  
**Estimate:** 5 SP  
**Dependencies:** QOS-4

**Acceptance criteria**

- Detects excessive subscribe/unsubscribe operations.
- Limits symbol enumeration and malformed payloads.
- Emits security metrics without logging tokens.

## SEC-4 — Provider licensing checklist

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** none

**Acceptance criteria**

Document whether the selected provider permits:

- caching;
- redistribution to end users;
- use in commercial subscriptions;
- derived-data products;
- display versus non-display use;
- historical storage;
- public benchmarks using provider data.

No commercial redistribution is enabled until these constraints are approved.

## SEC-5 — Secrets management

**Priority:** P0  
**Estimate:** 2 SP  
**Dependencies:** none

**Acceptance criteria**

- `.env.example` contains placeholders only.
- Secret files and credentials are ignored.
- CI includes secret scanning.
- Logs redact credentials and authorization headers.

---

# Epic 9 — Observability

## OBS-1 — Structured logging

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** none

**Acceptance criteria**

- JSON logs contain timestamp, service, severity, event, and correlation ID.
- Sensitive values are redacted.
- Logs distinguish client, provider, Redis, and policy errors.

## OBS-2 — Prometheus metrics

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** SUB-2, API-3

Required metrics:

- active WebSocket connections;
- client subscriptions;
- unique upstream subscriptions;
- subscription aggregation ratio;
- provider events per second;
- delivered events per second by tier;
- deduplicated events;
- queue depth and dropped/coalesced quote events;
- delivery latency;
- provider reconnects;
- Redis pending entries.

## OBS-3 — Grafana dashboard

**Priority:** P1  
**Estimate:** 5 SP  
**Dependencies:** OBS-2

**Acceptance criteria**

- Shows traffic reduction and aggregation ratio.
- Shows latency, errors, connections, and consumer lag.
- Includes Basic/Pro/Premium comparisons.

## OBS-4 — End-to-end correlation ID

**Priority:** P1  
**Estimate:** 3 SP  
**Dependencies:** OBS-1

**Acceptance criteria**

- Correlation ID links provider ingest, Redis processing, and client delivery.
- IDs are searchable in logs.
- Missing incoming IDs are generated safely.

## OBS-5 — Alerts

**Priority:** P1  
**Estimate:** 5 SP  
**Dependencies:** OBS-2

Alerts for:

- provider disconnected;
- stale quotes;
- high delivery latency;
- abnormal queue drops;
- Redis consumer lag;
- elevated authentication failures.

---

# Epic 10 — Testing, CI, and efficiency benchmark

## QA-1 — Unit test foundation

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** none

**Acceptance criteria**

- Covers event normalization, QoS decisions, subscription counting, and deduplication.
- Tests are deterministic.
- Coverage threshold is enforced in CI.

## QA-2 — Integration tests with Redis

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** REDIS-3

**Acceptance criteria**

- Runs against an isolated Redis container.
- Covers publish, consume, retry, cache, and reconnect cases.
- Cleans state between tests.

## QA-3 — WebSocket contract tests

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** API-3

**Acceptance criteria**

- Tests authentication, snapshot, subscribe, unsubscribe, limits, heartbeat, and errors.
- Validates schemas for all server events.

## PERF-1 — Baseline without smart aggregation

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** API-3, PROVIDER-2

**Acceptance criteria**

- Simulates independent per-client quote delivery.
- Captures CPU, memory, bandwidth, latency, and event counts.
- Test parameters are reproducible.

## PERF-2 — Smart-subscription benchmark

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** SUB-2, QOS-3, PERF-1

**Acceptance criteria**

- Runs the same workload with aggregation and QoS enabled.
- Compares provider subscriptions, processed events, delivered events, resource usage, and latency.
- Publishes percentage savings without presenting estimates as production facts.

## PERF-3 — 10,000-client load scenario

**Priority:** P1  
**Estimate:** 8 SP  
**Dependencies:** PERF-2

Suggested workload:

- 10,000 concurrent WebSocket clients;
- 20 requested symbols per client;
- overlapping symbol distribution;
- 70% Basic, 25% Pro, 5% Premium;
- normal and market-spike event profiles;
- healthy and slow consumers.

## PERF-4 — Node and cost-efficiency report

**Priority:** P1  
**Estimate:** 5 SP  
**Dependencies:** PERF-2, OBS-2

**Acceptance criteria**

- Estimates required node count from measured resource saturation.
- Separates measured values from extrapolations.
- Documents assumptions, confidence, and limitations.

## INFRA-1 — Docker Compose development stack

**Priority:** P0  
**Estimate:** 3 SP  
**Dependencies:** REDIS-1

**Acceptance criteria**

- Starts API, collector, Redis, PostgreSQL, and observability dependencies.
- Provides health checks and sample configuration.
- `docker compose up` produces a working local demo.

## INFRA-2 — GitHub Actions CI

**Priority:** P0  
**Estimate:** 5 SP  
**Dependencies:** QA-1

**Acceptance criteria**

- Runs linting, type checking, unit tests, and integration tests.
- Runs dependency and secret scanning.
- Fails on schema or contract incompatibility.

---

# MVP scope

The MVP includes:

1. Mock provider and provider adapter contract.
2. Normalized quote events.
3. Redis latest-value cache and Streams backbone.
4. Active subscription registry and aggregation.
5. Unified REST and WebSocket API.
6. Basic, Pro, and Premium frequency policies.
7. JWT authentication and entitlement resolution.
8. Slow-consumer protection with latest-value semantics.
9. Metrics and structured logs.
10. Reproducible baseline versus smart-subscription benchmark.
11. Docker Compose and CI.
12. Provider licensing checklist.

## Explicitly out of MVP

- Real-money trading or order placement.
- Exchange-grade order-book reconstruction.
- Multi-region active-active deployment.
- Automated billing and payments.
- Guaranteed production SLA.
- Commercial redistribution before licensing approval.
- Machine-learning-based QoS decisions.

---

# Post-MVP roadmap

## P1 — Production beta

- Real provider adapter.
- Consumer groups, retry, and dead-letter handling.
- Distributed rate limits.
- Grafana dashboards and alerts.
- Gap detection and snapshot recovery.
- 10,000-client load test.
- Kubernetes manifests and horizontal autoscaling.
- Multiple gateway instances with connection-aware routing.

## P2 — Commercial platform

- Billable usage records and billing integration.
- Historical market-data API.
- Extended order-book tiers.
- API keys and organization accounts.
- Per-tenant policies and white-label endpoints.
- Multi-provider failover and provider selection.
- Usage analytics and cost allocation.
- Customer portal for keys, limits, usage, and invoices.

---

# Definition of Done for MVP

The MVP is complete when:

- A mock market-data source emits normalized quote events.
- One symbol requested by many users creates one logical upstream subscription.
- REST returns the latest cached quote.
- WebSocket clients can subscribe and unsubscribe without reconnecting.
- Service tiers receive different quote frequencies according to configuration.
- Critical transactional-event design is isolated from quote throttling.
- Slow clients do not block or exhaust resources for healthy clients.
- Duplicate events are not processed twice.
- Provider reconnect restores active subscriptions.
- Metrics expose aggregation ratio, delivered event reduction, latency, and queue pressure.
- Baseline and optimized load tests are reproducible.
- Local environment starts with `docker compose up`.
- CI passes linting, type checking, unit, integration, contract, secret, and dependency checks.
- Architecture, API contracts, assumptions, and licensing constraints are documented.

---

# Suggested first sprint

## Sprint goal

Demonstrate one end-to-end quote flow and prove that multiple clients can share one logical subscription.

## Sprint backlog

- ARCH-1 — C4 System Context.
- ARCH-2 — C4 Container diagram.
- PROVIDER-1 — Provider adapter interface.
- PROVIDER-2 — Mock provider.
- PROVIDER-5 — Normalized event schema.
- REDIS-1 — Local Redis environment.
- REDIS-2 — Latest quote cache.
- SUB-1 — Active subscription registry.
- SUB-2 — Aggregate identical subscriptions.
- API-1 — Latest quote endpoint.
- API-3 — Minimal WebSocket stream.
- OBS-1 — Structured logging.
- QA-1 — Unit test foundation.
- INFRA-1 — Docker Compose development stack.

## Sprint demo

1. Start the stack with Docker Compose.
2. Connect 100 simulated clients to `AAPL`.
3. Show one logical provider subscription.
4. Return the current `AAPL` snapshot through REST.
5. Stream updates through WebSocket.
6. Display active clients, unique upstream subscriptions, and aggregation ratio.
