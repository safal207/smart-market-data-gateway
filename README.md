# Smart Market Data Gateway

Adaptive market-data gateway that reduces duplicated work before adding infrastructure. It aggregates identical subscriptions, applies tier-based Quality of Service, protects healthy clients from slow consumers, and exposes one REST/WebSocket contract for web, mobile, and external API clients.

## Architecture

```text
Web / Mobile / API clients
          │ REST + WebSocket
          ▼
 FastAPI Gateway instances
  ├─ JWT + entitlements + distributed rate limits
  ├─ Redis-backed subscription registry
  ├─ bounded latest-value client buffers
  └─ Prometheus metrics + JSON logs
          │ global first/last subscription transitions
          ▼
 Redis control stream ──► Collector ──► Market-data provider
                                  │ normalized QuoteEvent v1
                                  ▼
                         Redis quote stream
                                  │ consumer group
                                  ▼
                       validation + dedupe + gaps
                         latest cache + Pub/Sub
                                  │
                                  └────────► every gateway instance
```

Redis Streams provide durable, acknowledged processing. Redis Pub/Sub broadcasts processed quotes to every gateway instance that owns local WebSocket sessions. Reconnecting clients receive the latest cached snapshot before live updates.

## Implemented capabilities

- vendor-neutral provider adapter and deterministic mock provider;
- provider reconnect with capped exponential backoff and jitter;
- Redis Streams, consumer groups, bounded retries, dead-letter stream, and deduplication;
- sequence gap and out-of-order detection;
- distributed active-subscription registry with TTL and unsubscribe grace period;
- one global upstream transition for identical client subscriptions;
- REST endpoints for one or many latest quotes, including stale-data metadata;
- authenticated WebSocket subscribe/unsubscribe without reconnecting;
- Basic, Pro, and Premium symbol limits and update frequencies;
- optional Client Profile / Entitlements API integration with cache and fallback;
- distributed REST and subscription-operation limits;
- bounded latest-value backpressure for slow quote consumers;
- structured JSON logs with correlation IDs and sensitive-value redaction;
- Prometheus metrics, Grafana dashboard, and alert rules;
- idempotent auditable usage stream for future billing integration;
- deterministic before/after routing benchmark and deployed WebSocket load runner;
- OpenAPI, AsyncAPI, C4 diagrams, ADRs, provider licensing gate, tests, and CI.

Critical transactional events such as order statuses, trade confirmations, risk events, and critical alerts are deliberately excluded from quote-throttling and replacement semantics.

## Run the complete local stack

```bash
cp .env.example .env
docker compose up --build
```

Services:

| Service | Address |
|---|---|
| REST/OpenAPI | `http://localhost:8000/docs` |
| WebSocket | `ws://localhost:8000/v1/stream` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` (`admin` / `admin`) |
| Redis | `localhost:6379` |
| PostgreSQL | `localhost:5432` |

Development tokens are enabled only for the local configuration:

- `dev-basic:alice`
- `dev-pro:bob`
- `dev-premium:carol`

REST example:

```bash
curl -H 'Authorization: Bearer dev-basic:alice' \
  http://localhost:8000/v1/quotes/AAPL

curl -H 'Authorization: Bearer dev-pro:bob' \
  'http://localhost:8000/v1/quotes?symbols=AAPL,TSLA,NVDA'
```

WebSocket command:

```json
{
  "action": "subscribe",
  "symbols": ["AAPL", "TSLA"],
  "channels": ["quote"],
  "request_id": "demo-1"
}
```

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

ruff check .
ruff format --check .
mypy
pytest
```

Integration tests use Redis database 15 by default. Override it with `TEST_REDIS_URL`.

## Benchmarks

Deterministic baseline versus smart-routing comparison:

```bash
smdg-benchmark \
  --clients 10000 \
  --symbols-per-client 20 \
  --symbol-universe 500 \
  --events-per-symbol 100 \
  --output benchmark-results
```

Real WebSocket connections against a deployed stack:

```bash
python benchmarks/ws_load.py \
  --url ws://localhost:8000/v1/stream \
  --clients 100 \
  --symbols-per-client 5 \
  --messages 20
```

The CI smoke benchmark uploads raw JSON and Markdown artifacts. No simulated result is presented as a production fact. A real-provider benchmark will be performed only after selecting a provider, confirming its caching/redistribution/benchmark rights, and recording the exact environment and commit SHA.

## Documentation

- [Product backlog](docs/backlog.md)
- [Implementation plan](docs/implementation-plan.md)
- [C4 System Context](docs/architecture/context.md)
- [C4 Container Diagram](docs/architecture/container.md)
- [End-to-end quote flow](docs/architecture/quote-flow.md)
- [Architecture decisions](docs/architecture/adr/)
- [WebSocket AsyncAPI](docs/asyncapi.yaml)
- [Benchmark methodology](docs/benchmark-methodology.md)
- [Provider licensing checklist](docs/provider-licensing-checklist.md)

## Licensing warning

Commercial redistribution, caching, non-display use, historical storage, and public benchmark publication depend on provider and exchange terms. The real-provider adapter remains a separate gated step; no commercial redistribution is enabled by this repository alone.
