# Smart Market Data Gateway

Adaptive market-data gateway that reduces duplicated work before adding infrastructure. It aggregates identical subscriptions, applies tier-based Quality of Service, protects healthy clients from slow consumers, and exposes one REST/WebSocket contract for web, mobile, and external API clients.

## Architecture

```text
Web / Mobile / API clients
          │ REST + WebSocket
          ▼
 FastAPI Gateway instances
  ├─ JWT/JWKS + entitlements
  ├─ distributed token buckets and connection limits
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

REST / WebSocket usage events
          │ bounded asynchronous queue
          ▼
 Redis usage stream ──► Usage writer ──► PostgreSQL audit records
```

Redis Streams provide durable, acknowledged processing. Redis Pub/Sub broadcasts processed quotes to every gateway instance that owns local WebSocket sessions. Reconnecting clients receive the latest cached snapshot before live updates.

## Implemented capabilities

- vendor-neutral provider adapter and deterministic mock provider;
- provider reconnect with capped exponential backoff, jitter, restored symbols, outage metrics, and alerts;
- Redis Streams, consumer groups, stale-entry reclaim, bounded retries, dead-letter stream, and deduplication;
- sequence gap and out-of-order detection with degraded-stream metadata;
- distributed active-subscription registry with TTL and unsubscribe grace period;
- one global upstream transition for identical client subscriptions;
- REST endpoints for one or many latest quotes, including stale-data metadata and partial results;
- authenticated WebSocket subscribe/unsubscribe, snapshots, heartbeat, idle cleanup, and machine-readable errors;
- append-only WebSocket JSONL recording with SHA-256 evidence-ledger verification for deterministic TMI replay;
- backward-compatible `QuoteEvent` 1.1 evidence for volume, aggressor flow, trade count, and top-of-book depth with explicit capabilities and units;
- Basic, Pro, and Premium symbol, connection, operation, and update-frequency policies;
- optional Client Profile / Entitlements API integration with timeout, retry, cache, and fallback;
- distributed token-bucket burst and sustained limits;
- bounded latest-value backpressure and slow-consumer warnings;
- structured JSON logs with correlation IDs and sensitive-value redaction;
- Prometheus metrics, collector/provider metrics, Grafana dashboard, and alert rules;
- asynchronous idempotent usage capture and durable PostgreSQL audit records;
- deterministic before/after routing benchmark with payload-byte accounting;
- deployed WebSocket load runner with P50/P95/P99 latency, throughput, errors, and network bytes;
- OpenAPI, AsyncAPI, public v1 compatibility manifest, C4 diagrams, ADRs, licensing gate, tests, and CI.

Critical transactional events such as order statuses, trade confirmations, risk events, and critical alerts are deliberately excluded from quote-throttling and replacement semantics.

## Run the complete local stack

```bash
cp .env.example .env
docker compose up --build
```

Services:

| Service | Address / role |
|---|---|
| REST/OpenAPI | `http://localhost:8000/docs` |
| WebSocket | `ws://localhost:8000/v1/stream` |
| Collector | mock provider + control/quote streams |
| Usage writer | Redis usage stream → PostgreSQL |
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

WebSocket commands:

```json
{
  "action": "subscribe",
  "symbols": ["AAPL", "TSLA"],
  "channels": ["quote"],
  "request_id": "demo-1"
}
```

```json
{
  "action": "ping",
  "request_id": "heartbeat-1"
}
```

## Record a TMI replay session

```bash
export SMDG_RECORDER_TOKEN='dev-pro:bob'
python -m smart_market_data_gateway.recorder \
  --url ws://localhost:8000/v1/stream \
  --symbol AAPL \
  --symbol TSLA \
  --output recordings/mock-session.jsonl \
  --max-records 100
```

The recorder writes only validated normalized quote rows. It skips duplicates, marks sequence gaps, preserves completed rows across reconnects, rolls back failed partial appends, and never stores the bearer token. Every retained rich-evidence field is covered by the same canonical ledger hash. See [TMI WebSocket recorder](docs/tmi-recorder.md) and [Market evidence schema 1.1](docs/market-evidence-schema.md).

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

ruff check .
mypy
pytest
python scripts/check_contract_compatibility.py
```

Integration tests use Redis database 15 and PostgreSQL when `TEST_REDIS_URL` and `TEST_DATABASE_URL` are set.

## Benchmarks

Deterministic baseline versus smart-routing comparison:

```bash
smdg-benchmark \
  --clients 10000 \
  --symbols-per-client 20 \
  --symbol-universe 500 \
  --events-per-symbol 100 \
  --quote-payload-bytes 256 \
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

The CI smoke benchmark uploads raw JSON and Markdown artifacts. No simulated result is presented as a production fact. A real-provider benchmark will be performed only after selecting a provider, confirming its caching, redistribution, non-display, and benchmark rights, and recording the exact environment and commit SHA.

## Documentation

- [Product backlog](docs/backlog.md)
- [Implementation plan](docs/implementation-plan.md)
- [C4 System Context](docs/architecture/context.md)
- [C4 Container Diagram](docs/architecture/container.md)
- [End-to-end quote flow](docs/architecture/quote-flow.md)
- [Architecture decisions](docs/architecture/adr/)
- [WebSocket AsyncAPI](docs/asyncapi.yaml)
- [TMI WebSocket recorder](docs/tmi-recorder.md)
- [Market evidence schema 1.1](docs/market-evidence-schema.md)
- [Public API compatibility manifest](contracts/public-api-v1.json)
- [Benchmark methodology](docs/benchmark-methodology.md)
- [Provider licensing checklist](docs/provider-licensing-checklist.md)

## Licensing warning

Commercial redistribution, caching, non-display use, historical storage, and public benchmark publication depend on provider and exchange terms. The real-provider adapter remains a separate gated step; no commercial redistribution is enabled by this repository alone.
