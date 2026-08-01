# Smart Market Data Gateway

Adaptive market-data gateway that reduces duplicated work before adding infrastructure. It aggregates identical subscriptions, applies tier-based Quality of Service, protects healthy clients from slow consumers, and exposes one REST/WebSocket contract for web, mobile, and external API clients.

The repository now also contains the trusted-data foundation for a future **Temporal Market Intelligence Graph**: an accepted-event stream, point-in-time quality metadata, deterministic server-side candles, TimescaleDB history, and replay. It does not yet claim predictive alpha or expose trading recommendations.

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
                         Redis raw quote stream
                                  │ consumer group
                                  ▼
                       validation + dedupe + temporal gates
                          ├───────────────┐
                          ▼               ▼
                  accepted stream   rejected stream
                          │
                          ├────────► latest cache + Pub/Sub ──► clients
                          │
                          ▼
                    History worker
                          │
                          ├────────► TimescaleDB quote history
                          ├────────► finalized server candles
                          └────────► late-event audit

REST / WebSocket usage events
          │ bounded asynchronous queue
          ▼
 Redis usage stream ──► Usage writer ──► PostgreSQL audit records
```

Redis Streams provide durable, acknowledged processing. Redis Pub/Sub broadcasts processed quotes to gateway instances that own local WebSocket sessions. Reconnecting clients receive the latest cached snapshot before live updates.

History, candle construction, replay, future graph features, and future ML are outside the quote-delivery hot path.

## Implemented capabilities

### Gateway

- vendor-neutral provider adapter and deterministic mock provider;
- provider reconnect with capped exponential backoff, jitter, restored symbols, outage metrics, and alerts;
- Redis Streams, consumer groups, stale-entry reclaim, bounded retries, dead-letter stream, and deduplication;
- sequence gap and out-of-order detection with degraded-stream metadata;
- distributed active-subscription registry with TTL and unsubscribe grace period;
- one global upstream transition for identical client subscriptions;
- REST endpoints for one or many latest quotes, including stale-data metadata and partial results;
- authenticated WebSocket subscribe/unsubscribe, snapshots, heartbeat, idle cleanup, and machine-readable errors;
- Basic, Pro, and Premium symbol, connection, operation, and update-frequency policies;
- optional Client Profile / Entitlements API integration with timeout, retry, cache, and fallback;
- distributed token-bucket burst and sustained limits;
- bounded latest-value backpressure and slow-consumer warnings;
- structured JSON logs with correlation IDs and sensitive-value redaction;
- Prometheus metrics, Grafana dashboard, and alert rules;
- asynchronous idempotent usage capture and durable PostgreSQL audit records;
- deterministic before/after routing benchmark with payload-byte accounting;
- deployed WebSocket load runner with P50/P95/P99 latency, throughput, errors, and network bytes;
- OpenAPI, AsyncAPI, public v1 compatibility manifest, C4 diagrams, ADRs, tests, and CI.

### Temporal data foundation

- separate accepted and rejected quote streams;
- immutable `AcceptedQuoteEvent` envelope with `accepted_at`, `data_cutoff`, and quality metadata;
- stale and out-of-order events excluded from latest cache and accepted history;
- gap events accepted with degraded quality and explicit audit metadata;
- TimescaleDB-compatible `quote_events`, `candles`, and `late_quote_events` hypertables;
- deterministic quote-derived candles for 1 second, 10 seconds, 1 minute, and 5 minutes;
- explicit `event_count` instead of invented exchange volume;
- watermark-based lateness policy that does not silently rewrite finalized candles;
- deterministic replay at real time, accelerated time, or maximum speed;
- PostgreSQL fallback when the TimescaleDB extension is unavailable;
- retention policies disabled by default until licensing and audit requirements are agreed.

Critical transactional events such as order statuses, trade confirmations, risk events, and critical alerts are deliberately excluded from quote-throttling and replacement semantics.

## Run the local stack

```bash
cp .env.example .env
docker compose up --build
```

Services:

| Service | Address / role |
|---|---|
| REST/OpenAPI | `http://localhost:8000/docs` |
| WebSocket | `ws://localhost:8000/v1/stream` |
| Collector | mock provider + control/raw quote streams |
| History writer | accepted stream → TimescaleDB + candles |
| Usage writer | Redis usage stream → PostgreSQL |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` (`admin` / `admin`) |
| Redis | `localhost:6379` |
| TimescaleDB | `localhost:5432` |

Development tokens are enabled only for local configuration:

- `dev-basic:alice`
- `dev-pro:bob`
- `dev-premium:carol`

REST examples:

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

## Replay

Replay accepted events to JSONL:

```bash
smdg-replay \
  --from 2026-08-01T12:00:00Z \
  --to 2026-08-01T13:00:00Z \
  --symbol AAPL \
  --speed 10x \
  --output replay.jsonl
```

Supported speed values include `1`, `10`, `1x`, `10x`, and `max`.

Replay into a dedicated Redis stream:

```bash
smdg-replay \
  --from 2026-08-01T12:00:00Z \
  --speed max \
  --output-stream smdg:accepted-quotes:replay:v1
```

Use a dedicated replay stream unless duplicate live downstream processing is intentional.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

ruff check .
mypy
pytest
python scripts/check_contract_compatibility.py
docker compose config
```

Integration tests use Redis database 15 and PostgreSQL/TimescaleDB when `TEST_REDIS_URL` and `TEST_DATABASE_URL` are set.

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

The CI smoke benchmark uploads raw JSON and Markdown artifacts. No simulated result is presented as a production fact. A real-provider benchmark will be performed only after selecting a provider, confirming its caching, redistribution, non-display, historical-storage, and benchmark rights, and recording the exact environment and commit SHA.

## Documentation

- [Temporal Market Intelligence Graph](docs/TEMPORAL_MARKET_INTELLIGENCE_GRAPH.md)
- [Point-in-time consistency](docs/POINT_IN_TIME_CONSISTENCY.md)
- [Prediction outcome ledger](docs/PREDICTION_LEDGER.md)
- [Evaluation protocol](docs/EVALUATION_PROTOCOL.md)
- [Explainable Signal API](docs/SIGNAL_API.md)
- [Product backlog](docs/backlog.md)
- [Implementation plan](docs/implementation-plan.md)
- [C4 System Context](docs/architecture/context.md)
- [C4 Container Diagram](docs/architecture/container.md)
- [End-to-end quote flow](docs/architecture/quote-flow.md)
- [Architecture decisions](docs/architecture/adr/)
- [WebSocket AsyncAPI](docs/asyncapi.yaml)
- [Public API compatibility manifest](contracts/public-api-v1.json)
- [Benchmark methodology](docs/benchmark-methodology.md)
- [Provider licensing checklist](docs/provider-licensing-checklist.md)

## Licensing warning

Commercial redistribution, caching, non-display use, historical storage, and public benchmark publication depend on provider and exchange terms. The real-provider adapter remains a separate gated step; no commercial redistribution is enabled by this repository alone.
