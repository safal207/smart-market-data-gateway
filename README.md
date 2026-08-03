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
 Redis control stream ──► Collector ──► Mock or Tradernet/Freedom provider
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

- vendor-neutral provider interface, deterministic mock provider, and read-only Tradernet/Freedom quote adapter;
- Tradernet public/demo and SID-session WebSocket modes, explicit API-key gate, and HTTP snapshot fallback;
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
- Prometheus metrics, collector/provider metrics, Grafana dashboard, and alert rules;
- asynchronous idempotent usage capture and durable PostgreSQL audit records;
- deterministic before/after routing benchmark with payload-byte accounting;
- deployed WebSocket load runner with P50/P95/P99 latency, throughput, errors, and network bytes;
- Tradernet staged profiles plus reconnect-storm, zombie-cleanup, and frozen-stream resilience runners;
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
| Collector | selected provider + control/quote streams |
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

## Tradernet / Freedom quote provider

Public/demo mode:

```env
SMDG_PROVIDER=tradernet
SMDG_TRADERNET_MODE=public_demo
SMDG_TRADERNET_WEBSOCKET_URL=wss://wss.tradernet.com/
SMDG_TRADERNET_INTEGRATION_SYMBOLS=AAPL.US,MSFT.US
```

Authenticated SID integration test:

```env
SMDG_PROVIDER=tradernet
SMDG_TRADERNET_MODE=sid_session
SMDG_TRADERNET_SID=<secret>
SMDG_TRADERNET_USER_ID=<optional-user-id>
```

A previously tested development endpoint can be supplied without code changes:

```env
SMDG_TRADERNET_WEBSOCKET_URL=wss://wssdev.tradernet.dev
```

The adapter sends complete quote-watch lists as `["quotes", [symbols...]]`, accepts `q` quote events, normalizes decimal commas, derives deterministic event IDs, rejects SID sessions that silently fall back to demo mode, and uses `/securities/export?tickers=AAPL.US+MSFT.US` as a snapshot fallback. The literal `+` separator is preserved deliberately.

API-key mode is not guessed. It remains disabled until the current Tradernet HMAC canonical-string and signing contract are verified from authoritative documentation.

Run the direct opt-in provider check:

```bash
python scripts/tradernet_integration.py \
  --symbols AAPL.US,MSFT.US \
  --events 20 \
  --timeout 30
```

The generated report excludes SID, user ID, API key, and secret values.

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

Integration tests use Redis database 15 and PostgreSQL when `TEST_REDIS_URL` and `TEST_DATABASE_URL` are set. Tradernet network tests are opt-in and require explicit environment configuration.

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

Direct WebSocket load against the gateway:

```bash
python benchmarks/ws_load.py \
  --url ws://localhost:8000/v1/stream \
  --clients 100 \
  --symbols-per-client 5 \
  --messages 20
```

Every staged Tradernet profile requires a fresh target-bound attestation. Example `provider-attestation.json`:

```json
{
  "provider": "tradernet",
  "data_mode": "demo",
  "gateway_url": "ws://localhost:8000/v1/stream",
  "deployment_commit_sha": "<deployed-sha>",
  "environment": "isolated-test",
  "issuer": "<responsible-operator>",
  "issued_at": "2026-08-04T00:00:00Z",
  "licensing_approved_for_publication": false
}
```

Run a staged profile:

```bash
python benchmarks/run_tradernet_profile.py \
  --profile smoke \
  --attestation provider-attestation.json \
  --symbols AAPL.US,MSFT.US,NVDA.US,TSLA.US,AMZN.US \
  --market-session open
```

Available profiles include `smoke`, `medium-100`, `medium-500`, `load-1000`, `load-10000`, and `legacy-673`. SID-backed and `legacy-673` reports require a fresh attestation with `licensing_approved_for_publication: true`; `legacy-673` also requires a `--symbols-file` containing at least 673 real provider symbols.

Resilience scenarios:

```bash
python benchmarks/resilience_load.py --scenario reconnect-storm --clients 100
python benchmarks/resilience_load.py --scenario zombie-cleanup --clients 100
python benchmarks/resilience_load.py --scenario frozen-stream --clients 100
```

The frozen-stream scenario discards backlog until a matching gateway pong on each ordered WebSocket, then requires a later quote on that same stream. It does not compare clocks across hosts.

The CI smoke benchmark uploads raw JSON and Markdown artifacts. Synthetic, deployed-gateway, and provider-backed measurements are reported separately. Provider-backed publication requires confirmed caching, redistribution, non-display, historical-storage, and benchmark rights plus target-bound provenance.

## Documentation

- [Product backlog](docs/backlog.md)
- [Implementation plan](docs/implementation-plan.md)
- [C4 System Context](docs/architecture/context.md)
- [C4 Container Diagram](docs/architecture/container.md)
- [End-to-end quote flow](docs/architecture/quote-flow.md)
- [Architecture decisions](docs/architecture/adr/)
- [WebSocket AsyncAPI](docs/asyncapi.yaml)
- [Public API compatibility manifest](contracts/public-api-v1.json)
- [Benchmark methodology](docs/benchmark-methodology.md)
- [Tradernet/Freedom adapter](docs/providers/tradernet.md)
- [Provider licensing checklist](docs/provider-licensing-checklist.md)

## Licensing warning

A technically working API does not grant permission to cache, redistribute, sell, or publish market data. Commercial redistribution, non-display use, historical storage, and public provider-backed benchmark publication remain blocked until the applicable Tradernet/Freedom and exchange terms are reviewed and approved.
