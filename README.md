# Smart Market Data Gateway

Adaptive market-data gateway that reduces duplicated work before adding infrastructure. It aggregates identical subscriptions, applies tier-based Quality of Service, protects healthy clients from slow consumers, and exposes one REST/WebSocket contract for web, mobile, and external API clients.

The repository also contains the trusted-data foundation for a future **Temporal Market Intelligence Graph**: a real or mock provider adapter, accepted/rejected event boundaries, point-in-time quality metadata, durable PostgreSQL/TimescaleDB history, deterministic server-side candles, deterministic replay, and a tamper-evident accepted-event integrity chain. It does not claim predictive alpha or expose trading recommendations.

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
                          ├────────► PostgreSQL/TimescaleDB history
                          ├────────► append-only integrity chain
                          ├────────► finalized server candles
                          └────────► late-event audit

REST / WebSocket usage events
          │ bounded asynchronous queue
          ▼
 Redis usage stream ──► Usage writer ──► PostgreSQL audit records
```

Redis Streams provide durable, acknowledged processing. Redis Pub/Sub broadcasts processed quotes to gateway instances that own local WebSocket sessions. Reconnecting clients receive the latest cached snapshot before live updates.

History, candle construction, replay, future graph features, and future ML remain outside the quote-delivery hot path.

## Implemented capabilities

### Gateway and providers

- vendor-neutral provider contract, deterministic mock provider, and read-only Tradernet/Freedom adapter;
- Tradernet public/demo and authenticated SID modes, strict demo-fallback rejection, HTTP snapshot fallback, credential redaction, and an explicit API-key signing gate;
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
- Prometheus metrics, provider metrics, Grafana dashboard, and alert rules;
- asynchronous idempotent usage capture and durable PostgreSQL audit records;
- deterministic routing benchmark, deployed WebSocket load runner, provider integration checks, and resilience profiles;
- OpenAPI, AsyncAPI, public v1 compatibility manifest, C4 diagrams, ADRs, licensing gate, tests, and CI.

### Trusted temporal data

- separate accepted and rejected quote streams;
- immutable `AcceptedQuoteEvent` envelope with `accepted_at`, `data_cutoff`, and quality metadata;
- stale and out-of-order events excluded from latest cache and accepted history;
- gap events accepted with degraded quality and explicit audit metadata;
- PostgreSQL/TimescaleDB-compatible `quote_events`, `candles`, and `late_quote_events` tables;
- canonical SHA-256 digest for every accepted-event payload;
- append-only previous-hash integrity chain committed with each quote in one transaction;
- independent chain verifier that detects payload changes, gaps, broken links, and head mismatch;
- deterministic quote-derived candles for 1 second, 10 seconds, 1 minute, and 5 minutes;
- explicit `event_count` instead of invented exchange volume;
- watermark-based lateness policy that does not silently rewrite finalized candles;
- deterministic replay at real time, accelerated time, or maximum speed;
- PostgreSQL fallback when the TimescaleDB extension is unavailable;
- one active history writer owns candle state through a PostgreSQL advisory lock;
- raw-data retention fails closed until integrity-preserving checkpoints or verifiable truncation exist.

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
| Collector | selected provider + control/raw quote streams |
| Migration runner | ordered temporal schema migrations |
| History writer | accepted stream → history, integrity, candles, late-event audit |
| Usage writer | Redis usage stream → PostgreSQL |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` (`admin` / `admin`) |
| Redis | `localhost:6379` |
| TimescaleDB/PostgreSQL | `localhost:5432` |

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

The adapter replaces the complete quote watch list, normalizes decimal commas, derives deterministic event IDs, rejects SID sessions that silently fall back to demo mode, and preserves the literal `+` separator required by the snapshot endpoint. API-key mode remains disabled until the current HMAC canonical-string contract is verified from authoritative documentation.

Run the opt-in provider check:

```bash
python scripts/tradernet_integration.py \
  --symbols AAPL.US,MSFT.US \
  --events 20 \
  --timeout 30
```

The generated report excludes SID, user ID, API key, API secret, and raw credential-bearing errors.

## Replay

Replay accepted events to JSONL:

```bash
python -m smart_market_data_gateway.replay \
  --from 2026-08-01T12:00:00Z \
  --to 2026-08-01T13:00:00Z \
  --symbol AAPL \
  --speed 10x \
  --output replay.jsonl
```

Supported speed values include `1`, `10`, `1x`, `10x`, and `max`.

Replay into a dedicated Redis stream:

```bash
python -m smart_market_data_gateway.replay \
  --from 2026-08-01T12:00:00Z \
  --speed max \
  --output-stream smdg:accepted-quotes:replay:v1
```

Use a dedicated replay stream unless duplicate live downstream processing is intentional.

## History integrity

Verify accepted-event payload digests, sequence, previous-hash links, and the persisted chain head before replay or feature generation:

```bash
python -m smart_market_data_gateway.integrity
```

Any changed payload, missing record, sequence gap, broken link, profile mismatch, or incorrect head exits non-zero. This mechanism is tamper-evident; externally signed checkpoints are still required to resist an administrator able to rewrite the complete database chain.

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

Integration tests use Redis database 15 and PostgreSQL/TimescaleDB when `TEST_REDIS_URL` and `TEST_DATABASE_URL` are set. Tradernet network tests are opt-in and require explicit environment configuration.

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

Provider profile example:

```bash
python benchmarks/run_tradernet_profile.py \
  --profile smoke \
  --symbols AAPL.US,MSFT.US,NVDA.US,TSLA.US,AMZN.US \
  --commit-sha "$(git rev-parse HEAD)" \
  --market-session open
```

`legacy-673` and SID-backed report generation require explicit `--licensing-approved`. Synthetic, deployed-gateway, and provider-backed measurements are reported separately. No simulated measurement is presented as a production fact.

## Documentation

- [Temporal Market Intelligence Graph](docs/TEMPORAL_MARKET_INTELLIGENCE_GRAPH.md)
- [Point-in-time consistency](docs/POINT_IN_TIME_CONSISTENCY.md)
- [LiminalDB integrity alignment](docs/LIMINALDB_INTEGRITY_ALIGNMENT.md)
- [Prediction outcome ledger](docs/PREDICTION_LEDGER.md)
- [Evaluation protocol](docs/EVALUATION_PROTOCOL.md)
- [Explainable Signal API](docs/SIGNAL_API.md)
- [Tradernet/Freedom adapter](docs/providers/tradernet.md)
- [Provider implementation status](docs/provider-implementation-status.md)
- [Provider licensing checklist](docs/provider-licensing-checklist.md)
- [Product backlog](docs/backlog.md)
- [Implementation plan](docs/implementation-plan.md)
- [C4 System Context](docs/architecture/context.md)
- [C4 Container Diagram](docs/architecture/container.md)
- [End-to-end quote flow](docs/architecture/quote-flow.md)
- [Architecture decisions](docs/architecture/adr/)
- [WebSocket AsyncAPI](docs/asyncapi.yaml)
- [Public API compatibility manifest](contracts/public-api-v1.json)
- [Benchmark methodology](docs/benchmark-methodology.md)

## Licensing warning

A technically working provider adapter does not grant permission to cache, redistribute, sell, retain historically, or publish market data. Commercial redistribution, non-display use, historical storage, and public provider-backed benchmark publication remain blocked until the applicable provider and exchange terms are reviewed and approved.
