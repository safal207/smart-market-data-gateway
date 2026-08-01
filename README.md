# Smart Market Data Gateway

Adaptive market-data gateway with smart subscription aggregation, tier-based QoS, Redis Streams, and a unified REST/WebSocket API for web and mobile clients.

## Core idea

A traditional scaling approach adds cache and more nodes. This project reduces the amount of duplicated work first:

- combines identical subscriptions into one logical upstream subscription;
- caches the latest quote for fast client bootstrap;
- applies different update frequencies and limits for Basic, Pro, and Premium tiers;
- protects the platform from slow consumers;
- exposes one shared API contract for web and mobile;
- measures event reduction, resource savings, and estimated node efficiency;
- creates a foundation for monetized premium market-data access.

## Target architecture

```text
Market-data provider
        ↓
     Collector
        ↓
   Redis Streams
        ↓
Smart Subscription Service ← Client Profile / Entitlements API
        ↓
Unified REST + WebSocket Gateway
        ↓
 Web / Mobile / External API clients
```

## Current foundation

The first implementation slice contains:

- a FastAPI application with liveness and Redis readiness endpoints;
- a versioned, provider-independent quote event model;
- a vendor-neutral `MarketDataProvider` interface;
- a deterministic mock provider with duplicate and failure simulation;
- Redis and API containers in Docker Compose;
- C4 System Context and Container documentation;
- unit tests and CI checks.

## Run locally

```bash
# Start Redis and the API
docker compose up --build

# Liveness
curl http://localhost:8000/health/live

# Redis-backed readiness
curl http://localhost:8000/health/ready

# OpenAPI
open http://localhost:8000/docs
```

For local Python development:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
ruff check .
```

## Documentation

- [Product backlog and roadmap](docs/backlog.md)
- [C4 System Context](docs/architecture/context.md)
- [C4 Container Diagram](docs/architecture/container.md)

## Status

Foundation implementation in progress. The next vertical slice connects the mock provider to Redis, adds the latest-quote cache, and exposes `GET /v1/quotes/{symbol}`.

## Important licensing note

Commercial redistribution, caching, and resale of market data depend on the selected provider's license and exchange rules. The project must validate these constraints before using a real data source commercially.
