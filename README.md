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

## MVP

The MVP will include a mock market-data provider, normalized quote events, Redis-based delivery and caching, subscription aggregation, REST and WebSocket endpoints, tiered QoS, authentication, slow-consumer protection, observability, and a reproducible before/after load benchmark.

## Documentation

- [Product backlog and roadmap](docs/backlog.md)

## Status

Initial product design and backlog preparation.

## Important licensing note

Commercial redistribution, caching, and resale of market data depend on the selected provider's license and exchange rules. The project must validate these constraints before using a real data source commercially.
