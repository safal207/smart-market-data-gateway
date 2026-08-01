# End-to-end quote flow

```mermaid
sequenceDiagram
    participant Client
    participant API as WebSocket Gateway
    participant Registry as Subscription Registry
    participant Redis
    participant Collector
    participant Provider
    participant Processor

    Client->>API: subscribe(AAPL)
    API->>Registry: add connection + AAPL with TTL
    Registry->>Redis: atomic first-subscriber transition
    alt first global subscriber
        Registry->>Redis: XADD control subscribe AAPL
        Collector->>Redis: consume + ACK control event
        Collector->>Provider: subscribe AAPL
    end

    Provider-->>Collector: provider-specific quote
    Collector->>Collector: normalize to QuoteEvent v1
    Collector->>Redis: XADD quote stream
    Processor->>Redis: XREADGROUP quote
    Processor->>Redis: atomic dedupe + temporal watermark
    alt accepted or sequence gap
        Processor->>Redis: SET latest + PUBLISH fanout
    else out-of-order or timestamp regression
        Processor->>Redis: XADD quarantine + degraded marker
    end
    Processor->>Redis: XACK quote event
    Redis-->>API: Pub/Sub normalized quote
    API->>API: entitlement + QoS + latest-value buffer
    API-->>Client: quote event
```

## Failure paths

- **Provider disconnect:** collector reconnects with capped exponential backoff and jitter, then restores the active symbol set.
- **Duplicate event:** Redis dedupe rejects the repeated `event_id`; the stream entry is acknowledged and counted.
- **Sequence gap:** the event remains deliverable, but an anomaly metric and degraded marker are emitted so a provider adapter can request a snapshot.
- **Out-of-order or timestamp regression:** the event is atomically quarantined and cannot overwrite the latest quote or reach chart clients.
- **Malformed or poison event:** bounded retries are recorded; the payload moves to the dead-letter stream after the configured limit.
- **Processor restart:** unacknowledged stream entries remain pending and are reclaimed after the configured idle window. The atomic Redis decision prevents a retry from losing or double-publishing an already committed effect.
- **Gateway restart:** clients reconnect, authenticate, restore subscriptions, and receive the latest cached snapshot before live events.
- **Slow client:** newer unsent quotes replace older values for the same symbol. Healthy clients remain unaffected.
- **Last subscriber:** unsubscribe is delayed by a grace period to prevent churn; a new subscriber cancels release.
