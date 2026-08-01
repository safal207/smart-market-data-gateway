# ADR 0002 — At-least-once processing with idempotent effects

**Status:** Accepted

## Decision

Redis Streams are processed with explicit acknowledgement, so an event may be replayed after a crash. Every normalized event has an `event_id`; the processor stores an expiring Redis deduplication key before applying downstream effects. Events without a provider ID must receive a deterministic adapter-level fingerprint before entering the stream.

The latest quote cache is naturally idempotent. Usage records require a separate idempotency key. Duplicate events are counted and acknowledged without being published twice.

## Recovery

Pending entries remain visible in the consumer group. Failed events retry a bounded number of times and then move to the dead-letter stream with the original payload and failure reason.
