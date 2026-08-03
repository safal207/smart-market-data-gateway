# LiminalDB Integrity Alignment

## Purpose

This document describes the integrity boundary implemented by the Smart Market
Data Gateway and the specific ideas it reuses from the sibling LiminalDB
project. It is an alignment document, not a claim that the Python gateway has
LiminalDB's Rust WAL, snapshot, signed-checkpoint, or anti-rollback guarantees.

## Accepted-event boundary

Only an `AcceptedQuoteEvent` may enter durable market history. Acceptance occurs
after validation, deduplication, freshness checks, temporal ordering checks, and
quality scoring. Rejected, stale, and out-of-order events are audited separately
and do not update the latest cache or the accepted-event history.

The accepted event records:

- the immutable normalized quote;
- the acceptance timestamp and data cutoff;
- source provider and normalization version;
- quality score and gap metadata;
- the originating Redis Stream ID when one exists.

## Canonical payload digest

Every accepted-event payload is serialized as sorted canonical JSON and hashed
with SHA-256. The stored digest uses the profile-prefixed form:

```text
sha256:<64 lowercase hexadecimal characters>
```

The profile is versioned. A profile change must create a new profile identifier;
it must not silently reinterpret existing records.

## Append-only integrity chain

Each accepted event receives an integrity record containing:

- chain name;
- monotonic chain sequence;
- canonicalization profile;
- event identity and provider timestamp;
- source stream ID;
- payload digest;
- previous record hash;
- current record hash.

The current record hash commits to the record metadata, payload digest, and
previous hash. The chain head is stored separately and updated in the same
PostgreSQL transaction as the quote row and integrity record.

The database rejects update and delete operations against integrity records.
This prevents accidental in-place mutation but does not prevent a privileged
database administrator from replacing the entire database.

## Transaction boundary

For a new accepted event, one PostgreSQL transaction performs:

1. insert the canonical quote row;
2. lock the accepted-event chain head;
3. append the next integrity record;
4. update the chain head;
5. update deterministic candle state and late-event audit rows.

The transaction commits before the Redis accepted-stream entry is acknowledged.
A crash before commit leaves no database effects. A crash after commit but
before acknowledgment causes redelivery; the quote primary key makes the
transactional database effects idempotent and the existing integrity record is
not appended twice.

## Crash evidence

Two stable failpoints are available for integration tests:

- `after_ledger_append_before_commit`;
- `after_db_commit_before_ack`.

The first proves rollback of quote, integrity, and chain-head state before
commit. The second proves that redelivery after a committed transaction does not
append a second integrity record or mutate the chain.

These are deterministic process-crash simulations. They do not certify sudden
power loss, filesystem barriers, storage-controller caches, or arbitrary cloud
infrastructure.

## Verification

`smdg-verify-history` verifies the complete accepted-event chain by default. It
checks:

- chain sequence continuity;
- previous-hash continuity;
- canonical payload digest;
- record hash;
- profile identity;
- quote-row presence for every integrity record;
- absence of quote rows outside the integrity chain;
- stored chain-head sequence and hash.

The chain scan, quote count, and chain-head read execute inside one read-only
`repeatable_read` transaction and use a server-side cursor. Verification
therefore sees one consistent database snapshot without loading the complete
chain into process memory.

Replay uses that same transaction and snapshot for verification and event
iteration. A normal replay cannot include an event that was committed after the
verified snapshot. The emergency `--skip-integrity-verification` option is
explicit and must not be the default operational path.

## Retention boundary

Canonical quote payloads and their integrity records must remain available
together. Deleting quote rows while retaining chain records makes verification
fail with a missing-payload error. Automated quote and candle retention is
therefore disabled and any attempt to enable it fails closed until the project
implements verifiable truncation or integrity-preserving external checkpoints.

A future retention design must define and prove:

- the last retained chain checkpoint;
- the canonical hash and sequence immediately before the retained window;
- how verifier and replay authenticate that checkpoint;
- how checkpoint storage resists rollback and database-wide rewriting;
- how backup, licensing, and audit requirements interact with deletion.

## Replay relationship

Replay deterministically re-emits the accepted-event envelopes that passed
integrity verification. Its default Redis output stream is isolated from the
live accepted-history input, so replay does not silently rebuild or overwrite
live candles. A future candle rebuild worker must consume an explicitly named
replay stream and use the same versioned candle rules.

## Differences from LiminalDB

LiminalDB owns its WAL, snapshots, signed checkpoints, local file-sync boundary,
and anti-rollback interfaces. This gateway relies on Redis Streams for delivery
and PostgreSQL/TimescaleDB transactions for historical persistence.

The current chain is therefore **tamper-evident**, not tamper-proof. A database
administrator able to rewrite every payload, every chain row, and the chain head
can construct a new internally consistent history.

## Proven and unproven boundaries

This repository now has executable evidence for deterministic hashing, chain
verification, two important process-crash windows, exactly-once transactional
PostgreSQL database effects under redelivery, one-snapshot verification and
replay, singleton candle-state ownership, and fail-closed replay by default.

Accepted-event publication itself remains at-least-once. If the processing claim
expires after `publish_accepted_event` succeeds but before the processed marker
is written, another consumer may publish a duplicate accepted envelope. The
history database deduplicates that envelope by event identity, but this PR does
not prove exactly-once Redis publication. Closing that window requires a
claim-token fence, lease renewal, or an atomic publication/deduplication
protocol.

It does not prove sudden-power-loss durability on arbitrary hardware, hostile
storage correctness, distributed consensus, exchange-grade timestamp precision,
predictive alpha, or causal truth.

## Next evidence steps

1. Sign periodic chain-head checkpoints with a key held outside PostgreSQL.
2. Anchor checkpoint hashes in a separate account or immutable object store.
3. Add real subprocess termination tests in addition to in-process failpoints.
4. Verify chain checkpoints before replay, feature generation, and model runs.
5. Design and prove verifiable truncation before enabling raw-data retention.
6. Fence accepted-event publication with the processing claim token.
