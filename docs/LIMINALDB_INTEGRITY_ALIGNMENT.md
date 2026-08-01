# LiminalDB integrity alignment

This gateway reuses selected **design principles** from the sibling
[`safal207/LiminalDB`](https://github.com/safal207/LiminalDB) repository while
keeping the market-data service operationally independent.

No Rust source is vendored into this repository. LiminalDB is Apache-2.0; this
repository remains MIT. The alignment is architectural and protocol-level.

## Adopted principles

### 1. Durable fact and derived projection are different things

The accepted quote is the durable fact. Candles, future graph features, and
predictions are rebuildable projections. A projection must never become the
only surviving evidence of its input.

### 2. Every durable record has a canonical payload digest

`AcceptedQuoteEvent` is serialized into canonical sorted JSON and hashed with
SHA-256. The digest is stored separately from the JSONB payload.

### 3. Accepted records form an append-only hash chain

Each integrity record commits to:

- chain profile and chain name;
- monotonically increasing chain sequence;
- event identity and provider timestamp;
- source Redis Stream ID;
- canonical payload digest;
- previous record hash.

The quote row, integrity record, and chain-head update are committed in one
PostgreSQL transaction. A failure before commit rolls back all three.

### 4. Replay verification is independent from replay execution

`smdg-replay` reproduces accepted-event order. `smdg-verify-history` checks that
stored payloads still match their digests, all links are continuous, sequence
numbers have no gaps, and the persisted head matches the verified chain.

Replay should be blocked operationally when integrity verification fails.

### 5. Fail closed on ambiguous history

The verifier raises a hard error for missing payloads, sequence gaps, changed
payloads, profile changes, broken previous-hash links, or a mismatched chain
head. It does not silently repair evidence.

## Differences from LiminalDB

LiminalDB owns its WAL, snapshots, signed checkpoints, and local file-sync
boundary. This gateway relies on Redis Streams for accepted-event delivery and
PostgreSQL/TimescaleDB transactions for historical persistence.

The current chain is therefore **tamper-evident**, not tamper-proof. A database
administrator able to rewrite every payload, every chain row, and the chain head
can construct a new internally consistent history.

## Next evidence steps

1. Sign periodic chain-head checkpoints with a key held outside PostgreSQL.
2. Anchor checkpoint hashes in a separate account or immutable object store.
3. Add process-kill tests around event insert, integrity append, and commit.
4. Verify chain checkpoints before replay, feature generation, and model runs.
5. Preserve integrity records longer than raw market-data retention windows.

These steps follow the same evidence-first direction as LiminalDB without
claiming its full WAL, snapshot, signed-checkpoint, or anti-rollback guarantees.
