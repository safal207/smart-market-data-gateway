# LiminalDB integrity alignment

This gateway reuses selected **engineering invariants** from the sibling
[`safal207/LiminalDB`](https://github.com/safal207/LiminalDB) repository while
keeping both systems operationally independent.

No Rust source is vendored, no branches are merged, and LiminalDB is not a
runtime dependency. LiminalDB is Apache-2.0; this repository remains MIT. The
alignment is architectural, protocol-level, and independently implemented.

LiminalDB records and replays evidence and continuity. It does **not** create a
scientific or market verdict. The gateway remains responsible for market-data
normalization, temporal guards, accepted history, candles, and replay.

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
PostgreSQL transaction. Integrity rows reject updates and deletes.

### 4. Persistence precedes acknowledgement

The history worker acknowledges the Redis Stream message only after the
PostgreSQL transaction commits. A committed event may therefore be delivered
again after a process crash, but the quote primary key prevents a second
history row and a second integrity record.

### 5. Replay verification is independent from replay execution

`smdg-replay` reproduces accepted-event order. `smdg-verify-history` checks that
stored payloads still match their digests, all links are continuous, sequence
numbers have no gaps, the persisted head matches the verified chain, and every
quote-history row has a corresponding integrity record.

Replay verifies integrity by default. The explicit
`--skip-integrity-verification` flag exists for controlled forensic recovery,
not routine data processing. Feature generation and model training should use
the same fail-closed default.

### 6. Fail closed on ambiguous history

The verifier raises a hard error for missing payloads, sequence gaps, changed
payloads, profile changes, broken previous-hash links, a mismatched chain head,
or incomplete quote-history coverage. It does not silently repair evidence.

## History transaction

```text
accepted Redis event
  → begin PostgreSQL transaction
  → insert quote event (idempotent)
  → lock integrity head
  → append payload digest + previous hash + record hash
  → update integrity head
  → persist finalized candles / late-event audit
  → commit
  → Redis XACK
```

Graph calculation, feature generation, prediction, and ML remain outside this
transaction and outside the quote-delivery hot path.

## Executable crash matrix

The failpoints are disabled by default. They are enabled only for chaos and CI
validation through `SMDG_HISTORY_FAILPOINT`.

### `after_ledger_append_before_commit`

Expected and tested result:

- quote event rolls back;
- integrity record rolls back;
- chain head remains unchanged;
- Redis event remains unacknowledged;
- a clean worker can persist the event exactly once.

### `after_db_commit_before_ack`

Expected and tested result:

- quote event and integrity record remain durable;
- Redis event remains unacknowledged;
- redelivery detects the existing quote event;
- no second chain record is created;
- the message can then be acknowledged safely.

The integration suite also changes a stored quote payload deliberately and
requires `smdg-verify-history` to reject the history.

## Differences from LiminalDB

LiminalDB owns its WAL, snapshots, signed checkpoints, local file-sync boundary,
and anti-rollback interfaces. This gateway relies on Redis Streams for delivery
and PostgreSQL/TimescaleDB transactions for historical persistence.

The current chain is therefore **tamper-evident**, not tamper-proof. A database
administrator able to rewrite every payload, every chain row, and the chain head
can construct a new internally consistent history.

## Proven and unproven boundaries

This repository now has executable evidence for deterministic hashing, chain
verification, two important process-crash windows, exactly-once database
effects under redelivery, and fail-closed replay by default.

It does not prove sudden-power-loss durability on arbitrary hardware, hostile
storage correctness, distributed consensus, exchange-grade timestamp precision,
predictive alpha, or causal truth.

## Next evidence steps

1. Sign periodic chain-head checkpoints with a key held outside PostgreSQL.
2. Anchor checkpoint hashes in a separate account or immutable object store.
3. Add real subprocess termination tests in addition to in-process failpoints.
4. Verify chain checkpoints before replay, feature generation, and model runs.
5. Preserve integrity records longer than raw market-data retention windows.
