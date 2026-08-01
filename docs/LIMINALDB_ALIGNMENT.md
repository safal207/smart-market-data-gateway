# LiminalDB alignment

The Temporal Market Intelligence Graph foundation reuses **engineering invariants** proven in the separate [`safal207/LiminalDB`](https://github.com/safal207/LiminalDB) repository. It does not merge repositories, copy the Rust storage engine, or make LiminalDB a runtime dependency of the gateway.

## Boundary

LiminalDB remains an evidence and continuity database. It records and replays what was authorized, observed, reported, causally evaluated, and safe to continue. It does **not** create a scientific or market verdict.

The market gateway remains responsible for provider normalization, deduplication, temporal guards, accepted/rejected streams, quote delivery, history, and deterministic candles.

## Reused invariants

| LiminalDB invariant | Gateway adaptation |
|---|---|
| Append-only evidence records | `accepted_event_integrity` records are insert-only and reject row updates/deletes. |
| Canonical payload digest | Every accepted envelope is hashed from canonical sorted JSON. |
| Previous-record hash | Each integrity record commits to the prior record hash. |
| Explicit chain head | `integrity_chain_heads` is locked and advanced in the same database transaction. |
| Persist before acknowledgement | PostgreSQL transaction commits before the Redis consumer acknowledgement. |
| Replay must preserve evidence | `smdg-verify-history` recomputes payload and chain hashes from persisted history. |
| Crash boundaries must be executable | Stable failpoints cover pre-commit and post-commit/pre-ACK failures. |
| Ambiguous outcomes require replay, not guessing | Redelivery is idempotent through the quote-event primary key; committed events do not create a second integrity record. |

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

Graph calculation, feature generation, prediction, and ML remain outside this transaction and outside the gateway hot path.

## Executable crash matrix

### `after_ledger_append_before_commit`

Expected result:

- quote event rolls back;
- integrity record rolls back;
- chain head remains unchanged;
- Redis event remains unacknowledged;
- a clean restart can persist it exactly once.

### `after_db_commit_before_ack`

Expected result:

- quote event and integrity record are durable;
- Redis event remains pending;
- redelivery detects the existing quote event;
- no second chain record is created;
- the message can then be acknowledged safely.

The failpoints are disabled by default and are activated only by setting `SMDG_HISTORY_FAILPOINT` to one of the stable names above.

## What this proves — and what it does not

It proves that the repository has executable evidence for two important process-crash windows, deterministic hash verification, and exactly-once database effects under Redis redelivery.

It does not prove sudden-power-loss durability on arbitrary hardware, protection against a privileged database administrator who rewrites all related state, distributed consensus, exchange-grade timestamp precision, predictive alpha, or causal truth.
