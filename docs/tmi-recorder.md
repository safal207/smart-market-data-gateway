# TMI WebSocket Recorder

The recorder captures authenticated quote messages from the public gateway WebSocket and writes append-only, tamper-evident JSONL for deterministic Temporal Market Intelligence replay.

## Local recording

Start the local stack with the deterministic mock provider:

```bash
cp .env.example .env
docker compose up --build
```

In another shell, install the package and record two symbols:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

export SMDG_RECORDER_TOKEN='dev-pro:bob'
python -m smart_market_data_gateway.recorder \
  --url ws://localhost:8000/v1/stream \
  --symbol AAPL \
  --symbol TSLA \
  --output recordings/mock-session.jsonl \
  --max-records 100
```

The token is used only in the WebSocket `Authorization` header. It is never written to the recording or included in the final counter summary.

The command prints operational counters when it exits:

```json
{"dropped":0,"duplicates":0,"gaps":0,"malformed":0,"received":104,"reconnects":0,"written":100}
```

## Evidence ledger

Every persisted quote is extended with deterministic ledger metadata:

```json
{
  "ledger_version": "1.0",
  "ledger_algorithm": "sha256",
  "ledger_index": 42,
  "previous_record_hash": "...",
  "record_hash": "...",
  "recorder_session_id": "...",
  "provenance_system": "smart-market-data-gateway",
  "provenance_component": "websocket-jsonl-recorder",
  "provenance_transport": "websocket"
}
```

`record_hash` is SHA-256 over the complete canonical JSON object except the `record_hash` field itself. `previous_record_hash` links the row to the prior persisted row. The first row uses 64 zeroes as the genesis hash.

Before appending to an existing file, the recorder verifies every record, index, provenance field, hash, and chain link. It refuses to append when a row was edited, removed, reordered, truncated, or replaced without a valid recomputation of all following links.

A new recorder process receives a new `recorder_session_id` but continues the same file-level hash chain. This preserves restart provenance without breaking ledger continuity.

Verify a completed or active recording without a token:

```bash
python -m smart_market_data_gateway.recorder \
  --verify-ledger recordings/mock-session.jsonl
```

Example result:

```json
{
  "head_hash": "...",
  "records": 100,
  "session_ids": ["..."],
  "verified": true
}
```

The hash chain is tamper-evident, not a digital signature. A party that can rewrite the complete file can also recompute the full chain. Strong non-repudiation requires anchoring the final `head_hash` in an external trusted system or signing it with a protected key. That remains outside this recorder's transport-layer scope.

## Recording guarantees

- one normalized quote object per newline-terminated UTF-8 line;
- file mode `0600` for newly created recordings;
- append-only operation across reconnects and process restarts;
- complete chain verification before any append to an existing file;
- deterministic SHA-256 linkage between adjacent records;
- explicit recorder-session and gateway provenance;
- duplicate or out-of-order sequences are skipped;
- forward sequence gaps are counted and the affected row is marked `degraded_stream=true`;
- failed partial appends are truncated back to the previous file size without advancing the chain;
- credentials and raw WebSocket control payloads are never persisted;
- `fsync` is enabled by default and may be disabled only for disposable development captures with `--no-fsync`.

The recorder accepts both live `quote` messages and reconnect `snapshot` messages. Snapshot rows preserve their stale flag.

## Replay in Temporal Market Intelligence

Use the resulting JSONL file with the gateway-backed TMI path:

```bash
python -m tmi \
  examples/btc_gateway_event.json \
  --gateway-recording /absolute/path/to/recordings/mock-session.jsonl
```

The event asset must match a symbol present in the recording. TMI selects point-in-time evidence and rejects snapshots that are too distant from the requested timestamps. Confidence scoring and causal verdicts remain in TMI; the gateway records provenance and integrity evidence only.

## Interruption and recovery

Pressing `Ctrl+C` closes the current WebSocket connection and file descriptor. Completed lines remain reusable. If a low-level write fails, the recorder rolls the file back to its size before that record, so a partial append is not retained and the next successful row keeps the same ledger index and prior hash.

The client reconnects with capped exponential backoff. Existing rows are never truncated on reconnect. The `--max-reconnects` option limits consecutive failures.

## Data rights

Recording, caching, historical storage, non-display use, model training, benchmark publication, and redistribution depend on provider and exchange terms. The deterministic mock provider is safe for local development. Real-provider recording must pass the provider licensing checklist before use outside an approved environment.
