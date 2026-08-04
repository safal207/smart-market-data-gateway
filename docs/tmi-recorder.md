# TMI WebSocket Recorder

The recorder captures authenticated quote messages from the public gateway WebSocket and writes append-only JSONL for deterministic Temporal Market Intelligence replay.

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

## Recording guarantees

- one normalized quote object per newline-terminated UTF-8 line;
- file mode `0600` for newly created recordings;
- append-only operation across reconnects;
- duplicate or out-of-order sequences are skipped;
- forward sequence gaps are counted and the affected row is marked `degraded_stream=true`;
- failed partial appends are truncated back to the previous file size;
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

The event asset must match a symbol present in the recording. TMI selects point-in-time evidence and rejects snapshots that are too distant from the requested timestamps.

## Interruption and recovery

Pressing `Ctrl+C` closes the current WebSocket connection and file descriptor. Completed lines remain reusable. If a low-level write fails, the recorder rolls the file back to its size before that record, so a partial append is not retained.

The client reconnects with capped exponential backoff. Existing rows are never truncated on reconnect. The `--max-reconnects` option limits consecutive failures.

## Data rights

Recording, caching, historical storage, non-display use, model training, benchmark publication, and redistribution depend on provider and exchange terms. The deterministic mock provider is safe for local development. Real-provider recording must pass the provider licensing checklist before use outside an approved environment.
