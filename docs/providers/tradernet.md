# Tradernet / Freedom quote provider

This adapter is read-only and implements market-data ingestion only. It does not expose portfolio, order, or trading commands.

## Confirmed protocol facts

The following facts are supported by the current Tradernet documentation snapshot and project tests:

- Public WebSocket endpoint: `wss://wss.tradernet.com/`.
- SID session connection: `wss://wss.tradernet.com/?SID=<sid>`.
- Requests and responses use JSON frames shaped as `[event, data]`.
- Quote subscription replaces the watched list with `["quotes", ["AAPL.US", "MSFT.US"]]`.
- Quote updates arrive as event `q`; the adapter also accepts `quotes` for compatibility with previously observed traffic.
- Quote fields include `c`, `ltp`, `bbp`, `bap`, and `ltt`.
- On connection the server can emit `userData` with `isDemo` and `mode`.
- A rejected or expired SID can silently fall back to demo mode.
- Public HTTP snapshots are available from `/securities/export?tickers=...`.
- Multiple snapshot tickers require a literal `+` separator.
- Tradernet API errors can be returned inside an HTTP 200 response.

## Adapter modes and authentication boundary

| Mode | Purpose | Required values | Behaviour |
|---|---|---|---|
| `public_demo` | Isolated minimum-symbol technical testing | none | SID and user ID values are ignored even if present in the environment |
| `sid_session` | Closed authenticated integration test | SID; optional user ID | Quote delivery and snapshot fallback remain blocked until non-demo `userData` explicitly confirms the session |
| `api_key` | Future signed API integration | API key and secret | Rejected until the HMAC canonical-string contract is verified |

Strict SID mode is fail-closed. Quote frames received before confirmation are withheld, demo fallback closes the socket, and a session that never confirms produces no quote output. Credentials are never placed in public-demo URLs, logs, reports, fixtures, or benchmark artifacts.

## Normalization decisions

- Symbols are upper-cased and retain exchange suffixes such as `AAPL.US`.
- Decimal commas are converted to decimal points.
- Price selection order is `ltp`, bid/ask midpoint, bid, ask.
- A deterministic UUID is derived from the observed quote fields because the documented protocol does not expose a stable event ID.
- Timezone-aware `ltt` values are preserved; timezone-free values use gateway receive time to avoid false exchange-latency claims.
- No sequence number is invented.

## Snapshot fallback

When a confirmed stream fails, the adapter can fetch one current HTTP snapshot for the active symbols before reconnect. Strict SID sessions cannot use snapshot fallback before authentication confirmation.

```env
SMDG_TRADERNET_SNAPSHOT_FALLBACK=true
```

## Local setup

Public demo:

```env
SMDG_PROVIDER=tradernet
SMDG_TRADERNET_MODE=public_demo
SMDG_TRADERNET_WEBSOCKET_URL=wss://wss.tradernet.com/
SMDG_TRADERNET_INTEGRATION_SYMBOLS=AAPL.US,MSFT.US
```

Authenticated SID test:

```env
SMDG_PROVIDER=tradernet
SMDG_TRADERNET_MODE=sid_session
SMDG_TRADERNET_SID=<secret>
SMDG_TRADERNET_USER_ID=<optional-user-id>
```

Run the opt-in direct integration check:

```bash
python scripts/tradernet_integration.py \
  --symbols AAPL.US,MSFT.US \
  --events 20 \
  --timeout 30
```

## Benchmark attestation

Every staged provider benchmark requires a fresh JSON attestation bound to the exact gateway URL. There is no default data-mode flag that can silently mislabel an authenticated target.

```json
{
  "provider": "tradernet",
  "data_mode": "demo",
  "gateway_url": "ws://localhost:8000/v1/stream",
  "deployment_commit_sha": "<deployed-sha>",
  "environment": "isolated-test",
  "issuer": "<responsible-operator>",
  "issued_at": "2026-08-04T00:00:00Z",
  "licensing_approved_for_publication": false
}
```

Run a profile:

```bash
python benchmarks/run_tradernet_profile.py \
  --profile smoke \
  --attestation provider-attestation.json \
  --symbols AAPL.US,MSFT.US,NVDA.US,TSLA.US,AMZN.US \
  --market-session open
```

The attestation must identify Tradernet, match the target URL, be timezone-aware, and be no older than 24 hours by default. SID-backed and `legacy-673` reports require `licensing_approved_for_publication: true`.

## Resilience profiles

| Profile | Purpose |
|---|---|
| `reconnect-storm` | Simultaneous disconnect and reconnect behaviour |
| `frozen-stream` | Credit recovery only when a quote carries a gateway `received_at` later than the resume cutoff; queued backlog cannot pass |
| `zombie-cleanup` | Abrupt client loss and subscription cleanup |

## Unresolved release gates

Written provider and exchange terms must still resolve:

- temporary and persistent caching rights;
- display and non-display use;
- redistribution through REST or WebSocket;
- commercial subscription tiers;
- historical storage;
- publication of raw payloads or provider-backed benchmark results;
- attribution, geography, device, and user-count obligations.

Until these are approved, Tradernet remains limited to isolated minimum-symbol technical testing.
