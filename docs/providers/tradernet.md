# Tradernet / Freedom quote provider

This adapter is read-only and implements market-data ingestion only. It does not expose portfolio, order, or trading commands.

## Confirmed protocol facts

The following facts are supported by the current Tradernet documentation snapshot and prior project tests:

- Public WebSocket endpoint: `wss://wss.tradernet.com/`.
- SID session connection: `wss://wss.tradernet.com/?SID=<sid>`.
- Requests and responses use JSON frames shaped as `[event, data]`.
- Quote subscription replaces the watched list with `[
  "quotes", ["AAPL.US", "MSFT.US"]
]`.
- Quote updates arrive as event `q`; the adapter also accepts `quotes` for compatibility with previously observed traffic.
- Quote fields include `c` (ticker), `ltp` (last price), `bbp` (best bid), `bap` (best ask), and `ltt` (last trade time).
- On connection the server can emit `userData` with `isDemo` and `mode`.
- A rejected or expired SID can silently fall back to demo mode; strict SID mode therefore treats demo `userData` as authentication failure.
- Public HTTP snapshots are available from `/securities/export?tickers=...`.
- Multiple snapshot tickers are separated with a literal `+`. Encoding the separator as `%2B` previously returned an empty result, so the adapter builds this URL explicitly.
- Tradernet API errors can be returned inside an HTTP 200 response. The adapter checks `code`, `error`, and `errMsg` before accepting a payload.

Prior project testing also observed a development endpoint shaped as:

```text
wss://wssdev.tradernet.dev?user_id=<id>&SID=<sid>
```

The endpoint is not hard-coded. It can be selected through `SMDG_TRADERNET_WEBSOCKET_URL`.

## Adapter modes

| Mode | Purpose | Required values | Behaviour |
|---|---|---|---|
| `public_demo` | Public/demo quotes and publishable technical tests | none | Demo `userData` is accepted |
| `sid_session` | Closed integration test for an authenticated user | `SMDG_TRADERNET_SID`; optional `SMDG_TRADERNET_USER_ID` | Demo fallback is rejected by default |
| `api_key` | Future signed API integration | API key and secret | Deliberately raises `NotImplementedError` until the current HMAC canonical-string contract is verified |

No credential is placed in URLs written to logs, reports, fixtures, or benchmark artifacts.

## Normalization decisions

- Symbols are upper-cased and retain exchange suffixes such as `AAPL.US`.
- Decimal commas are converted to decimal points.
- Price selection order is `ltp`, bid/ask midpoint, bid, ask.
- Tradernet does not provide a stable event ID in documented quote events. A deterministic UUID is derived from ticker, price fields, trade time, size, volume, and trade count so identical frames can be deduplicated.
- Documented `ltt` examples have no timezone. Timezone-aware values are preserved; timezone-free values use gateway receive time. This avoids producing false exchange-latency measurements.
- No sequence number is invented. Gap detection remains available when a future payload supplies a real sequence.

## Snapshot fallback

When the WebSocket receive loop fails, the adapter can fetch one current HTTP snapshot for all active symbols before the collector reconnects. The fallback is controlled by:

```env
SMDG_TRADERNET_SNAPSHOT_FALLBACK=true
```

The collector still owns exponential backoff and subscription restoration.

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

The report verifies delivery before and after reconnect, duplicate event IDs, and timestamp rollbacks. It contains no SID, user ID, API key, or secret.

## Benchmark profiles

| Profile | Gateway clients | Symbols per client | Purpose |
|---|---:|---:|---|
| `smoke` | 10 | 5 | Contract and connectivity |
| `medium` | 100–500 | 20 | Aggregation, QoS, latency |
| `load` | 1,000–10,000 | 20 | Capacity and resource saturation |
| `legacy-673` | configurable | 673 total provider symbols | Regression for the historical oversized subscription set |
| `reconnect-storm` | configurable | 5–20 | Simultaneous disconnect/reconnect behaviour |
| `frozen-stream` | configurable | 5–20 | Pause reads, resume, and verify timestamps advance |
| `zombie-cleanup` | configurable | 5–20 | Abrupt client loss and subscription cleanup |

Synthetic and deployed measurements must be reported separately. Real-provider artifacts must record the commit SHA, environment, symbol set, market session, account mode, and licensing approval.

## Unresolved release gates

The following remain unknown until written provider/exchange terms are reviewed:

- temporary and persistent caching rights;
- display and non-display use;
- redistribution to multiple end users through REST or WebSocket;
- commercial subscription tiers;
- historical storage;
- publication of raw payloads or provider-backed benchmark results;
- attribution, geography, device, and user-count obligations.

Until these are resolved, Tradernet is enabled only for isolated technical testing with the minimum symbol set.

## Reference documentation

- `https://tradernet.com/tradernet-api/auth-login?site_lang=en`
- `https://tradernet.com/tradernet-api/quotes-get-changes?site_lang=en`
- `https://tradernet.com/tradernet-api/quotes-get?site_lang=en`
