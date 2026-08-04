# Market Chart Reference

A production-shaped React/TypeScript reference workspace for the Smart Market Data Gateway.

## Run locally

```bash
cd apps/chart-reference
pnpm install --frozen-lockfile
pnpm dev
```

The default mode is deterministic synthetic replay. Open `/?source=gateway` to connect to the local gateway WebSocket endpoint.

Gateway mode uses:

- `VITE_GATEWAY_WS_URL` for the live quote stream, defaulting to `/v1/stream` on the current origin;
- `VITE_GATEWAY_HTTP_URL` for `GET /v1/candles/{symbol}`, defaulting to the current origin.

Use a Pro or Premium development token such as `dev-pro:chart-reference` to load server history. A Basic token continues to receive its permitted live stream while the history panel reports the tier denial explicitly.

## History and live-data stitching

1. The workspace requests up to 500 fully closed canonical candles for the selected symbol and timeframe.
2. The bearer token is sent only in the `Authorization` header and remains in React memory.
3. The HTTP response is validated against the public candle contract before it can reach the chart.
4. Server candles are authoritative through the latest returned closed bucket.
5. WebSocket candles are appended only after that boundary, preventing REST/stream duplicates and rejecting stale snapshots behind canonical history.
6. Switching symbol, timeframe, token, or mode aborts the previous request and fences late completions by generation.
7. Manual reconnect refreshes both the WebSocket generation and the history bootstrap.

`activity_count` is displayed as observed quote activity. It is not exchange-reported trade volume. Empty intervals and partial historical buckets are not fabricated.

## Trust boundaries

- `Live` is shown only after the first accepted quote reaches the workspace.
- An open socket without quotes remains `Connecting` and becomes a recoverable `No data` state after five seconds.
- Gateway tokens are held in React memory and are never written to local storage or placed in REST URLs.
- Only symbol, timeframe, and theme preferences are persisted.
- Replay mode aggregates candles entirely in the browser.
- Gateway mode bootstraps canonical closed history from the server and builds only the current live tail in the browser.
- Server-history loading, entitlement, empty, and failure states are independent from the WebSocket connection state.
- The application is display-only and does not place orders or provide investment advice.

## Validate

```bash
pnpm audit --audit-level high
pnpm lint
pnpm typecheck
pnpm test:coverage
pnpm build
pnpm exec playwright install --with-deps chromium
pnpm test:e2e
```
