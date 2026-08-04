# Market Chart Reference

A production-shaped React/TypeScript reference workspace for the Smart Market Data Gateway.

## Run locally

```bash
cd apps/chart-reference
pnpm install --frozen-lockfile
pnpm dev
```

The default mode is deterministic synthetic replay. Open `/?source=gateway` to connect to the local gateway WebSocket endpoint.

## Trust boundaries

- `Live` is shown only after the first accepted quote reaches the workspace.
- An open socket without quotes remains `Connecting` and becomes a recoverable `No data` state after five seconds.
- Gateway tokens are held in React memory and are never written to local storage.
- Only symbol, timeframe, and theme preferences are persisted.
- Candles are aggregated in the browser for this reference implementation; they are not canonical server-side historical candles.
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
