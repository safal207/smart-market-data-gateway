# Coinbase research provider

The Coinbase Advanced Trade adapter is an opt-in research integration. It is disabled by default and is not a general production or redistribution entitlement.

## Data-rights boundary

Before enabling the adapter, review the current official documents:

- [Advanced Trade WebSocket overview](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview)
- [Advanced Trade WebSocket channels](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels)
- [Coinbase Market Data Terms of Use](https://www.coinbase.com/legal/market_data)

The repository does not accept those terms on your behalf. The adapter requires an explicit local acknowledgement and refuses to connect in `production`.

The default profile is intentionally narrow:

- personal or internal research only;
- no third-party redistribution or public display;
- no committed raw market-data recordings;
- no model-training, benchmark-publication, or commercial rights implied;
- no claim that a public endpoint grants permission beyond the governing terms.

Obtain separate written rights before expanding any of those uses.

## Enable locally

Keep the gateway on a private development machine and set:

```bash
export SMDG_ENVIRONMENT=research
export SMDG_MARKET_DATA_PROVIDER=coinbase
export SMDG_COINBASE_USE_MODE=personal_research
export SMDG_COINBASE_MARKET_DATA_TERMS_ACCEPTED=true
export SMDG_ALLOW_ANONYMOUS_DEV=false
```

Then start the normal stack and collector:

```bash
docker compose up redis gateway processor
python -m smart_market_data_gateway.collector
```

Subscribe with a private development token using Coinbase product IDs such as `BTC-USD`:

```json
{
  "action": "subscribe",
  "symbols": ["BTC-USD"],
  "channels": ["quote"],
  "request_id": "coinbase-research-1"
}
```

To capture a local evidence ledger:

```bash
export SMDG_RECORDER_TOKEN='dev-pro:bob'
python -m smart_market_data_gateway.recorder \
  --url ws://127.0.0.1:8000/v1/stream \
  --symbol BTC-USD \
  --output recordings/coinbase-btc-usd.jsonl \
  --max-records 100
```

The `recordings/` directory is ignored by Git. Do not attach a recording to an issue or pull request unless separate redistribution and publication rights have been confirmed.

## Normalization semantics

The adapter subscribes to the public `ticker`, `market_trades`, and `heartbeats` channels.

- `ticker` supplies the latest price, best bid, best ask, and top-of-book quantities.
- `market_trades` supplies provider-batched trades.
- Coinbase documents trade `side` as the maker side. TMI uses aggressor flow, so the adapter maps maker `SELL` to aggressive buy volume and maker `BUY` to aggressive sell volume.
- The provider batches market trades over approximately 250 ms. The emitted volume semantics therefore use interval `base_asset` units with a 250 ms aggregation window.
- Volume and aggressor flow are marked `gateway_derived`; top-of-book quantities are marked `native`.
- If ticker evidence has not arrived yet, trade evidence is emitted without bid, ask, or depth rather than inventing them.

## Operational limitations

- Public channels do not require JWT, but Coinbase recommends authentication for the most reliable connection.
- The adapter uses local per-symbol sequence numbers because upstream channel sequence values are not a provider-neutral per-symbol sequence.
- Reconnects are handled by the existing collector loop.
- A local SHA-256 ledger is tamper-evident, not externally signed or timestamped.
- The integration proves transport and normalization, not trading alpha, causality, or investment suitability.
