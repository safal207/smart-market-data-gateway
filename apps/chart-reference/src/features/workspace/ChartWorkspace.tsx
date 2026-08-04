import { useEffect, useState } from "react";

import { MarketChart } from "../../chart";
import { ConnectionStatus } from "./ConnectionStatus";
import { HistoryStatus } from "./HistoryStatus";
import { InstrumentControl } from "./InstrumentControl";
import { MarketDataSummary } from "./MarketDataSummary";
import { QuoteStrip } from "./QuoteStrip";
import type {
  ChartWorkspaceActions,
  ChartWorkspaceModel,
  WorkspaceConnectionState,
} from "./types";

const FIRST_MARKET_DATA_TIMEOUT_MS = 5_000;

export interface ChartWorkspaceProps {
  readonly model: ChartWorkspaceModel;
  readonly actions: ChartWorkspaceActions;
}

export function ChartWorkspace({ model, actions }: ChartWorkspaceProps) {
  const waitingForGatewayData =
    model.sourceMode === "gateway" && model.connectionState === "live" && model.quote == null;
  const [firstDataTimedOut, setFirstDataTimedOut] = useState(false);

  useEffect(() => {
    setFirstDataTimedOut(false);
    if (!waitingForGatewayData) return undefined;
    const timer = window.setTimeout(() => setFirstDataTimedOut(true), FIRST_MARKET_DATA_TIMEOUT_MS);
    return () => window.clearTimeout(timer);
  }, [model.symbol, model.timeframe, waitingForGatewayData]);

  const effectiveConnectionState: WorkspaceConnectionState = waitingForGatewayData
    ? firstDataTimedOut ? "no_data" : "connecting"
    : model.connectionState;
  const effectiveConnectionDetail = waitingForGatewayData
    ? firstDataTimedOut
      ? "The gateway is connected, but no market data arrived for this instrument. Check the symbol, permissions, provider session, or reconnect."
      : "Connected to the gateway. Waiting for the first market-data update before marking the stream Live."
    : model.connectionDetail;
  const emptyMessage = model.sourceMode === "replay"
    ? "Waiting for replay data…"
    : firstDataTimedOut
      ? "No market data is available for this instrument and timeframe."
      : "Waiting for market data…";

  return (
    <div className="app-shell" data-theme={model.theme}>
      <a className="skip-link" href="#chart-workspace-main">Skip to chart workspace</a>
      <header className="topbar">
        <div className="brand-lockup">
          <span className="brand-mark" aria-hidden="true">SM</span>
          <div><p className="eyebrow">Smart Market Data Gateway</p><h1>Market Chart Reference</h1></div>
        </div>
        <span className="environment-tag">
          {model.sourceMode === "replay" ? "Synthetic data" : "Local gateway"}
        </span>
      </header>

      <InstrumentControl
        symbol={model.symbol}
        symbolOptions={model.symbolOptions}
        timeframe={model.timeframe}
        timeframeOptions={model.timeframeOptions}
        theme={model.theme}
        sourceMode={model.sourceMode}
        gatewayToken={model.gatewayToken}
        paused={model.paused}
        onSelectSymbol={actions.selectSymbol}
        onSelectTimeframe={actions.selectTimeframe}
        onSelectTheme={actions.selectTheme}
        onSelectSourceMode={actions.selectSourceMode}
        onGatewayTokenChange={actions.setGatewayToken}
        onTogglePaused={actions.togglePaused}
        onReconnect={actions.reconnect}
      />

      <main id="chart-workspace-main" className="workspace-grid" tabIndex={-1}>
        <MarketChart
          candles={model.candles}
          symbol={model.symbol}
          timeframeLabel={model.timeframeLabel}
          precision={model.precision}
          theme={model.theme}
          paused={model.paused}
          emptyMessage={emptyMessage}
        />
        <aside className="workspace-sidebar" aria-label="Market-data details">
          <ConnectionStatus
            state={effectiveConnectionState}
            detail={effectiveConnectionDetail}
            reconnectAttempt={model.reconnectAttempt}
            lastUpdateMs={model.lastUpdateMs}
            onReconnect={actions.reconnect}
          />
          {model.history == null ? null : <HistoryStatus history={model.history} />}
          <QuoteStrip quote={model.quote} precision={model.precision} />
          <MarketDataSummary
            latest={model.candles.at(-1) ?? null}
            precision={model.precision}
            activityLabel={model.activityLabel}
            diagnostics={model.diagnostics}
          />
        </aside>
      </main>

      <footer className="app-footer">
        <p>
          Charts powered by{" "}
          <a href="https://www.tradingview.com/" target="_blank" rel="noreferrer">
            TradingView Lightweight Charts™
          </a>
          . Copyright © 2025 TradingView, Inc.
        </p>
        <p>Reference display only. No trading or investment advice.</p>
      </footer>
    </div>
  );
}
