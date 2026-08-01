import { MarketChart } from "../../chart";
import { ConnectionStatus } from "./ConnectionStatus";
import { InstrumentControl } from "./InstrumentControl";
import { MarketDataSummary } from "./MarketDataSummary";
import { QuoteStrip } from "./QuoteStrip";
import type { ChartWorkspaceActions, ChartWorkspaceModel } from "./types";

export interface ChartWorkspaceProps {
  readonly model: ChartWorkspaceModel;
  readonly actions: ChartWorkspaceActions;
}

export function ChartWorkspace({ model, actions }: ChartWorkspaceProps) {
  return (
    <div className="app-shell" data-theme={model.theme}>
      <a className="skip-link" href="#chart-workspace-main">
        Skip to chart workspace
      </a>
      <header className="topbar">
        <div className="brand-lockup">
          <span className="brand-mark" aria-hidden="true">SM</span>
          <div>
            <p className="eyebrow">Smart Market Data Gateway</p>
            <h1>Market Chart Reference</h1>
          </div>
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
        />
        <aside className="workspace-sidebar" aria-label="Market-data details">
          <ConnectionStatus
            state={model.connectionState}
            detail={model.connectionDetail}
            reconnectAttempt={model.reconnectAttempt}
            lastUpdateMs={model.lastUpdateMs}
            onReconnect={actions.reconnect}
          />
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
          . Copyright (с) 2025 TradingView, Inc.
        </p>
        <p>Reference display only. No trading or investment advice.</p>
      </footer>
    </div>
  );
}
