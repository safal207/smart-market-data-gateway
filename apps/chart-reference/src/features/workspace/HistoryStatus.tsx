import type { WorkspaceHistory } from "./types";

const LABELS: Readonly<Record<WorkspaceHistory["state"], string>> = Object.freeze({
  disabled: "Disabled",
  unsupported: "Live only",
  loading: "Loading",
  ready: "Ready",
  empty: "No closed candles",
  unauthorized: "Authentication required",
  forbidden: "Upgrade required",
  rate_limited: "Rate limited",
  error: "Unavailable",
});

export interface HistoryStatusProps {
  readonly history: WorkspaceHistory;
}

export function HistoryStatus({ history }: HistoryStatusProps) {
  return (
    <section className="side-card" aria-labelledby="history-status-title">
      <div className="side-card__heading">
        <h2 id="history-status-title">Server history</h2>
        <span className="environment-tag">{LABELS[history.state]}</span>
      </div>
      <p aria-live="polite">{history.detail}</p>
      {history.source != null ? (
        <dl className="diagnostics-grid" aria-label="Server history details">
          <div>
            <dt>Closed candles</dt>
            <dd>{history.count}</dd>
          </div>
          <div>
            <dt>Source</dt>
            <dd>Observed quotes</dd>
          </div>
        </dl>
      ) : null}
      {history.source === "observed_quote_aggregation" ? (
        <p className="diagnostics-note">
          Activity counts accepted quote observations. It is not exchange-reported trade volume.
        </p>
      ) : null}
      {history.warnings.length > 0 ? (
        <details>
          <summary>Data limitations</summary>
          <ul>
            {history.warnings.map((warning, index) => (
              <li key={`${index}-${warning}`}>{warning}</li>
            ))}
          </ul>
        </details>
      ) : null}
    </section>
  );
}
