import type { ChartCandle } from "../../chart";
import type { WorkspaceDiagnostics } from "./types";

function number(value: number, precision: number): string {
  return value.toFixed(precision);
}

export interface MarketDataSummaryProps {
  readonly latest: ChartCandle | null;
  readonly precision: number;
  readonly activityLabel: "Volume" | "Updates" | "Activity";
  readonly diagnostics: WorkspaceDiagnostics;
}

export function MarketDataSummary({
  latest,
  precision,
  activityLabel,
  diagnostics,
}: MarketDataSummaryProps) {
  return (
    <section className="side-card" aria-labelledby="summary-title">
      <div className="side-card__heading">
        <h2 id="summary-title">Accessible data summary</h2>
      </div>
      {latest == null ? (
        <p className="empty-state">No completed or partial bar yet.</p>
      ) : (
        <div className="table-scroll" tabIndex={0} role="region" aria-label="Latest OHLC bar" >
          <table className="data-table">
            <caption className="visually-hidden">
              Latest open, high, low, close, and {activityLabel.toLowerCase()}
            </caption>
            <thead>
              <tr>
                <th scope="col">Open</th>
                <th scope="col">High</th>
                <th scope="col">Low</th>
                <th scope="col">Close</th>
                <th scope="col">{activityLabel}</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>{number(latest.open, precision)}</td>
                <td>{number(latest.high, precision)}</td>
                <td>{number(latest.low, precision)}</td>
                <td>{number(latest.close, precision)}</td>
                <td>{latest.activityValue}</td>
              </tr>
            </tbody>
          </table>
        </div>
      )}
      <dl className="diagnostics-grid" aria-label="Stream diagnostics">
        <div>
          <dt>Accepted</dt>
          <dd>{diagnostics.accepted}</dd>
        </div>
        <div>
          <dt>Duplicates</dt>
          <dd>{diagnostics.duplicates}</dd>
        </div>
        <div>
          <dt>Rejected</dt>
          <dd>{diagnostics.rejected}</dd>
        </div>
        <div>
          <dt>Sequence jumps</dt>
          <dd>{diagnostics.gaps}</dd>
        </div>
      </dl>
      <p className="diagnostics-note">
        Sequence jumps are observed at delivery; gateway tier coalescing can cause them.
      </p>
    </section>
  );
}
