import type { WorkspaceConnectionState } from "./types";

const STATUS_LABELS: Record<WorkspaceConnectionState, string> = {
  idle: "Idle",
  connecting: "Connecting",
  live: "Live",
  stale: "Stale",
  reconnecting: "Reconnecting",
  replay: "Synthetic replay",
  paused: "Display paused",
  stopped: "Stopped",
  error: "Connection error",
};

function formatTime(timestampMs: number): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(timestampMs);
}
export interface ConnectionStatusProps {
  readonly state: WorkspaceConnectionState;
  readonly detail?: string;
  readonly reconnectAttempt?: number;
  readonly lastUpdateMs?: number;
  readonly onReconnect: () => void;
}

export function ConnectionStatus({
  state,
  detail,
  reconnectAttempt,
  lastUpdateMs,
  onReconnect,
}: ConnectionStatusProps) {
  const needsAction = state === "error" || state === "stopped";

  return (
    <section className="side-card" aria-labelledby="connection-title">
      <div className="side-card__heading">
        <h2 id="connection-title">Connection</h2>
        <div className={`status-badge status-badge--${state}`} role="status" aria-live="polite">
          <span aria-hidden="true" className="status-badge__dot" />
          {STATUS_LABELS[state]}
        </div>
      </div>
      {detail != null && detail !== "" ? (
        <p className={state === "error" ? "connection-detail connection-detail--error" : "connection-detail"}>
          {detail}
        </p>
      ) : null}
      <dl className="compact-details">
        <div>
          <dt>Last update</dt>
          <dd>
            {lastUpdateMs == null ? (
              "—"
            ) : (
              <time dateTime={new Date(lastUpdateMs).toISOString()}>{formatTime(lastUpdateMs)}</time>
            )}
          </dd>
        </div>
        {reconnectAttempt != null && reconnectAttempt > 0 ? (
          <div>
            <dt>Retry</dt>
            <dd>{reconnectAttempt}</dd>
          </div>
        ) : null}
      </dl>
      {needsAction ? (
        <button className="button button--secondary button--full" type="button" onClick={onReconnect}>
          Reconnect
        </button>
      ) : null}
    </section>
  );
}
