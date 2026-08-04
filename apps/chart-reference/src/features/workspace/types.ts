import type { ChartCandle, ChartTheme } from "../../chart";
import type { CandleHistoryState } from "../../history";
import type { MarketDataOrigin } from "../../market-data";

export type WorkspaceConnectionState =
  | "idle"
  | "connecting"
  | "live"
  | "stale"
  | "no_data"
  | "reconnecting"
  | "replay"
  | "paused"
  | "stopped"
  | "error";

export type WorkspaceSourceMode = "replay" | "gateway";

export interface TimeframeOption {
  readonly value: string;
  readonly label: string;
}

export interface WorkspaceQuote {
  readonly symbol: string;
  readonly price: number;
  readonly bid?: number;
  readonly ask?: number;
  readonly provider: string;
  readonly providerTimestampMs: number;
  readonly origin: MarketDataOrigin;
  readonly sourceStale: boolean;
  readonly ageMs: number;
  readonly direction: "up" | "down" | "flat";
  readonly stale: boolean;
}

export interface WorkspaceDiagnostics {
  readonly accepted: number;
  readonly duplicates: number;
  readonly rejected: number;
  readonly gaps: number;
}

export interface WorkspaceHistory {
  readonly state: CandleHistoryState;
  readonly count: number;
  readonly source?: "observed_quote_aggregation";
  readonly warnings: readonly string[];
  readonly detail: string;
}

export interface ChartWorkspaceModel {
  readonly symbol: string;
  readonly symbolOptions: readonly string[];
  readonly timeframe: string;
  readonly timeframeOptions: readonly TimeframeOption[];
  readonly timeframeLabel: string;
  readonly theme: ChartTheme;
  readonly sourceMode: WorkspaceSourceMode;
  readonly connectionState: WorkspaceConnectionState;
  readonly connectionDetail?: string;
  readonly reconnectAttempt?: number;
  readonly lastUpdateMs?: number;
  readonly quote: WorkspaceQuote | null;
  readonly candles: readonly ChartCandle[];
  readonly precision: number;
  readonly activityLabel: "Volume" | "Updates" | "Activity";
  readonly diagnostics: WorkspaceDiagnostics;
  readonly history?: WorkspaceHistory;
  readonly paused: boolean;
  readonly gatewayToken: string;
}

export interface ChartWorkspaceActions {
  readonly selectSymbol: (symbol: string) => void;
  readonly selectTimeframe: (timeframe: string) => void;
  readonly selectTheme: (theme: ChartTheme) => void;
  readonly selectSourceMode: (mode: WorkspaceSourceMode) => void;
  readonly setGatewayToken: (token: string) => void;
  readonly togglePaused: () => void;
  readonly reconnect: () => void;
}
