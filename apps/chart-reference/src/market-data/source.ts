import type { NormalizedQuote } from "./types";

export type SourceConnectionState =
  | "idle"
  | "connecting"
  | "live"
  | "reconnecting"
  | "stopped";

export interface SourceStatus {
  readonly state: SourceConnectionState;
  readonly generation: number;
  readonly reconnectAttempt: number;
  readonly nextRetryMs?: number;
  readonly lastError?: string;
}

export type MarketDataOrigin = "live" | "snapshot" | "replay";

export interface MarketDataPoint {
  readonly type: "market-data";
  readonly quote: NormalizedQuote;
  readonly origin: MarketDataOrigin;
  readonly generation: number;
  readonly stale: boolean;
  readonly ageMs: number;
  readonly requestId?: string;
}

export interface SubscriptionAckEvent {
  readonly type: "subscription-ack";
  readonly action: "subscribe" | "unsubscribe";
  readonly symbols: readonly string[];
  readonly generation: number;
  readonly requestId?: string;
}

export interface PongEvent {
  readonly type: "pong";
  readonly generation: number;
  readonly requestId?: string;
}

export interface SourceNoticeEvent {
  readonly type: "notice";
  readonly level: "warning" | "error";
  readonly code: string;
  readonly message: string;
  readonly generation: number;
  readonly requestId?: string;
}

export type MarketDataSourceEvent =
  | MarketDataPoint
  | SubscriptionAckEvent
  | PongEvent
  | SourceNoticeEvent;

export type Unsubscribe = () => void;
export type SourceEventListener = (event: MarketDataSourceEvent) => void;
export type SourceStatusListener = (status: SourceStatus) => void;

export interface MarketDataSource {
  start(): void;
  stop(): void;
  replaceSymbols(symbols: readonly string[]): void;
  ping(): void;
  subscribe(listener: SourceEventListener): Unsubscribe;
  subscribeStatus(listener: SourceStatusListener): Unsubscribe;
  getStatus(): SourceStatus;
}

export function normalizeSymbols(symbols: readonly string[]): readonly string[] {
  return Object.freeze(
    [...new Set(symbols.map((symbol) => symbol.trim().toUpperCase()).filter(Boolean))].sort(),
  );
}
