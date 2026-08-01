import type { NormalizedQuote } from "../market-data";

export const TIMEFRAMES = ["5s", "1m", "5m", "15m", "1h", "1d"] as const;
export type Timeframe = (typeof TIMEFRAMES)[number];

export const TIMEFRAME_DURATION_MS: Readonly<Record<Timeframe, number>> = Object.freeze({
  "5s": 5_000,
  "1m": 60_000,
  "5m": 5 * 60_000,
  "15m": 15 * 60_000,
  "1h": 60 * 60_000,
  "1d": 24 * 60 * 60_000,
});

export type CandleVolumeSource =
  | "last-size"
  | "cumulative-delta"
  | "update-count"
  | "mixed";

export interface Candle {
  readonly symbol: string;
  readonly timeframe: Timeframe;
  readonly openTimeMs: number;
  readonly closeTimeMs: number;
  readonly open: number;
  readonly high: number;
  readonly low: number;
  readonly close: number;
  readonly volume: number;
  readonly volumeSource: CandleVolumeSource;
  readonly updateCount: number;
  readonly firstEventId: string;
  readonly lastEventId: string;
  readonly revision: number;
  readonly complete: boolean;
}

export interface AcceptedMarketDataLogEntry {
  readonly quote: NormalizedQuote;
  readonly generation: number;
  readonly acceptedReason: string;
}

export function isTimeframe(value: unknown): value is Timeframe {
  return typeof value === "string" && (TIMEFRAMES as readonly string[]).includes(value);
}
