import type { MarketDataPoint } from "../market-data";

export const DEFAULT_STALE_AFTER_MS = 5_000;

export function marketDataAgeMs(point: MarketDataPoint, nowMs: number): number {
  if (!Number.isFinite(nowMs)) {
    throw new RangeError("nowMs must be finite");
  }
  return Math.max(0, point.ageMs, nowMs - point.quote.receivedAtMs);
}

export function isMarketDataPointStale(
  point: MarketDataPoint,
  nowMs: number,
  staleAfterMs = DEFAULT_STALE_AFTER_MS,
): boolean {
  if (!Number.isFinite(staleAfterMs) || staleAfterMs <= 0) {
    throw new RangeError("staleAfterMs must be a positive finite number");
  }
  return point.stale || marketDataAgeMs(point, nowMs) > staleAfterMs;
}
