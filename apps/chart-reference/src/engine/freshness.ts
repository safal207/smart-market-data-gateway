import type { MarketDataPoint } from "../market-data";

export const DEFAULT_STALE_AFTER_MS = 5_000;
const observedAtByPoint = new WeakMap<MarketDataPoint, number>();

export function marketDataAgeMs(point: MarketDataPoint, nowMs: number): number {
  if (!Number.isFinite(nowMs)) throw new RangeError("nowMs must be finite");
  let observedAtMs = observedAtByPoint.get(point);
  if (observedAtMs == null) {
    observedAtMs = nowMs;
    observedAtByPoint.set(point, observedAtMs);
  }
  const localElapsedMs = Math.max(0, nowMs - observedAtMs);
  return Math.max(0, point.ageMs + localElapsedMs);
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
