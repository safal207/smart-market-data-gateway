import type { MarketDataPoint } from "../market-data";

export const DEFAULT_STALE_AFTER_MS = 5_000;
const observedAtByPoint = new WeakMap<MarketDataPoint, number>();

/** Binds local elapsed-time freshness to the moment a point enters application state. */
export function recordMarketDataObservation(
  point: MarketDataPoint,
  observedAtMs: number,
): void {
  if (!Number.isFinite(observedAtMs)) {
    throw new RangeError("observedAtMs must be finite");
  }
  if (!observedAtByPoint.has(point)) {
    observedAtByPoint.set(point, observedAtMs);
  }
}

export function marketDataAgeMs(point: MarketDataPoint, nowMs: number): number {
  if (!Number.isFinite(nowMs)) throw new RangeError("nowMs must be finite");
  if (!observedAtByPoint.has(point)) {
    recordMarketDataObservation(point, nowMs);
  }
  const observedAtMs = observedAtByPoint.get(point) ?? nowMs;
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
