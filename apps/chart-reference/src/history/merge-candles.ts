import type { ChartCandle } from "../chart";

/**
 * Server candles are authoritative for every closed bucket they cover. Live bars are appended only
 * after the latest historical bucket, which prevents duplicate REST/WebSocket boundary candles and
 * ignores stale snapshots that arrive behind canonical history.
 */
export function mergeHistoryWithLive(
  history: readonly ChartCandle[],
  live: readonly ChartCandle[],
): readonly ChartCandle[] {
  if (history.length === 0) return orderedUnique(live);

  const canonical = orderedUnique(history);
  const latestHistoricalTime = canonical.at(-1)?.timeMs;
  if (latestHistoricalTime == null) return orderedUnique(live);

  const merged = new Map(canonical.map((candle) => [candle.timeMs, candle] as const));
  for (const candle of live) {
    if (candle.timeMs > latestHistoricalTime) merged.set(candle.timeMs, candle);
  }
  return Object.freeze([...merged.values()].sort(compareCandleTime));
}

function orderedUnique(candles: readonly ChartCandle[]): readonly ChartCandle[] {
  const byTime = new Map<number, ChartCandle>();
  for (const candle of candles) byTime.set(candle.timeMs, candle);
  return Object.freeze([...byTime.values()].sort(compareCandleTime));
}

function compareCandleTime(left: ChartCandle, right: ChartCandle): number {
  return left.timeMs - right.timeMs;
}
