import { describe, expect, it } from "vitest";
import { makePoint } from "../test/market-data-fixtures";
import { CandleAggregator, bucketStart } from "./candle-aggregator";
import { TIMEFRAMES, TIMEFRAME_DURATION_MS } from "./types";

describe("CandleAggregator", () => {
  it("builds deterministic OHLC candles and uses update count as volume fallback", () => {
    const aggregator = new CandleAggregator("5s");
    aggregator.ingest(makePoint(1, { quote: { price: 100 } }));
    aggregator.ingest(makePoint(2, { quote: { price: 105 } }));
    aggregator.ingest(makePoint(3, { quote: { price: 98 } }));
    expect(aggregator.getCandles("AAPL.US")[0]).toMatchObject({
      open: 100, high: 105, low: 98, close: 98, volume: 3,
      volumeSource: "update-count", updateCount: 3, revision: 3, complete: false,
    });
  });
  it("normalizes symbol keys on write and read", () => {
    const aggregator = new CandleAggregator("5s");
    aggregator.ingest(makePoint(1, { quote: { symbol: "aapl.us" } }));
    expect(aggregator.getSymbols()).toEqual(["AAPL.US"]);
    expect(aggregator.getCandles("aapl.us")).toHaveLength(1);
    expect(aggregator.getCandles("AAPL.US")[0]?.symbol).toBe("AAPL.US");
  });
  it("closes the prior bucket exactly at the boundary", () => {
    const aggregator = new CandleAggregator("5s");
    aggregator.ingest(makePoint(1, { quote: { providerTimestampMs: 4_999 } }));
    const update = aggregator.ingest(makePoint(2, { quote: { providerTimestampMs: 5_000 } }));
    expect(update.newlyCompleted).toHaveLength(1);
    expect(update.newlyCompleted[0]?.complete).toBe(true);
    expect(aggregator.getCandles("AAPL.US").map((candle) => candle.openTimeMs)).toEqual([0, 5_000]);
  });
  it("uses cumulative deltas and last size when available", () => {
    const aggregator = new CandleAggregator("5s");
    aggregator.ingest(makePoint(1, { quote: { cumulativeVolume: 100 } }));
    aggregator.ingest(makePoint(2, { quote: { cumulativeVolume: 105 } }));
    aggregator.ingest(makePoint(3, { quote: { lastSize: 2, cumulativeVolume: 107 } }));
    expect(aggregator.getCandles("AAPL.US")[0]).toMatchObject({ volume: 7, volumeSource: "mixed" });
  });
  it("supports every required UTC-aligned timeframe", () => {
    for (const timeframe of TIMEFRAMES) {
      const timestamp = 172_812_345;
      const start = bucketStart(timestamp, timeframe);
      expect(start % TIMEFRAME_DURATION_MS[timeframe]).toBe(0);
      expect(start).toBeLessThanOrEqual(timestamp);
      expect(timestamp).toBeLessThan(start + TIMEFRAME_DURATION_MS[timeframe]);
    }
  });
  it("rebuilds identically regardless of input ordering", () => {
    const points = [makePoint(1, { quote: { price: 100 } }), makePoint(2, { quote: { price: 105 } }), makePoint(3, { quote: { price: 99 } })];
    const ordered = new CandleAggregator("1m");
    const reversed = new CandleAggregator("1m");
    ordered.rebuild(points);
    reversed.rebuild([...points].reverse());
    expect(reversed.getCandles("AAPL.US")).toEqual(ordered.getCandles("AAPL.US"));
  });
});
