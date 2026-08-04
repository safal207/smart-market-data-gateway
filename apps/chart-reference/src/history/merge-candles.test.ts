import { describe, expect, it } from "vitest";

import type { ChartCandle } from "../chart";
import { mergeHistoryWithLive } from "./merge-candles";

describe("mergeHistoryWithLive", () => {
  it("keeps canonical history at the REST/WebSocket boundary and appends the live bar", () => {
    const history = [candle(0, 100), candle(60_000, 101)];
    const live = [
      candle(60_000, 999),
      candle(120_000, 102),
    ];

    const merged = mergeHistoryWithLive(history, live);

    expect(merged.map(({ timeMs, close }) => [timeMs, close])).toEqual([
      [0, 100],
      [60_000, 101],
      [120_000, 102],
    ]);
  });

  it("drops stale live snapshots behind the latest canonical candle", () => {
    const merged = mergeHistoryWithLive(
      [candle(60_000, 101), candle(120_000, 102)],
      [candle(0, 90), candle(180_000, 103)],
    );

    expect(merged.map(({ timeMs }) => timeMs)).toEqual([60_000, 120_000, 180_000]);
  });

  it("uses ordered unique live data when server history is empty", () => {
    const merged = mergeHistoryWithLive([], [
      candle(120_000, 3),
      candle(60_000, 1),
      candle(60_000, 2),
    ]);

    expect(merged.map(({ timeMs, close }) => [timeMs, close])).toEqual([
      [60_000, 2],
      [120_000, 3],
    ]);
  });
});

function candle(timeMs: number, close: number): ChartCandle {
  return Object.freeze({
    timeMs,
    open: close,
    high: close,
    low: close,
    close,
    updateCount: 1,
    activityValue: 1,
    activitySource: "updates",
  });
}
