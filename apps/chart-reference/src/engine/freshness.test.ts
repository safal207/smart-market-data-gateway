import { describe, expect, it } from "vitest";
import { makePoint } from "../test/market-data-fixtures";
import { DEFAULT_STALE_AFTER_MS, isMarketDataPointStale, marketDataAgeMs } from "./freshness";

describe("market-data freshness", () => {
  it("preserves authoritative source age and stale metadata", () => {
    const point = makePoint(1, { ageMs: 7_000, stale: true });
    expect(marketDataAgeMs(point, 100_000)).toBe(7_000);
    expect(isMarketDataPointStale(point, 100_000)).toBe(true);
  });
  it("uses local observation time instead of subtracting gateway and browser clocks", () => {
    const point = makePoint(1, { quote: { receivedAtMs: 1_000 } });
    const browserNow = 1_000_000;
    expect(marketDataAgeMs(point, browserNow)).toBe(0);
    expect(isMarketDataPointStale(point, browserNow + DEFAULT_STALE_AFTER_MS)).toBe(false);
    expect(isMarketDataPointStale(point, browserNow + DEFAULT_STALE_AFTER_MS + 1)).toBe(true);
  });
});
