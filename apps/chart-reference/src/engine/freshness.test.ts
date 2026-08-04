import { describe, expect, it } from "vitest";
import { makePoint } from "../test/market-data-fixtures";
import {
  DEFAULT_STALE_AFTER_MS,
  isMarketDataPointStale,
  marketDataAgeMs,
} from "./freshness";

describe("market-data freshness", () => {
  it("preserves authoritative source age and stale metadata", () => {
    const point = makePoint(1, { ageMs: 7_000, stale: true });
    expect(marketDataAgeMs(point, point.quote.receivedAtMs)).toBe(7_000);
    expect(isMarketDataPointStale(point, point.quote.receivedAtMs)).toBe(true);
  });

  it("becomes stale as wall time advances without another frame", () => {
    const point = makePoint(1);
    expect(
      isMarketDataPointStale(
        point,
        point.quote.receivedAtMs + DEFAULT_STALE_AFTER_MS,
      ),
    ).toBe(false);
    expect(
      isMarketDataPointStale(
        point,
        point.quote.receivedAtMs + DEFAULT_STALE_AFTER_MS + 1,
      ),
    ).toBe(true);
  });
});
