import { describe, expect, it } from "vitest";
import { makeQuote } from "../test/market-data-fixtures";
import type { MarketDataPoint, MarketDataSourceEvent } from "./source";
import { ReplayMarketDataSource } from "./replay-source";

describe("ReplayMarketDataSource", () => {
  it("advances only through an explicit virtual clock in stable frame order", () => {
    const source = new ReplayMarketDataSource([
      { atMs: 20, quote: makeQuote(3) },
      { atMs: 10, quote: makeQuote(1) },
      { atMs: 10, quote: makeQuote(2) },
    ]);
    const events: MarketDataSourceEvent[] = [];
    source.subscribe((event) => events.push(event));
    source.replaceSymbols(["aapl.us", "AAPL.US"]);
    source.start();

    expect(source.advanceTo(9)).toEqual([]);
    expect(source.advanceTo(10).map((point) => point.quote.eventId)).toEqual([
      makeQuote(1).eventId,
      makeQuote(2).eventId,
    ]);
    expect(source.advanceBy(10).map((point) => point.quote.eventId)).toEqual([
      makeQuote(3).eventId,
    ]);
    expect(events.filter(isPoint)).toHaveLength(3);
    expect(source.exhausted).toBe(true);
  });

  it("filters unsubscribed symbols and fences a seek with a new generation", () => {
    const source = new ReplayMarketDataSource([
      { atMs: 0, quote: makeQuote(1) },
      { atMs: 0, quote: makeQuote(2, { symbol: "MSFT.US" }) },
    ]);
    source.replaceSymbols(["MSFT.US"]);
    source.start();
    const firstGeneration = source.getStatus().generation;
    const emitted = source.advanceTo(0);
    expect(emitted.map((point) => point.quote.symbol)).toEqual(["MSFT.US"]);

    source.seek(0);
    expect(source.getStatus().generation).toBeGreaterThan(firstGeneration);
    source.replaceSymbols(["AAPL.US"]);
    expect(source.advanceTo(0).map((point) => point.quote.symbol)).toEqual([
      "AAPL.US",
    ]);
  });

  it("does not emit while stopped", () => {
    const source = new ReplayMarketDataSource([{ atMs: 1, quote: makeQuote(1) }]);
    source.replaceSymbols(["AAPL.US"]);
    source.start();
    source.stop();
    expect(source.advanceTo(1)).toEqual([]);
    expect(source.getStatus().state).toBe("stopped");
  });
});

function isPoint(event: MarketDataSourceEvent): event is MarketDataPoint {
  return event.type === "market-data";
}
