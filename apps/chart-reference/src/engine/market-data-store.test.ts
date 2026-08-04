import { describe, expect, it, vi } from "vitest";
import { ReplayMarketDataSource } from "../market-data";
import { makePoint, makeQuote } from "../test/market-data-fixtures";
import { MarketDataStore } from "./market-data-store";

describe("MarketDataStore", () => {
  it("exposes a stable useSyncExternalStore snapshot and notifies on accepted data", () => {
    const store = new MarketDataStore({ timeframe: "5s" });
    const initial = store.getSnapshot();
    expect(store.getSnapshot()).toBe(initial);
    const listener = vi.fn();
    store.subscribe(listener);
    const point = makePoint(1);

    expect(store.ingest(point).outcome).toBe("accepted");
    expect(listener).toHaveBeenCalledOnce();
    expect(store.getSnapshot()).not.toBe(initial);
    expect(store.getSnapshot().latestQuotes["AAPL.US"]?.eventId).toBe(
      point.quote.eventId,
    );

    store.ingest(point);
    expect(listener).toHaveBeenCalledTimes(2);
    expect(store.getSnapshot().duplicateCounts["AAPL.US"]).toBe(1);
  });

  it("publishes duplicate counts while preserving current transport freshness", () => {
    const store = new MarketDataStore();
    const initial = makePoint(1, { ageMs: 10, stale: false });
    store.ingest(initial);
    const duplicateSnapshot = makePoint(1, {
      generation: 2,
      origin: "snapshot",
      ageMs: 8_000,
      stale: true,
      quote: { price: 999 },
    });

    expect(store.ingest(duplicateSnapshot).outcome).toBe("duplicate");
    const snapshot = store.getSnapshot();
    expect(snapshot.duplicateCounts["AAPL.US"]).toBe(1);
    expect(snapshot.latestQuotes["AAPL.US"]?.price).toBe(initial.quote.price);
    expect(snapshot.latestPoints["AAPL.US"]).toMatchObject({
      origin: "snapshot",
      stale: true,
      ageMs: 8_000,
      generation: 2,
    });
  });

  it("requires current-generation data after reconnect while retaining history", () => {
    const store = new MarketDataStore({ timeframe: "5s" });
    store.setConnectionStatus({ state: "connecting", generation: 1, reconnectAttempt: 0 });
    const first = makePoint(1, {
      generation: 1,
      quote: { providerTimestampMs: 1_000 },
    });
    expect(store.ingest(first).outcome).toBe("accepted");
    expect(store.getSnapshot().latestPoints["AAPL.US"]?.generation).toBe(1);
    expect(store.getSnapshot().candles["AAPL.US"]).toHaveLength(1);

    store.setConnectionStatus({ state: "reconnecting", generation: 2, reconnectAttempt: 0 });
    expect(store.getSnapshot().latestPoints["AAPL.US"]).toBeUndefined();
    expect(store.getSnapshot().candles["AAPL.US"]).toHaveLength(1);
    expect(store.getSnapshot().acceptedLog).toHaveLength(1);

    store.setConnectionStatus({ state: "live", generation: 2, reconnectAttempt: 0 });
    expect(store.getSnapshot().latestPoints["AAPL.US"]).toBeUndefined();

    const repeatedSnapshot = makePoint(1, {
      generation: 2,
      origin: "snapshot",
      stale: true,
      ageMs: 2_000,
      quote: { providerTimestampMs: 1_000, price: 999 },
    });
    expect(store.ingest(repeatedSnapshot).outcome).toBe("duplicate");
    expect(store.getSnapshot().latestQuotes["AAPL.US"]?.price).toBe(first.quote.price);
    expect(store.getSnapshot().latestPoints["AAPL.US"]).toMatchObject({
      generation: 2,
      origin: "snapshot",
      stale: true,
      ageMs: 2_000,
    });
  });

  it("keeps the latest quote monotonic and records quarantined regressions", () => {
    const store = new MarketDataStore({ logRetention: 2 });
    store.ingest(makePoint(10));
    const decision = store.ingest(
      makePoint(9, { quote: { providerTimestampMs: 9_000, sequence: 11 } }),
    );
    expect(decision).toMatchObject({
      outcome: "quarantined",
      reason: "timestamp_rollback",
    });
    expect(store.getSnapshot().latestQuotes["AAPL.US"]?.eventId).toBe(
      makeQuote(10).eventId,
    );
    expect(store.getSnapshot().quarantineLog).toHaveLength(1);
  });

  it("caps retention and deterministically rebuilds candles for a new timeframe", () => {
    const store = new MarketDataStore({
      timeframe: "5s",
      eventRetentionPerSymbol: 3,
      candleRetentionPerSymbol: 2,
      logRetention: 2,
    });
    store.ingest(makePoint(1, { quote: { providerTimestampMs: 1_000 } }));
    store.ingest(makePoint(2, { quote: { providerTimestampMs: 6_000 } }));
    store.ingest(makePoint(3, { quote: { providerTimestampMs: 12_000 } }));
    expect(store.getSnapshot().acceptedLog).toHaveLength(2);
    expect(store.getSnapshot().candles["AAPL.US"]).toHaveLength(2);

    store.setTimeframe("1m");
    expect(store.getSnapshot().timeframe).toBe("1m");
    expect(store.getSnapshot().candles["AAPL.US"]).toHaveLength(1);
    expect(store.getSnapshot().candles["AAPL.US"]?.[0]?.updateCount).toBe(3);
  });

  it("can attach to a source and mirrors connection status", () => {
    const source = new ReplayMarketDataSource([{ atMs: 0, quote: makeQuote(1) }]);
    source.replaceSymbols(["AAPL.US"]);
    const store = new MarketDataStore();
    store.attachSource(source);
    source.start();
    source.advanceTo(0);
    expect(store.getSnapshot().connectionStatus.state).toBe("live");
    expect(store.getSnapshot().latestQuotes["AAPL.US"]).toBeDefined();
  });
});
