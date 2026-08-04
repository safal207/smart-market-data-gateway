import { describe, expect, it, vi } from "vitest";
import { makePoint } from "../test/market-data-fixtures";
import type {
  MarketDataSource,
  MarketDataSourceEvent,
  SourceEventListener,
  SourceStatus,
  SourceStatusListener,
} from "./source";
import { SmartSubscriptionManager } from "./subscription-manager";

class FakeSource implements MarketDataSource {
  readonly desiredHistory: string[][] = [];
  readonly events = new Set<SourceEventListener>();
  readonly statuses = new Set<SourceStatusListener>();
  status: SourceStatus = Object.freeze({
    state: "live",
    generation: 1,
    reconnectAttempt: 0,
  });

  start(): void {}
  stop(): void {}
  ping(): void {}

  replaceSymbols(symbols: readonly string[]): void {
    this.desiredHistory.push([...symbols]);
  }

  subscribe(listener: SourceEventListener) {
    this.events.add(listener);
    return () => this.events.delete(listener);
  }

  subscribeStatus(listener: SourceStatusListener) {
    this.statuses.add(listener);
    return () => this.statuses.delete(listener);
  }

  getStatus(): SourceStatus {
    return this.status;
  }

  emit(event: MarketDataSourceEvent): void {
    for (const listener of this.events) listener(event);
  }

  setStatus(status: SourceStatus): void {
    this.status = Object.freeze(status);
    for (const listener of this.statuses) listener(this.status);
  }
}

describe("SmartSubscriptionManager", () => {
  it("ref-counts consumers, deduplicates intent, and sends sorted desired symbols", () => {
    const source = new FakeSource();
    const manager = new SmartSubscriptionManager(source, { autoStart: false });
    manager.subscribe("chart", "MSFT.US");
    manager.subscribe("chart", "MSFT.US");
    manager.subscribe("watchlist", "MSFT.US");
    manager.subscribe("chart", "AAPL.US");

    const snapshot = manager.getSnapshot();
    expect(snapshot.totalReferences).toBe(3);
    expect(snapshot.uniqueSubscriptions).toBe(2);
    expect(source.desiredHistory.at(-1)).toEqual(["AAPL.US", "MSFT.US"]);
    manager.unsubscribe("chart", "MSFT.US");
    expect(manager.getSnapshot().subscriptions.find((entry) => entry.key.symbol === "MSFT.US")?.refCount).toBe(1);
  });

  it("accepts a fresh snapshot before ack, but ack alone remains pending", () => {
    const source = new FakeSource();
    const manager = new SmartSubscriptionManager(source, { autoStart: false });
    const dataListener = vi.fn();
    manager.subscribeData(dataListener);
    manager.subscribe("chart", "AAPL.US");

    source.emit(makePoint(1, { origin: "snapshot", generation: 1 }));
    expect(dataListener).toHaveBeenCalledOnce();
    expect(manager.getSnapshot().subscriptions[0]?.status).toBe("pending");

    source.emit({
      type: "subscription-ack",
      action: "subscribe",
      symbols: ["AAPL.US"],
      generation: 1,
    });
    expect(manager.getSnapshot().subscriptions[0]?.status).toBe("live");

    manager.subscribe("other", "MSFT.US");
    source.emit({
      type: "subscription-ack",
      action: "subscribe",
      symbols: ["MSFT.US"],
      generation: 1,
    });
    expect(
      manager.getSnapshot().subscriptions.find((entry) => entry.key.symbol === "MSFT.US")
        ?.status,
    ).toBe("pending");
    source.emit(
      makePoint(2, {
        origin: "snapshot",
        generation: 1,
        stale: true,
        quote: { symbol: "MSFT.US" },
      }),
    );
    expect(
      manager.getSnapshot().subscriptions.find((entry) => entry.key.symbol === "MSFT.US")
        ?.status,
    ).toBe("pending");
    source.emit(
      makePoint(3, {
        origin: "live",
        generation: 1,
        quote: { symbol: "MSFT.US" },
      }),
    );
    expect(
      manager.getSnapshot().subscriptions.find((entry) => entry.key.symbol === "MSFT.US")
        ?.status,
    ).toBe("live");
  });

  it("clears ack and freshness on generation change and ignores stale generation data", () => {
    const source = new FakeSource();
    const manager = new SmartSubscriptionManager(source, { autoStart: false });
    const dataListener = vi.fn();
    manager.subscribeData(dataListener);
    manager.subscribe("chart", "AAPL.US");
    source.emit(makePoint(1, { origin: "snapshot", generation: 1 }));
    source.emit({
      type: "subscription-ack",
      action: "subscribe",
      symbols: ["AAPL.US"],
      generation: 1,
    });
    expect(manager.getSnapshot().subscriptions[0]?.status).toBe("live");

    source.setStatus({ state: "reconnecting", generation: 2, reconnectAttempt: 1 });
    expect(manager.getSnapshot().subscriptions[0]?.status).toBe("pending");
    source.emit(makePoint(2, { generation: 1 }));
    expect(dataListener).toHaveBeenCalledTimes(1);
  });
});
