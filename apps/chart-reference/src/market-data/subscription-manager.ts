import {
  type MarketDataPoint,
  type MarketDataSource,
  type SourceStatus,
  type Unsubscribe,
} from "./source";
import { symbolSchema } from "./types";

export type MarketDataChannel = "quote";

export interface SubscriptionKey {
  readonly channel: MarketDataChannel;
  readonly symbol: string;
}

export type ManagedSubscriptionStatus = "pending" | "live";

export interface ManagedSubscription {
  readonly key: SubscriptionKey;
  readonly refCount: number;
  readonly consumers: readonly string[];
  readonly status: ManagedSubscriptionStatus;
  readonly generation: number;
}

export interface SubscriptionManagerSnapshot {
  readonly generation: number;
  readonly sourceStatus: SourceStatus;
  readonly subscriptions: readonly ManagedSubscription[];
  readonly totalReferences: number;
  readonly uniqueSubscriptions: number;
}

export interface SmartSubscriptionManagerOptions {
  readonly autoStart?: boolean;
  readonly stopSourceOnDispose?: boolean;
}

type ManagerListener = () => void;
type DataListener = (point: MarketDataPoint) => void;

/** Ref-counts consumer intent and sends only sorted, deduplicated desired symbols. */
export class SmartSubscriptionManager {
  private readonly references = new Map<string, Set<string>>();
  private readonly managerListeners = new Set<ManagerListener>();
  private readonly dataListeners = new Set<DataListener>();
  private readonly acknowledgedSymbols = new Set<string>();
  private readonly freshSymbols = new Set<string>();
  private readonly autoStart: boolean;
  private readonly stopSourceOnDispose: boolean;
  private readonly unsubscribeSourceEvents: Unsubscribe;
  private readonly unsubscribeSourceStatus: Unsubscribe;
  private sourceStatus: SourceStatus;
  private snapshot: SubscriptionManagerSnapshot;
  private disposed = false;

  constructor(
    readonly source: MarketDataSource,
    options: SmartSubscriptionManagerOptions = {},
  ) {
    this.autoStart = options.autoStart ?? true;
    this.stopSourceOnDispose = options.stopSourceOnDispose ?? true;
    this.sourceStatus = source.getStatus();
    this.snapshot = this.buildSnapshot();
    this.unsubscribeSourceEvents = source.subscribe((event) => {
      if (event.generation !== this.sourceStatus.generation) return;
      if (event.type === "subscription-ack") {
        for (const symbol of event.symbols) {
          if (event.action === "subscribe") this.acknowledgedSymbols.add(symbol);
          else {
            this.acknowledgedSymbols.delete(symbol);
            this.freshSymbols.delete(symbol);
          }
        }
        this.publishSnapshot();
      } else if (event.type === "market-data") {
        const key = serializeKey({ channel: "quote", symbol: event.quote.symbol });
        if (!this.references.has(key)) return;
        // Snapshots are valid before the subscribe ack; generation, not ack order, fences them.
        // A current-generation live quote is the explicit fallback when no cache snapshot exists.
        if (!event.stale) {
          const changed = !this.freshSymbols.has(event.quote.symbol);
          this.freshSymbols.add(event.quote.symbol);
          if (changed) this.publishSnapshot();
        }
        for (const listener of [...this.dataListeners]) listener(event);
      }
    });
    this.unsubscribeSourceStatus = source.subscribeStatus((status) => {
      if (status.generation !== this.sourceStatus.generation) {
        this.acknowledgedSymbols.clear();
        this.freshSymbols.clear();
      }
      this.sourceStatus = status;
      this.publishSnapshot();
    });
  }

  subscribe(
    consumerId: string,
    input: SubscriptionKey | string,
  ): Unsubscribe {
    this.assertUsable();
    const key = normalizeKey(input);
    const normalizedConsumerId = nonEmptyConsumer(consumerId);
    const serialized = serializeKey(key);
    let consumers = this.references.get(serialized);
    if (consumers == null) {
      consumers = new Set();
      this.references.set(serialized, consumers);
    }
    const changed = !consumers.has(normalizedConsumerId);
    consumers.add(normalizedConsumerId);
    if (changed) {
      this.syncDesiredSymbols();
      this.publishSnapshot();
    }
    if (this.autoStart && isStartable(this.sourceStatus.state)) this.source.start();
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      this.unsubscribe(normalizedConsumerId, key);
    };
  }

  unsubscribe(consumerId: string, input: SubscriptionKey | string): void {
    if (this.disposed) return;
    const normalizedConsumerId = nonEmptyConsumer(consumerId);
    const key = normalizeKey(input);
    const serialized = serializeKey(key);
    const consumers = this.references.get(serialized);
    if (consumers == null || !consumers.delete(normalizedConsumerId)) return;
    if (consumers.size === 0) {
      this.references.delete(serialized);
      this.acknowledgedSymbols.delete(key.symbol);
      this.freshSymbols.delete(key.symbol);
    }
    this.syncDesiredSymbols();
    this.publishSnapshot();
  }

  replaceConsumerSubscriptions(
    consumerId: string,
    inputs: readonly (SubscriptionKey | string)[],
  ): void {
    this.assertUsable();
    const normalizedConsumerId = nonEmptyConsumer(consumerId);
    const desired = new Set(inputs.map((input) => serializeKey(normalizeKey(input))));
    let changed = false;
    for (const [serialized, consumers] of this.references) {
      if (consumers.has(normalizedConsumerId) && !desired.has(serialized)) {
        consumers.delete(normalizedConsumerId);
        if (consumers.size === 0) this.references.delete(serialized);
        changed = true;
      }
    }
    for (const serialized of desired) {
      let consumers = this.references.get(serialized);
      if (consumers == null) {
        consumers = new Set();
        this.references.set(serialized, consumers);
      }
      if (!consumers.has(normalizedConsumerId)) {
        consumers.add(normalizedConsumerId);
        changed = true;
      }
    }
    if (!changed) return;
    this.syncDesiredSymbols();
    if (this.references.size > 0 && this.autoStart && isStartable(this.sourceStatus.state)) {
      this.source.start();
    }
    this.publishSnapshot();
  }

  start(): void {
    this.assertUsable();
    this.source.start();
  }

  stop(): void {
    if (this.disposed) return;
    this.source.stop();
  }

  subscribeSnapshot(listener: ManagerListener): Unsubscribe {
    this.managerListeners.add(listener);
    return () => this.managerListeners.delete(listener);
  }

  /** Alias matching the useSyncExternalStore store contract. */
  onChange(listener: ManagerListener): Unsubscribe {
    return this.subscribeSnapshot(listener);
  }

  subscribeData(listener: DataListener): Unsubscribe {
    this.dataListeners.add(listener);
    return () => this.dataListeners.delete(listener);
  }

  getSnapshot(): SubscriptionManagerSnapshot {
    return this.snapshot;
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.unsubscribeSourceEvents();
    this.unsubscribeSourceStatus();
    this.references.clear();
    this.acknowledgedSymbols.clear();
    this.freshSymbols.clear();
    this.managerListeners.clear();
    this.dataListeners.clear();
    this.source.replaceSymbols([]);
    if (this.stopSourceOnDispose) this.source.stop();
  }

  private syncDesiredSymbols(): void {
    const symbols = [...this.references.keys()]
      .map((serialized) => deserializeKey(serialized).symbol)
      .sort();
    this.source.replaceSymbols(symbols);
  }

  private publishSnapshot(): void {
    this.snapshot = this.buildSnapshot();
    for (const listener of [...this.managerListeners]) listener();
  }

  private buildSnapshot(): SubscriptionManagerSnapshot {
    const subscriptions = [...this.references.entries()]
      .map(([serialized, consumers]): ManagedSubscription => {
        const key = deserializeKey(serialized);
        return Object.freeze({
          key: Object.freeze(key),
          refCount: consumers.size,
          consumers: Object.freeze([...consumers].sort()),
          status:
            this.sourceStatus.state === "live" &&
            this.acknowledgedSymbols.has(key.symbol) &&
            this.freshSymbols.has(key.symbol)
              ? "live"
              : "pending",
          generation: this.sourceStatus.generation,
        });
      })
      .sort((left, right) => serializeKey(left.key).localeCompare(serializeKey(right.key)));
    return Object.freeze({
      generation: this.sourceStatus.generation,
      sourceStatus: this.sourceStatus,
      subscriptions: Object.freeze(subscriptions),
      totalReferences: subscriptions.reduce((sum, entry) => sum + entry.refCount, 0),
      uniqueSubscriptions: subscriptions.length,
    });
  }

  private assertUsable(): void {
    if (this.disposed) throw new Error("SmartSubscriptionManager is disposed");
  }
}

function normalizeKey(input: SubscriptionKey | string): SubscriptionKey {
  const candidate =
    typeof input === "string"
      ? { channel: "quote" as const, symbol: input }
      : input;
  if (candidate.channel !== "quote") {
    throw new Error("Unsupported market-data channel");
  }
  const symbol = symbolSchema.parse(candidate.symbol.trim().toUpperCase());
  return Object.freeze({ channel: "quote", symbol });
}

function serializeKey(key: SubscriptionKey): string {
  return `${key.channel}:${key.symbol}`;
}

function deserializeKey(serialized: string): SubscriptionKey {
  const separator = serialized.indexOf(":");
  return {
    channel: serialized.slice(0, separator) as MarketDataChannel,
    symbol: serialized.slice(separator + 1),
  };
}

function nonEmptyConsumer(consumerId: string): string {
  const normalized = consumerId.trim();
  if (!normalized) throw new Error("consumerId must not be empty");
  return normalized;
}

function isStartable(state: SourceStatus["state"]): boolean {
  return state === "idle" || state === "stopped";
}
