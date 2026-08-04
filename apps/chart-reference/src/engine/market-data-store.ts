import type { MarketDataPoint, MarketDataSource, SourceStatus, Unsubscribe } from "../market-data";
import type { SmartSubscriptionManager } from "../market-data/subscription-manager";
import { CandleAggregator } from "./candle-aggregator";
import {
  TemporalIntegrityGuard,
  type IntegrityDecision,
  type QuarantinedMarketData,
  type TemporalIntegrityGuardOptions,
} from "./temporal-integrity";
import type { AcceptedMarketDataLogEntry, Candle, Timeframe } from "./types";

export interface MarketDataStoreSnapshot {
  readonly revision: number;
  readonly timeframe: Timeframe;
  readonly connectionStatus: SourceStatus;
  readonly latestQuotes: Readonly<Record<string, MarketDataPoint["quote"]>>;
  readonly latestPoints: Readonly<Record<string, MarketDataPoint>>;
  readonly candles: Readonly<Record<string, readonly Candle[]>>;
  readonly acceptedLog: readonly AcceptedMarketDataLogEntry[];
  readonly quarantineLog: readonly QuarantinedMarketData[];
  readonly duplicateCounts: Readonly<Record<string, number>>;
}
export interface MarketDataStoreOptions {
  readonly timeframe?: Timeframe;
  readonly eventRetentionPerSymbol?: number;
  readonly candleRetentionPerSymbol?: number;
  readonly logRetention?: number;
  readonly integrity?: TemporalIntegrityGuardOptions;
}
type StoreListener = () => void;

/** Framework-neutral external store; getSnapshot is referentially stable between changes. */
export class MarketDataStore {
  private readonly listeners = new Set<StoreListener>();
  private readonly eventsBySymbol = new Map<string, MarketDataPoint[]>();
  private readonly latestBySymbol = new Map<string, MarketDataPoint>();
  private readonly candleSnapshotsBySymbol = new Map<string, readonly Candle[]>();
  private readonly duplicateCountsBySymbol = new Map<string, number>();
  private readonly acceptedEntries: AcceptedMarketDataLogEntry[] = [];
  private readonly quarantineEntries: QuarantinedMarketData[] = [];
  private readonly eventRetentionPerSymbol: number;
  private readonly candleRetentionPerSymbol: number;
  private readonly logRetention: number;
  private readonly integrityGuard: TemporalIntegrityGuard;
  private aggregator: CandleAggregator;
  private timeframe: Timeframe;
  private connectionStatus: SourceStatus = Object.freeze({ state: "idle", generation: 0, reconnectAttempt: 0 });
  private revision = 0;
  private batchDepth = 0;
  private batchDirty = false;
  private snapshot: MarketDataStoreSnapshot;
  private detachFeed: Unsubscribe | undefined;

  constructor(options: MarketDataStoreOptions = {}) {
    this.timeframe = options.timeframe ?? "1m";
    this.eventRetentionPerSymbol = positiveInteger(options.eventRetentionPerSymbol ?? 10_000, "eventRetentionPerSymbol");
    this.candleRetentionPerSymbol = positiveInteger(options.candleRetentionPerSymbol ?? 2_000, "candleRetentionPerSymbol");
    this.logRetention = positiveInteger(options.logRetention ?? 500, "logRetention");
    this.integrityGuard = new TemporalIntegrityGuard({ quarantineRetention: this.logRetention, ...options.integrity });
    this.aggregator = this.createAggregator(this.timeframe);
    this.snapshot = this.buildSnapshot();
  }

  subscribe(listener: StoreListener): Unsubscribe { this.listeners.add(listener); return () => this.listeners.delete(listener); }
  getSnapshot(): MarketDataStoreSnapshot { return this.snapshot; }

  ingest(point: MarketDataPoint): IntegrityDecision {
    const decision = this.integrityGuard.evaluate(point);
    const symbol = point.quote.symbol.trim().toUpperCase();
    if (decision.outcome === "accepted") {
      let retained = this.eventsBySymbol.get(symbol);
      if (retained == null) { retained = []; this.eventsBySymbol.set(symbol, retained); }
      retained.push(point);
      trimStart(retained, this.eventRetentionPerSymbol);
      this.latestBySymbol.set(symbol, point);
      this.aggregator.ingest(point);
      this.refreshCandleSnapshot(symbol);
      this.acceptedEntries.push(Object.freeze({
        quote: point.quote, generation: point.generation, acceptedReason: decision.reason,
      }));
      trimStart(this.acceptedEntries, this.logRetention);
      this.publish();
    } else if (decision.outcome === "duplicate") {
      this.duplicateCountsBySymbol.set(symbol, (this.duplicateCountsBySymbol.get(symbol) ?? 0) + 1);
      let latest = this.latestBySymbol.get(symbol);
      if (latest == null) {
        const retained = this.eventsBySymbol.get(symbol);
        if (retained != null) {
          for (let index = retained.length - 1; index >= 0; index -= 1) {
            const candidate = retained[index];
            if (candidate?.quote.eventId === point.quote.eventId) { latest = candidate; break; }
          }
        }
      }
      if (latest != null && latest.quote.eventId === point.quote.eventId && point.generation >= latest.generation) {
        this.latestBySymbol.set(symbol, Object.freeze({ ...point, quote: latest.quote }));
      }
      this.publish();
    } else {
      this.quarantineEntries.push(decision.quarantine);
      trimStart(this.quarantineEntries, this.logRetention);
      this.publish();
    }
    return decision;
  }

  ingestMany(points: readonly MarketDataPoint[]): readonly IntegrityDecision[] {
    this.batchDepth += 1;
    try {
      return Object.freeze(points.map((point) => this.ingest(point)));
    } finally {
      this.batchDepth -= 1;
      if (this.batchDepth === 0 && this.batchDirty) {
        this.batchDirty = false;
        this.commitPublish();
      }
    }
  }

  setTimeframe(timeframe: Timeframe): void {
    if (timeframe === this.timeframe) return;
    this.timeframe = timeframe;
    this.aggregator = this.createAggregator(timeframe);
    this.aggregator.rebuild([...this.eventsBySymbol.values()].flatMap((points) => points));
    this.rebuildCandleSnapshots();
    this.publish();
  }

  setConnectionStatus(status: SourceStatus): void {
    if (sameStatus(this.connectionStatus, status)) return;
    const generationChanged = status.generation !== this.connectionStatus.generation;
    this.connectionStatus = Object.freeze({ ...status });
    if (generationChanged) this.latestBySymbol.clear();
    this.publish();
  }

  attachSource(source: MarketDataSource): Unsubscribe {
    this.detachCurrentFeed();
    this.setConnectionStatus(source.getStatus());
    const unsubscribeEvents = source.subscribe((event) => { if (event.type === "market-data") this.ingest(event); });
    const unsubscribeStatus = source.subscribeStatus((status) => this.setConnectionStatus(status));
    const detach = () => {
      unsubscribeEvents(); unsubscribeStatus();
      if (this.detachFeed === detach) this.detachFeed = undefined;
    };
    this.detachFeed = detach;
    return detach;
  }

  attachSubscriptionManager(manager: SmartSubscriptionManager): Unsubscribe {
    this.detachCurrentFeed();
    this.setConnectionStatus(manager.getSnapshot().sourceStatus);
    const unsubscribeData = manager.subscribeData((point) => this.ingest(point));
    const unsubscribeSnapshot = manager.subscribeSnapshot(() => this.setConnectionStatus(manager.getSnapshot().sourceStatus));
    const detach = () => {
      unsubscribeData(); unsubscribeSnapshot();
      if (this.detachFeed === detach) this.detachFeed = undefined;
    };
    this.detachFeed = detach;
    return detach;
  }

  clear(): void {
    this.integrityGuard.reset();
    this.eventsBySymbol.clear();
    this.latestBySymbol.clear();
    this.candleSnapshotsBySymbol.clear();
    this.duplicateCountsBySymbol.clear();
    this.acceptedEntries.length = 0;
    this.quarantineEntries.length = 0;
    this.aggregator.reset();
    this.publish();
  }
  dispose(): void { this.detachCurrentFeed(); this.listeners.clear(); }

  private createAggregator(timeframe: Timeframe): CandleAggregator {
    return new CandleAggregator(timeframe, { maxCandlesPerSymbol: this.candleRetentionPerSymbol });
  }
  private refreshCandleSnapshot(symbol: string): void {
    this.candleSnapshotsBySymbol.set(symbol, this.aggregator.getCandles(symbol));
  }
  private rebuildCandleSnapshots(): void {
    this.candleSnapshotsBySymbol.clear();
    for (const symbol of this.aggregator.getSymbols()) this.refreshCandleSnapshot(symbol);
  }
  private detachCurrentFeed(): void {
    const detach = this.detachFeed;
    this.detachFeed = undefined;
    detach?.();
  }
  private publish(): void {
    if (this.batchDepth > 0) { this.batchDirty = true; return; }
    this.commitPublish();
  }
  private commitPublish(): void {
    this.revision += 1;
    this.snapshot = this.buildSnapshot();
    for (const listener of [...this.listeners]) listener();
  }
  private buildSnapshot(): MarketDataStoreSnapshot {
    const latestQuotes: Record<string, MarketDataPoint["quote"]> = {};
    const latestPoints: Record<string, MarketDataPoint> = {};
    for (const [symbol, point] of [...this.latestBySymbol.entries()].sort(([left], [right]) => left.localeCompare(right))) {
      latestQuotes[symbol] = point.quote;
      latestPoints[symbol] = point;
    }
    const duplicateCounts: Record<string, number> = {};
    for (const [symbol, count] of [...this.duplicateCountsBySymbol.entries()].sort(([left], [right]) => left.localeCompare(right))) {
      duplicateCounts[symbol] = count;
    }
    const candles: Record<string, readonly Candle[]> = {};
    for (const [symbol, snapshot] of [...this.candleSnapshotsBySymbol.entries()].sort(([left], [right]) => left.localeCompare(right))) {
      candles[symbol] = snapshot;
    }
    return Object.freeze({
      revision: this.revision,
      timeframe: this.timeframe,
      connectionStatus: this.connectionStatus,
      latestQuotes: Object.freeze(latestQuotes),
      latestPoints: Object.freeze(latestPoints),
      candles: Object.freeze(candles),
      acceptedLog: Object.freeze([...this.acceptedEntries]),
      quarantineLog: Object.freeze([...this.quarantineEntries]),
      duplicateCounts: Object.freeze(duplicateCounts),
    });
  }
}

function trimStart<T>(items: T[], maximum: number): void {
  if (items.length > maximum) items.splice(0, items.length - maximum);
}
function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive integer`);
  return value;
}
function sameStatus(left: SourceStatus, right: SourceStatus): boolean {
  return left.state === right.state
    && left.generation === right.generation
    && left.reconnectAttempt === right.reconnectAttempt
    && left.nextRetryMs === right.nextRetryMs
    && left.lastError === right.lastError;
}
