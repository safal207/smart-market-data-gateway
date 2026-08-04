import {
  type MarketDataPoint,
  type MarketDataSource,
  type MarketDataSourceEvent,
  type SourceEventListener,
  type SourceStatus,
  type SourceStatusListener,
  type Unsubscribe,
  normalizeSymbols,
} from "./source";
import type { NormalizedQuote } from "./types";

export interface ReplayFrame {
  readonly atMs: number;
  readonly quote: NormalizedQuote;
  readonly origin?: "replay" | "snapshot";
  readonly stale?: boolean;
  readonly ageMs?: number;
}

interface OrderedReplayFrame extends ReplayFrame {
  readonly ordinal: number;
}

/**
 * A virtual-clock source. It never schedules real timers, so a test or demo owns
 * every transition by calling advanceBy/advanceTo.
 */
export class ReplayMarketDataSource implements MarketDataSource {
  private readonly frames: readonly OrderedReplayFrame[];
  private readonly eventListeners = new Set<SourceEventListener>();
  private readonly statusListeners = new Set<SourceStatusListener>();
  private desiredSymbols: readonly string[] = Object.freeze([]);
  private desiredSymbolSet = new Set<string>();
  private cursorMs = 0;
  private nextFrameIndex = 0;
  private generation = 0;
  private running = false;
  private status: SourceStatus = Object.freeze({
    state: "idle",
    generation: 0,
    reconnectAttempt: 0,
  });

  constructor(frames: readonly ReplayFrame[]) {
    this.frames = Object.freeze(
      frames
        .map((frame, ordinal) => {
          if (!Number.isFinite(frame.atMs) || frame.atMs < 0) {
            throw new RangeError("Replay frame atMs must be a non-negative finite number");
          }
          return Object.freeze({ ...frame, ordinal });
        })
        .sort((left, right) => left.atMs - right.atMs || left.ordinal - right.ordinal),
    );
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.generation += 1;
    this.setStatus({
      state: "live",
      generation: this.generation,
      reconnectAttempt: 0,
    });
    this.emitSubscriptionDiff([], this.desiredSymbols);
  }

  stop(): void {
    if (!this.running && this.status.state === "stopped") return;
    this.running = false;
    this.generation += 1;
    this.setStatus({
      state: "stopped",
      generation: this.generation,
      reconnectAttempt: 0,
    });
  }

  replaceSymbols(symbols: readonly string[]): void {
    const next = normalizeSymbols(symbols);
    if (sameSymbols(this.desiredSymbols, next)) return;
    const previous = this.desiredSymbols;
    this.desiredSymbols = next;
    this.desiredSymbolSet = new Set(next);
    if (this.running) this.emitSubscriptionDiff(previous, next);
  }

  ping(): void {
    if (!this.running) return;
    this.emit({
      type: "pong",
      generation: this.generation,
      requestId: `replay-ping-${this.generation}-${this.cursorMs}`,
    });
  }

  subscribe(listener: SourceEventListener): Unsubscribe {
    this.eventListeners.add(listener);
    return () => this.eventListeners.delete(listener);
  }

  subscribeStatus(listener: SourceStatusListener): Unsubscribe {
    this.statusListeners.add(listener);
    return () => this.statusListeners.delete(listener);
  }

  getStatus(): SourceStatus {
    return this.status;
  }

  get nowMs(): number {
    return this.cursorMs;
  }

  get exhausted(): boolean {
    return this.nextFrameIndex >= this.frames.length;
  }

  advanceBy(deltaMs: number): readonly MarketDataPoint[] {
    if (!Number.isFinite(deltaMs) || deltaMs < 0) {
      throw new RangeError("Replay delta must be a non-negative finite number");
    }
    return this.advanceTo(this.cursorMs + deltaMs);
  }

  advanceTo(targetMs: number): readonly MarketDataPoint[] {
    if (!Number.isFinite(targetMs) || targetMs < this.cursorMs) {
      throw new RangeError("Replay clock cannot move backwards; call seek or reset instead");
    }
    this.cursorMs = targetMs;
    if (!this.running) return Object.freeze([]);

    const emitted: MarketDataPoint[] = [];
    while (
      this.nextFrameIndex < this.frames.length &&
      this.frames[this.nextFrameIndex]!.atMs <= targetMs
    ) {
      const frame = this.frames[this.nextFrameIndex]!;
      this.nextFrameIndex += 1;
      if (!this.desiredSymbolSet.has(frame.quote.symbol)) continue;
      const event: MarketDataPoint = Object.freeze({
        type: "market-data",
        quote: frame.quote,
        origin: frame.origin ?? "replay",
        generation: this.generation,
        stale: frame.stale ?? false,
        ageMs: frame.ageMs ?? 0,
      });
      emitted.push(event);
      this.emit(event);
    }
    return Object.freeze(emitted);
  }

  /** Seek begins a new logical generation so consumers can fence old results. */
  seek(targetMs: number): void {
    if (!Number.isFinite(targetMs) || targetMs < 0) {
      throw new RangeError("Replay seek target must be a non-negative finite number");
    }
    this.cursorMs = targetMs;
    this.nextFrameIndex = this.frames.findIndex((frame) => frame.atMs >= targetMs);
    if (this.nextFrameIndex < 0) this.nextFrameIndex = this.frames.length;
    if (this.running) {
      this.generation += 1;
      this.setStatus({
        state: "live",
        generation: this.generation,
        reconnectAttempt: 0,
      });
      this.emitSubscriptionDiff([], this.desiredSymbols);
    }
  }

  reset(): void {
    this.seek(0);
  }

  private emitSubscriptionDiff(
    previous: readonly string[],
    next: readonly string[],
  ): void {
    const previousSet = new Set(previous);
    const nextSet = new Set(next);
    const removed = previous.filter((symbol) => !nextSet.has(symbol));
    const added = next.filter((symbol) => !previousSet.has(symbol));
    if (removed.length > 0) {
      this.emit({
        type: "subscription-ack",
        action: "unsubscribe",
        symbols: Object.freeze(removed),
        generation: this.generation,
        requestId: `replay-unsub-${this.generation}-${this.cursorMs}`,
      });
    }
    if (added.length > 0) {
      this.emit({
        type: "subscription-ack",
        action: "subscribe",
        symbols: Object.freeze(added),
        generation: this.generation,
        requestId: `replay-sub-${this.generation}-${this.cursorMs}`,
      });
    }
  }

  private emit(event: MarketDataSourceEvent): void {
    for (const listener of [...this.eventListeners]) listener(event);
  }

  private setStatus(status: SourceStatus): void {
    this.status = Object.freeze({ ...status });
    for (const listener of [...this.statusListeners]) listener(this.status);
  }
}

function sameSymbols(left: readonly string[], right: readonly string[]): boolean {
  return (
    left.length === right.length &&
    left.every((symbol, index) => symbol === right[index])
  );
}
