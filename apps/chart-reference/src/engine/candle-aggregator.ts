import type { MarketDataPoint } from "../market-data";
import {
  TIMEFRAME_DURATION_MS,
  type Candle,
  type CandleVolumeSource,
  type Timeframe,
} from "./types";

export interface CandleAggregationUpdate {
  readonly candle: Candle;
  readonly newlyCompleted: readonly Candle[];
}

export interface CandleAggregatorOptions {
  readonly maxCandlesPerSymbol?: number;
}

interface MutableCandle {
  symbol: string;
  openTimeMs: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  volumeSources: Set<Exclude<CandleVolumeSource, "mixed">>;
  updateCount: number;
  firstEventId: string;
  lastEventId: string;
  revision: number;
}

/** Event-time OHLCV aggregation with deterministic UTC-aligned fixed buckets. */
export class CandleAggregator {
  private readonly candlesBySymbol = new Map<string, Map<number, MutableCandle>>();
  private readonly latestBucketBySymbol = new Map<string, number>();
  private readonly cumulativeVolumeBySymbol = new Map<string, number>();
  private readonly maxCandlesPerSymbol: number;

  constructor(
    readonly timeframe: Timeframe,
    options: CandleAggregatorOptions = {},
  ) {
    this.maxCandlesPerSymbol = positiveInteger(
      options.maxCandlesPerSymbol ?? 2_000,
      "maxCandlesPerSymbol",
    );
  }

  ingest(point: MarketDataPoint): CandleAggregationUpdate {
    const { quote } = point;
    const openTimeMs = bucketStart(quote.providerTimestampMs, this.timeframe);
    let candles = this.candlesBySymbol.get(quote.symbol);
    if (candles == null) {
      candles = new Map();
      this.candlesBySymbol.set(quote.symbol, candles);
    }

    const previousLatest = this.latestBucketBySymbol.get(quote.symbol);
    const newlyCompleted: Candle[] = [];
    if (previousLatest == null || openTimeMs > previousLatest) {
      if (previousLatest != null) {
        const previous = candles.get(previousLatest);
        if (previous != null) newlyCompleted.push(this.materialize(previous, true));
      }
      this.latestBucketBySymbol.set(quote.symbol, openTimeMs);
    }

    const volume = this.volumeContribution(point);
    let candle = candles.get(openTimeMs);
    if (candle == null) {
      candle = {
        symbol: quote.symbol,
        openTimeMs,
        open: quote.price,
        high: quote.price,
        low: quote.price,
        close: quote.price,
        volume: volume.amount,
        volumeSources: new Set([volume.source]),
        updateCount: 1,
        firstEventId: quote.eventId,
        lastEventId: quote.eventId,
        revision: 1,
      };
      candles.set(openTimeMs, candle);
    } else {
      candle.high = Math.max(candle.high, quote.price);
      candle.low = Math.min(candle.low, quote.price);
      candle.close = quote.price;
      candle.volume += volume.amount;
      candle.volumeSources.add(volume.source);
      candle.updateCount += 1;
      candle.lastEventId = quote.eventId;
      candle.revision += 1;
    }

    this.trim(quote.symbol, candles);
    const latest = this.latestBucketBySymbol.get(quote.symbol);
    return Object.freeze({
      candle: this.materialize(candle, latest != null && openTimeMs < latest),
      newlyCompleted: Object.freeze(newlyCompleted),
    });
  }

  getCandles(symbol: string): readonly Candle[] {
    const normalized = symbol.trim().toUpperCase();
    const candles = this.candlesBySymbol.get(normalized);
    if (candles == null) return Object.freeze([]);
    const latest = this.latestBucketBySymbol.get(normalized);
    return Object.freeze(
      [...candles.values()]
        .sort((left, right) => left.openTimeMs - right.openTimeMs)
        .map((candle) =>
          this.materialize(
            candle,
            latest != null && candle.openTimeMs < latest,
          ),
        ),
    );
  }

  getSymbols(): readonly string[] {
    return Object.freeze([...this.candlesBySymbol.keys()].sort());
  }

  rebuild(points: readonly MarketDataPoint[]): void {
    this.reset();
    const ordered = [...points].sort(comparePoints);
    for (const point of ordered) this.ingest(point);
  }

  reset(): void {
    this.candlesBySymbol.clear();
    this.latestBucketBySymbol.clear();
    this.cumulativeVolumeBySymbol.clear();
  }

  private volumeContribution(point: MarketDataPoint): {
    amount: number;
    source: Exclude<CandleVolumeSource, "mixed">;
  } {
    const { quote } = point;
    if (quote.lastSize != null) {
      if (quote.cumulativeVolume != null) {
        this.cumulativeVolumeBySymbol.set(quote.symbol, quote.cumulativeVolume);
      }
      return { amount: quote.lastSize, source: "last-size" };
    }
    if (quote.cumulativeVolume != null) {
      const previous = this.cumulativeVolumeBySymbol.get(quote.symbol);
      this.cumulativeVolumeBySymbol.set(quote.symbol, quote.cumulativeVolume);
      if (previous == null) return { amount: 0, source: "cumulative-delta" };
      return {
        amount:
          quote.cumulativeVolume >= previous
            ? quote.cumulativeVolume - previous
            : quote.cumulativeVolume,
        source: "cumulative-delta",
      };
    }
    return { amount: 1, source: "update-count" };
  }

  private materialize(candle: MutableCandle, complete: boolean): Candle {
    const durationMs = TIMEFRAME_DURATION_MS[this.timeframe];
    const volumeSource: CandleVolumeSource =
      candle.volumeSources.size === 1
        ? [...candle.volumeSources][0]!
        : "mixed";
    return Object.freeze({
      symbol: candle.symbol,
      timeframe: this.timeframe,
      openTimeMs: candle.openTimeMs,
      closeTimeMs: candle.openTimeMs + durationMs,
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
      volume: candle.volume,
      volumeSource,
      updateCount: candle.updateCount,
      firstEventId: candle.firstEventId,
      lastEventId: candle.lastEventId,
      revision: candle.revision,
      complete,
    });
  }

  private trim(symbol: string, candles: Map<number, MutableCandle>): void {
    while (candles.size > this.maxCandlesPerSymbol) {
      const oldest = Math.min(...candles.keys());
      candles.delete(oldest);
    }
    if (candles.size === 0) {
      this.candlesBySymbol.delete(symbol);
      this.latestBucketBySymbol.delete(symbol);
    }
  }
}

export function bucketStart(timestampMs: number, timeframe: Timeframe): number {
  if (!Number.isFinite(timestampMs) || timestampMs < 0) {
    throw new RangeError("timestampMs must be a non-negative finite number");
  }
  const durationMs = TIMEFRAME_DURATION_MS[timeframe];
  return Math.floor(timestampMs / durationMs) * durationMs;
}

function comparePoints(left: MarketDataPoint, right: MarketDataPoint): number {
  return (
    left.quote.providerTimestampMs - right.quote.providerTimestampMs ||
    left.quote.receivedAtMs - right.quote.receivedAtMs ||
    left.quote.eventId.localeCompare(right.quote.eventId)
  );
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive integer`);
  }
  return value;
}
