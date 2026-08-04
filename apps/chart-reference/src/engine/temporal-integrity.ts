import type { MarketDataPoint } from "../market-data";

export type IntegrityAcceptedReason =
  | "initial"
  | "advanced"
  | "same_timestamp"
  | "generation_advanced"
  | "sequence_gap";

export type QuarantineReason =
  | "stale_generation"
  | "timestamp_rollback"
  | "sequence_regression";

export interface AcceptedDecision {
  readonly outcome: "accepted";
  readonly reason: IntegrityAcceptedReason;
  readonly point: MarketDataPoint;
  readonly sequenceGap?: number;
}

export interface DuplicateDecision {
  readonly outcome: "duplicate";
  readonly reason: "duplicate_event";
  readonly point: MarketDataPoint;
}

export interface QuarantinedMarketData {
  readonly reason: QuarantineReason;
  readonly point: MarketDataPoint;
  readonly expectedGeneration: number;
  readonly previousProviderTimestampMs?: number;
  readonly previousSequence?: number;
  readonly quarantinedAtMs: number;
}

export interface QuarantinedDecision {
  readonly outcome: "quarantined";
  readonly reason: QuarantineReason;
  readonly point: MarketDataPoint;
  readonly quarantine: QuarantinedMarketData;
}

export type IntegrityDecision =
  | AcceptedDecision
  | DuplicateDecision
  | QuarantinedDecision;

export interface TemporalIntegrityGuardOptions {
  readonly seenEventRetention?: number;
  readonly quarantineRetention?: number;
  readonly allowSequenceResetOnGeneration?: boolean;
}

interface StreamWatermark {
  generation: number;
  providerTimestampMs?: number;
  sequence?: number;
  pendingSequenceResetGeneration?: number;
}

/** Rejects old generations and temporal regressions before they reach chart state. */
export class TemporalIntegrityGuard {
  private readonly watermarks = new Map<string, StreamWatermark>();
  private readonly seenEventIds = new Set<string>();
  private readonly seenEventOrder: string[] = [];
  private readonly quarantineEntries: QuarantinedMarketData[] = [];
  private readonly seenEventRetention: number;
  private readonly quarantineRetention: number;
  private readonly allowSequenceResetOnGeneration: boolean;

  constructor(options: TemporalIntegrityGuardOptions = {}) {
    this.seenEventRetention = positiveInteger(
      options.seenEventRetention ?? 10_000,
      "seenEventRetention",
    );
    this.quarantineRetention = positiveInteger(
      options.quarantineRetention ?? 500,
      "quarantineRetention",
    );
    this.allowSequenceResetOnGeneration =
      options.allowSequenceResetOnGeneration ?? true;
  }

  evaluate(point: MarketDataPoint): IntegrityDecision {
    const key = streamKey(point);
    const watermark = this.watermarks.get(key) ?? { generation: point.generation };
    if (point.generation < watermark.generation) {
      return this.quarantine("stale_generation", point, watermark);
    }

    const generationAdvanced = point.generation > watermark.generation;
    if (generationAdvanced) {
      watermark.generation = point.generation;
      watermark.pendingSequenceResetGeneration = point.generation;
    }
    const canResetSequence =
      watermark.pendingSequenceResetGeneration === point.generation &&
      this.allowSequenceResetOnGeneration;
    this.watermarks.set(key, watermark);

    if (this.seenEventIds.has(point.quote.eventId)) {
      return Object.freeze({
        outcome: "duplicate",
        reason: "duplicate_event",
        point,
      });
    }

    if (
      watermark.providerTimestampMs != null &&
      point.quote.providerTimestampMs < watermark.providerTimestampMs
    ) {
      return this.quarantine("timestamp_rollback", point, watermark);
    }

    if (
      !canResetSequence &&
      watermark.sequence != null &&
      point.quote.sequence != null &&
      point.quote.sequence <= watermark.sequence
    ) {
      return this.quarantine("sequence_regression", point, watermark);
    }

    let reason: IntegrityAcceptedReason;
    let sequenceGap: number | undefined;
    if (watermark.providerTimestampMs == null) {
      reason = "initial";
    } else if (generationAdvanced || canResetSequence) {
      reason = "generation_advanced";
    } else if (
      watermark.sequence != null &&
      point.quote.sequence != null &&
      point.quote.sequence > watermark.sequence + 1
    ) {
      reason = "sequence_gap";
      sequenceGap = point.quote.sequence - watermark.sequence - 1;
    } else if (point.quote.providerTimestampMs === watermark.providerTimestampMs) {
      reason = "same_timestamp";
    } else {
      reason = "advanced";
    }

    watermark.providerTimestampMs = Math.max(
      watermark.providerTimestampMs ?? Number.NEGATIVE_INFINITY,
      point.quote.providerTimestampMs,
    );
    if (point.quote.sequence != null) watermark.sequence = point.quote.sequence;
    watermark.pendingSequenceResetGeneration = undefined;
    this.rememberEvent(point.quote.eventId);
    return Object.freeze({
      outcome: "accepted",
      reason,
      point,
      ...(sequenceGap == null ? {} : { sequenceGap }),
    });
  }

  getQuarantine(): readonly QuarantinedMarketData[] {
    return Object.freeze([...this.quarantineEntries]);
  }

  reset(): void {
    this.watermarks.clear();
    this.seenEventIds.clear();
    this.seenEventOrder.length = 0;
    this.quarantineEntries.length = 0;
  }

  private quarantine(
    reason: QuarantineReason,
    point: MarketDataPoint,
    watermark: StreamWatermark,
  ): QuarantinedDecision {
    const entry: QuarantinedMarketData = Object.freeze({
      reason,
      point,
      expectedGeneration: watermark.generation,
      ...(watermark.providerTimestampMs == null
        ? {}
        : { previousProviderTimestampMs: watermark.providerTimestampMs }),
      ...(watermark.sequence == null ? {} : { previousSequence: watermark.sequence }),
      quarantinedAtMs: point.quote.receivedAtMs,
    });
    this.quarantineEntries.push(entry);
    if (this.quarantineEntries.length > this.quarantineRetention) {
      this.quarantineEntries.splice(
        0,
        this.quarantineEntries.length - this.quarantineRetention,
      );
    }
    return Object.freeze({ outcome: "quarantined", reason, point, quarantine: entry });
  }

  private rememberEvent(eventId: string): void {
    this.seenEventIds.add(eventId);
    this.seenEventOrder.push(eventId);
    while (this.seenEventOrder.length > this.seenEventRetention) {
      const expired = this.seenEventOrder.shift();
      if (expired != null) this.seenEventIds.delete(expired);
    }
  }
}

function streamKey(point: MarketDataPoint): string {
  return `${point.quote.provider}:${point.quote.symbol}`;
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive integer`);
  }
  return value;
}
