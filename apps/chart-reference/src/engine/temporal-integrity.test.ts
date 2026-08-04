import { describe, expect, it } from "vitest";
import { makePoint } from "../test/market-data-fixtures";
import { TemporalIntegrityGuard } from "./temporal-integrity";

describe("TemporalIntegrityGuard", () => {
  it("accepts progression, identifies gaps, and deduplicates event IDs", () => {
    const guard = new TemporalIntegrityGuard();
    expect(guard.evaluate(makePoint(1))).toMatchObject({ outcome: "accepted", reason: "initial" });
    expect(guard.evaluate(makePoint(4, { quote: { providerTimestampMs: 4_000, sequence: 4 } }))).toMatchObject({
      outcome: "accepted", reason: "sequence_gap", sequenceGap: 2,
    });
    expect(guard.evaluate(makePoint(4))).toMatchObject({ outcome: "duplicate", reason: "duplicate_event" });
  });
  it("quarantines timestamp and sequence regressions", () => {
    const guard = new TemporalIntegrityGuard();
    guard.evaluate(makePoint(5));
    expect(guard.evaluate(makePoint(6, { quote: { providerTimestampMs: 4_000, sequence: 6 } }))).toMatchObject({ outcome: "quarantined", reason: "timestamp_rollback" });
    expect(guard.evaluate(makePoint(7, { quote: { providerTimestampMs: 7_000, sequence: 4 } }))).toMatchObject({ outcome: "quarantined", reason: "sequence_regression" });
    expect(guard.getQuarantine()).toHaveLength(2);
  });
  it("reports a sequence jump even when provider timestamps are equal", () => {
    const guard = new TemporalIntegrityGuard();
    guard.evaluate(makePoint(10, { quote: { providerTimestampMs: 10_000, sequence: 10 } }));
    expect(guard.evaluate(makePoint(15, { quote: { providerTimestampMs: 10_000, sequence: 15 } }))).toMatchObject({ outcome: "accepted", reason: "sequence_gap", sequenceGap: 4 });
  });
  it("does not advance generation when a reconnect snapshot is quarantined", () => {
    const guard = new TemporalIntegrityGuard();
    guard.evaluate(makePoint(10, { generation: 1, quote: { providerTimestampMs: 10_000, sequence: 10 } }));
    expect(guard.evaluate(makePoint(2, { generation: 2, origin: "snapshot", quote: { providerTimestampMs: 2_000, sequence: 2 } }))).toMatchObject({ outcome: "quarantined", reason: "timestamp_rollback" });
    expect(guard.evaluate(makePoint(11, { generation: 1, quote: { providerTimestampMs: 11_000, sequence: 11 } }))).toMatchObject({ outcome: "accepted", reason: "advanced" });
    expect(guard.evaluate(makePoint(12, { generation: 2, quote: { providerTimestampMs: 12_000, sequence: 1 } }))).toMatchObject({ outcome: "accepted", reason: "generation_advanced" });
  });
  it("does not let a duplicate reconnect snapshot mutate the temporal watermark", () => {
    const guard = new TemporalIntegrityGuard();
    const original = makePoint(10, { generation: 1 });
    guard.evaluate(original);
    expect(guard.evaluate({ ...original, generation: 2, origin: "snapshot" })).toMatchObject({ outcome: "duplicate" });
    expect(guard.evaluate(makePoint(11, { generation: 1, quote: { providerTimestampMs: 11_000, sequence: 11 } }))).toMatchObject({ outcome: "accepted", reason: "advanced" });
    expect(guard.evaluate(makePoint(12, { generation: 2, quote: { providerTimestampMs: 12_000, sequence: 1 } }))).toMatchObject({ outcome: "accepted", reason: "generation_advanced" });
  });
  it("caps quarantine retention", () => {
    const guard = new TemporalIntegrityGuard({ quarantineRetention: 2 });
    guard.evaluate(makePoint(10));
    for (const ordinal of [1, 2, 3]) guard.evaluate(makePoint(ordinal, { quote: { providerTimestampMs: ordinal, sequence: ordinal } }));
    expect(guard.getQuarantine()).toHaveLength(2);
  });
});
