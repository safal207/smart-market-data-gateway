import type {
  MarketDataOrigin,
  MarketDataPoint,
  NormalizedQuote,
} from "../market-data";

export function makeQuote(
  ordinal = 1,
  overrides: Partial<NormalizedQuote> = {},
): NormalizedQuote {
  const timestamp = ordinal * 1_000;
  return Object.freeze({
    schemaVersion: "1.0",
    eventId: eventId(ordinal),
    symbol: "AAPL.US",
    price: 100 + ordinal,
    bid: 99.99 + ordinal,
    ask: 100.01 + ordinal,
    providerTimestampMs: timestamp,
    receivedAtMs: timestamp + 10,
    sequence: ordinal,
    provider: "fixture",
    ...overrides,
  });
}

export function makePoint(
  ordinal = 1,
  overrides: {
    quote?: Partial<NormalizedQuote>;
    generation?: number;
    origin?: MarketDataOrigin;
    stale?: boolean;
    ageMs?: number;
  } = {},
): MarketDataPoint {
  return Object.freeze({
    type: "market-data",
    quote: makeQuote(ordinal, overrides.quote),
    origin: overrides.origin ?? "live",
    generation: overrides.generation ?? 1,
    stale: overrides.stale ?? false,
    ageMs: overrides.ageMs ?? 0,
  });
}

export function wireQuote(ordinal = 1) {
  const quote = makeQuote(ordinal);
  return {
    schema_version: quote.schemaVersion,
    event_id: quote.eventId,
    symbol: quote.symbol,
    price: quote.price.toFixed(2),
    bid: quote.bid?.toFixed(2) ?? null,
    ask: quote.ask?.toFixed(2) ?? null,
    provider_timestamp: new Date(quote.providerTimestampMs).toISOString(),
    received_at: new Date(quote.receivedAtMs).toISOString(),
    sequence: quote.sequence ?? null,
    provider: quote.provider,
  };
}

export function eventId(ordinal: number): string {
  return `00000000-0000-4000-8000-${Math.abs(ordinal)
    .toString()
    .padStart(12, "0")
    .slice(-12)}`;
}
