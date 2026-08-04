import { describe, expect, it } from "vitest";
import { wireQuote } from "../test/market-data-fixtures";
import { normalizeWireQuote, parseWireMessage, safeParseWireMessage } from "./types";

function quoteEnvelope(quote: unknown = wireQuote()) {
  return { type: "quote", timestamp: "2026-08-01T12:00:00.000Z", request_id: null, data: { quote } };
}

describe("gateway wire parsing", () => {
  it("parses Decimal strings into a normalized quote", () => {
    const message = parseWireMessage(JSON.stringify(quoteEnvelope({ ...wireQuote(), last_size: "0", cumulative_volume: "1234.5", trade_count: 42 })));
    if (message.type !== "quote") throw new Error("expected quote");
    const quote = normalizeWireQuote(message.data.quote);
    expect(quote).toMatchObject({ price: 101, symbol: "AAPL.US", providerTimestampMs: 1_000, lastSize: 0, cumulativeVolume: 1234.5, tradeCount: 42 });
  });
  it("accepts zero bid or ask as an absent book side without dropping the price", () => {
    const message = parseWireMessage(quoteEnvelope({ ...wireQuote(), bid: 0, ask: "0" }));
    if (message.type !== "quote") throw new Error("expected quote");
    expect(normalizeWireQuote(message.data.quote)).toMatchObject({ price: 101, bid: 0, ask: 0 });
  });
  it("accepts and strips additive fields at every public object level", () => {
    const quoteMessage = parseWireMessage({
      ...quoteEnvelope(), envelope_extension: "future envelope field",
      data: { ...quoteEnvelope().data, data_extension: "future data field", quote: { ...wireQuote(), quote_extension: "future quote field" } },
    });
    expect(quoteMessage).not.toHaveProperty("envelope_extension");
    expect(quoteMessage.data).not.toHaveProperty("data_extension");
    if (quoteMessage.type !== "quote") throw new Error("expected quote");
    expect(quoteMessage.data.quote).not.toHaveProperty("quote_extension");
  });
  it("accepts finite scientific Decimal strings", () => {
    const message = parseWireMessage(quoteEnvelope({ ...wireQuote(), price: "1.01E+2", bid: "+1e2", ask: "1.02e+2", last_size: "0E-8", cumulative_volume: ".125e+4" }));
    if (message.type !== "quote") throw new Error("expected quote");
    expect(message.data.quote).toMatchObject({ price: 101, bid: 100, ask: 102, last_size: 0, cumulative_volume: 1_250 });
  });
  it.each(["NaN", "Infinity", "-Infinity", "1e309", "1e", "1.2.3", " 1"])("rejects an invalid positive Decimal string: %s", (price) => {
    expect(safeParseWireMessage(quoteEnvelope({ ...wireQuote(), price })).success).toBe(false);
  });
  it.each(["-1", "-1e-3", "NaN", "Infinity", "1e309", "1e", "0x10"])("rejects an invalid nonnegative Decimal string: %s", (lastSize) => {
    expect(safeParseWireMessage(quoteEnvelope({ ...wireQuote(), last_size: lastSize })).success).toBe(false);
  });
  it("rejects zero and negative positive Decimal values", () => {
    expect(safeParseWireMessage(quoteEnvelope({ ...wireQuote(), price: "0E+10" })).success).toBe(false);
    expect(safeParseWireMessage(quoteEnvelope({ ...wireQuote(), price: "-1e-3" })).success).toBe(false);
  });
  it("rejects crossed markets, naive timestamps, and malformed JSON", () => {
    expect(safeParseWireMessage(quoteEnvelope({ ...wireQuote(), bid: "105", ask: "100" })).success).toBe(false);
    expect(safeParseWireMessage(quoteEnvelope({ ...wireQuote(), provider_timestamp: "2026-08-01T12:00:00" })).success).toBe(false);
    expect(safeParseWireMessage("not-json").success).toBe(false);
  });
});
