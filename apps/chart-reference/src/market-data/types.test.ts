import { describe, expect, it } from "vitest";
import { wireQuote } from "../test/market-data-fixtures";
import {
  normalizeWireQuote,
  parseWireMessage,
  safeParseWireMessage,
} from "./types";

function quoteEnvelope(quote: unknown = wireQuote()) {
  return {
    type: "quote",
    timestamp: "2026-08-01T12:00:00.000Z",
    request_id: null,
    data: { quote },
  };
}

describe("gateway wire parsing", () => {
  it("parses Decimal strings into a normalized quote", () => {
    const message = parseWireMessage(
      JSON.stringify(
        quoteEnvelope({
          ...wireQuote(),
          last_size: "0",
          cumulative_volume: "1234.5",
          trade_count: 42,
        }),
      ),
    );
    expect(message.type).toBe("quote");
    if (message.type !== "quote") throw new Error("expected quote");
    const quote = normalizeWireQuote(message.data.quote);
    expect(quote.price).toBe(101);
    expect(quote.symbol).toBe("AAPL.US");
    expect(quote.providerTimestampMs).toBe(1_000);
    expect(quote.lastSize).toBe(0);
    expect(quote.cumulativeVolume).toBe(1234.5);
    expect(quote.tradeCount).toBe(42);
  });

  it("accepts and strips additive fields at every public object level", () => {
    const quoteMessage = parseWireMessage({
      ...quoteEnvelope({
        ...wireQuote(),
        quote_extension: "future quote field",
      }),
      envelope_extension: "future envelope field",
      data: {
        ...quoteEnvelope().data,
        data_extension: "future data field",
        quote: {
          ...wireQuote(),
          quote_extension: "future quote field",
        },
      },
    });

    expect(quoteMessage).not.toHaveProperty("envelope_extension");
    expect(quoteMessage.data).not.toHaveProperty("data_extension");
    if (quoteMessage.type !== "quote") throw new Error("expected quote");
    expect(quoteMessage.data.quote).not.toHaveProperty("quote_extension");

    const connectedMessage = parseWireMessage({
      type: "connected",
      timestamp: "2026-08-01T12:00:00.000Z",
      request_id: null,
      envelope_extension: true,
      data: {
        connection_id: "connection-1",
        client_id: "client-1",
        tier: "basic",
        policy: {
          max_symbols: 10,
          max_connections: 1,
          updates_per_second: 5,
          rest_requests_per_minute: 60,
          subscription_ops_per_minute: 30,
          market_depth: "top",
          historical_data: false,
          policy_extension: "future policy field",
        },
        correlation_id: "correlation-1",
        active_connections: 1,
        data_extension: true,
      },
    });

    expect(connectedMessage).not.toHaveProperty("envelope_extension");
    expect(connectedMessage.data).not.toHaveProperty("data_extension");
    if (connectedMessage.type !== "connected") {
      throw new Error("expected connected");
    }
    expect(connectedMessage.data.policy).not.toHaveProperty("policy_extension");
  });

  it("accepts finite scientific Decimal strings", () => {
    const message = parseWireMessage(
      quoteEnvelope({
        ...wireQuote(),
        price: "1.01E+2",
        bid: "+1e2",
        ask: "1.02e+2",
        last_size: "0E-8",
        cumulative_volume: ".125e+4",
      }),
    );

    if (message.type !== "quote") throw new Error("expected quote");
    expect(message.data.quote).toMatchObject({
      price: 101,
      bid: 100,
      ask: 102,
      last_size: 0,
      cumulative_volume: 1_250,
    });
  });

  it.each(["NaN", "Infinity", "-Infinity", "1e309", "1e", "1.2.3", " 1"])(
    "rejects an invalid positive Decimal string: %s",
    (price) => {
      expect(
        safeParseWireMessage(quoteEnvelope({ ...wireQuote(), price })).success,
      ).toBe(false);
    },
  );

  it.each(["-1", "-1e-3", "NaN", "Infinity", "1e309", "1e", "0x10"])(
    "rejects an invalid nonnegative Decimal string: %s",
    (lastSize) => {
      expect(
        safeParseWireMessage(
          quoteEnvelope({ ...wireQuote(), last_size: lastSize }),
        ).success,
      ).toBe(false);
    },
  );

  it("rejects zero and negative positive Decimal values", () => {
    expect(
      safeParseWireMessage(
        quoteEnvelope({ ...wireQuote(), price: "0E+10" }),
      ).success,
    ).toBe(false);
    expect(
      safeParseWireMessage(
        quoteEnvelope({ ...wireQuote(), price: "-1e-3" }),
      ).success,
    ).toBe(false);
  });

  it("rejects crossed markets, naive timestamps, and malformed JSON", () => {
    expect(
      safeParseWireMessage(
        quoteEnvelope({ ...wireQuote(), bid: "105", ask: "100" }),
      ).success,
    ).toBe(false);
    expect(
      safeParseWireMessage(
        quoteEnvelope({ ...wireQuote(), provider_timestamp: "2026-08-01T12:00:00" }),
      ).success,
    ).toBe(false);
    expect(safeParseWireMessage("not-json").success).toBe(false);
  });
});
