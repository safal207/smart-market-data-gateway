import { describe, expect, it, vi } from "vitest";

import {
  CandleHistoryClient,
  CandleHistoryError,
  CandleHistoryLoader,
  type CandleHistoryRequest,
  type CandleHistorySeries,
  type CandleHistoryTransport,
} from "./candle-history";

const VALID_PAYLOAD = {
  schema_version: "1.0",
  symbol: "AAPL.US",
  timeframe: "5m",
  source: "observed_quote_aggregation",
  requested_limit: 500,
  returned_count: 1,
  retention_seconds: 2_592_000,
  period_start: "2026-08-04T10:00:00Z",
  period_end: "2026-08-04T10:10:00Z",
  data: [
    {
      symbol: "AAPL.US",
      timeframe: "5m",
      open_time: "2026-08-04T10:00:00Z",
      close_time: "2026-08-04T10:05:00Z",
      open: "201.10",
      high: "202.25",
      low: "200.50",
      close: "201.75",
      activity_count: 17,
      first_observation: "2026-08-04T10:00:04Z",
      last_observation: "2026-08-04T10:04:58Z",
      closed: true,
    },
  ],
  warnings: ["Trade volume is unavailable."],
} as const;

describe("CandleHistoryClient", () => {
  it("loads and validates canonical history without persisting the bearer token", async () => {
    const fetcher = vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = input instanceof URL
        ? input
        : new URL(typeof input === "string" ? input : input.url);
      expect(url.pathname).toBe("/v1/candles/AAPL.US");
      expect(url.searchParams.get("timeframe")).toBe("5m");
      expect(url.searchParams.get("limit")).toBe("500");
      expect(new Headers(init?.headers).get("Authorization")).toBe("Bearer dev-pro:test-client");
      expect(url.toString()).not.toContain("dev-pro:test-client");
      return Promise.resolve(new Response(JSON.stringify(VALID_PAYLOAD), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }));
    });
    const client = new CandleHistoryClient({
      baseUrl: "http://gateway.test:8000",
      fetch: fetcher,
    });

    const series = await client.load({
      symbol: "aapl.us",
      timeframe: "5m",
      token: "dev-pro:test-client",
    });

    expect(series.returnedCount).toBe(1);
    expect(series.candles[0]).toMatchObject({
      timeMs: Date.parse("2026-08-04T10:00:00Z"),
      open: 201.1,
      high: 202.25,
      low: 200.5,
      close: 201.75,
      activityValue: 17,
      activitySource: "updates",
    });
    expect(fetcher).toHaveBeenCalledOnce();
  });

  it("classifies a Basic tier denial as forbidden", async () => {
    const fetcher = vi.fn((): Promise<Response> => Promise.resolve(new Response(
      JSON.stringify({ detail: "historical data is not available for the basic tier" }),
      { status: 403, headers: { "Content-Type": "application/json" } },
    )));
    const client = new CandleHistoryClient({
      baseUrl: "http://gateway.test:8000",
      fetch: fetcher,
    });

    await expect(client.load({ symbol: "AAPL.US", timeframe: "1m" })).rejects.toMatchObject({
      name: "CandleHistoryError",
      kind: "forbidden",
      status: 403,
    });
  });

  it("fails closed when the response count contradicts its data", async () => {
    const fetcher = vi.fn((): Promise<Response> => Promise.resolve(new Response(JSON.stringify({
      ...VALID_PAYLOAD,
      returned_count: 2,
    }), { status: 200, headers: { "Content-Type": "application/json" } })));
    const client = new CandleHistoryClient({
      baseUrl: "http://gateway.test:8000",
      fetch: fetcher,
    });

    await expect(client.load({ symbol: "AAPL.US", timeframe: "5m" })).rejects.toMatchObject({
      name: "CandleHistoryError",
      kind: "invalid_response",
    });
  });
});

describe("CandleHistoryLoader", () => {
  it("fences AAPL → TSLA → AAPL races even when the transport ignores abort", async () => {
    const pending: Array<{
      request: CandleHistoryRequest;
      resolve: (series: CandleHistorySeries) => void;
    }> = [];
    const transport: CandleHistoryTransport = {
      load: (request) => new Promise((resolve) => pending.push({ request, resolve })),
    };
    const loader = new CandleHistoryLoader(transport);

    const first = loader.load({ symbol: "AAPL.US", timeframe: "1m" });
    const second = loader.load({ symbol: "TSLA.US", timeframe: "1m" });
    const third = loader.load({ symbol: "AAPL.US", timeframe: "5m" });
    expect(pending.map(({ request }) => `${request.symbol}:${request.timeframe}`)).toEqual([
      "AAPL.US:1m",
      "TSLA.US:1m",
      "AAPL.US:5m",
    ]);

    pending[1]!.resolve(series("TSLA.US", "1m", 2));
    pending[0]!.resolve(series("AAPL.US", "1m", 1));
    pending[2]!.resolve(series("AAPL.US", "5m", 3));

    await expect(first).resolves.toEqual({ kind: "superseded" });
    await expect(second).resolves.toEqual({ kind: "superseded" });
    await expect(third).resolves.toMatchObject({
      kind: "success",
      series: { symbol: "AAPL.US", timeframe: "5m", returnedCount: 3 },
    });
  });

  it("normalizes unexpected transport failures", async () => {
    const loader = new CandleHistoryLoader({
      load: () => Promise.reject(new Error("boom")),
    });
    const result = await loader.load({ symbol: "AAPL.US", timeframe: "1m" });
    expect(result.kind).toBe("failure");
    if (result.kind === "failure") {
      expect(result.error).toBeInstanceOf(CandleHistoryError);
      expect(result.error.kind).toBe("network");
    }
  });
});

function series(
  symbol: string,
  timeframe: "1m" | "5m",
  returnedCount: number,
): CandleHistorySeries {
  return Object.freeze({
    symbol,
    timeframe,
    source: "observed_quote_aggregation",
    requestedLimit: 500,
    returnedCount,
    retentionSeconds: 2_592_000,
    periodStartMs: 0,
    periodEndMs: 1,
    candles: Object.freeze([]),
    warnings: Object.freeze([]),
  });
}
