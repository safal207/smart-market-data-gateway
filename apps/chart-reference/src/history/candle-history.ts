import { z } from "zod";

import type { ChartCandle } from "../chart";
import type { Timeframe } from "../engine";

export type ServerHistoryTimeframe = Exclude<Timeframe, "5s">;
export type CandleHistoryFailureKind =
  | "unauthorized"
  | "forbidden"
  | "rate_limited"
  | "network"
  | "invalid_response"
  | "server";

const serverHistoryTimeframeSchema = z.enum(["1m", "5m", "15m", "1h", "1d"]);
const positiveNumberSchema = z.union([z.number(), z.string()]).transform((value, context) => {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    context.addIssue({ code: "custom", message: "expected a positive finite number" });
    return z.NEVER;
  }
  return parsed;
});
const utcTimestampSchema = z.string().transform((value, context) => {
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) {
    context.addIssue({ code: "custom", message: "expected an ISO timestamp" });
    return z.NEVER;
  }
  return timestamp;
});

const candleSchema = z.object({
  symbol: z.string().min(1),
  timeframe: serverHistoryTimeframeSchema,
  open_time: utcTimestampSchema,
  close_time: utcTimestampSchema,
  open: positiveNumberSchema,
  high: positiveNumberSchema,
  low: positiveNumberSchema,
  close: positiveNumberSchema,
  activity_count: z.number().int().positive(),
  first_observation: utcTimestampSchema,
  last_observation: utcTimestampSchema,
  closed: z.literal(true),
}).strict();

const candleSeriesSchema = z.object({
  schema_version: z.literal("1.0"),
  symbol: z.string().min(1),
  timeframe: serverHistoryTimeframeSchema,
  source: z.literal("observed_quote_aggregation"),
  requested_limit: z.number().int().positive().max(1_000),
  returned_count: z.number().int().nonnegative(),
  retention_seconds: z.number().int().positive(),
  period_start: utcTimestampSchema,
  period_end: utcTimestampSchema,
  data: z.array(candleSchema),
  warnings: z.array(z.string()),
}).strict().superRefine((series, context) => {
  if (series.returned_count !== series.data.length) {
    context.addIssue({
      code: "custom",
      path: ["returned_count"],
      message: "returned_count must equal data.length",
    });
  }
  for (const [index, candle] of series.data.entries()) {
    if (candle.symbol !== series.symbol || candle.timeframe !== series.timeframe) {
      context.addIssue({
        code: "custom",
        path: ["data", index],
        message: "candle context must match the containing series",
      });
    }
    if (candle.open_time >= candle.close_time) {
      context.addIssue({
        code: "custom",
        path: ["data", index, "close_time"],
        message: "close_time must be later than open_time",
      });
    }
    if (candle.last_observation > series.period_end) {
      context.addIssue({
        code: "custom",
        path: ["data", index, "last_observation"],
        message: "last_observation must not exceed period_end",
      });
    }
  }
});

export interface CandleHistorySeries {
  readonly symbol: string;
  readonly timeframe: ServerHistoryTimeframe;
  readonly source: "observed_quote_aggregation";
  readonly requestedLimit: number;
  readonly returnedCount: number;
  readonly retentionSeconds: number;
  readonly periodStartMs: number;
  readonly periodEndMs: number;
  readonly candles: readonly ChartCandle[];
  readonly warnings: readonly string[];
}

export interface CandleHistoryRequest {
  readonly symbol: string;
  readonly timeframe: ServerHistoryTimeframe;
  readonly token?: string;
  readonly limit?: number;
  readonly signal?: AbortSignal;
}

export class CandleHistoryError extends Error {
  constructor(
    readonly kind: CandleHistoryFailureKind,
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = "CandleHistoryError";
  }
}

export interface CandleHistoryTransport {
  load(request: CandleHistoryRequest): Promise<CandleHistorySeries>;
}

export interface CandleHistoryClientOptions {
  readonly baseUrl: string;
  readonly fetch?: typeof globalThis.fetch;
}

export class CandleHistoryClient implements CandleHistoryTransport {
  private readonly fetcher: typeof globalThis.fetch;
  private readonly baseUrl: string;

  constructor(options: CandleHistoryClientOptions) {
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.baseUrl = normalizedBaseUrl(options.baseUrl);
  }

  async load(request: CandleHistoryRequest): Promise<CandleHistorySeries> {
    const symbol = request.symbol.trim().toUpperCase();
    if (symbol === "") throw new RangeError("symbol must not be empty");
    const limit = request.limit ?? 500;
    if (!Number.isInteger(limit) || limit < 1 || limit > 1_000) {
      throw new RangeError("limit must be an integer between 1 and 1000");
    }

    const url = new URL(`v1/candles/${encodeURIComponent(symbol)}`, this.baseUrl);
    url.searchParams.set("timeframe", request.timeframe);
    url.searchParams.set("limit", String(limit));
    const headers = new Headers({ Accept: "application/json" });
    const token = request.token?.trim();
    if (token) headers.set("Authorization", `Bearer ${token}`);

    let response: Response;
    try {
      response = await this.fetcher(url, { method: "GET", headers, signal: request.signal });
    } catch (error) {
      if (isAbortError(error)) throw error;
      throw new CandleHistoryError("network", "Unable to reach the candle history endpoint");
    }

    if (!response.ok) {
      const detail = await responseDetail(response);
      throw new CandleHistoryError(
        failureKind(response.status),
        detail ?? `Candle history request failed with HTTP ${response.status}`,
        response.status,
      );
    }

    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      throw new CandleHistoryError("invalid_response", "Candle history response was not valid JSON");
    }
    const parsed = candleSeriesSchema.safeParse(payload);
    if (!parsed.success) {
      throw new CandleHistoryError(
        "invalid_response",
        `Candle history response violated the public contract: ${z.prettifyError(parsed.error)}`,
      );
    }
    const series = parsed.data;
    return Object.freeze({
      symbol: series.symbol,
      timeframe: series.timeframe,
      source: series.source,
      requestedLimit: series.requested_limit,
      returnedCount: series.returned_count,
      retentionSeconds: series.retention_seconds,
      periodStartMs: series.period_start,
      periodEndMs: series.period_end,
      candles: Object.freeze(series.data.map((candle) => Object.freeze({
        timeMs: candle.open_time,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        updateCount: candle.activity_count,
        activityValue: candle.activity_count,
        activitySource: "updates" as const,
      }))),
      warnings: Object.freeze([...series.warnings]),
    });
  }
}

export type CandleHistoryLoadOutcome =
  | { readonly kind: "success"; readonly series: CandleHistorySeries }
  | { readonly kind: "failure"; readonly error: CandleHistoryError }
  | { readonly kind: "superseded" };

/** Cancels prior HTTP work and fences late completions even when a transport ignores AbortSignal. */
export class CandleHistoryLoader {
  private generation = 0;
  private controller: AbortController | undefined;

  constructor(private readonly transport: CandleHistoryTransport) {}

  async load(request: Omit<CandleHistoryRequest, "signal">): Promise<CandleHistoryLoadOutcome> {
    this.controller?.abort();
    const generation = ++this.generation;
    const controller = new AbortController();
    this.controller = controller;
    try {
      const series = await this.transport.load({ ...request, signal: controller.signal });
      if (generation !== this.generation || controller.signal.aborted) {
        return Object.freeze({ kind: "superseded" });
      }
      return Object.freeze({ kind: "success", series });
    } catch (error) {
      if (generation !== this.generation || controller.signal.aborted || isAbortError(error)) {
        return Object.freeze({ kind: "superseded" });
      }
      const normalized = error instanceof CandleHistoryError
        ? error
        : new CandleHistoryError("network", "Unable to load candle history");
      return Object.freeze({ kind: "failure", error: normalized });
    } finally {
      if (generation === this.generation && this.controller === controller) {
        this.controller = undefined;
      }
    }
  }

  cancel(): void {
    this.generation += 1;
    this.controller?.abort();
    this.controller = undefined;
  }
}

export function isServerHistoryTimeframe(timeframe: Timeframe): timeframe is ServerHistoryTimeframe {
  return timeframe !== "5s";
}

export function gatewayHttpBaseUrl(): string {
  const configured = import.meta.env.VITE_GATEWAY_HTTP_URL;
  return configured != null && configured.trim() !== ""
    ? normalizedBaseUrl(configured)
    : normalizedBaseUrl(window.location.origin);
}

function normalizedBaseUrl(value: string): string {
  const url = new URL(value, typeof window === "undefined" ? "http://localhost" : window.location.origin);
  url.pathname = `${url.pathname.replace(/\/+$/, "")}/`;
  return url.toString();
}

function failureKind(status: number): CandleHistoryFailureKind {
  if (status === 401) return "unauthorized";
  if (status === 403) return "forbidden";
  if (status === 429) return "rate_limited";
  return status >= 500 ? "server" : "invalid_response";
}

async function responseDetail(response: Response): Promise<string | undefined> {
  try {
    const payload = await response.json() as { detail?: unknown };
    return typeof payload.detail === "string" ? payload.detail : undefined;
  } catch {
    return undefined;
  }
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}
