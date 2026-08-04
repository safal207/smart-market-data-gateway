import { z } from "zod";

const decimalStringWireSchema = z
  .string()
  .regex(/^[+-]?(?:(?:\d+(?:\.\d*)?)|(?:\.\d+))(?:[eE][+-]?\d+)?$/)
  .refine((value) => Number.isFinite(Number(value)), {
    message: "Decimal value must be finite",
  });
const decimalWireSchema = z
  .union([z.number().finite(), decimalStringWireSchema])
  .transform((value) => Number(value))
  .pipe(z.number().finite().positive());
const nonNegativeDecimalWireSchema = z
  .union([z.number().finite(), decimalStringWireSchema])
  .transform((value) => Number(value))
  .pipe(z.number().finite().nonnegative());
const optionalNonNegativeDecimalWireSchema = z
  .union([nonNegativeDecimalWireSchema, z.null()])
  .optional();

export const symbolSchema = z
  .string()
  .trim()
  .min(1)
  .max(32)
  .regex(/^[A-Z0-9._:-]+$/);
export const wireQuoteSchema = z
  .object({
    schema_version: z.string().regex(/^1\.\d+$/),
    event_id: z.string().uuid(),
    symbol: symbolSchema,
    price: decimalWireSchema,
    bid: optionalNonNegativeDecimalWireSchema,
    ask: optionalNonNegativeDecimalWireSchema,
    provider_timestamp: z.string().datetime({ offset: true }),
    received_at: z.string().datetime({ offset: true }),
    sequence: z.number().int().nonnegative().nullable().optional(),
    provider: z.string().min(1).max(64),
    last_size: optionalNonNegativeDecimalWireSchema,
    cumulative_volume: optionalNonNegativeDecimalWireSchema,
    trade_count: z.number().int().nonnegative().nullable().optional(),
  })
  .strip()
  .superRefine((quote, context) => {
    if (quote.bid != null && quote.ask != null && quote.bid > quote.ask) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "bid must be less than or equal to ask",
        path: ["bid"],
      });
    }
  });

const requestIdSchema = z.string().min(1).max(128).nullable().optional();
const envelopeTimestampSchema = z.string().datetime({ offset: true });
const connectedMessageSchema = z
  .object({
    type: z.literal("connected"),
    timestamp: envelopeTimestampSchema,
    request_id: requestIdSchema,
    data: z
      .object({
        connection_id: z.string().min(1),
        client_id: z.string().min(1),
        tier: z.enum(["basic", "pro", "premium"]),
        policy: z
          .object({
            max_symbols: z.number().int().positive(),
            max_connections: z.number().int().positive(),
            updates_per_second: z.number().positive(),
            rest_requests_per_minute: z.number().int().positive(),
            subscription_ops_per_minute: z.number().int().positive(),
            market_depth: z.string().min(1),
            historical_data: z.boolean(),
          })
          .strip(),
        correlation_id: z.string().min(1),
        active_connections: z.number().int().nonnegative(),
      })
      .strip(),
  })
  .strip();
const subscribeAckMessageSchema = z
  .object({
    type: z.literal("ack"),
    timestamp: envelopeTimestampSchema,
    request_id: requestIdSchema,
    data: z
      .object({
        action: z.enum(["subscribe", "unsubscribe"]),
        symbols: z.array(symbolSchema),
        upstream_transitions: z.array(symbolSchema).optional(),
      })
      .strip(),
  })
  .strip();
const pongAckMessageSchema = z
  .object({
    type: z.literal("ack"),
    timestamp: envelopeTimestampSchema,
    request_id: requestIdSchema,
    data: z.object({ action: z.literal("pong") }).strip(),
  })
  .strip();
const snapshotMessageSchema = z
  .object({
    type: z.literal("snapshot"),
    timestamp: envelopeTimestampSchema,
    request_id: requestIdSchema,
    data: z
      .object({
        quote: wireQuoteSchema,
        stale: z.boolean(),
        age_ms: z.number().int().nonnegative(),
      })
      .strip(),
  })
  .strip();
const quoteMessageSchema = z
  .object({
    type: z.literal("quote"),
    timestamp: envelopeTimestampSchema,
    request_id: requestIdSchema,
    data: z.object({ quote: wireQuoteSchema }).strip(),
  })
  .strip();
const heartbeatMessageSchema = z
  .object({
    type: z.literal("heartbeat"),
    timestamp: envelopeTimestampSchema,
    request_id: requestIdSchema,
    data: z.object({ connection_id: z.string().min(1) }).strip(),
  })
  .strip();
const warningMessageSchema = z
  .object({
    type: z.literal("warning"),
    timestamp: envelopeTimestampSchema,
    request_id: requestIdSchema,
    data: z
      .object({
        code: z.string().min(1),
        message: z.string().min(1),
        coalesced_events: z.number().int().nonnegative().optional(),
      })
      .strip(),
  })
  .strip();
const errorMessageSchema = z
  .object({
    type: z.literal("error"),
    timestamp: envelopeTimestampSchema,
    request_id: requestIdSchema,
    data: z
      .object({
        code: z.string().min(1),
        message: z.string().min(1),
      })
      .strip(),
  })
  .strip();

export const wireMessageSchema = z.union([
  connectedMessageSchema,
  subscribeAckMessageSchema,
  pongAckMessageSchema,
  snapshotMessageSchema,
  quoteMessageSchema,
  heartbeatMessageSchema,
  warningMessageSchema,
  errorMessageSchema,
]);
export type WireQuote = z.infer<typeof wireQuoteSchema>;
export type WireMessage = z.infer<typeof wireMessageSchema>;
export interface NormalizedQuote {
  readonly schemaVersion: string;
  readonly eventId: string;
  readonly symbol: string;
  readonly price: number;
  readonly bid?: number;
  readonly ask?: number;
  readonly providerTimestampMs: number;
  readonly receivedAtMs: number;
  readonly sequence?: number;
  readonly provider: string;
  readonly lastSize?: number;
  readonly cumulativeVolume?: number;
  readonly tradeCount?: number;
}
export function parseWireMessage(input: unknown): WireMessage {
  return wireMessageSchema.parse(decodeInput(input));
}
export function safeParseWireMessage(input: unknown) {
  try {
    return wireMessageSchema.safeParse(decodeInput(input));
  } catch (error) {
    return {
      success: false as const,
      error:
        error instanceof Error
          ? error
          : new Error("Market-data frame is not valid JSON"),
    };
  }
}
function decodeInput(input: unknown): unknown {
  return typeof input === "string" ? (JSON.parse(input) as unknown) : input;
}
export function normalizeWireQuote(quote: WireQuote): NormalizedQuote {
  return Object.freeze({
    schemaVersion: quote.schema_version,
    eventId: quote.event_id,
    symbol: quote.symbol,
    price: quote.price,
    ...(quote.bid == null ? {} : { bid: quote.bid }),
    ...(quote.ask == null ? {} : { ask: quote.ask }),
    providerTimestampMs: Date.parse(quote.provider_timestamp),
    receivedAtMs: Date.parse(quote.received_at),
    ...(quote.sequence == null ? {} : { sequence: quote.sequence }),
    provider: quote.provider,
    ...(quote.last_size == null ? {} : { lastSize: quote.last_size }),
    ...(quote.cumulative_volume == null
      ? {}
      : { cumulativeVolume: quote.cumulative_volume }),
    ...(quote.trade_count == null ? {} : { tradeCount: quote.trade_count }),
  });
}
