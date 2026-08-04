import { describe, expect, it } from "vitest";

import { wireQuote } from "../test/market-data-fixtures";
import {
  GatewayWebSocketSource,
  type SourceScheduler,
  type WebSocketLike,
} from "./gateway-websocket-source";
import type { MarketDataSourceEvent } from "./source";

class FakeSocket implements WebSocketLike {
  readyState = 0;
  onopen: ((event: unknown) => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  onclose: ((event: unknown) => void) | null = null;

  open(): void {
    this.readyState = 1;
    this.onopen?.({});
  }

  message(payload: unknown): void {
    this.onmessage?.({ data: JSON.stringify(payload) });
  }

  send(): void {}
  close(): void {
    this.readyState = 3;
  }
}

class SkewedScheduler implements SourceScheduler {
  now(): number {
    return Date.UTC(2036, 0, 1);
  }

  setTimeout(): number {
    return 1;
  }

  clearTimeout(): void {}
}

describe("GatewayWebSocketSource live quote freshness", () => {
  it("starts live quote age at zero despite browser and gateway clock skew", () => {
    const socket = new FakeSocket();
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => socket,
      scheduler: new SkewedScheduler(),
    });
    const events: MarketDataSourceEvent[] = [];
    source.subscribe((event) => events.push(event));
    source.start();
    socket.open();

    socket.message({
      type: "quote",
      timestamp: "2026-08-01T12:00:00.000Z",
      request_id: null,
      data: {
        quote: {
          ...wireQuote(),
          received_at: "2026-08-01T12:00:00.000Z",
          provider_timestamp: "2026-08-01T12:00:00.000Z",
        },
      },
    });

    const event = events.find((candidate) => candidate.type === "market-data");
    expect(event).toMatchObject({
      type: "market-data",
      origin: "live",
      ageMs: 0,
      stale: false,
    });
  });
});
