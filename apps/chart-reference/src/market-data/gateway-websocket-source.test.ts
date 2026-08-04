import { describe, expect, it, vi } from "vitest";
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
  readonly sent: string[] = [];

  open(): void {
    this.readyState = 1;
    this.onopen?.({});
  }

  message(payload: unknown): void {
    this.onmessage?.({ data: JSON.stringify(payload) });
  }

  serverClose(): void {
    this.readyState = 3;
    this.onclose?.({});
  }

  send(payload: string): void {
    this.sent.push(payload);
  }

  close(): void {
    this.readyState = 3;
  }
}

class FakeScheduler implements SourceScheduler {
  currentMs = 50_000;
  nextId = 1;
  readonly tasks = new Map<number, { callback: () => void; delayMs: number }>();

  now(): number {
    return this.currentMs;
  }

  setTimeout(callback: () => void, delayMs: number): number {
    const id = this.nextId++;
    this.tasks.set(id, { callback, delayMs });
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.tasks.delete(handle as number);
  }

  runNext(): number {
    const entry = [...this.tasks.entries()].sort(
      (left, right) => left[1].delayMs - right[1].delayMs,
    )[0];
    if (entry == null) throw new Error("no scheduled task");
    const [id, task] = entry;
    this.tasks.delete(id);
    this.currentMs += task.delayMs;
    task.callback();
    return task.delayMs;
  }
}

function snapshot(requestId = "subscribe-1-1") {
  return {
    type: "snapshot",
    timestamp: "2026-08-01T12:00:00.000Z",
    request_id: requestId,
    data: { quote: wireQuote(), stale: false, age_ms: 12 },
  };
}

function subscriptionAck(
  requestId = "subscribe-1-1",
  action: "subscribe" | "unsubscribe" = "subscribe",
  symbols = ["AAPL.US"],
) {
  return {
    type: "ack",
    timestamp: "2026-08-01T12:00:00.000Z",
    request_id: requestId,
    data: { action, symbols, upstream_transitions: [] },
  };
}

function gatewayError(
  requestId: string,
  code = "rate_limit",
  message = "subscription limit exceeded",
) {
  return {
    type: "error",
    timestamp: "2026-08-01T12:00:00.000Z",
    request_id: requestId,
    data: { code, message },
  };
}

describe("GatewayWebSocketSource", () => {
  it("delivers a snapshot before its ack and keeps the token out of public state", () => {
    const socket = new FakeSocket();
    const observedUrls: string[] = [];
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      getToken: () => "short lived/token",
      webSocketFactory: (url) => {
        observedUrls.push(url);
        return socket;
      },
      scheduler: new FakeScheduler(),
    });
    const events: MarketDataSourceEvent[] = [];
    source.subscribe((event) => events.push(event));
    source.replaceSymbols(["aapl.us"]);
    source.start();
    socket.open();
    const command = JSON.parse(socket.sent[0]!) as Record<string, unknown>;
    const requestId = String(command.request_id);

    socket.message(snapshot(requestId));
    socket.message(subscriptionAck(requestId));

    expect(events.map((event) => event.type)).toEqual([
      "market-data",
      "subscription-ack",
    ]);
    expect(observedUrls[0]).toContain("token=short+lived%2Ftoken");
    expect(JSON.stringify(source.getStatus())).not.toContain("short lived/token");
  });

  it("commits subscription state only after the matching acknowledgement", () => {
    const socket = new FakeSocket();
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => socket,
      scheduler: new FakeScheduler(),
    });
    const events: MarketDataSourceEvent[] = [];
    source.subscribe((event) => events.push(event));
    source.replaceSymbols(["AAPL.US"]);
    source.start();
    socket.open();
    const subscribe = JSON.parse(socket.sent[0]!) as Record<string, unknown>;
    const requestId = String(subscribe.request_id);

    socket.message(subscriptionAck("another-request"));
    source.replaceSymbols([]);
    expect(socket.sent).toHaveLength(1);
    expect(events.some((event) => event.type === "subscription-ack")).toBe(false);
    expect(events).toContainEqual(
      expect.objectContaining({
        type: "notice",
        code: "unmatched_subscription_ack",
      }),
    );

    socket.message(subscriptionAck(requestId));
    expect(events).toContainEqual(
      expect.objectContaining({
        type: "subscription-ack",
        action: "subscribe",
        symbols: ["AAPL.US"],
      }),
    );
    expect(JSON.parse(socket.sent[1]!)).toMatchObject({
      action: "unsubscribe",
      symbols: ["AAPL.US"],
    });
  });

  it("rolls back rejected operations and retries them with capped backoff", () => {
    const scheduler = new FakeScheduler();
    const socket = new FakeSocket();
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => socket,
      scheduler,
      random: () => 0,
      pingIntervalMs: 10_000,
      initialBackoffMs: 100,
      maximumBackoffMs: 250,
    });
    const events: MarketDataSourceEvent[] = [];
    source.subscribe((event) => events.push(event));
    source.replaceSymbols(["AAPL.US"]);
    source.start();
    socket.open();

    for (const expectedDelay of [100, 200, 250]) {
      const command = JSON.parse(socket.sent.at(-1)!) as Record<string, unknown>;
      socket.message(
        gatewayError(
          String(command.request_id),
          "rate_limit",
          "Bearer local-secret-that-must-not-surface",
        ),
      );
      expect(source.getStatus()).toMatchObject({
        state: "live",
        nextRetryMs: expectedDelay,
        lastError: "subscription_rate_limit",
      });
      expect(scheduler.runNext()).toBe(expectedDelay);
    }

    const retry = JSON.parse(socket.sent.at(-1)!) as Record<string, unknown>;
    socket.message(subscriptionAck(String(retry.request_id)));
    expect(source.getStatus().lastError).toBeUndefined();
    expect(events).toContainEqual(
      expect.objectContaining({ type: "subscription-ack", symbols: ["AAPL.US"] }),
    );
    expect(JSON.stringify({ events, status: source.getStatus() })).not.toContain(
      "local-secret-that-must-not-surface",
    );
  });

  it("clears an operation retry when stopped", () => {
    const scheduler = new FakeScheduler();
    const socket = new FakeSocket();
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => socket,
      scheduler,
      random: () => 0,
      pingIntervalMs: 10_000,
      initialBackoffMs: 100,
    });
    source.replaceSymbols(["AAPL.US"]);
    source.start();
    socket.open();
    const command = JSON.parse(socket.sent[0]!) as Record<string, unknown>;
    socket.message(gatewayError(String(command.request_id)));
    expect(scheduler.tasks.size).toBe(3);

    source.stop();
    expect(scheduler.tasks.size).toBe(0);
    expect(source.getStatus().state).toBe("stopped");
  });

  it("surfaces terminal subscription errors without an automatic retry", () => {
    const scheduler = new FakeScheduler();
    const socket = new FakeSocket();
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => socket,
      scheduler,
      random: () => 0,
      pingIntervalMs: 10_000,
      initialBackoffMs: 100,
    });
    source.replaceSymbols(["AAPL.US"]);
    source.start();
    socket.open();
    const command = JSON.parse(socket.sent[0]!) as Record<string, unknown>;

    socket.message(gatewayError(String(command.request_id), "forbidden"));
    expect(source.getStatus()).toMatchObject({
      state: "live",
      lastError: "subscription_forbidden",
    });
    expect(source.getStatus().nextRetryMs).toBeUndefined();
    expect(scheduler.tasks.size).toBe(2);

    source.replaceSymbols(["MSFT.US"]);
    expect(source.getStatus().lastError).toBeUndefined();
    expect(JSON.parse(socket.sent[1]!)).toMatchObject({
      action: "subscribe",
      symbols: ["MSFT.US"],
    });
  });

  it("fences late messages from an old socket generation", () => {
    const scheduler = new FakeScheduler();
    const first = new FakeSocket();
    const second = new FakeSocket();
    const sockets = [first, second];
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => sockets.shift()!,
      scheduler,
      random: () => 0,
      initialBackoffMs: 100,
    });
    const events: MarketDataSourceEvent[] = [];
    source.subscribe((event) => events.push(event));
    source.replaceSymbols(["AAPL.US"]);
    source.start();
    first.open();
    const staleHandler = first.onmessage;
    first.serverClose();
    expect(scheduler.runNext()).toBe(100);
    second.open();
    const currentGeneration = source.getStatus().generation;

    staleHandler?.({ data: JSON.stringify(snapshot()) });
    second.message(snapshot("subscribe-2-2"));

    expect(events.filter((event) => event.type === "market-data")).toHaveLength(1);
    expect(
      events.find((event) => event.type === "market-data")?.generation,
    ).toBe(currentGeneration);
  });

  it("uses capped exponential backoff and sends periodic ping commands", () => {
    const scheduler = new FakeScheduler();
    const first = new FakeSocket();
    let attempts = 0;
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => {
        attempts += 1;
        if (attempts === 1) return first;
        throw new Error("offline");
      },
      scheduler,
      random: () => 0,
      pingIntervalMs: 1_000,
      initialBackoffMs: 100,
      maximumBackoffMs: 250,
    });
    source.start();
    first.open();
    expect(scheduler.runNext()).toBe(1_000);
    expect(first.sent[0]).toContain('"action":"ping"');

    first.serverClose();
    expect(source.getStatus().nextRetryMs).toBe(100);
    expect(scheduler.runNext()).toBe(100);
    expect(source.getStatus().nextRetryMs).toBe(200);
    expect(scheduler.runNext()).toBe(200);
    expect(source.getStatus().nextRetryMs).toBe(250);
  });

  it("keeps exponential backoff across sockets that open and immediately flap", () => {
    const scheduler = new FakeScheduler();
    const first = new FakeSocket();
    const second = new FakeSocket();
    const third = new FakeSocket();
    const sockets = [first, second, third];
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => sockets.shift()!,
      scheduler,
      random: () => 0,
      pingIntervalMs: 10_000,
      heartbeatTimeoutMs: 10_000,
      connectionStabilityMs: 10_000,
      initialBackoffMs: 100,
      maximumBackoffMs: 250,
    });
    source.start();
    first.open();

    first.serverClose();
    expect(source.getStatus().nextRetryMs).toBe(100);
    expect(scheduler.runNext()).toBe(100);
    second.open();
    second.serverClose();
    expect(source.getStatus().nextRetryMs).toBe(200);
    expect(scheduler.runNext()).toBe(200);
    third.open();
    third.serverClose();
    expect(source.getStatus().nextRetryMs).toBe(250);
  });

  it("resets reconnect backoff only after valid traffic survives the stability window", () => {
    const scheduler = new FakeScheduler();
    const first = new FakeSocket();
    const second = new FakeSocket();
    const sockets = [first, second];
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => sockets.shift()!,
      scheduler,
      random: () => 0,
      pingIntervalMs: 10_000,
      heartbeatTimeoutMs: 1_000,
      connectionStabilityMs: 500,
      initialBackoffMs: 100,
    });
    source.start();
    first.open();
    first.serverClose();
    expect(scheduler.runNext()).toBe(100);
    second.open();
    expect(source.getStatus().reconnectAttempt).toBe(1);

    second.message({
      type: "heartbeat",
      timestamp: "2026-08-01T12:00:00.000Z",
      data: { connection_id: "connection-2" },
    });
    expect(scheduler.runNext()).toBe(500);
    expect(source.getStatus()).toMatchObject({ state: "live", reconnectAttempt: 0 });
  });

  it("reconnects and replays desired state when a subscription ack times out", () => {
    const scheduler = new FakeScheduler();
    const first = new FakeSocket();
    const second = new FakeSocket();
    const sockets = [first, second];
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => sockets.shift()!,
      scheduler,
      random: () => 0,
      pingIntervalMs: 1_000,
      heartbeatTimeoutMs: 3_000,
      subscriptionAckTimeoutMs: 250,
      initialBackoffMs: 100,
    });
    const events: MarketDataSourceEvent[] = [];
    source.subscribe((event) => events.push(event));
    source.replaceSymbols(["AAPL.US"]);
    source.start();
    first.open();

    expect(scheduler.runNext()).toBe(250);
    expect(first.readyState).toBe(3);
    expect(source.getStatus()).toMatchObject({
      state: "reconnecting",
      lastError: "subscription_ack_timeout",
      nextRetryMs: 100,
    });
    expect(events).toContainEqual(
      expect.objectContaining({ type: "notice", code: "subscription_ack_timeout" }),
    );

    expect(scheduler.runNext()).toBe(100);
    second.open();
    expect(JSON.parse(second.sent[0]!)).toMatchObject({
      action: "subscribe",
      symbols: ["AAPL.US"],
    });
  });

  it("fences an open socket that stops delivering heartbeat traffic", () => {
    const scheduler = new FakeScheduler();
    const socket = new FakeSocket();
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => socket,
      scheduler,
      random: () => 0,
      pingIntervalMs: 1_000,
      heartbeatTimeoutMs: 250,
      initialBackoffMs: 100,
    });
    source.start();
    socket.open();

    expect(scheduler.runNext()).toBe(250);
    expect(socket.readyState).toBe(3);
    expect(source.getStatus()).toMatchObject({
      state: "reconnecting",
      lastError: "heartbeat_timeout",
      nextRetryMs: 100,
    });
  });

  it("turns malformed frames into a sanitized notice", () => {
    const socket = new FakeSocket();
    const source = new GatewayWebSocketSource({
      url: "wss://gateway.example/v1/stream",
      webSocketFactory: () => socket,
      scheduler: new FakeScheduler(),
    });
    const listener = vi.fn();
    source.subscribe(listener);
    source.start();
    socket.open();
    socket.message({ type: "quote", token: "must-not-leak" });
    expect(listener).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "notice",
        code: "invalid_wire_message",
      }),
    );
    expect(JSON.stringify(listener.mock.calls)).not.toContain("must-not-leak");
  });
});
