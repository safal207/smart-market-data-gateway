import {
  type MarketDataSource,
  type MarketDataSourceEvent,
  type SourceEventListener,
  type SourceStatus,
  type SourceStatusListener,
  type Unsubscribe,
  normalizeSymbols,
} from "./source";
import {
  normalizeWireQuote,
  safeParseWireMessage,
  type WireMessage,
} from "./types";

interface SocketMessageEvent {
  readonly data: unknown;
}

type SubscriptionAction = "subscribe" | "unsubscribe";

interface PendingSubscriptionOperation {
  readonly requestId: string;
  readonly action: SubscriptionAction;
  readonly symbols: readonly string[];
  readonly generation: number;
}

export interface WebSocketLike {
  readonly readyState: number;
  onopen: ((event: unknown) => void) | null;
  onmessage: ((event: SocketMessageEvent) => void) | null;
  onerror: ((event: unknown) => void) | null;
  onclose: ((event: unknown) => void) | null;
  send(payload: string): void;
  close(code?: number, reason?: string): void;
}

export interface SourceScheduler {
  now(): number;
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

export interface GatewayWebSocketSourceOptions {
  readonly url: string;
  /** Called for every connection attempt. The source never writes this value to storage. */
  readonly getToken?: () => string | null | undefined;
  readonly webSocketFactory?: (url: string) => WebSocketLike;
  readonly scheduler?: SourceScheduler;
  readonly random?: () => number;
  readonly pingIntervalMs?: number;
  readonly heartbeatTimeoutMs?: number;
  readonly connectionStabilityMs?: number;
  readonly subscriptionAckTimeoutMs?: number;
  readonly initialBackoffMs?: number;
  readonly maximumBackoffMs?: number;
  readonly backoffFactor?: number;
  readonly jitterRatio?: number;
}

const OPEN = 1;

const defaultScheduler: SourceScheduler = {
  now: () => Date.now(),
  setTimeout: (callback, delayMs) => globalThis.setTimeout(callback, delayMs),
  clearTimeout: (handle) =>
    globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
};

/** Browser gateway source with explicit desired state and per-socket generation fencing. */
export class GatewayWebSocketSource implements MarketDataSource {
  private readonly eventListeners = new Set<SourceEventListener>();
  private readonly statusListeners = new Set<SourceStatusListener>();
  private readonly scheduler: SourceScheduler;
  private readonly socketFactory: (url: string) => WebSocketLike;
  private readonly random: () => number;
  private readonly pingIntervalMs: number;
  private readonly heartbeatTimeoutMs: number;
  private readonly connectionStabilityMs: number;
  private readonly subscriptionAckTimeoutMs: number;
  private readonly initialBackoffMs: number;
  private readonly maximumBackoffMs: number;
  private readonly backoffFactor: number;
  private readonly jitterRatio: number;
  private socket: WebSocketLike | null = null;
  private reconnectTimer: unknown;
  private pingTimer: unknown;
  private heartbeatTimer: unknown;
  private connectionStabilityTimer: unknown;
  private subscriptionAckTimer: unknown;
  private subscriptionRetryTimer: unknown;
  private desiredSymbols: readonly string[] = Object.freeze([]);
  private confirmedSymbols: readonly string[] = Object.freeze([]);
  private readonly pendingSubscriptionOperations = new Map<
    string,
    PendingSubscriptionOperation
  >();
  private generation = 0;
  private reconnectAttempt = 0;
  private subscriptionRetryAttempt = 0;
  private requestSequence = 0;
  private running = false;
  private status: SourceStatus = Object.freeze({
    state: "idle",
    generation: 0,
    reconnectAttempt: 0,
  });

  constructor(private readonly options: GatewayWebSocketSourceOptions) {
    this.scheduler = options.scheduler ?? defaultScheduler;
    this.socketFactory =
      options.webSocketFactory ??
      ((url) => new WebSocket(url) as unknown as WebSocketLike);
    this.random = options.random ?? Math.random;
    this.pingIntervalMs = positive(options.pingIntervalMs ?? 10_000, "pingIntervalMs");
    this.heartbeatTimeoutMs = positive(
      options.heartbeatTimeoutMs ?? 30_000,
      "heartbeatTimeoutMs",
    );
    this.connectionStabilityMs = positive(
      options.connectionStabilityMs ?? this.heartbeatTimeoutMs,
      "connectionStabilityMs",
    );
    this.subscriptionAckTimeoutMs = positive(
      options.subscriptionAckTimeoutMs ?? 10_000,
      "subscriptionAckTimeoutMs",
    );
    this.initialBackoffMs = positive(
      options.initialBackoffMs ?? 500,
      "initialBackoffMs",
    );
    this.maximumBackoffMs = positive(
      options.maximumBackoffMs ?? 30_000,
      "maximumBackoffMs",
    );
    this.backoffFactor = positive(options.backoffFactor ?? 2, "backoffFactor");
    this.jitterRatio = boundedRatio(options.jitterRatio ?? 0.25);
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.reconnectAttempt = 0;
    this.openSocket(false);
  }

  stop(): void {
    if (!this.running && this.status.state === "stopped") return;
    this.running = false;
    this.generation += 1;
    this.clearTimers();
    const socket = this.socket;
    this.socket = null;
    this.resetSubscriptionState();
    if (socket != null) {
      this.detachSocket(socket);
      try {
        socket.close(1000, "client stopped");
      } catch {
        // The socket is already fenced; close failures are non-actionable.
      }
    }
    this.setStatus({
      state: "stopped",
      generation: this.generation,
      reconnectAttempt: 0,
    });
  }

  replaceSymbols(symbols: readonly string[]): void {
    const normalized = normalizeSymbols(symbols);
    if (sameSymbols(this.desiredSymbols, normalized)) return;
    this.desiredSymbols = normalized;
    this.clearSubscriptionRetryTimer();
    this.subscriptionRetryAttempt = 0;
    this.clearSubscriptionError();
    if (
      this.pendingSubscriptionOperations.size === 0 &&
      sameSymbols(this.confirmedSymbols, this.desiredSymbols)
    ) {
      this.clearSubscriptionRetryTimer();
      this.subscriptionRetryAttempt = 0;
      this.clearSubscriptionError();
      return;
    }
    this.flushDesiredSymbols();
  }

  ping(): void {
    const socket = this.socket;
    const generation = this.generation;
    if (!this.isCurrentOpenSocket(socket, generation)) return;
    this.sendCommand(socket, generation, {
      action: "ping",
      request_id: this.nextRequestId("ping", generation),
    });
  }

  subscribe(listener: SourceEventListener): Unsubscribe {
    this.eventListeners.add(listener);
    return () => this.eventListeners.delete(listener);
  }

  subscribeStatus(listener: SourceStatusListener): Unsubscribe {
    this.statusListeners.add(listener);
    return () => this.statusListeners.delete(listener);
  }

  getStatus(): SourceStatus {
    return this.status;
  }

  /** Immediately fences the current socket and starts a fresh generation. */
  reconnectNow(): void {
    if (!this.running) return;
    this.clearTimers();
    this.reconnectAttempt = 0;
    const socket = this.socket;
    this.socket = null;
    this.resetSubscriptionState();
    if (socket != null) {
      this.detachSocket(socket);
      try {
        socket.close(1012, "client reconnect");
      } catch {
        // A fresh generation is opened below regardless.
      }
    }
    this.openSocket(true);
  }

  private openSocket(isReconnect: boolean): void {
    if (!this.running) return;
    this.generation += 1;
    const generation = this.generation;
    this.resetSubscriptionState();
    this.setStatus({
      state: isReconnect ? "reconnecting" : "connecting",
      generation,
      reconnectAttempt: this.reconnectAttempt,
    });

    let socket: WebSocketLike;
    try {
      socket = this.socketFactory(this.connectionUrl());
    } catch {
      this.scheduleReconnect(generation, "connection_factory_failed");
      return;
    }
    this.socket = socket;
    socket.onopen = () => this.handleOpen(socket, generation);
    socket.onmessage = (event) => this.handleMessage(socket, generation, event.data);
    socket.onerror = () => this.handleSocketFailure(socket, generation, "connection_error");
    socket.onclose = () => this.handleSocketFailure(socket, generation, "connection_closed");
  }

  private handleOpen(socket: WebSocketLike, generation: number): void {
    if (!this.isCurrentSocket(socket, generation)) return;
    this.setStatus({
      state: "live",
      generation,
      reconnectAttempt: this.reconnectAttempt,
    });
    this.flushDesiredSymbols();
    this.schedulePing(socket, generation);
    this.scheduleHeartbeatTimeout(socket, generation);
  }

  private handleMessage(
    socket: WebSocketLike,
    generation: number,
    raw: unknown,
  ): void {
    if (!this.isCurrentSocket(socket, generation)) return;
    const parsed = safeParseWireMessage(raw);
    if (!parsed.success) {
      this.emit({
        type: "notice",
        level: "warning",
        code: "invalid_wire_message",
        message: "The gateway sent a frame that did not match the public wire contract",
        generation,
      });
      return;
    }
    this.scheduleHeartbeatTimeout(socket, generation);
    this.scheduleConnectionStability(socket, generation);
    this.emitWireMessage(parsed.data, generation);
  }

  private emitWireMessage(message: WireMessage, generation: number): void {
    const requestId = message.request_id ?? undefined;
    switch (message.type) {
      case "snapshot":
        // Gateway snapshots intentionally arrive before their subscribe acknowledgement.
        this.emit({
          type: "market-data",
          quote: normalizeWireQuote(message.data.quote),
          origin: "snapshot",
          generation,
          stale: message.data.stale,
          ageMs: message.data.age_ms,
          ...(requestId == null ? {} : { requestId }),
        });
        return;
      case "quote": {
        const quote = normalizeWireQuote(message.data.quote);
        this.emit({
          type: "market-data",
          quote,
          origin: "live",
          generation,
          stale: false,
          ageMs: 0,
          ...(requestId == null ? {} : { requestId }),
        });
        return;
      }
      case "ack":
        if (message.data.action === "pong") {
          this.emit({
            type: "pong",
            generation,
            ...(requestId == null ? {} : { requestId }),
          });
        } else {
          this.handleSubscriptionAck(message.data.action, requestId, generation);
        }
        return;
      case "warning":
        this.emit({
          type: "notice",
          level: "warning",
          code: message.data.code,
          message: message.data.message,
          generation,
          ...(requestId == null ? {} : { requestId }),
        });
        return;
      case "error":
        this.handleGatewayError(message.data.code, requestId, generation);
        return;
      case "connected":
      case "heartbeat":
        return;
    }
  }

  private handleSocketFailure(
    socket: WebSocketLike,
    generation: number,
    code: string,
  ): void {
    if (!this.isCurrentSocket(socket, generation)) return;
    this.socket = null;
    this.detachSocket(socket);
    this.clearPingTimer();
    this.clearHeartbeatTimer();
    this.clearConnectionStabilityTimer();
    this.resetSubscriptionState();
    try {
      socket.close();
    } catch {
      // The failed socket is already detached and fenced.
    }
    this.scheduleReconnect(generation, code);
  }

  private scheduleReconnect(generation: number, code: string): void {
    if (!this.running || generation !== this.generation) return;
    this.reconnectAttempt += 1;
    const exponent = Math.max(0, this.reconnectAttempt - 1);
    const baseDelay = Math.min(
      this.maximumBackoffMs,
      this.initialBackoffMs * this.backoffFactor ** exponent,
    );
    const jitter = baseDelay * this.jitterRatio * clampRandom(this.random());
    const delayMs = Math.round(Math.min(this.maximumBackoffMs, baseDelay + jitter));
    this.setStatus({
      state: "reconnecting",
      generation,
      reconnectAttempt: this.reconnectAttempt,
      nextRetryMs: delayMs,
      lastError: code,
    });
    this.reconnectTimer = this.scheduler.setTimeout(() => {
      this.reconnectTimer = undefined;
      if (!this.running || generation !== this.generation) return;
      this.openSocket(true);
    }, delayMs);
  }

  private flushDesiredSymbols(): void {
    const socket = this.socket;
    const generation = this.generation;
    if (!this.isCurrentOpenSocket(socket, generation)) return;
    if (
      this.pendingSubscriptionOperations.size > 0 ||
      this.subscriptionRetryTimer !== undefined
    ) {
      return;
    }
    const confirmedSet = new Set(this.confirmedSymbols);
    const desiredSet = new Set(this.desiredSymbols);
    const removed = this.confirmedSymbols.filter((symbol) => !desiredSet.has(symbol));
    const added = this.desiredSymbols.filter((symbol) => !confirmedSet.has(symbol));
    const action: SubscriptionAction | undefined =
      removed.length > 0 ? "unsubscribe" : added.length > 0 ? "subscribe" : undefined;
    const symbols = action === "unsubscribe" ? removed : added;
    if (action == null || symbols.length === 0) {
      this.clearSubscriptionError();
      return;
    }

    const requestId = this.nextRequestId(action, generation);
    this.pendingSubscriptionOperations.set(
      requestId,
      Object.freeze({
        requestId,
        action,
        symbols: Object.freeze([...symbols]),
        generation,
      }),
    );
    this.scheduleSubscriptionAckTimeout(socket, generation, requestId);
    this.sendCommand(socket, generation, {
      action,
      symbols,
      channels: ["quote"],
      request_id: requestId,
    });
  }

  private handleSubscriptionAck(
    action: SubscriptionAction,
    requestId: string | undefined,
    generation: number,
  ): void {
    const operation =
      requestId == null ? undefined : this.pendingSubscriptionOperations.get(requestId);
    if (
      operation == null ||
      operation.generation !== generation ||
      operation.action !== action
    ) {
      this.emit({
        type: "notice",
        level: "warning",
        code: "unmatched_subscription_ack",
        message: "The gateway acknowledged an unknown subscription operation",
        generation,
        ...(requestId == null ? {} : { requestId }),
      });
      return;
    }

    this.pendingSubscriptionOperations.delete(operation.requestId);
    this.clearSubscriptionAckTimer();
    const confirmed = new Set(this.confirmedSymbols);
    for (const symbol of operation.symbols) {
      if (operation.action === "subscribe") confirmed.add(symbol);
      else confirmed.delete(symbol);
    }
    this.confirmedSymbols = Object.freeze([...confirmed].sort());
    this.subscriptionRetryAttempt = 0;
    this.clearSubscriptionError();
    this.emit({
      type: "subscription-ack",
      action: operation.action,
      symbols: operation.symbols,
      generation,
      requestId: operation.requestId,
    });
    this.flushDesiredSymbols();
  }

  private handleGatewayError(
    rawCode: string,
    requestId: string | undefined,
    generation: number,
  ): void {
    const code = sanitizeGatewayCode(rawCode);
    const operation =
      requestId == null ? undefined : this.pendingSubscriptionOperations.get(requestId);
    if (operation != null && operation.generation === generation) {
      this.pendingSubscriptionOperations.delete(operation.requestId);
      this.clearSubscriptionAckTimer();
    }

    this.emit({
      type: "notice",
      level: "error",
      code,
      message:
        operation == null
          ? `The gateway reported an error (${code})`
          : `The gateway rejected the ${operation.action} operation (${code})`,
      generation,
      ...(requestId == null ? {} : { requestId }),
    });

    if (operation != null) {
      const statusCode = `subscription_${code}`;
      if (isTransientSubscriptionError(code)) {
        this.scheduleSubscriptionRetry(generation, statusCode);
      } else {
        this.setStatus({
          state: this.status.state,
          generation,
          reconnectAttempt: this.status.reconnectAttempt,
          lastError: statusCode,
        });
      }
      return;
    }
    this.setStatus({
      state: this.status.state,
      generation,
      reconnectAttempt: this.status.reconnectAttempt,
      lastError: `gateway_${code}`,
    });
  }

  private scheduleSubscriptionRetry(generation: number, code: string): void {
    if (!this.running || generation !== this.generation) return;
    if (sameSymbols(this.confirmedSymbols, this.desiredSymbols)) {
      this.clearSubscriptionError();
      return;
    }
    if (this.subscriptionRetryTimer !== undefined) return;

    this.subscriptionRetryAttempt += 1;
    const delayMs = this.retryDelay(this.subscriptionRetryAttempt);
    this.setStatus({
      state: this.status.state,
      generation,
      reconnectAttempt: this.status.reconnectAttempt,
      nextRetryMs: delayMs,
      lastError: code,
    });
    this.subscriptionRetryTimer = this.scheduler.setTimeout(() => {
      this.subscriptionRetryTimer = undefined;
      if (!this.running || generation !== this.generation) return;
      this.setStatus({
        state: this.status.state,
        generation,
        reconnectAttempt: this.status.reconnectAttempt,
        lastError: code,
      });
      this.flushDesiredSymbols();
    }, delayMs);
  }

  private sendCommand(
    socket: WebSocketLike,
    generation: number,
    command: Readonly<Record<string, unknown>>,
  ): void {
    if (!this.isCurrentOpenSocket(socket, generation)) return;
    try {
      socket.send(JSON.stringify(command));
    } catch {
      this.handleSocketFailure(socket, generation, "send_failed");
    }
  }

  private schedulePing(socket: WebSocketLike, generation: number): void {
    this.clearPingTimer();
    this.pingTimer = this.scheduler.setTimeout(() => {
      this.pingTimer = undefined;
      if (!this.isCurrentOpenSocket(socket, generation)) return;
      this.ping();
      this.schedulePing(socket, generation);
    }, this.pingIntervalMs);
  }

  private scheduleHeartbeatTimeout(socket: WebSocketLike, generation: number): void {
    this.clearHeartbeatTimer();
    this.heartbeatTimer = this.scheduler.setTimeout(() => {
      this.heartbeatTimer = undefined;
      if (!this.isCurrentOpenSocket(socket, generation)) return;
      this.handleSocketFailure(socket, generation, "heartbeat_timeout");
    }, this.heartbeatTimeoutMs);
  }

  private scheduleConnectionStability(socket: WebSocketLike, generation: number): void {
    if (this.reconnectAttempt === 0 || this.connectionStabilityTimer !== undefined) return;
    this.connectionStabilityTimer = this.scheduler.setTimeout(() => {
      this.connectionStabilityTimer = undefined;
      if (!this.isCurrentOpenSocket(socket, generation)) return;
      this.reconnectAttempt = 0;
      const lastError = this.status.lastError;
      const preserveError =
        lastError?.startsWith("subscription_") || lastError?.startsWith("gateway_");
      this.setStatus({
        state: "live",
        generation,
        reconnectAttempt: 0,
        ...(preserveError ? { lastError } : {}),
      });
    }, this.connectionStabilityMs);
  }

  private scheduleSubscriptionAckTimeout(
    socket: WebSocketLike,
    generation: number,
    requestId: string,
  ): void {
    this.clearSubscriptionAckTimer();
    this.subscriptionAckTimer = this.scheduler.setTimeout(() => {
      this.subscriptionAckTimer = undefined;
      if (!this.isCurrentOpenSocket(socket, generation)) return;
      const operation = this.pendingSubscriptionOperations.get(requestId);
      if (operation == null || operation.generation !== generation) return;
      this.emit({
        type: "notice",
        level: "error",
        code: "subscription_ack_timeout",
        message: "The gateway did not acknowledge the subscription operation in time",
        generation,
        requestId,
      });
      this.handleSocketFailure(socket, generation, "subscription_ack_timeout");
    }, this.subscriptionAckTimeoutMs);
  }

  private retryDelay(attempt: number): number {
    const exponent = Math.max(0, attempt - 1);
    const baseDelay = Math.min(
      this.maximumBackoffMs,
      this.initialBackoffMs * this.backoffFactor ** exponent,
    );
    const jitter = baseDelay * this.jitterRatio * clampRandom(this.random());
    return Math.round(Math.min(this.maximumBackoffMs, baseDelay + jitter));
  }

  private connectionUrl(): string {
    const url = new URL(this.options.url);
    const token = this.options.getToken?.();
    if (token) url.searchParams.set("token", token);
    return url.toString();
  }

  private nextRequestId(kind: string, generation: number): string {
    this.requestSequence += 1;
    return `${kind}-${generation}-${this.requestSequence}`;
  }

  private isCurrentSocket(
    socket: WebSocketLike | null,
    generation: number,
  ): socket is WebSocketLike {
    return (
      socket != null &&
      this.running &&
      this.socket === socket &&
      this.generation === generation
    );
  }

  private isCurrentOpenSocket(
    socket: WebSocketLike | null,
    generation: number,
  ): socket is WebSocketLike {
    return this.isCurrentSocket(socket, generation) && socket.readyState === OPEN;
  }

  private emit(event: MarketDataSourceEvent): void {
    for (const listener of [...this.eventListeners]) listener(event);
  }

  private setStatus(status: SourceStatus): void {
    this.status = Object.freeze({ ...status });
    for (const listener of [...this.statusListeners]) listener(this.status);
  }

  private clearTimers(): void {
    if (this.reconnectTimer !== undefined) {
      this.scheduler.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.clearPingTimer();
    this.clearHeartbeatTimer();
    this.clearConnectionStabilityTimer();
    this.clearSubscriptionAckTimer();
    this.clearSubscriptionRetryTimer();
  }

  private clearPingTimer(): void {
    if (this.pingTimer !== undefined) {
      this.scheduler.clearTimeout(this.pingTimer);
      this.pingTimer = undefined;
    }
  }

  private clearHeartbeatTimer(): void {
    if (this.heartbeatTimer !== undefined) {
      this.scheduler.clearTimeout(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
  }

  private clearConnectionStabilityTimer(): void {
    if (this.connectionStabilityTimer !== undefined) {
      this.scheduler.clearTimeout(this.connectionStabilityTimer);
      this.connectionStabilityTimer = undefined;
    }
  }

  private clearSubscriptionAckTimer(): void {
    if (this.subscriptionAckTimer !== undefined) {
      this.scheduler.clearTimeout(this.subscriptionAckTimer);
      this.subscriptionAckTimer = undefined;
    }
  }

  private clearSubscriptionRetryTimer(): void {
    if (this.subscriptionRetryTimer !== undefined) {
      this.scheduler.clearTimeout(this.subscriptionRetryTimer);
      this.subscriptionRetryTimer = undefined;
    }
  }

  private clearSubscriptionError(): void {
    if (
      this.status.lastError == null ||
      (!this.status.lastError.startsWith("subscription_") &&
        !this.status.lastError.startsWith("gateway_"))
    ) {
      return;
    }
    this.setStatus({
      state: this.status.state,
      generation: this.status.generation,
      reconnectAttempt: this.status.reconnectAttempt,
    });
  }

  private resetSubscriptionState(): void {
    this.clearSubscriptionAckTimer();
    this.clearSubscriptionRetryTimer();
    this.pendingSubscriptionOperations.clear();
    this.confirmedSymbols = Object.freeze([]);
    this.subscriptionRetryAttempt = 0;
  }

  private detachSocket(socket: WebSocketLike): void {
    socket.onopen = null;
    socket.onmessage = null;
    socket.onerror = null;
    socket.onclose = null;
  }
}

function positive(value: number, name: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive finite number`);
  }
  return value;
}

function boundedRatio(value: number): number {
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new RangeError("jitterRatio must be between 0 and 1");
  }
  return value;
}

function clampRandom(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.min(1, Math.max(0, value));
}

function sameSymbols(left: readonly string[], right: readonly string[]): boolean {
  return (
    left.length === right.length &&
    left.every((symbol, index) => symbol === right[index])
  );
}

function sanitizeGatewayCode(value: string): string {
  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "_")
    .slice(0, 64);
  return normalized || "gateway_error";
}

function isTransientSubscriptionError(code: string): boolean {
  return code === "rate_limit";
}
