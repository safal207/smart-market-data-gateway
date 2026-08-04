import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";

import {
  activityLabel as deriveActivityLabel,
  type ChartCandle,
  type ChartTheme,
} from "./chart";
import {
  DEFAULT_WORKSPACE,
  MarketDataStore,
  derivePricePrecision,
  isMarketDataPointStale,
  isTimeframe,
  loadWorkspace,
  marketDataAgeMs,
  saveWorkspace,
  type Candle,
  type MarketDataStoreSnapshot,
  type Timeframe,
} from "./engine";
import {
  GatewayWebSocketSource,
  ReplayMarketDataSource,
  SmartSubscriptionManager,
  type GatewayWebSocketSourceOptions,
  type NormalizedQuote,
  type ReplayFrame,
} from "./market-data";
import {
  ChartWorkspace,
  type ChartWorkspaceActions,
  type ChartWorkspaceModel,
  type WorkspaceConnectionState,
  type WorkspaceSourceMode,
} from "./features/workspace";

const SYMBOLS = Object.freeze([
  "AAPL.US", "MSFT.US", "NVDA.US", "TSLA.US", "AAPL", "MSFT", "NVDA", "TSLA",
]);
const TIMEFRAME_OPTIONS = Object.freeze([
  { value: "5s", label: "5 seconds" },
  { value: "1m", label: "1 minute" },
  { value: "5m", label: "5 minutes" },
  { value: "15m", label: "15 minutes" },
]);
const EMPTY_SNAPSHOT: MarketDataStoreSnapshot = Object.freeze({
  revision: 0,
  timeframe: "1m",
  connectionStatus: Object.freeze({ state: "idle", generation: 0, reconnectAttempt: 0 }),
  latestQuotes: Object.freeze({}),
  latestPoints: Object.freeze({}),
  candles: Object.freeze({}),
  acceptedLog: Object.freeze([]),
  quarantineLog: Object.freeze([]),
  duplicateCounts: Object.freeze({}),
});

interface Runtime {
  readonly source: ReplayMarketDataSource | GatewayWebSocketSource;
  readonly manager: SmartSubscriptionManager;
  readonly store: MarketDataStore;
}

let replayFrames: readonly ReplayFrame[] | undefined;

export function App() {
  const initialWorkspace = useMemo(() => loadWorkspace(), []);
  const [symbol, setSymbol] = useState(initialWorkspace.selectedSymbol);
  const [timeframe, setTimeframe] = useState<Timeframe>(initialWorkspace.timeframe);
  const [theme, setTheme] = useState<ChartTheme>(() => resolveTheme(initialWorkspace.theme));
  const [sourceMode, setSourceMode] = useState<WorkspaceSourceMode>(() =>
    new URLSearchParams(window.location.search).get("source") === "gateway" ? "gateway" : "replay",
  );
  const [gatewayToken, setGatewayToken] = useState(() =>
    import.meta.env.DEV ? "dev-basic:chart-reference" : "",
  );
  const [paused, setPaused] = useState(false);
  const [frozenSnapshot, setFrozenSnapshot] = useState<MarketDataStoreSnapshot | null>(null);
  const [runtime, setRuntime] = useState<Runtime | null>(null);
  const [runtimeRevision, setRuntimeRevision] = useState(0);
  const [freshnessNowMs, setFreshnessNowMs] = useState(() => Date.now());
  const tokenRef = useRef(gatewayToken);
  const pausedRef = useRef(paused);

  useEffect(() => { tokenRef.current = gatewayToken; }, [gatewayToken]);
  useEffect(() => { pausedRef.current = paused; }, [paused]);

  useEffect(() => {
    if (sourceMode !== "gateway") return;
    setFreshnessNowMs(Date.now());
    const freshnessTimer = window.setInterval(() => setFreshnessNowMs(Date.now()), 1_000);
    return () => window.clearInterval(freshnessTimer);
  }, [sourceMode]);

  useEffect(() => {
    const source = createSource(sourceMode, tokenRef);
    const manager = new SmartSubscriptionManager(source);
    const store = new MarketDataStore({ timeframe });
    store.attachSubscriptionManager(manager);
    setRuntime({ source, manager, store });
    const replayTimer = source instanceof ReplayMarketDataSource
      ? window.setInterval(() => {
          if (!pausedRef.current && !source.exhausted) source.advanceBy(1_000);
        }, 250)
      : undefined;
    return () => {
      if (replayTimer != null) window.clearInterval(replayTimer);
      store.dispose();
      manager.dispose();
    };
    // Timeframe is applied independently; reconnecting should not reset chart preferences.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourceMode, runtimeRevision]);

  useEffect(() => {
    if (runtime == null) return;
    runtime.store.clear();
    runtime.manager.replaceConsumerSubscriptions("chart-workspace", [symbol]);
    if (runtime.source instanceof ReplayMarketDataSource) {
      runtime.source.seek(0);
      runtime.source.advanceTo(300_000);
    }
  }, [runtime, symbol]);

  useEffect(() => { runtime?.store.setTimeframe(timeframe); }, [runtime, timeframe]);

  useEffect(() => {
    saveWorkspace(window.localStorage, {
      ...DEFAULT_WORKSPACE,
      selectedSymbol: symbol,
      timeframe,
      theme,
    });
  }, [symbol, timeframe, theme]);

  const subscribeStore = useCallback(
    (listener: () => void) => runtime?.store.subscribe(listener) ?? (() => undefined),
    [runtime],
  );
  const getStoreSnapshot = useCallback(
    () => runtime?.store.getSnapshot() ?? EMPTY_SNAPSHOT,
    [runtime],
  );
  const liveSnapshot = useSyncExternalStore(subscribeStore, getStoreSnapshot, getStoreSnapshot);
  const snapshot = paused && frozenSnapshot != null ? frozenSnapshot : liveSnapshot;

  const model = useMemo<ChartWorkspaceModel>(() => {
    const latestPoint = snapshot.latestPoints[symbol] ?? null;
    const quote = latestPoint?.quote ?? snapshot.latestQuotes[symbol] ?? null;
    const candles = (snapshot.candles[symbol] ?? []).map(toChartCandle);
    const accepted = snapshot.acceptedLog.filter((entry) => entry.quote.symbol === symbol);
    const recentPrices = accepted.slice(-64).map((entry) => entry.quote.price);
    const precision = derivePricePrecision({ observedPrices: recentPrices }).precision;
    const previousQuote = accepted.at(-2)?.quote;
    const latestAgeMs = latestPoint == null ? undefined : marketDataAgeMs(latestPoint, freshnessNowMs);
    const stale = sourceMode === "gateway" && latestPoint != null
      && isMarketDataPointStale(latestPoint, freshnessNowMs);
    const connectionState = workspaceStatus(
      snapshot.connectionStatus.state, sourceMode, paused, stale,
    );

    return {
      symbol,
      symbolOptions: SYMBOLS,
      timeframe,
      timeframeOptions: TIMEFRAME_OPTIONS,
      timeframeLabel: TIMEFRAME_OPTIONS.find((option) => option.value === timeframe)?.label ?? timeframe,
      theme,
      sourceMode,
      connectionState,
      connectionDetail: connectionDetail(sourceMode, snapshot.connectionStatus.lastError),
      reconnectAttempt: snapshot.connectionStatus.reconnectAttempt,
      lastUpdateMs: quote?.receivedAtMs,
      quote: quote == null ? null : {
        symbol: quote.symbol,
        price: quote.price,
        ...(quote.bid == null ? {} : { bid: quote.bid }),
        ...(quote.ask == null ? {} : { ask: quote.ask }),
        provider: quote.provider,
        providerTimestampMs: quote.providerTimestampMs,
        origin: latestPoint?.origin ?? "live",
        sourceStale: latestPoint?.stale ?? false,
        ageMs: latestAgeMs ?? 0,
        direction: previousQuote == null || quote.price === previousQuote.price
          ? "flat"
          : quote.price > previousQuote.price ? "up" : "down",
        stale,
      },
      candles,
      precision,
      activityLabel: deriveActivityLabel(candles),
      diagnostics: {
        accepted: accepted.length,
        duplicates: snapshot.duplicateCounts[symbol] ?? 0,
        rejected: snapshot.quarantineLog.filter((entry) => entry.point.quote.symbol === symbol).length,
        gaps: accepted.filter((entry) => entry.acceptedReason === "sequence_gap").length,
      },
      paused,
      gatewayToken,
    };
  }, [freshnessNowMs, gatewayToken, paused, snapshot, sourceMode, symbol, theme, timeframe]);

  const actions = useMemo<ChartWorkspaceActions>(() => ({
    selectSymbol: setSymbol,
    selectTimeframe: (value) => { if (isTimeframe(value)) setTimeframe(value); },
    selectTheme: setTheme,
    selectSourceMode: setSourceMode,
    setGatewayToken,
    togglePaused: () => {
      setFrozenSnapshot(paused ? null : liveSnapshot);
      setPaused(!paused);
    },
    reconnect: () => {
      if (runtime?.source instanceof GatewayWebSocketSource) runtime.source.reconnectNow();
      else setRuntimeRevision((current) => current + 1);
    },
  }), [liveSnapshot, paused, runtime]);

  return <ChartWorkspace model={model} actions={actions} />;
}

function getReplayFrames(): readonly ReplayFrame[] {
  replayFrames ??= createReplayFrames();
  return replayFrames;
}

function createSource(
  mode: WorkspaceSourceMode,
  tokenRef: { readonly current: string },
): ReplayMarketDataSource | GatewayWebSocketSource {
  if (mode === "replay") return new ReplayMarketDataSource(getReplayFrames());
  const options: GatewayWebSocketSourceOptions = {
    url: gatewayWebSocketUrl(),
    getToken: () => tokenRef.current,
  };
  return new GatewayWebSocketSource(options);
}

function gatewayWebSocketUrl(): string {
  const configured = import.meta.env.VITE_GATEWAY_WS_URL;
  if (configured != null && configured.trim() !== "") return configured;
  const url = new URL("/v1/stream", window.location.origin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

function resolveTheme(theme: "dark" | "light" | "system"): ChartTheme {
  if (theme !== "system") return theme;
  return window.matchMedia?.("(prefers-color-scheme: light)").matches === true ? "light" : "dark";
}

function toChartCandle(candle: Candle): ChartCandle {
  const activitySource = candle.volumeSource === "update-count"
    ? "updates"
    : candle.volumeSource === "mixed" ? "mixed" : "volume";
  return {
    timeMs: candle.openTimeMs,
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
    updateCount: candle.updateCount,
    activityValue: candle.volume,
    activitySource,
  };
}

function workspaceStatus(
  sourceState: MarketDataStoreSnapshot["connectionStatus"]["state"],
  mode: WorkspaceSourceMode,
  paused: boolean,
  stale: boolean,
): WorkspaceConnectionState {
  if (paused) return "paused";
  if (stale) return "stale";
  if (mode === "replay" && sourceState === "live") return "replay";
  return sourceState;
}

function connectionDetail(mode: WorkspaceSourceMode, error: string | undefined): string {
  if (error != null) {
    const detail = error.replaceAll("_", " ");
    if (error.startsWith("gateway_") || (error.startsWith("subscription_") && error !== "subscription_rate_limit")) {
      return `The gateway rejected the request (${detail}). Change the request or reconnect.`;
    }
    return `The stream will retry automatically (${detail}).`;
  }
  return mode === "replay"
    ? "Deterministic synthetic quotes advancing at 4× speed."
    : "Browser WebSocket routed to the local Smart Market Data Gateway.";
}

function createReplayFrames(): readonly ReplayFrame[] {
  const instruments = [
    { symbol: "AAPL.US", base: 215.4 }, { symbol: "MSFT.US", base: 429.15 },
    { symbol: "NVDA.US", base: 117.8 }, { symbol: "TSLA.US", base: 248.1 },
    { symbol: "AAPL", base: 215.4 }, { symbol: "MSFT", base: 429.15 },
    { symbol: "NVDA", base: 117.8 }, { symbol: "TSLA", base: 248.1 },
  ] as const;
  const baseTimestamp = Date.UTC(2026, 0, 5, 14, 30, 0);
  const frames: ReplayFrame[] = [];
  let eventNumber = 0;
  for (let second = 0; second < 900; second += 1) {
    for (const [instrumentIndex, instrument] of instruments.entries()) {
      eventNumber += 1;
      const drift = second * 0.0015;
      const wave = Math.sin((second + instrumentIndex * 7) / 13) * 0.42;
      const micro = Math.cos((second + instrumentIndex * 3) / 5) * 0.08;
      const price = Math.round((instrument.base + drift + wave + micro) * 100) / 100;
      const quote: NormalizedQuote = Object.freeze({
        schemaVersion: "1.0",
        eventId: `00000000-0000-4000-8000-${eventNumber.toString(16).padStart(12, "0")}`,
        symbol: instrument.symbol,
        price,
        bid: Math.round((price - 0.02) * 100) / 100,
        ask: Math.round((price + 0.02) * 100) / 100,
        providerTimestampMs: baseTimestamp + second * 1_000,
        receivedAtMs: baseTimestamp + second * 1_000 + 12,
        sequence: second + 1,
        provider: "synthetic-replay",
      });
      frames.push(Object.freeze({ atMs: second * 1_000, quote }));
    }
  }
  return Object.freeze(frames);
}
