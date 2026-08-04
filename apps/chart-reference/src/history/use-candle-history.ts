import { useEffect, useRef, useState } from "react";

import type { ChartCandle } from "../chart";
import type { Timeframe } from "../engine";
import {
  CandleHistoryClient,
  CandleHistoryLoader,
  gatewayHttpBaseUrl,
  isServerHistoryTimeframe,
  type CandleHistoryFailureKind,
} from "./candle-history";

export type CandleHistoryState =
  | "disabled"
  | "unsupported"
  | "loading"
  | "ready"
  | "empty"
  | "unauthorized"
  | "forbidden"
  | "rate_limited"
  | "error";

export interface CandleHistorySnapshot {
  readonly state: CandleHistoryState;
  readonly candles: readonly ChartCandle[];
  readonly count: number;
  readonly source?: "observed_quote_aggregation";
  readonly warnings: readonly string[];
  readonly detail: string;
}

export interface UseCandleHistoryOptions {
  readonly enabled: boolean;
  readonly symbol: string;
  readonly timeframe: Timeframe;
  readonly token: string;
  readonly reloadRevision?: number;
  readonly limit?: number;
}

const DISABLED: CandleHistorySnapshot = Object.freeze({
  state: "disabled",
  candles: Object.freeze([]),
  count: 0,
  warnings: Object.freeze([]),
  detail: "Server history is disabled in synthetic replay mode.",
});

export function useCandleHistory(options: UseCandleHistoryOptions): CandleHistorySnapshot {
  const loaderRef = useRef<CandleHistoryLoader | null>(null);
  loaderRef.current ??= new CandleHistoryLoader(
    new CandleHistoryClient({ baseUrl: gatewayHttpBaseUrl() }),
  );
  const [snapshot, setSnapshot] = useState<CandleHistorySnapshot>(DISABLED);

  useEffect(() => {
    const loader = loaderRef.current;
    if (loader == null) return undefined;
    if (!options.enabled) {
      loader.cancel();
      setSnapshot(DISABLED);
      return undefined;
    }
    if (!isServerHistoryTimeframe(options.timeframe)) {
      loader.cancel();
      setSnapshot(Object.freeze({
        state: "unsupported",
        candles: Object.freeze([]),
        count: 0,
        warnings: Object.freeze([]),
        detail: "Canonical server history starts at 1 minute. The 5-second view uses live browser aggregation.",
      }));
      return undefined;
    }

    setSnapshot(Object.freeze({
      state: "loading",
      candles: Object.freeze([]),
      count: 0,
      warnings: Object.freeze([]),
      detail: `Loading canonical ${options.timeframe} candles for ${options.symbol}…`,
    }));
    void loader.load({
      symbol: options.symbol,
      timeframe: options.timeframe,
      token: options.token,
      limit: options.limit ?? 500,
    }).then((outcome) => {
      if (outcome.kind === "superseded") return;
      if (outcome.kind === "failure") {
        setSnapshot(failureSnapshot(outcome.error.kind, outcome.error.message));
        return;
      }
      const { series } = outcome;
      setSnapshot(Object.freeze({
        state: series.returnedCount === 0 ? "empty" : "ready",
        candles: series.candles,
        count: series.returnedCount,
        source: series.source,
        warnings: series.warnings,
        detail: series.returnedCount === 0
          ? "No fully closed candles are available in the retained window. Empty intervals are not fabricated."
          : `${series.returnedCount} canonical closed candles loaded from the gateway.`,
      }));
    });

    return () => loader.cancel();
  }, [
    options.enabled,
    options.limit,
    options.reloadRevision,
    options.symbol,
    options.timeframe,
    options.token,
  ]);

  return snapshot;
}

function failureSnapshot(kind: CandleHistoryFailureKind, message: string): CandleHistorySnapshot {
  const state: CandleHistoryState =
    kind === "unauthorized" ? "unauthorized"
      : kind === "forbidden" ? "forbidden"
        : kind === "rate_limited" ? "rate_limited"
          : "error";
  const detail =
    kind === "forbidden"
      ? "Historical candles require a Pro or Premium entitlement. Live streaming can continue on the current tier."
      : kind === "unauthorized"
        ? "The gateway rejected the history credential. Enter a valid token and reconnect."
        : kind === "rate_limited"
          ? "The history rate limit was reached. Keep the live stream open and retry after the limit resets."
          : `Server history is temporarily unavailable (${message}). Live data remains isolated from this failure.`;
  return Object.freeze({
    state,
    candles: Object.freeze([]),
    count: 0,
    warnings: Object.freeze([]),
    detail,
  });
}
