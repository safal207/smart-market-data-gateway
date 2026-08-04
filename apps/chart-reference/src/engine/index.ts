export {
  TIMEFRAMES,
  TIMEFRAME_DURATION_MS,
  isTimeframe,
} from "./types";
export type {
  AcceptedMarketDataLogEntry,
  Candle,
  CandleVolumeSource,
  Timeframe,
} from "./types";

export { CandleAggregator, bucketStart } from "./candle-aggregator";
export type {
  CandleAggregationUpdate,
  CandleAggregatorOptions,
} from "./candle-aggregator";

export { TemporalIntegrityGuard } from "./temporal-integrity";
export type {
  AcceptedDecision,
  DuplicateDecision,
  IntegrityAcceptedReason,
  IntegrityDecision,
  QuarantineReason,
  QuarantinedDecision,
  QuarantinedMarketData,
  TemporalIntegrityGuardOptions,
} from "./temporal-integrity";

export { MarketDataStore } from "./market-data-store";
export type {
  MarketDataStoreOptions,
  MarketDataStoreSnapshot,
} from "./market-data-store";

export {
  decimalPlaces,
  derivePricePrecision,
  formatPrice,
} from "./precision";
export type { PricePrecision, PricePrecisionOptions } from "./precision";

export {
  DEFAULT_STALE_AFTER_MS,
  isMarketDataPointStale,
  marketDataAgeMs,
} from "./freshness";

export {
  DEFAULT_WORKSPACE,
  WORKSPACE_STORAGE_KEY,
  WORKSPACE_VERSION,
  clearWorkspace,
  loadWorkspace,
  saveBrowserWorkspace,
  saveWorkspace,
} from "./workspace-persistence";
export type {
  WorkspaceState,
  WorkspaceStorage,
  WorkspaceTheme,
} from "./workspace-persistence";
