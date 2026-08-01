import { z } from "zod";
import { TIMEFRAMES, type Timeframe } from "./types";

export const WORKSPACE_STORAGE_KEY = "smdg.chart-reference.workspace";
export const WORKSPACE_VERSION = 1 as const;

const workspaceSchema = z
  .object({
    version: z.literal(WORKSPACE_VERSION),
    selectedSymbol: z
      .string()
      .trim()
      .transform((value) => value.toUpperCase())
      .pipe(z.string().min(1).max(32).regex(/^[A-Z0-9._:-]+$/)),
    timeframe: z.enum(TIMEFRAMES),
    theme: z.enum(["dark", "light", "system"]),
    showVolume: z.boolean(),
    autoReconnect: z.boolean(),
  })
  .strict();

const legacyWorkspaceSchema = z
  .object({
    version: z.literal(0).optional(),
    symbol: z.string(),
    resolution: z.string(),
    theme: z.enum(["dark", "light", "system"]).optional(),
    volume: z.boolean().optional(),
  })
  .passthrough();

export type WorkspaceTheme = "dark" | "light" | "system";

export interface WorkspaceState {
  readonly version: typeof WORKSPACE_VERSION;
  readonly selectedSymbol: string;
  readonly timeframe: Timeframe;
  readonly theme: WorkspaceTheme;
  readonly showVolume: boolean;
  readonly autoReconnect: boolean;
}

export interface WorkspaceStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

export const DEFAULT_WORKSPACE: WorkspaceState = Object.freeze({
  version: WORKSPACE_VERSION,
  selectedSymbol: "AAPL.US",
  timeframe: "1m",
  theme: "dark",
  showVolume: true,
  autoReconnect: true,
});

export function loadWorkspace(
  storage: WorkspaceStorage | undefined = browserStorage(),
  key = WORKSPACE_STORAGE_KEY,
  fallback: WorkspaceState = DEFAULT_WORKSPACE,
): WorkspaceState {
  if (storage == null) return Object.freeze({ ...fallback });
  try {
    const serialized = storage.getItem(key);
    if (serialized == null) return Object.freeze({ ...fallback });
    const raw: unknown = JSON.parse(serialized);
    const current = workspaceSchema.safeParse(raw);
    if (current.success) return Object.freeze(current.data);
    const migrated = migrateLegacyWorkspace(raw, fallback);
    return migrated ?? Object.freeze({ ...fallback });
  } catch {
    return Object.freeze({ ...fallback });
  }
}

/** Persists preferences only. Tokens, quote history, and bars are never serialized. */
export function saveWorkspace(
  storage: WorkspaceStorage | undefined,
  input: WorkspaceState,
  key = WORKSPACE_STORAGE_KEY,
): boolean {
  if (storage == null) return false;
  try {
    const safe = workspaceSchema.parse({
      version: WORKSPACE_VERSION,
      selectedSymbol: input.selectedSymbol,
      timeframe: input.timeframe,
      theme: input.theme,
      showVolume: input.showVolume,
      autoReconnect: input.autoReconnect,
    });
    storage.setItem(key, JSON.stringify(safe));
    return true;
  } catch {
    return false;
  }
}

export function clearWorkspace(
  storage: WorkspaceStorage | undefined,
  key = WORKSPACE_STORAGE_KEY,
): boolean {
  if (storage == null) return false;
  try {
    storage.removeItem(key);
    return true;
  } catch {
    return false;
  }
}

function migrateLegacyWorkspace(
  input: unknown,
  fallback: WorkspaceState,
): WorkspaceState | undefined {
  const legacy = legacyWorkspaceSchema.safeParse(input);
  if (!legacy.success) return undefined;
  const timeframe = normalizeLegacyTimeframe(legacy.data.resolution);
  if (timeframe == null) return undefined;
  const migrated = workspaceSchema.safeParse({
    version: WORKSPACE_VERSION,
    selectedSymbol: legacy.data.symbol,
    timeframe,
    theme: legacy.data.theme ?? fallback.theme,
    showVolume: legacy.data.volume ?? fallback.showVolume,
    autoReconnect: fallback.autoReconnect,
  });
  return migrated.success ? Object.freeze(migrated.data) : undefined;
}

function normalizeLegacyTimeframe(value: string): Timeframe | undefined {
  const normalized = value.trim().toLowerCase();
  const aliases: Readonly<Record<string, Timeframe>> = {
    "5": "5s",
    "5s": "5s",
    "60": "1m",
    "1m": "1m",
    "300": "5m",
    "5m": "5m",
    "900": "15m",
    "15m": "15m",
    "3600": "1h",
    "1h": "1h",
    d: "1d",
    "1d": "1d",
  };
  return aliases[normalized];
}

function browserStorage(): WorkspaceStorage | undefined {
  if (typeof window === "undefined") return undefined;
  try {
    return window.localStorage;
  } catch {
    return undefined;
  }
}
