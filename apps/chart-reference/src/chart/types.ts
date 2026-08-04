export type ChartTheme = "dark" | "light";
export type ChartActivitySource = "volume" | "updates" | "mixed";

export interface ChartCandle {
  readonly timeMs: number;
  readonly open: number;
  readonly high: number;
  readonly low: number;
  readonly close: number;
  readonly updateCount: number;
  readonly activityValue: number;
  readonly activitySource: ChartActivitySource;
}

export type CrosshairValue = ChartCandle;

export interface ChartMountOptions {
  readonly theme: ChartTheme;
  readonly precision: number;
  readonly onCrosshairMove?: (value: CrosshairValue | null) => void;
}

export interface ChartRenderer {
  mount(container: HTMLElement, options: ChartMountOptions): void;
  setCandles(candles: readonly ChartCandle[]): void;
  setTheme(theme: ChartTheme): void;
  setPrecision(precision: number): void;
  resize(width: number, height: number): void;
  destroy(): void;
}

export type ChartRendererFactory = () => ChartRenderer;

export function activityLabel(candles: readonly ChartCandle[]): "Volume" | "Updates" | "Activity" {
  const sources = new Set(candles.map((candle) => candle.activitySource));
  if (sources.size !== 1) return "Activity";
  return sources.has("volume") ? "Volume" : "Updates";
}
