export type ChartTheme = "dark" | "light";

export interface ChartCandle {
  readonly timeMs: number;
  readonly open: number;
  readonly high: number;
  readonly low: number;
  readonly close: number;
  readonly updateCount: number;
  readonly activityValue: number;
  readonly activitySource: "volume" | "updates" | "mixed";
}

export interface CrosshairValue {
  readonly timeMs: number;
  readonly open: number;
  readonly high: number;
  readonly low: number;
  readonly close: number;
  readonly updateCount: number;
  readonly activityValue: number;
  readonly activitySource: "volume" | "updates" | "mixed";
}

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
