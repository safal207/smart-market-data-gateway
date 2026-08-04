import {
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  createChart,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from "lightweight-charts";

import type {
  ChartCandle,
  ChartMountOptions,
  ChartRenderer,
  ChartTheme,
} from "./types";

const UP_COLOR = "#22c58b";
const DOWN_COLOR = "#ff6b7a";

interface Palette {
  readonly background: string;
  readonly grid: string;
  readonly text: string;
  readonly border: string;
  readonly crosshair: string;
}

const PALETTES: Record<ChartTheme, Palette> = {
  dark: {
    background: "#0b101b",
    grid: "#1c2738",
    text: "#a9b7cb",
    border: "#2a3850",
    crosshair: "#8297b7",
  },
  light: {
    background: "#f8fafc",
    grid: "#dfe6ef",
    text: "#40506a",
    border: "#c7d1df",
    crosshair: "#5f7089",
  },
};

function seconds(timeMs: number): UTCTimestamp {
  return Math.floor(timeMs / 1_000) as UTCTimestamp;
}

function candleData(candle: ChartCandle): CandlestickData<UTCTimestamp> {
  return {
    time: seconds(candle.timeMs),
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
  };
}

function updateData(candle: ChartCandle): HistogramData<UTCTimestamp> {
  return {
    time: seconds(candle.timeMs),
    value: candle.activityValue,
    color: candle.close >= candle.open ? `${UP_COLOR}88` : `${DOWN_COLOR}88`,
  };
}

function sameCandle(left: ChartCandle, right: ChartCandle): boolean {
  return (
    left.timeMs === right.timeMs &&
    left.open === right.open &&
    left.high === right.high &&
    left.low === right.low &&
    left.close === right.close &&
    left.updateCount === right.updateCount
    && left.activityValue === right.activityValue
    && left.activitySource === right.activitySource
  );
}

function canIncrementallyUpdate(
  previous: readonly ChartCandle[],
  next: readonly ChartCandle[],
): boolean {
  if (next.length === 0 || previous.length === 0) {
    return false;
  }
  if (next.length !== previous.length && next.length !== previous.length + 1) {
    return false;
  }
  const stableLength = next.length - 1;
  if (stableLength > previous.length) {
    return false;
  }
  for (let index = 0; index < stableLength; index += 1) {
    const previousCandle = previous[index];
    const nextCandle = next[index];
    if (previousCandle == null || nextCandle == null || !sameCandle(previousCandle, nextCandle)) {
      return false;
    }
  }
  return true;
}

export class LightweightChartsRenderer implements ChartRenderer {
  private chart: IChartApi | null = null;
  private candles: ISeriesApi<"Candlestick"> | null = null;
  private updates: ISeriesApi<"Histogram"> | null = null;
  private previous: readonly ChartCandle[] = [];
  private metadataByTime = new Map<
    number,
    Pick<ChartCandle, "updateCount" | "activityValue" | "activitySource">
  >();

  mount(container: HTMLElement, options: ChartMountOptions): void {
    this.destroy();
    const palette = PALETTES[options.theme];
    const chart = createChart(container, {
      layout: {
        background: { type: ColorType.Solid, color: palette.background },
        textColor: palette.text,
        // The visible footer supplies the licence-required TradingView link.
        attributionLogo: false,
        panes: {
          separatorColor: palette.border,
          separatorHoverColor: palette.crosshair,
          enableResize: true,
        },
      },
      grid: {
        vertLines: { color: palette.grid },
        horzLines: { color: palette.grid },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: palette.crosshair, labelBackgroundColor: palette.border },
        horzLine: { color: palette.crosshair, labelBackgroundColor: palette.border },
      },
      rightPriceScale: { borderColor: palette.border },
      timeScale: {
        borderColor: palette.border,
        timeVisible: true,
        secondsVisible: true,
        rightOffset: 4,
      },
      handleScroll: true,
      handleScale: true,
    });

    const candles = chart.addSeries(
      CandlestickSeries,
      {
        title: "Price",
        upColor: UP_COLOR,
        downColor: DOWN_COLOR,
        borderVisible: false,
        wickUpColor: UP_COLOR,
        wickDownColor: DOWN_COLOR,
        priceFormat: this.priceFormat(options.precision),
      },
      0,
    );
    const updates = chart.addSeries(
      HistogramSeries,
      {
        title: "Updates",
        priceFormat: { type: "volume" },
        priceLineVisible: false,
        lastValueVisible: false,
      },
      1,
    );

    updates.priceScale().applyOptions({
      scaleMargins: { top: 0.2, bottom: 0 },
    });

    if (options.onCrosshairMove != null) {
      chart.subscribeCrosshairMove((parameter) => {
        if (parameter.time == null) {
          options.onCrosshairMove?.(null);
          return;
        }
        const value = parameter.seriesData.get(candles);
        if (value == null || !("open" in value) || typeof parameter.time !== "number") {
          options.onCrosshairMove?.(null);
          return;
        }
        const timeMs = Number(parameter.time) * 1_000;
        const metadata = this.metadataByTime.get(timeMs);
        options.onCrosshairMove?.({
          timeMs,
          open: value.open,
          high: value.high,
          low: value.low,
          close: value.close,
          updateCount: metadata?.updateCount ?? 0,
          activityValue: metadata?.activityValue ?? 0,
          activitySource: metadata?.activitySource ?? "updates",
        });
      });
    }

    this.chart = chart;
    this.candles = candles;
    this.updates = updates;
  }

  setCandles(candles: readonly ChartCandle[]): void {
    if (this.chart == null || this.candles == null || this.updates == null) {
      return;
    }
    this.metadataByTime = new Map(
      candles.map((candle) => [
        Math.floor(candle.timeMs / 1_000) * 1_000,
        {
          updateCount: candle.updateCount,
          activityValue: candle.activityValue,
          activitySource: candle.activitySource,
        },
      ]),
    );
    const sources = new Set(candles.map((candle) => candle.activitySource));
    const activityTitle =
      sources.size === 1 && sources.has("volume")
        ? "Volume"
        : sources.size === 1 && sources.has("updates")
          ? "Updates"
          : "Activity";
    this.updates.applyOptions({ title: activityTitle });

    if (canIncrementallyUpdate(this.previous, candles)) {
      const last = candles.at(-1);
      if (last != null) {
        this.candles.update(candleData(last));
        this.updates.update(updateData(last));
      }
    } else {
      this.candles.setData(candles.map(candleData));
      this.updates.setData(candles.map(updateData));
      if (this.previous.length === 0 && candles.length > 0) {
        this.chart.timeScale().fitContent();
      }
    }
    this.previous = candles.map((candle) => ({ ...candle }));
  }

  setTheme(theme: ChartTheme): void {
    if (this.chart == null) {
      return;
    }
    const palette = PALETTES[theme];
    this.chart.applyOptions({
      layout: {
        background: { type: ColorType.Solid, color: palette.background },
        textColor: palette.text,
        panes: {
          separatorColor: palette.border,
          separatorHoverColor: palette.crosshair,
        },
      },
      grid: {
        vertLines: { color: palette.grid },
        horzLines: { color: palette.grid },
      },
      rightPriceScale: { borderColor: palette.border },
      timeScale: { borderColor: palette.border },
    });
  }

  setPrecision(precision: number): void {
    this.candles?.applyOptions({ priceFormat: this.priceFormat(precision) });
  }

  resize(width: number, height: number): void {
    if (this.chart == null || width < 1 || height < 1) {
      return;
    }
    this.chart.resize(Math.floor(width), Math.floor(height));
  }

  destroy(): void {
    this.chart?.remove();
    this.chart = null;
    this.candles = null;
    this.updates = null;
    this.previous = [];
    this.metadataByTime.clear();
  }

  private priceFormat(precision: number) {
    const safePrecision = Math.max(0, Math.min(8, Math.floor(precision)));
    return {
      type: "price" as const,
      precision: safePrecision,
      minMove: 10 ** -safePrecision,
    };
  }
}
