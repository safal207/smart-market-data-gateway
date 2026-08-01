import { useEffect, useRef, useState } from "react";

import { LightweightChartsRenderer } from "./LightweightChartsRenderer";
import type {
  ChartCandle,
  ChartRenderer,
  ChartRendererFactory,
  ChartTheme,
  CrosshairValue,
} from "./types";

const defaultRendererFactory: ChartRendererFactory = () => new LightweightChartsRenderer();

function formatNumber(value: number, precision: number): string {
  return new Intl.NumberFormat(undefined, {
    minimumFractionDigits: precision,
    maximumFractionDigits: precision,
  }).format(value);
}

export interface MarketChartProps {
  readonly candles: readonly ChartCandle[];
  readonly symbol: string;
  readonly timeframeLabel: string;
  readonly precision: number;
  readonly theme: ChartTheme;
  readonly paused: boolean;
  readonly rendererFactory?: ChartRendererFactory;
}

export function MarketChart({
  candles,
  symbol,
  timeframeLabel,
  precision,
  theme,
  paused,
  rendererFactory = defaultRendererFactory,
}: MarketChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const rendererRef = useRef<ChartRenderer | null>(null);
  const initialMountOptionsRef = useRef({ theme, precision });
  const [crosshair, setCrosshair] = useState<CrosshairValue | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (container == null) {
      return undefined;
    }
    const renderer = rendererFactory();
    rendererRef.current = renderer;
    renderer.mount(container, {
      ...initialMountOptionsRef.current,
      onCrosshairMove: setCrosshair,
    });

    const resizeObserver = new ResizeObserver(([entry]) => {
      if (entry == null) {
        return;
      }
      renderer.resize(entry.contentRect.width, entry.contentRect.height);
    });
    resizeObserver.observe(container);

    return () => {
      resizeObserver.disconnect();
      renderer.destroy();
      if (rendererRef.current === renderer) {
        rendererRef.current = null;
      }
    };
  }, [rendererFactory]);

  useEffect(() => {
    rendererRef.current?.setCandles(candles);
  }, [candles]);

  useEffect(() => {
    rendererRef.current?.setTheme(theme);
  }, [theme]);

  useEffect(() => {
    rendererRef.current?.setPrecision(precision);
  }, [precision]);

  const latest = crosshair ?? candles.at(-1) ?? null;
  const label = crosshair == null ? "Latest bar" : "Crosshair bar";
  const activityLabel =
    latest?.activitySource === "volume"
      ? "Volume"
      : latest?.activitySource === "mixed"
        ? "Activity"
        : "Updates";

  return (
    <section className="chart-panel" aria-labelledby="market-chart-title">
      <div className="chart-panel__header">
        <div>
          <p className="eyebrow">{paused ? "Display paused" : "Streaming workspace"}</p>
          <h2 id="market-chart-title">
            {symbol} · {timeframeLabel}
          </h2>
        </div>
        {latest == null ? (
          <p className="crosshair-readout">Waiting for the first synthetic update…</p>
        ) : (
          <dl className="crosshair-readout" aria-label={label}>
            <div>
              <dt>O</dt>
              <dd>{formatNumber(latest.open, precision)}</dd>
            </div>
            <div>
              <dt>H</dt>
              <dd>{formatNumber(latest.high, precision)}</dd>
            </div>
            <div>
              <dt>L</dt>
              <dd>{formatNumber(latest.low, precision)}</dd>
            </div>
            <div>
              <dt>C</dt>
              <dd>{formatNumber(latest.close, precision)}</dd>
            </div>
            <div>
              <dt>{activityLabel}</dt>
              <dd>{latest.activityValue}</dd>
            </div>
          </dl>
        )}
      </div>
      <div
        ref={containerRef}
        className="market-chart"
        data-testid="market-chart-canvas"
        role="img"
        aria-label={`Interactive candlestick chart for ${symbol}, ${timeframeLabel}. Numeric values are available in the adjacent data summary.`}
      />
      <p className="chart-panel__note">
        {activityLabel === "Updates"
          ? "The lower pane counts delivered quote updates. It is not exchange-reported volume."
          : activityLabel === "Volume"
            ? "The lower pane uses reported last-size or cumulative-volume deltas."
            : "The lower pane contains mixed reported volume and quote-update fallback values."}
      </p>
    </section>
  );
}
