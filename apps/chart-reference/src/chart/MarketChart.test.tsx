import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MarketChart } from "./MarketChart";
import type { ChartCandle, ChartRenderer } from "./types";

const candle: ChartCandle = {
  timeMs: Date.UTC(2026, 0, 5, 14, 30),
  open: 100,
  high: 102,
  low: 99,
  close: 101.25,
  updateCount: 12,
  activityValue: 12,
  activitySource: "updates",
};

afterEach(cleanup);

describe("MarketChart", () => {
  it("owns and cleans up an injected renderer while exposing textual OHLC", async () => {
    const mount = vi.fn();
    const setCandles = vi.fn();
    const setTheme = vi.fn();
    const setPrecision = vi.fn();
    const destroy = vi.fn();
    const renderer: ChartRenderer = {
      mount,
      setCandles,
      setTheme,
      setPrecision,
      resize: vi.fn(),
      destroy,
    };
    const rendererFactory = vi.fn(() => renderer);
    const view = render(
      <MarketChart
        candles={[candle]}
        symbol="AAPL.US"
        timeframeLabel="1 minute"
        precision={2}
        theme="dark"
        paused={false}
        rendererFactory={rendererFactory}
      />,
    );

    expect(screen.getByRole("heading", { name: "AAPL.US · 1 minute" })).toBeVisible();
    expect(screen.getByText(/101[.,]25/)).toBeVisible();
    expect(screen.getByText(/not exchange-reported volume/i)).toBeVisible();
    expect(
      screen.getByRole("img", { name: /Interactive candlestick chart for AAPL\.US, 1 minute/i }),
    ).toBeVisible();
    expect(screen.getByTestId("market-chart-canvas")).not.toHaveAttribute("aria-hidden");
    await waitFor(() => expect(mount).toHaveBeenCalledOnce());
    expect(setCandles).toHaveBeenCalledWith([candle]);
    expect(setTheme).toHaveBeenCalledWith("dark");
    expect(setPrecision).toHaveBeenCalledWith(2);

    view.unmount();
    expect(destroy).toHaveBeenCalledOnce();
  });

  it("describes a genuine volume fallback accurately", () => {
    render(
      <MarketChart
        candles={[{ ...candle, activityValue: 1_250, activitySource: "volume" }]}
        symbol="MSFT.US"
        timeframeLabel="5 minutes"
        precision={2}
        theme="light"
        paused={false}
        rendererFactory={() => ({
          mount: () => undefined,
          setCandles: () => undefined,
          setTheme: () => undefined,
          setPrecision: () => undefined,
          resize: () => undefined,
          destroy: () => undefined,
        })}
      />,
    );

    expect(screen.getByText("Volume")).toBeVisible();
    expect(screen.getByText(/reported last-size or cumulative-volume deltas/i)).toBeVisible();
  });
});
