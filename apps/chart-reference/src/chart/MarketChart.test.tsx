import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MarketChart } from "./MarketChart";
import type { ChartCandle, ChartMountOptions, ChartRenderer } from "./types";

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

function rendererDouble() {
  const mount = vi.fn<(container: HTMLElement, options: ChartMountOptions) => void>();
  const setCandles = vi.fn<ChartRenderer["setCandles"]>();
  const setTheme = vi.fn<ChartRenderer["setTheme"]>();
  const setPrecision = vi.fn<ChartRenderer["setPrecision"]>();
  const resize = vi.fn<ChartRenderer["resize"]>();
  const destroy = vi.fn<ChartRenderer["destroy"]>();
  const renderer: ChartRenderer = {
    mount,
    setCandles,
    setTheme,
    setPrecision,
    resize,
    destroy,
  };
  return { renderer, mount, setCandles, setTheme, setPrecision, destroy };
}

describe("MarketChart", () => {
  it("owns and cleans up an injected renderer while exposing textual OHLC", async () => {
    const { renderer, mount, setCandles, setTheme, setPrecision, destroy } = rendererDouble();
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
      screen.getByRole("img", {
        name: /Interactive candlestick chart for AAPL\.US, 1 minute/i,
      }),
    ).toBeVisible();
    await waitFor(() => expect(mount).toHaveBeenCalledOnce());
    expect(setCandles).toHaveBeenCalledWith([candle]);
    expect(setTheme).toHaveBeenCalledWith("dark");
    expect(setPrecision).toHaveBeenCalledWith(2);
    view.unmount();
    expect(destroy).toHaveBeenCalledOnce();
  });

  it("hydrates a replacement renderer with current candles, theme, and precision", async () => {
    const first = rendererDouble();
    const second = rendererDouble();
    const firstFactory = () => first.renderer;
    const secondFactory = () => second.renderer;
    const next = { ...candle, timeMs: candle.timeMs + 60_000, close: 103 };
    const view = render(
      <MarketChart
        candles={[candle]}
        symbol="AAPL.US"
        timeframeLabel="1 minute"
        precision={2}
        theme="dark"
        paused={false}
        rendererFactory={firstFactory}
      />,
    );
    view.rerender(
      <MarketChart
        candles={[candle, next]}
        symbol="AAPL.US"
        timeframeLabel="1 minute"
        precision={4}
        theme="light"
        paused={false}
        rendererFactory={secondFactory}
      />,
    );
    await waitFor(() => expect(second.mount).toHaveBeenCalledOnce());
    expect(second.mount).toHaveBeenCalledWith(
      expect.any(HTMLElement),
      expect.objectContaining({ theme: "light", precision: 4 }),
    );
    expect(second.setCandles).toHaveBeenCalledWith([candle, next]);
  });

  it("keeps mixed activity labeling stable while the crosshair moves", () => {
    const { renderer, mount } = rendererDouble();
    render(
      <MarketChart
        candles={[
          candle,
          {
            ...candle,
            timeMs: candle.timeMs + 60_000,
            activitySource: "volume",
            activityValue: 1_250,
          },
        ]}
        symbol="MSFT.US"
        timeframeLabel="5 minutes"
        precision={2}
        theme="light"
        paused={false}
        rendererFactory={() => renderer}
      />,
    );
    const options = mount.mock.calls[0]?.[1];
    act(() => options?.onCrosshairMove?.({ ...candle, activitySource: "updates" }));
    expect(screen.getByText("Activity")).toBeVisible();
    expect(screen.getByText(/mixed reported volume/i)).toBeVisible();
  });

  it("labels an all-mixed candle set as Activity", () => {
    const { renderer } = rendererDouble();
    render(
      <MarketChart
        candles={[{ ...candle, activitySource: "mixed", activityValue: 42 }]}
        symbol="NVDA.US"
        timeframeLabel="1 minute"
        precision={2}
        theme="dark"
        paused={false}
        rendererFactory={() => renderer}
      />,
    );
    expect(screen.getByText("Activity")).toBeVisible();
    expect(screen.getByText(/mixed reported volume/i)).toBeVisible();
  });

  it("clears a previous-series crosshair when the instrument identity changes", async () => {
    const { renderer, mount } = rendererDouble();
    const rendererFactory = () => renderer;
    const next: ChartCandle = {
      ...candle,
      timeMs: candle.timeMs + 300_000,
      open: 200,
      high: 202,
      low: 199,
      close: 201.5,
    };
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
    const options = mount.mock.calls[0]?.[1];
    act(() => options?.onCrosshairMove?.({ ...candle, close: 777 }));
    expect(screen.getByLabelText("Crosshair bar")).toHaveTextContent(/777/);

    view.rerender(
      <MarketChart
        candles={[next]}
        symbol="MSFT.US"
        timeframeLabel="5 minutes"
        precision={2}
        theme="dark"
        paused={false}
        rendererFactory={rendererFactory}
      />,
    );

    await waitFor(() => expect(screen.getByLabelText("Latest bar")).toBeVisible());
    expect(screen.getByLabelText("Latest bar")).not.toHaveTextContent(/777/);
    expect(screen.getByLabelText("Latest bar")).toHaveTextContent(/201[.,]50/);
  });

  it("renders the supplied empty state", () => {
    const { renderer } = rendererDouble();
    render(
      <MarketChart
        candles={[]}
        symbol="AAPL.US"
        timeframeLabel="1 minute"
        precision={2}
        theme="dark"
        paused={false}
        emptyMessage="No canonical data"
        rendererFactory={() => renderer}
      />,
    );
    expect(screen.getByText("No canonical data")).toBeVisible();
  });
});
