import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ChartWorkspace } from "./ChartWorkspace";
import type { ChartWorkspaceActions, ChartWorkspaceModel } from "./types";

vi.mock("../../chart", () => ({
  MarketChart: ({
    symbol,
    timeframeLabel,
    emptyMessage,
  }: {
    symbol: string;
    timeframeLabel: string;
    emptyMessage?: string;
  }) => (
    <section aria-label="Market chart test double">
      <h2>{`${symbol} · ${timeframeLabel}`}</h2>
      {emptyMessage == null ? null : <p>{emptyMessage}</p>}
    </section>
  ),
}));

const model: ChartWorkspaceModel = {
  symbol: "AAPL.US",
  symbolOptions: ["AAPL.US", "MSFT.US"],
  timeframe: "1m",
  timeframeOptions: [
    { value: "5s", label: "5 seconds" },
    { value: "1m", label: "1 minute" },
  ],
  timeframeLabel: "1 minute",
  theme: "dark",
  sourceMode: "replay",
  connectionState: "replay",
  connectionDetail: "Deterministic synthetic replay.",
  reconnectAttempt: 0,
  lastUpdateMs: Date.UTC(2026, 0, 5, 14, 30),
  quote: {
    symbol: "AAPL.US",
    price: 215.42,
    bid: 215.4,
    ask: 215.44,
    provider: "synthetic-replay",
    providerTimestampMs: Date.UTC(2026, 0, 5, 14, 30),
    origin: "replay",
    sourceStale: false,
    ageMs: 0,
    direction: "up",
    stale: false,
  },
  candles: [
    {
      timeMs: Date.UTC(2026, 0, 5, 14, 30),
      open: 215.1,
      high: 215.7,
      low: 214.95,
      close: 215.42,
      updateCount: 38,
      activityValue: 38,
      activitySource: "updates",
    },
  ],
  precision: 2,
  activityLabel: "Updates",
  diagnostics: { accepted: 38, duplicates: 1, rejected: 2, gaps: 0 },
  paused: false,
  gatewayToken: "",
};

function actions(): ChartWorkspaceActions {
  return {
    selectSymbol: vi.fn(),
    selectTimeframe: vi.fn(),
    selectTheme: vi.fn(),
    selectSourceMode: vi.fn(),
    setGatewayToken: vi.fn(),
    togglePaused: vi.fn(),
    reconnect: vi.fn(),
  };
}

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("ChartWorkspace", () => {
  it("renders semantic quote, connection, summary, and attribution content", () => {
    render(<ChartWorkspace model={model} actions={actions()} />);

    expect(screen.getByRole("heading", { name: "Market Chart Reference" })).toBeVisible();
    expect(screen.getByRole("status")).toHaveTextContent("Synthetic replay");
    expect(screen.getByRole("heading", { name: "Latest quote" })).toBeVisible();
    expect(screen.getByRole("region", { name: "Latest OHLC bar" })).toBeVisible();
    expect(screen.getByRole("link", { name: /TradingView Lightweight Charts/i })).toHaveAttribute(
      "href",
      "https://www.tradingview.com/",
    );
    expect(screen.getByText(/Copyright \(с\) 2025 TradingView, Inc\./)).toBeVisible();
  });

  it("routes keyboard-friendly controls through workspace actions", async () => {
    const handlers = actions();
    const user = userEvent.setup();
    render(<ChartWorkspace model={model} actions={handlers} />);

    const symbol = screen.getByRole("combobox", { name: "Instrument" });
    await user.clear(symbol);
    await user.type(symbol, "msft.us");
    await user.click(screen.getByRole("button", { name: "Load" }));
    expect(handlers.selectSymbol).toHaveBeenCalledWith("MSFT.US");

    await user.selectOptions(screen.getByRole("combobox", { name: "Timeframe" }), "5s");
    expect(handlers.selectTimeframe).toHaveBeenCalledWith("5s");

    await user.click(screen.getByRole("button", { name: "Pause display" }));
    expect(handlers.togglePaused).toHaveBeenCalledOnce();
    await user.click(screen.getByRole("button", { name: "Use light theme" }));
    expect(handlers.selectTheme).toHaveBeenCalledWith("light");
  });

  it("labels the local token as memory-only and forwards it without persistence", async () => {
    const handlers = actions();
    const user = userEvent.setup();
    render(
      <ChartWorkspace
        model={{ ...model, sourceMode: "gateway", gatewayToken: "local-token" }}
        actions={handlers}
      />,
    );

    await user.click(screen.getByText("Data source"));
    const token = screen.getByLabelText("Local gateway token");
    expect(token).toHaveAttribute("type", "password");
    expect(screen.getByText(/Never saved to browser storage/i)).toBeVisible();
    fireEvent.change(token, { target: { value: "replacement-token" } });
    expect(handlers.setGatewayToken).toHaveBeenLastCalledWith("replacement-token");
    await user.click(screen.getByRole("button", { name: "Connect with token" }));
    expect(handlers.reconnect).toHaveBeenCalledOnce();
  });

  it("does not claim Live before the first gateway quote and times out to No data", () => {
    vi.useFakeTimers();
    render(
      <ChartWorkspace
        model={{
          ...model,
          sourceMode: "gateway",
          connectionState: "live",
          connectionDetail: "Socket opened.",
          lastUpdateMs: undefined,
          quote: null,
          candles: [],
        }}
        actions={actions()}
      />,
    );

    expect(screen.getByRole("status")).toHaveTextContent("Connecting");
    expect(screen.getAllByText("Waiting for market data…")).toHaveLength(2);
    expect(screen.queryByText(/^Live$/)).not.toBeInTheDocument();

    act(() => {
      vi.advanceTimersByTime(5_000);
    });

    expect(screen.getByRole("status")).toHaveTextContent("No data");
    expect(
      screen.getByText("No market data is available for this instrument and timeframe."),
    ).toBeVisible();
    expect(screen.getByRole("button", { name: "Reconnect" })).toBeVisible();
  });
});
