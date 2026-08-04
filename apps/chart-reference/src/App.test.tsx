import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import { wireQuote } from "./test/market-data-fixtures";

vi.mock("./chart", () => ({
  MarketChart: ({ symbol, timeframeLabel }: { symbol: string; timeframeLabel: string }) => (
      <section aria-label="Market chart test double">
        <h2>{`${symbol} · ${timeframeLabel}`}</h2>
      </section>
  ),
}));

class AppTestSocket {
  static readonly instances: AppTestSocket[] = [];

  readyState = 0;
  onopen: ((event: unknown) => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  onclose: ((event: unknown) => void) | null = null;
  readonly sent: string[] = [];

  constructor(readonly url: string) {
    AppTestSocket.instances.push(this);
  }

  open(): void {
    this.readyState = 1;
    this.onopen?.({});
  }

  message(payload: unknown): void {
    this.onmessage?.({ data: JSON.stringify(payload) });
  }

  send(payload: string): void {
    this.sent.push(payload);
  }

  close(): void {
    this.readyState = 3;
  }
}

beforeEach(() => {
  window.localStorage.clear();
  window.history.replaceState(null, "", "/");
  AppTestSocket.instances.length = 0;
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/");
});

describe("App", () => {
  it("boots the synthetic replay and persists preferences without credentials", async () => {
    const view = render(<App />);

    await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("Synthetic replay"));
    await waitFor(() => expect(screen.getByText("synthetic-replay")).toBeVisible());

    const stored = window.localStorage.getItem("smdg.chart-reference.workspace");
    expect(stored).toContain('"selectedSymbol":"AAPL.US"');
    expect(stored).not.toContain("dev-basic:chart-reference");
    expect(stored).not.toContain("token");
    expect(stored).not.toContain("showVolume");
    expect(stored).not.toContain("autoReconnect");
    view.unmount();
  });

  it("marks a live gateway quote stale when the freshness watchdog advances without frames", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-02T12:00:00.000Z"));
    vi.stubGlobal("WebSocket", AppTestSocket);
    window.history.replaceState(null, "", "/?source=gateway");
    render(<App />);
    await act(async () => Promise.resolve());
    const socket = AppTestSocket.instances[0];
    expect(socket).toBeDefined();

    act(() => socket?.open());
    const timestamp = new Date(Date.now()).toISOString();
    act(() => {
      socket?.message({
        type: "quote",
        timestamp,
        request_id: null,
        data: {
          quote: {
            ...wireQuote(),
            provider_timestamp: timestamp,
            received_at: timestamp,
          },
        },
      });
    });
    expect(screen.getByRole("status")).toHaveTextContent("Live");

    act(() => {
      vi.advanceTimersByTime(6_000);
    });
    expect(screen.getByRole("status")).toHaveTextContent("Stale");
  });

  it("requires a quote from the new generation after reconnect", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-02T12:00:00.000Z"));
    vi.stubGlobal("WebSocket", AppTestSocket);
    window.history.replaceState(null, "", "/?source=gateway");
    render(<App />);
    await act(async () => Promise.resolve());
    const firstSocket = AppTestSocket.instances[0];
    expect(firstSocket).toBeDefined();

    act(() => firstSocket?.open());
    const timestamp = new Date(Date.now()).toISOString();
    act(() => {
      firstSocket?.message({
        type: "quote",
        timestamp,
        request_id: null,
        data: {
          quote: {
            ...wireQuote(),
            provider_timestamp: timestamp,
            received_at: timestamp,
          },
        },
      });
    });
    expect(screen.getByRole("status")).toHaveTextContent("Live");

    fireEvent.change(screen.getByLabelText("Local gateway token"), {
      target: { value: "replacement-token" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Connect with token" }));
    const secondSocket = AppTestSocket.instances[1];
    expect(secondSocket).toBeDefined();
    act(() => secondSocket?.open());

    expect(screen.getByRole("status")).toHaveTextContent("Connecting");
    expect(screen.getByRole("status")).not.toHaveTextContent("Live");
    act(() => {
      vi.advanceTimersByTime(5_000);
    });
    expect(screen.getByRole("status")).toHaveTextContent("No data");
  });
});
