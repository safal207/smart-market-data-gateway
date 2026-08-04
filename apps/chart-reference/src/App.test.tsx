import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import { wireQuote } from "./test/market-data-fixtures";

vi.mock("./chart", () => ({
  MarketChart: ({
    symbol,
    timeframeLabel,
  }: {
    symbol: string;
    timeframeLabel: string;
  }) => (
    <section aria-label="Market chart test double">
      <h2>{`${symbol} · ${timeframeLabel}`}</h2>
    </section>
  ),
  activityLabel: (
    candles: readonly {
      activitySource: "volume" | "updates" | "mixed";
    }[],
  ) => {
    const sources = new Set(candles.map((candle) => candle.activitySource));
    if (sources.size !== 1) return "Activity";
    return sources.has("volume") ? "Volume" : "Updates";
  },
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

    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent("Synthetic replay"),
    );
    await waitFor(() => expect(screen.getByText("synthetic-replay")).toBeVisible());

    const stored = window.localStorage.getItem("smdg.chart-reference.workspace");
    expect(stored).toContain('"selectedSymbol":"AAPL.US"');
    expect(stored).not.toContain("dev-basic:chart-reference");
    expect(stored).not.toContain("token");
    expect(stored).not.toContain("showVolume");
    expect(stored).not.toContain("autoReconnect");
    view.unmount();
  });

  it("boots when browser storage access is denied", async () => {
    const storageGetter = vi.spyOn(window, "localStorage", "get").mockImplementation(() => {
      throw new DOMException("Storage access denied", "SecurityError");
    });

    try {
      render(<App />);
      await waitFor(() =>
        expect(screen.getByRole("status")).toHaveTextContent("Synthetic replay"),
      );
    } finally {
      storageGetter.mockRestore();
    }
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

  it("keeps paused incoming data aged so resume cannot briefly restore Live", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-02T12:00:00.000Z"));
    vi.stubGlobal("WebSocket", AppTestSocket);
    window.history.replaceState(null, "", "/?source=gateway");
    render(<App />);
    await act(async () => Promise.resolve());
    const socket = AppTestSocket.instances[0];
    expect(socket).toBeDefined();

    act(() => socket?.open());
    const firstTimestamp = new Date(Date.now()).toISOString();
    act(() => {
      socket?.message({
        type: "quote",
        timestamp: firstTimestamp,
        request_id: null,
        data: {
          quote: {
            ...wireQuote(),
            provider_timestamp: firstTimestamp,
            received_at: firstTimestamp,
          },
        },
      });
    });
    expect(screen.getByRole("status")).toHaveTextContent("Live");
    fireEvent.click(screen.getByRole("button", { name: "Pause display" }));
    expect(screen.getByRole("status")).toHaveTextContent("Display paused");

    act(() => {
      vi.advanceTimersByTime(1_000);
    });
    const pausedTimestamp = new Date(Date.now()).toISOString();
    act(() => {
      socket?.message({
        type: "quote",
        timestamp: pausedTimestamp,
        request_id: null,
        data: {
          quote: {
            ...wireQuote(),
            event_id: "00000000-0000-4000-8000-000000000002",
            sequence: 2,
            provider_timestamp: pausedTimestamp,
            received_at: pausedTimestamp,
          },
        },
      });
    });
    act(() => {
      vi.advanceTimersByTime(6_000);
    });

    fireEvent.click(screen.getByRole("button", { name: "Resume display" }));
    expect(screen.getByRole("status")).toHaveTextContent("Stale");
    expect(screen.getByRole("status")).not.toHaveTextContent("Live");
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
