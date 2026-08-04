import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { HISTORY_TOKEN_SETTLE_MS, useDebouncedValue } from "./use-candle-history";

describe("useDebouncedValue", () => {
  afterEach(() => vi.useRealTimers());

  it("publishes only the final credential after the settle window", () => {
    vi.useFakeTimers();
    const { result, rerender } = renderHook(
      ({ token }: { readonly token: string }) => useDebouncedValue(token, HISTORY_TOKEN_SETTLE_MS),
      { initialProps: { token: "dev-basic:old" } },
    );

    rerender({ token: "d" });
    rerender({ token: "dev" });
    rerender({ token: "dev-pro:chart-reference" });

    expect(result.current).toBe("dev-basic:old");
    act(() => vi.advanceTimersByTime(HISTORY_TOKEN_SETTLE_MS - 1));
    expect(result.current).toBe("dev-basic:old");
    act(() => vi.advanceTimersByTime(1));
    expect(result.current).toBe("dev-pro:chart-reference");
  });
});
