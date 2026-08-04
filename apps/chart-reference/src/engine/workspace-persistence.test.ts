import { describe, expect, it } from "vitest";
import {
  DEFAULT_WORKSPACE,
  WORKSPACE_STORAGE_KEY,
  clearWorkspace,
  loadWorkspace,
  saveWorkspace,
  type WorkspaceState,
  type WorkspaceStorage,
} from "./workspace-persistence";

class MemoryStorage implements WorkspaceStorage {
  readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }
}

describe("versioned workspace persistence", () => {
  it("persists the exact declared allowlist and never runtime controls or market data", () => {
    const storage = new MemoryStorage();
    const unsafeRuntimeState = {
      ...DEFAULT_WORKSPACE,
      selectedSymbol: "msft.us",
      showVolume: false,
      autoReconnect: false,
      token: "sensitive",
      bars: [{ close: 100 }],
    } as WorkspaceState & { token: string; bars: unknown[] };

    expect(saveWorkspace(storage, unsafeRuntimeState)).toBe(true);
    const serialized = storage.getItem(WORKSPACE_STORAGE_KEY)!;
    const persisted = JSON.parse(serialized) as Record<string, unknown>;
    expect(Object.keys(persisted).sort()).toEqual([
      "selectedSymbol",
      "theme",
      "timeframe",
      "version",
    ]);
    expect(persisted).toEqual({
      version: 1,
      selectedSymbol: "MSFT.US",
      timeframe: "1m",
      theme: "dark",
    });
    expect(serialized).not.toContain("sensitive");
    expect(serialized).not.toContain("bars");
    expect(serialized).not.toContain("showVolume");
    expect(serialized).not.toContain("autoReconnect");
    expect(loadWorkspace(storage)).toEqual({
      ...DEFAULT_WORKSPACE,
      selectedSymbol: "MSFT.US",
    });
  });

  it("loads expanded v1 storage and strips runtime-only fields on the next save", () => {
    const storage = new MemoryStorage();
    storage.setItem(
      WORKSPACE_STORAGE_KEY,
      JSON.stringify({
        version: 1,
        selectedSymbol: "msft.us",
        timeframe: "5m",
        theme: "light",
        showVolume: false,
        autoReconnect: false,
      }),
    );

    const loaded = loadWorkspace(storage);
    expect(loaded).toMatchObject({
      selectedSymbol: "MSFT.US",
      timeframe: "5m",
      theme: "light",
      showVolume: false,
      autoReconnect: false,
    });
    expect(saveWorkspace(storage, loaded)).toBe(true);
    expect(storage.getItem(WORKSPACE_STORAGE_KEY)).not.toContain("showVolume");
    expect(storage.getItem(WORKSPACE_STORAGE_KEY)).not.toContain("autoReconnect");
  });

  it.each(["   ", "bad symbol!"])(
    "falls back safely for invalid expanded-v1 symbol %j",
    (selectedSymbol) => {
      const storage = new MemoryStorage();
      storage.setItem(
        WORKSPACE_STORAGE_KEY,
        JSON.stringify({
          version: 1,
          selectedSymbol,
          timeframe: "5m",
          theme: "light",
          showVolume: false,
          autoReconnect: false,
        }),
      );

      expect(loadWorkspace(storage)).toEqual(DEFAULT_WORKSPACE);
    },
  );

  it("migrates a legacy resolution and falls back safely on corrupt data", () => {
    const storage = new MemoryStorage();
    storage.setItem(
      WORKSPACE_STORAGE_KEY,
      JSON.stringify({ symbol: "aapl.us", resolution: "300", volume: false }),
    );
    expect(loadWorkspace(storage)).toMatchObject({
      selectedSymbol: "AAPL.US",
      timeframe: "5m",
      showVolume: false,
    });

    storage.setItem(WORKSPACE_STORAGE_KEY, "not-json");
    expect(loadWorkspace(storage)).toEqual(DEFAULT_WORKSPACE);
    expect(clearWorkspace(storage)).toBe(true);
    expect(storage.getItem(WORKSPACE_STORAGE_KEY)).toBeNull();
  });
});
