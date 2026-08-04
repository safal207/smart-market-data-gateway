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
  it("round-trips only preference fields and never persists tokens or market bars", () => {
    const storage = new MemoryStorage();
    const unsafeRuntimeState = {
      ...DEFAULT_WORKSPACE,
      selectedSymbol: "msft.us",
      token: "sensitive",
      bars: [{ close: 100 }],
    } as WorkspaceState & { token: string; bars: unknown[] };

    expect(saveWorkspace(storage, unsafeRuntimeState)).toBe(true);
    const serialized = storage.getItem(WORKSPACE_STORAGE_KEY)!;
    expect(serialized).not.toContain("sensitive");
    expect(serialized).not.toContain("bars");
    expect(loadWorkspace(storage)).toMatchObject({
      version: 1,
      selectedSymbol: "MSFT.US",
    });
  });

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
