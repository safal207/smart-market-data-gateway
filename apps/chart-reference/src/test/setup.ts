import "@testing-library/jest-dom/vitest";

class TestResizeObserver implements ResizeObserver {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

Object.defineProperty(globalThis, "ResizeObserver", {
  value: TestResizeObserver,
  writable: true,
});
