import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

test.beforeEach(async ({ page }) => {
  await page.goto("/?source=replay");
  await expect(page.getByRole("status")).toContainText("Synthetic replay");
});

test("replay workspace remains usable and contained at every configured viewport", async ({
  page,
}) => {
  await expect(page.getByRole("heading", { name: "Market Chart Reference" })).toBeVisible();
  await expect(page.getByRole("heading", { name: /AAPL\.US · 1 minute/ })).toBeVisible();
  await expect(page.getByText("synthetic-replay")).toBeVisible();

  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth > document.documentElement.clientWidth,
  );
  expect(overflow).toBe(false);

  const chart = page.getByTestId("market-chart-canvas");
  const bounds = await chart.boundingBox();
  expect(bounds).not.toBeNull();
  expect(bounds?.width ?? 0).toBeGreaterThan(250);
  expect(bounds?.width ?? Number.POSITIVE_INFINITY).toBeLessThanOrEqual(
    await page.evaluate(() => document.documentElement.clientWidth),
  );

  await page.getByRole("combobox", { name: "Timeframe" }).selectOption("5s");
  await expect(page.getByRole("heading", { name: /AAPL\.US · 5 seconds/ })).toBeVisible();

  await page.getByRole("button", { name: "Pause display" }).click();
  await expect(page.getByRole("status")).toContainText("Display paused");
  await expect(page.getByRole("button", { name: "Resume display" })).toBeVisible();
});

test("instrument controls support keyboard operation", async ({ page }) => {
  const instrument = page.getByRole("combobox", { name: "Instrument" });
  await instrument.focus();
  await instrument.fill("MSFT.US");
  await page.keyboard.press("Enter");
  await expect(page.getByRole("heading", { name: /MSFT\.US · 1 minute/ })).toBeVisible();

  await page.keyboard.press("Tab");
  await expect(page.getByRole("button", { name: "Load" })).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(page.getByRole("combobox", { name: "Timeframe" })).toBeFocused();
});

test("workspace has no automatically detectable WCAG A or AA violations", async ({ page }) => {
  const results = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
    .analyze();
  expect(results.violations).toEqual([]);
});

test("gateway token remains out of persisted workspace settings", async ({ page }) => {
  await page.getByText("Data source").click();
  await page.getByLabel("Local gateway").check();
  const token = page.getByLabel("Local gateway token");
  await token.fill("local-e2e-token-that-must-not-persist");

  const persisted = await page.evaluate(() => JSON.stringify(window.localStorage));
  expect(persisted).not.toContain("local-e2e-token-that-must-not-persist");
});

test("an open gateway socket without quotes never claims Live", async ({ page }) => {
  await page.addInitScript(() => {
    class SilentWebSocket {
      readyState = 0;
      onopen: ((event: Event) => void) | null = null;
      onmessage: ((event: MessageEvent) => void) | null = null;
      onerror: ((event: Event) => void) | null = null;
      onclose: ((event: CloseEvent) => void) | null = null;

      constructor(readonly url: string | URL) {
        window.setTimeout(() => {
          this.readyState = 1;
          this.onopen?.(new Event("open"));
        }, 0);
      }

      send(): void {
        // The silent gateway accepts commands but intentionally publishes no quotes.
      }

      close(): void {
        this.readyState = 3;
      }
    }

    Object.defineProperty(window, "WebSocket", {
      configurable: true,
      writable: true,
      value: SilentWebSocket,
    });
  });

  await page.goto("/?source=gateway");
  await expect(page.getByRole("status")).toContainText("Connecting");
  await expect(page.getByText("Waiting for market data…")).toBeVisible();
  await expect(page.getByRole("status")).not.toContainText("Live");

  await expect(page.getByRole("status")).toContainText("No data", { timeout: 7_000 });
  await expect(
    page.getByText("No market data is available for this instrument and timeframe."),
  ).toBeVisible();
  await expect(page.getByRole("button", { name: "Reconnect" })).toBeVisible();
});
