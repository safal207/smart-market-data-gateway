import { defineConfig } from "@playwright/test";

const baseUse = {
  baseURL: "http://127.0.0.1:4173",
  colorScheme: "dark" as const,
  reducedMotion: "reduce" as const,
  trace: "retain-on-failure" as const,
};

export default defineConfig({
  testDir: "./tests/e2e",
  fullyParallel: false,
  workers: process.env.CI || process.platform === "win32" ? 1 : 3,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI
    ? [
        ["github"],
        ["html", { outputFolder: "playwright-report", open: "never" }],
      ]
    : "list",
  use: baseUse,
  projects: [
    {
      name: "mobile-390",
      use: { viewport: { width: 390, height: 844 }, hasTouch: true },
    },
    {
      name: "tablet-768",
      use: { viewport: { width: 768, height: 1024 }, hasTouch: true },
    },
    {
      name: "desktop-1440",
      use: { viewport: { width: 1440, height: 900 } },
    },
  ],
  webServer: {
    command: "vite preview --host 127.0.0.1 --port 4173",
    url: "http://127.0.0.1:4173/",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
