import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 90_000,
  use: {
    headless: true,
    launchOptions: { executablePath: process.env.CHROMIUM_PATH || undefined },
  },
});
