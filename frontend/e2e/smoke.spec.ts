// Smoke against the live stack: compose up, worker (LLM_MODEL=test), agent-api, `pnpm dev`.
// TestModel calls every tool, so the first turn always blocks on create_ticket approval.
import { expect, test } from "@playwright/test";

const UI = process.env.UI_URL ?? "http://localhost:3000";

test("send, block on approval, approve, complete", async ({ page }) => {
  const conv = `e2e-${Date.now().toString(36)}`;
  await page.goto(`${UI}/?c=${conv}`);
  await page.getByLabel("prompt").fill("open a ticket about the login bug");
  await page.getByLabel("send").click();
  await expect(page.getByTestId("status")).toHaveText("blocked", { timeout: 30_000 });
  await expect(page.getByTestId("approval-card")).toBeVisible();
  await expect(page.locator("[data-role=assistant]").last()).toContainText("create_ticket");
  await page.getByRole("button", { name: "Approve" }).click();
  await expect(page.getByTestId("status")).toHaveText("idle", { timeout: 30_000 });
  await expect(page.locator("[data-role=assistant]")).toHaveCount(2, { timeout: 30_000 });
  await expect(page.locator("[data-role=assistant]").last()).toContainText("created");
  await expect(page.getByText("network error")).toHaveCount(0);
});
