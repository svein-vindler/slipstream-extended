import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";

describe("RefreshCoordinator write budgets", () => {
  it("enforces cooldowns and an exact UTC-day limit", async () => {
    const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
    const now = Date.UTC(2026, 8, 23, 10, 0, 0);

    await expect(stub.reserveBudgeted("lease", "budget", now, 1_000, 2))
      .resolves.toMatchObject({ acquired: true, remaining: 1 });
    await expect(stub.reserveBudgeted("lease", "budget", now + 500, 1_000, 2))
      .resolves.toMatchObject({ acquired: false, reason: "cooldown" });
    await expect(stub.reserveBudgeted("lease", "budget", now + 1_001, 1_000, 2))
      .resolves.toMatchObject({ acquired: true, remaining: 0 });
    await expect(stub.reserveBudgeted("lease", "budget", now + 2_002, 1_000, 2))
      .resolves.toMatchObject({ acquired: false, reason: "daily_limit" });
    await expect(stub.reserveBudgeted("lease", "budget", now + 86_400_000, 1_000, 2))
      .resolves.toMatchObject({ acquired: true, remaining: 1 });
  });

  it("admits no more concurrent writes than the shared budget", async () => {
    const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
    const now = Date.UTC(2026, 8, 23, 12, 0, 0);
    const results = await Promise.all(Array.from({ length: 10 }, (_, index) =>
      stub.reserveBudgeted(`lease-${index}`, "shared-budget", now, 1_000, 3)));

    expect(results.filter((result) => result.acquired)).toHaveLength(3);
    expect(results.filter((result) => result.reason === "daily_limit")).toHaveLength(7);
  });
});
