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

  it("treats SQL-looking lease and budget keys as literal bound values", async () => {
    const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
    const now = Date.UTC(2026, 9, 5, 12);
    const injected = "x'); DROP TABLE daily_budgets; --";
    const first = await stub.reserveBudgeted(injected, injected, now, 1_000, 1);
    expect(first).toMatchObject({ acquired: true, remaining: 0 });
    await expect(stub.reserveBudgeted(injected, injected, now + 1_001, 1_000, 1))
      .resolves.toMatchObject({ acquired: false, reason: "daily_limit" });
    await expect(stub.reserveBudgeted("ordinary", "ordinary", now, 1_000, 1))
      .resolves.toMatchObject({ acquired: true, remaining: 0 });
    await expect(stub.release(injected, "' OR 1=1 --"))
      .resolves.toMatchObject({ released: false });
    await expect(stub.release(injected, first.token!))
      .resolves.toMatchObject({ released: true });
  });

  it("preserves SQL-looking fresh-job context without executing it", async () => {
    const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
    const now = Date.UTC(2026, 9, 5, 12);
    const scope = "night'); DROP TABLE fresh_jobs; --";
    const request = JSON.stringify({ note: "'; UPDATE fresh_jobs SET state='failed'; --" });
    const result = await stub.beginFresh(scope, "night", request, now);
    expect(result.acquired).toBe(true);
    const job = result.job!;
    await expect(stub.freshJob("' OR 1=1 --")).resolves.toBeNull();
    await expect(stub.freshJob(job.request_id)).resolves.toMatchObject({ scope, request, state: "active" });
    await stub.bindFreshRun(job.request_id, 123);
    await stub.finishFresh(job.request_id, true);
    await expect(stub.freshJobForRun(123)).resolves.toMatchObject({ scope, request, state: "completed" });
  });
});
