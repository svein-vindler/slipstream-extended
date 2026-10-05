import { DurableObject } from "cloudflare:workers";

export interface RefreshLease {
  acquired: boolean;
  token?: string;
  retryAfterMs?: number;
}

export interface BudgetedLease extends RefreshLease {
  reason?: "cooldown" | "daily_limit";
  remaining?: number;
}

export type FreshJob = {
  request_id: string;
  scope: string;
  kind: "activity" | "night";
  request: string;
  run_id: number | null;
  state: "active" | "completed" | "failed";
  expires_at: number;
  polls: number;
}

export class RefreshCoordinator extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS refresh_leases (
        lease_key TEXT PRIMARY KEY,
        token TEXT NOT NULL,
        expires_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS daily_budgets (
        budget_key TEXT NOT NULL,
        utc_day INTEGER NOT NULL,
        used INTEGER NOT NULL,
        PRIMARY KEY (budget_key, utc_day)
      );
      CREATE TABLE IF NOT EXISTS fresh_jobs (
        request_id TEXT PRIMARY KEY,
        scope TEXT NOT NULL,
        kind TEXT NOT NULL,
        request TEXT NOT NULL,
        run_id INTEGER,
        state TEXT NOT NULL,
        expires_at INTEGER NOT NULL,
        polls INTEGER NOT NULL DEFAULT 0
      );
    `);
  }

  freshJob(requestId: string): FreshJob | null {
    return this.ctx.storage.sql.exec<FreshJob>(
      "SELECT * FROM fresh_jobs WHERE request_id = ?", requestId,
    ).toArray()[0] ?? null;
  }

  freshJobForRun(runId: number): FreshJob | null {
    return this.ctx.storage.sql.exec<FreshJob>(
      "SELECT * FROM fresh_jobs WHERE run_id = ? ORDER BY expires_at DESC LIMIT 1", runId,
    ).toArray()[0] ?? null;
  }

  activeFreshJob(scope: string, _nowMs: number): FreshJob | null {
    return this.ctx.storage.sql.exec<FreshJob>(
      "SELECT * FROM fresh_jobs WHERE scope = ? AND state = 'active' LIMIT 1",
      scope,
    ).toArray()[0] ?? null;
  }

  beginFresh(scope: string, kind: "activity" | "night", request: string, nowMs: number, repairOnly = false):
    { job?: FreshJob; acquired: boolean; reason?: string; retryAfterMs?: number } {
    if (!scope || request.length > 1024 || !["activity", "night"].includes(kind)) {
      throw new Error("Invalid fresh request");
    }
    const active = this.activeFreshJob(scope, nowMs);
    if (active) return { acquired: false, job: active, reason: "shared_job" };
    const lease = this.reserveBudgeted(`sync-latest-${kind}${repairOnly ? '-repair' : ''}`, `sync-latest-${kind}`,
      nowMs, repairOnly ? 60_000 : 5 * 60_000, 12);
    if (!lease.acquired || !lease.token) return lease;
    // Persist before dispatch. An uncertain POST must retain the reservation;
    // a second request cannot spend another budget unit or attach a wrong run.
    this.ctx.storage.sql.exec(
      "DELETE FROM fresh_jobs WHERE state != 'active' AND expires_at < ?", nowMs - 2 * 86_400_000,
    );
    this.ctx.storage.sql.exec(
      `INSERT INTO fresh_jobs (request_id, scope, kind, request, state, expires_at)
       VALUES (?, ?, ?, ?, 'active', ?)`,
      lease.token, scope, kind, request, nowMs + 2 * 60 * 60_000,
    );
    return { acquired: true, job: this.freshJob(lease.token)! };
  }

  bindFreshRun(requestId: string, runId: number): void {
    if (!Number.isSafeInteger(runId) || runId <= 0) throw new Error("Invalid run ID");
    this.ctx.storage.sql.exec(
      "UPDATE fresh_jobs SET run_id = ? WHERE request_id = ? AND run_id IS NULL", runId, requestId,
    );
  }

  finishFresh(requestId: string, success: boolean): void {
    this.ctx.storage.sql.exec("UPDATE fresh_jobs SET state = ? WHERE request_id = ?",
      success ? "completed" : "failed", requestId);
  }

  takeFreshPoll(requestId: string): boolean {
    const job = this.freshJob(requestId);
    if (!job || job.polls >= 3 || job.state !== "active") return false;
    this.ctx.storage.sql.exec("UPDATE fresh_jobs SET polls = polls + 1 WHERE request_id = ?", requestId);
    return true;
  }

  reserve(leaseKey: string, nowMs: number, ttlMs: number): RefreshLease {
    if (!Number.isFinite(nowMs) || !Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new Error("Invalid refresh lease parameters.");
    }
    const current = this.ctx.storage.sql.exec<{ token: string; expires_at: number }>(
      "SELECT token, expires_at FROM refresh_leases WHERE lease_key = ?",
      leaseKey,
    ).toArray()[0];
    if (current && current.expires_at > nowMs) {
      return { acquired: false, retryAfterMs: current.expires_at - nowMs };
    }

    const token = crypto.randomUUID();
    this.ctx.storage.sql.exec(
      `INSERT INTO refresh_leases (lease_key, token, expires_at)
       VALUES (?, ?, ?)
       ON CONFLICT(lease_key) DO UPDATE SET token = excluded.token, expires_at = excluded.expires_at`,
      leaseKey,
      token,
      nowMs + ttlMs,
    );
    return { acquired: true, token };
  }

  release(leaseKey: string, token: string): { released: boolean } {
    const result = this.ctx.storage.sql.exec(
      "DELETE FROM refresh_leases WHERE lease_key = ? AND token = ?",
      leaseKey,
      token,
    );
    return { released: result.rowsWritten > 0 };
  }

  reserveBudgeted(
    leaseKey: string,
    budgetKey: string,
    nowMs: number,
    ttlMs: number,
    dailyLimit: number,
  ): BudgetedLease {
    if (!leaseKey || !budgetKey || !Number.isFinite(nowMs) || !Number.isFinite(ttlMs)
      || ttlMs <= 0 || !Number.isSafeInteger(dailyLimit) || dailyLimit <= 0) {
      throw new Error("Invalid budgeted lease parameters.");
    }

    const current = this.ctx.storage.sql.exec<{ expires_at: number }>(
      "SELECT expires_at FROM refresh_leases WHERE lease_key = ?",
      leaseKey,
    ).toArray()[0];
    if (current && current.expires_at > nowMs) {
      return {
        acquired: false,
        reason: "cooldown",
        retryAfterMs: current.expires_at - nowMs,
      };
    }

    const utcDay = Math.floor(nowMs / 86_400_000);
    const used = this.ctx.storage.sql.exec<{ used: number }>(
      "SELECT used FROM daily_budgets WHERE budget_key = ? AND utc_day = ?",
      budgetKey,
      utcDay,
    ).toArray()[0]?.used ?? 0;
    if (used >= dailyLimit) {
      const nextDayMs = (utcDay + 1) * 86_400_000;
      return {
        acquired: false,
        reason: "daily_limit",
        retryAfterMs: Math.max(1, nextDayMs - nowMs),
        remaining: 0,
      };
    }

    const token = crypto.randomUUID();
    this.ctx.storage.sql.exec(
      `INSERT INTO refresh_leases (lease_key, token, expires_at)
       VALUES (?, ?, ?)
       ON CONFLICT(lease_key) DO UPDATE SET token = excluded.token, expires_at = excluded.expires_at`,
      leaseKey,
      token,
      nowMs + ttlMs,
    );
    this.ctx.storage.sql.exec(
      `INSERT INTO daily_budgets (budget_key, utc_day, used)
       VALUES (?, ?, 1)
       ON CONFLICT(budget_key, utc_day) DO UPDATE SET used = used + 1`,
      budgetKey,
      utcDay,
    );
    this.ctx.storage.sql.exec(
      "DELETE FROM daily_budgets WHERE utc_day < ?",
      utcDay - 2,
    );
    return { acquired: true, token, remaining: dailyLimit - used - 1 };
  }
}

