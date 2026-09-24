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
    `);
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

