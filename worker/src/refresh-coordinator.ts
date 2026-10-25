import { COACH_PROFILE_CAPACITY, COACH_PROFILE_SLOT_PREFIX } from "./coach-profile-capacity";
import { DispatchOperations, type DispatchDescriptor, type DispatchIdentity, type DispatchClaim, type DispatchOperation } from "./dispatch-operations";
import type { DispatchPostOutcome } from "./github";
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

export type CoachProfileLease = Omit<BudgetedLease, "reason"> & {
  reason?: BudgetedLease["reason"] | "profile_capacity" | "profile_in_progress";
};

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
  private readonly dispatch: DispatchOperations;
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
    this.dispatch = new DispatchOperations(ctx.storage);
  }

  dispatchOperation(id: string) { return this.dispatch.get(id); }
  adoptFreshDispatch(id: string) {
    return this.ctx.storage.transactionSync(() => this.dispatch.adoptFresh(id));
  }
  dispatchForRun(id: number) { return this.dispatch.forRun(id); }
  activeDispatch(ownerKey: string) { return this.dispatch.active(ownerKey); }
  blockingDispatch(mode: DispatchDescriptor["mode"], ownerKey: string) {
    return this.ctx.storage.transactionSync(() => this.dispatch.blocking(mode, ownerKey));
  }
  beginGeneralDispatch(token: string, descriptor: DispatchDescriptor, nowMs: number) {
    return this.ctx.storage.transactionSync(() => this.dispatch.beginGeneral(token, descriptor, nowMs));
  }
  takeDispatchRead(identity: DispatchIdentity, claim: DispatchClaim, expectedRevision: number,
    kind: "run" | "receipt" | "report" | "diagnostics", _nowMs: number) {
    return this.ctx.storage.transactionSync(() => this.dispatch.take(identity, claim, expectedRevision, kind));
  }
  mergeDispatchPost(identity: DispatchIdentity, outcome: DispatchPostOutcome, nowMs: number) {
    return this.ctx.storage.transactionSync(() => this.dispatch.mergePost(identity, outcome, nowMs));
  }
  claimDispatchRead(identity: DispatchIdentity, intent: DispatchClaim["intent"], actorKey: string, nowMs: number) {
    return this.ctx.storage.transactionSync(() => this.dispatch.claim(identity, intent, actorKey, nowMs,
      (key, now, ttl) => this.reserve(key, now, ttl).acquired));
  }
  recordDispatchReceipt(identity: DispatchIdentity, claim: DispatchClaim, expectedRevision: number, receipt: unknown, nowMs: number) {
    return this.ctx.storage.transactionSync(() => this.dispatch.receipt(identity, claim, expectedRevision, receipt, nowMs,
      (key, now, ttl) => this.reserve(key, now,ttl).acquired));
  }
  finishDispatchRead(identity: DispatchIdentity, claim: DispatchClaim, expectedRevision: number, evidence: unknown, nowMs: number) {
    return this.ctx.storage.transactionSync(() => this.dispatch.finish(identity, claim, expectedRevision, evidence, nowMs));
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

  beginFresh(scope: string, kind: "activity" | "night", request: string, nowMs: number, repairOnly = false, descriptor?: DispatchDescriptor):
    { job?: FreshJob; operation?: DispatchOperation; acquired: boolean; reason?: string; retryAfterMs?: number } {
    return this.ctx.storage.transactionSync(() => {
    if (!scope || request.length > 1024 || !["activity", "night"].includes(kind)) {
      throw new Error("Invalid fresh request");
    }
    const active = this.activeFreshJob(scope, nowMs);
    if (active) return { acquired: false, job: active, reason: "shared_job" };
    const blocked = this.dispatch.blocking(kind, "targeted:" + scope);
    if (blocked) return { acquired: false, operation: blocked, reason: "dispatch_overlap" };
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
    if (descriptor) this.dispatch.create({ operation_id: lease.token, owner_key: "targeted:" + scope,
      owner_token: lease.token }, descriptor, repairOnly ? "targeted_repair" : kind === "night" ? "targeted_night" : "targeted_activity");
    return { acquired: true, job: this.freshJob(lease.token)! };
    });
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

  reserveCoachProfileWrite(
    profileKey: string, existingKeys: string[], actorKey: string, nowMs: number, dailyLimit: number,
  ): CoachProfileLease {
    if (!/^coach\/profiles\/v1\/\d{4}-\d{2}-\d{2}\/hr-[a-f0-9]{24}\.json$/.test(profileKey)
      || !Array.isArray(existingKeys) || existingKeys.length > COACH_PROFILE_CAPACITY
      || existingKeys.some(key => typeof key !== "string" || key.length > 1024
        || !key.startsWith("coach/profiles/v1/"))) {
      throw new Error("Invalid coach profile reservation.");
    }
    return this.ctx.storage.transactionSync(() => {
      const reserved = this.ctx.storage.sql.exec<{ token: string }>(
        "SELECT token FROM refresh_leases WHERE lease_key GLOB ? LIMIT ?",
        `${COACH_PROFILE_SLOT_PREFIX}*`, COACH_PROFILE_CAPACITY + 1,
      ).toArray();
      if (reserved.some(row => row.token === profileKey)) {
        return { acquired: false, reason: "profile_in_progress" };
      }
      const occupied = new Set([...existingKeys, ...reserved.map(row => row.token)]);
      if (occupied.size >= COACH_PROFILE_CAPACITY) {
        return { acquired: false, reason: "profile_capacity" };
      }
      const writeKey = `mcp-write:${actorKey}`;
      const lease = this.reserveBudgeted(writeKey, writeKey, nowMs, 60_000, dailyLimit);
      if (!lease.acquired) return lease;
      // Reuse the existing durable table, without a DO/schema migration. An
      // immutable profile owns its slot permanently, including uncertain PUTs.
      // Never expire/refund it while a delayed R2 write can still arrive.
      this.ctx.storage.sql.exec(
        "INSERT INTO refresh_leases (lease_key, token, expires_at) VALUES (?, ?, ?)",
        `${COACH_PROFILE_SLOT_PREFIX}${profileKey}`, profileKey, Number.MAX_SAFE_INTEGER,
      );
      return lease;
    });
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

  /** A pending R2 append cannot expire while its delayed PUT can still arrive. */
  reserveActivityContextWrite(activityId: string): RefreshLease {
    if (!/^\d{1,20}$/.test(activityId)) throw new Error("Invalid context reservation.");
    return this.ctx.storage.transactionSync(() => {
      const key = `mcp-context-pending:${activityId}`;
      if (this.ctx.storage.sql.exec("SELECT token FROM refresh_leases WHERE lease_key = ?", key).toArray().length) {
        return { acquired: false };
      }
      const token = crypto.randomUUID();
      this.ctx.storage.sql.exec("INSERT INTO refresh_leases (lease_key, token, expires_at) VALUES (?, ?, ?)",
        key, token, Number.MAX_SAFE_INTEGER);
      return { acquired: true, token };
    });
  }

  finishActivityContextWrite(activityId: string, token: string): { released: boolean } {
    if (!/^\d{1,20}$/.test(activityId) || !token) throw new Error("Invalid context completion.");
    return this.release(`mcp-context-pending:${activityId}`, token);
  }
}

