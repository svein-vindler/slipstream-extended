import { z } from "zod";
import type { DispatchPostOutcome } from "./github";

export const dispatchDescriptorSchema = z.strictObject({
  repository: z.string().regex(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/).max(256),
  workflow: z.string().min(1).max(256), ref: z.string().min(1).max(256),
  mode: z.enum(["general", "activity", "night"]), scope: z.string().max(320).nullable(),
  date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).nullable(),
  activity_id: z.string().regex(/^\d{1,20}$/).nullable(),
  new_expected: z.boolean(), repair_only: z.boolean(), include_granular: z.boolean(),
  components: z.array(z.string().min(1).max(40)).max(10),
});
export type DispatchDescriptor = z.infer<typeof dispatchDescriptorSchema>;
const requestedCoverageSchema = dispatchDescriptorSchema.omit({ repository: true, workflow: true, ref: true })
  .extend({ date: z.iso.date().nullable(), components: z.array(z.enum([
    "summary", "health", "body", "activity_files", "coach_input", "context", "sleep", "hrv",
  ])).max(10) });
export const dispatchReceiptSchema = z.strictObject({
  schema_version: z.literal(1), kind: z.literal("refresh-dispatch-receipt"),
  request_id: z.uuid(), run_id: z.number().int().safe().positive(),
  source: z.strictObject({ repository: z.string().min(1).max(256),
    workflow_path: z.string().min(1).max(256), event: z.literal("workflow_dispatch"),
    ref: z.string().min(1).max(256),
    source_sha: z.string().regex(/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/) }),
  request: requestedCoverageSchema,
});
export type DispatchStartupReceipt = z.infer<typeof dispatchReceiptSchema>;
function receiptMatches(identity: DispatchIdentity, descriptor: DispatchDescriptor | null, receipt: DispatchStartupReceipt): boolean {
  if (!descriptor) return false;
  const { repository, workflow, ref, ...coverage } = descriptor;
  const path = workflow.split(String.fromCharCode(92)).join("/");
  return receipt.request_id === identity.operation_id && receipt.source.repository === repository
    && receipt.source.workflow_path === (path.includes("/") ? path : ".github/workflows/" + path)
    && receipt.source.ref === (ref.startsWith("refs/heads/") ? ref : "refs/heads/" + ref)
    && JSON.stringify(receipt.request) === JSON.stringify(requestedCoverageSchema.parse(coverage));
}
const postOutcomeSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("ack_candidate"), run_id: z.number().int().safe().positive() }),
  z.strictObject({ kind: z.literal("post_rejected"), method: z.literal("POST"), phase: z.literal("dispatch"),
    status: z.number().int().refine(value => [400, 401, 403, 404, 422, 429].includes(value)) }),
  z.strictObject({ kind: z.literal("unconfirmed"), reason: z.enum(["transport", "deadline", "empty_ack", "invalid_ack", "other_http"]) }),
]);
export type DispatchIdentity = { operation_id: string; owner_key: string; owner_token: string };
export type DispatchClaim = { claim_id: string; claim_revision: number;
  intent: "initial" | "shared" | "status" | "terminal_inspection"; run_get_limit: number; run_gets_used: number;
  receipt_used: boolean; report_used: boolean; diagnostics_used: boolean;
  status_actor?: string; status_run_reserved?: boolean };
export type DispatchBinding = { run_id: number; repository: string; workflow: string; ref: string;
  source_sha: string; correlation_source: string; coverage: DispatchDescriptor };
const bindingSchema = z.strictObject({ run_id: z.number().int().safe().positive(),
  repository: z.string().max(256), workflow: z.string().max(256), ref: z.string().max(256),
  source_sha: z.string().regex(/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/),
  correlation_source: z.enum(["request_receipt", "direct_ack", "legacy_run_and_report"]),
  coverage: dispatchDescriptorSchema });
export const dispatchReadEvidenceSchema = z.strictObject({ run_id: z.number().int().safe().positive(),
  status: z.enum(["queued", "in_progress", "completed", "waiting", "pending", "requested"]),
  conclusion: z.enum(["success", "failure", "cancelled", "timed_out", "action_required", "neutral", "skipped", "startup_failure", "stale"]).nullable(),
  binding: bindingSchema.nullable(),
}).refine(value => value.status === "completed" ? value.conclusion !== null : value.conclusion === null);
export type DispatchReadEvidence = z.infer<typeof dispatchReadEvidenceSchema>;
export type DispatchOperation = DispatchIdentity & {
  contract_version: 1; revision: number;
  origin: "targeted_activity" | "targeted_night" | "targeted_repair" | "general";
  descriptor: DispatchDescriptor | null;
  dispatch_state: "unconfirmed" | "candidate" | "verified_active" | "verified_terminal" | "post_rejected";
  candidate_run_id: number | null; verified_binding: DispatchBinding | null;
  last_observation: { status: string; conclusion: string | null; observed_at: number } | null;
  evidence_conflict: boolean; owner_released_at: number | null; read_claim: DispatchClaim | null;
  general_initial_reads_used: number | null;
  startup_receipt?: DispatchStartupReceipt | null;
  terminal_inspection_used?: true;
};
const claimSchema = z.strictObject({ claim_id: z.uuid(), claim_revision: z.number().int().safe().nonnegative(),
  intent: z.enum(["initial", "shared", "status", "terminal_inspection"]), run_get_limit: z.number().int().min(1).max(5),
  run_gets_used: z.number().int().min(0).max(5), receipt_used: z.boolean(), report_used: z.boolean(), diagnostics_used: z.boolean(),
  status_actor: z.string().min(1).max(128).optional(), status_run_reserved: z.boolean().optional(),
}).refine(value => value.run_gets_used <= value.run_get_limit);
const operationSchema = z.strictObject({ operation_id: z.uuid(), owner_key: z.string().min(1).max(360), owner_token: z.uuid(),
  contract_version: z.literal(1), revision: z.number().int().safe().nonnegative(),
  origin: z.enum(["targeted_activity", "targeted_night", "targeted_repair", "general"]),
  descriptor: dispatchDescriptorSchema.nullable(),
  dispatch_state: z.enum(["unconfirmed", "candidate", "verified_active", "verified_terminal", "post_rejected"]),
  candidate_run_id: z.number().int().safe().positive().nullable(), verified_binding: bindingSchema.nullable(),
  last_observation: z.strictObject({ status: z.string().min(1).max(40), conclusion: z.string().max(40).nullable(),
    observed_at: z.number().finite().nonnegative() }).nullable(),
  evidence_conflict: z.boolean(), owner_released_at: z.number().finite().nonnegative().nullable(),
  read_claim: claimSchema.nullable(), startup_receipt: dispatchReceiptSchema.nullable().optional(), general_initial_reads_used: z.number().int().min(0).max(5).nullable(),
  terminal_inspection_used: z.literal(true).optional(),
}).refine(value => value.operation_id === value.owner_token)
  .refine(value => !value.terminal_inspection_used || value.origin !== "general"
    && value.descriptor !== null && "startup_receipt" in value)
  .refine(value => value.read_claim?.intent !== "terminal_inspection" || value.terminal_inspection_used === true)
  .refine(value => !value.startup_receipt || receiptMatches(value, value.descriptor, value.startup_receipt)
    && value.candidate_run_id === value.startup_receipt.run_id
    && (!value.verified_binding || value.verified_binding.source_sha === value.startup_receipt.source.source_sha))
  .refine(value => value.origin === "general" ? value.general_initial_reads_used !== null : value.general_initial_reads_used === null)
  .refine(value => ["verified_terminal", "post_rejected"].includes(value.dispatch_state)
    ? value.owner_released_at !== null && value.read_claim === null : value.owner_released_at === null)
  .refine(value => {
    if (value.dispatch_state === "post_rejected")
      return value.candidate_run_id === null && value.verified_binding === null && value.last_observation === null;
    if (!["verified_active", "verified_terminal"].includes(value.dispatch_state))
      return value.verified_binding === null && value.last_observation === null;
    const binding = value.verified_binding, observation = value.last_observation;
    if (!binding || !observation || !value.descriptor || binding.run_id !== value.candidate_run_id
      || binding.repository !== value.descriptor.repository || binding.workflow !== value.descriptor.workflow
      || binding.ref !== value.descriptor.ref || JSON.stringify(binding.coverage) !== JSON.stringify(value.descriptor)) return false;
    return dispatchReadEvidenceSchema.safeParse({ run_id: binding.run_id, status: observation.status,
      conclusion: observation.conclusion, binding }).success
      && (value.dispatch_state === "verified_terminal") === (observation.status === "completed");
  });

/** Synchronous operations; the coordinator wraps mutations in transactionSync. */
export class DispatchOperations {
  constructor(private readonly storage: DurableObjectStorage) {
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS dispatch_operations_v1 (
      operation_id TEXT PRIMARY KEY, owner_key TEXT NOT NULL, owner_token TEXT NOT NULL,
      revision INTEGER NOT NULL, candidate_run_id INTEGER, verified_run_id INTEGER,
      owner_released_at INTEGER, payload TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS dispatch_active_owner_v1
      ON dispatch_operations_v1(owner_key) WHERE owner_released_at IS NULL;
    CREATE INDEX IF NOT EXISTS dispatch_candidate_v1 ON dispatch_operations_v1(candidate_run_id);
    CREATE INDEX IF NOT EXISTS dispatch_verified_v1 ON dispatch_operations_v1(verified_run_id);`);
  }

  private decode(payload: string): DispatchOperation {
    try {
      const parsed = operationSchema.safeParse(JSON.parse(payload));
      if (parsed.success) return parsed.data;
    } catch { /* A corrupt row is unverified, never a missing installation. */ }
    throw new Error("Unverified dispatch operation contract.");
  }
  get(id: string): DispatchOperation | null {
    const row = this.storage.sql.exec<{ payload: string }>(
      "SELECT payload FROM dispatch_operations_v1 WHERE operation_id = ?", id).toArray()[0];
    const op = row ? this.decode(row.payload) : null;
    if (op && op.operation_id !== id) throw new Error("Unverified dispatch operation contract.");
    return op;
  }
  active(ownerKey: string): DispatchOperation | null {
    const row = this.storage.sql.exec<{ payload: string }>(
      "SELECT payload FROM dispatch_operations_v1 WHERE owner_key = ? AND owner_released_at IS NULL", ownerKey).toArray()[0];
    const op = row ? this.decode(row.payload) : null;
    if (op && (op.owner_key !== ownerKey || op.owner_released_at !== null)) throw new Error("Unverified dispatch operation contract.");
    return op;
  }
  blocking(mode: DispatchDescriptor["mode"], ownerKey: string): DispatchOperation | null {
    if (!["general", "activity", "night"].includes(mode)) throw new Error("Invalid dispatch coverage.");
    const row = this.storage.sql.exec<{ payload: string }>(
      `SELECT payload FROM dispatch_operations_v1 INDEXED BY dispatch_active_owner_v1
       WHERE owner_released_at IS NULL AND owner_key != ? AND
       (? = 'general' OR json_extract(payload, '$.verified_binding') IS NULL
        OR json_extract(payload, '$.descriptor.mode') IS NULL
        OR json_extract(payload, '$.descriptor.mode') IN ('general', ?)) LIMIT 1`,
      ownerKey, mode, mode).toArray()[0];
    if (row) return this.decode(row.payload);
    // Lookup/adopt at most one still-active legacy state row, never history or R2.
    const legacy = this.storage.sql.exec<{ request_id: string }>(
      `SELECT request_id FROM fresh_jobs WHERE state = 'active'
       AND NOT EXISTS (SELECT 1 FROM dispatch_operations_v1 WHERE operation_id = fresh_jobs.request_id) LIMIT 1`).toArray()[0];
    return legacy ? this.adoptFresh(legacy.request_id) : null;
  }
  beginGeneral(token: string, descriptor: DispatchDescriptor, nowMs: number): DispatchOperation | null {
    const lease = this.storage.sql.exec<{ token: string; expires_at: number }>(
      "SELECT token, expires_at FROM refresh_leases WHERE lease_key = 'refresh-today'").toArray()[0];
    const active = this.active("general-refresh");
    if (active) return active;
    const blocked = this.blocking("general", "general-refresh");
    if (blocked) return blocked;
    if (!lease || lease.token !== token || lease.expires_at <= nowMs || descriptor.mode !== "general") return null;
    return this.create({ operation_id: token, owner_key: "general-refresh", owner_token: token }, descriptor, "general");
  }
  forRun(id: number): DispatchOperation | null {
    if (!Number.isSafeInteger(id) || id <= 0) throw new Error("Invalid dispatch run identity.");
    const rows = this.storage.sql.exec<{ payload: string }>(
      "SELECT payload FROM dispatch_operations_v1 WHERE candidate_run_id = ? OR verified_run_id = ? LIMIT 2", id, id).toArray();
    if (rows.length > 1) throw new Error("Ambiguous dispatch run identity.");
    const op = rows.length ? this.decode(rows[0].payload) : null;
    if (op && op.candidate_run_id !== id && op.verified_binding?.run_id !== id) throw new Error("Unverified dispatch operation contract.");
    return op;
  }
  private current(identity: DispatchIdentity): DispatchOperation | null {
    const op = this.get(identity.operation_id);
    return op && op.owner_key === identity.owner_key && op.owner_token === identity.owner_token ? op : null;
  }
  private owns(op: DispatchOperation): boolean {
    return op.owner_released_at === null && this.active(op.owner_key)?.operation_id === op.operation_id;
  }
  private otherRunOwner(runId: number, operationId: string): boolean {
    return this.storage.sql.exec("SELECT 1 FROM dispatch_operations_v1 WHERE operation_id != ? AND (candidate_run_id = ? OR verified_run_id = ?) LIMIT 1",
      operationId, runId, runId).toArray().length > 0;
  }
  private linkFreshCandidate(op: DispatchOperation): void {
    if (op.origin !== "general" && op.candidate_run_id !== null)
      this.storage.sql.exec("UPDATE fresh_jobs SET run_id = ? WHERE request_id = ? AND run_id IS NULL",
        op.candidate_run_id, op.operation_id);
  }
  private save(op: DispatchOperation, previous: number): DispatchOperation {
    const result = this.storage.sql.exec(
      `UPDATE dispatch_operations_v1 SET revision = ?, candidate_run_id = ?, verified_run_id = ?,
       owner_released_at = ?, payload = ? WHERE operation_id = ? AND owner_key = ? AND owner_token = ? AND revision = ? RETURNING operation_id`,
      op.revision, op.candidate_run_id, op.verified_binding?.run_id ?? null, op.owner_released_at, JSON.stringify(op),
      op.operation_id, op.owner_key, op.owner_token, previous);
    if (result.toArray().length !== 1) throw new Error("Dispatch operation changed.");
    return op;
  }

  create(identity: DispatchIdentity, descriptor: DispatchDescriptor | null,
    origin: DispatchOperation["origin"], candidate: number | null = null): DispatchOperation {
    if (!z.uuid().safeParse(identity.operation_id).success || identity.operation_id !== identity.owner_token
      || !identity.owner_key || identity.owner_key.length > 360) throw new Error("Invalid dispatch owner.");
    const checked = dispatchDescriptorSchema.nullable().safeParse(descriptor);
    if (!checked.success) throw new Error("Invalid dispatch descriptor.");
    const parsed = checked.data;
    const existing = this.get(identity.operation_id);
    if (existing) {
      if (existing.owner_key !== identity.owner_key || existing.owner_token !== identity.owner_token
        || JSON.stringify(existing.descriptor) !== JSON.stringify(parsed)) throw new Error("Dispatch identity is immutable.");
      return existing;
    }
    const op: DispatchOperation = { ...identity, contract_version: 1, revision: 0, origin, descriptor: parsed,
      dispatch_state: "unconfirmed", candidate_run_id: candidate, verified_binding: null, last_observation: null,
      evidence_conflict: false, owner_released_at: null, read_claim: null,
      general_initial_reads_used: origin === "general" ? 0 : null,
      ...(parsed ? { startup_receipt: null } : {}) };
    this.storage.sql.exec(`INSERT INTO dispatch_operations_v1
      (operation_id, owner_key, owner_token, revision, candidate_run_id, payload) VALUES (?, ?, ?, 0, ?, ?)`,
    op.operation_id, op.owner_key, op.owner_token, candidate, JSON.stringify(op));
    return op;
  }

  adoptFresh(id: string): DispatchOperation | null {
    const existing = this.get(id);
    if (existing) return existing;
    const job = this.storage.sql.exec<{ scope: string; kind: string; run_id: number | null; state: string }>(
      "SELECT scope, kind, run_id, state FROM fresh_jobs WHERE request_id = ?", id).toArray()[0];
    if (!job || job.state !== "active") return null;
    if (!["activity", "night"].includes(job.kind) || job.run_id !== null && (!Number.isSafeInteger(job.run_id) || job.run_id <= 0))
      throw new Error("Unverified legacy dispatch contract.");
    return this.create({ operation_id: id, owner_key: "targeted:" + job.scope, owner_token: id }, null,
      job.kind === "night" ? "targeted_night" : "targeted_activity", job.run_id);
  }

  mergePost(identity: DispatchIdentity, outcome: DispatchPostOutcome, nowMs: number): DispatchOperation | null {
    if (!postOutcomeSchema.safeParse(outcome).success || !Number.isFinite(nowMs))
      throw new Error("Invalid dispatch outcome.");
    const op = this.current(identity);
    if (!op || !this.owns(op) || outcome.kind === "unconfirmed"
      || op.dispatch_state === "verified_terminal" || op.dispatch_state === "post_rejected") return op;
    if (outcome.kind === "post_rejected") {
      if (op.evidence_conflict) return op;
      const previous = op.revision++;
      if (op.candidate_run_id !== null || op.verified_binding) {
        op.evidence_conflict = true;
      } else {
        op.dispatch_state = "post_rejected";
        op.owner_released_at = nowMs;
        op.read_claim = null;
        this.storage.sql.exec("UPDATE fresh_jobs SET state = 'failed' WHERE request_id = ? AND state = 'active'", op.operation_id);
      }
      return this.save(op, previous);
    }
    if (op.evidence_conflict || op.candidate_run_id === outcome.run_id) return op;
    const previous = op.revision;
    if (op.candidate_run_id !== null || op.verified_binding && op.verified_binding.run_id !== outcome.run_id
      || this.otherRunOwner(outcome.run_id, op.operation_id)) {
      op.evidence_conflict = true;
    } else {
      op.candidate_run_id = outcome.run_id;
      this.linkFreshCandidate(op);
      if (!op.verified_binding) op.dispatch_state = "candidate";
    }
    op.revision++;
    return this.save(op, previous);
  }

  claim(identity: DispatchIdentity, intent: DispatchClaim["intent"], actorKey: string, nowMs: number,
    reserveStatus: (key: string, now: number, ttl: number) => boolean):
    { acquired: boolean; operation: DispatchOperation | null; claim?: DispatchClaim } {
    if (!Number.isFinite(nowMs) || !["initial", "shared", "status", "terminal_inspection"].includes(intent)) throw new Error("Invalid dispatch claim.");
    const op = this.current(identity);
    if (!op || !this.owns(op) || op.read_claim || op.evidence_conflict || !("startup_receipt" in op)) return { acquired: false, operation: op };
    let limit: number;
    if (op.origin === "general") {
      if (intent === "shared" || intent === "terminal_inspection") return { acquired: false, operation: op };
      if (intent === "initial") {
        const lease = this.storage.sql.exec<{ token: string; expires_at: number }>(
          "SELECT token, expires_at FROM refresh_leases WHERE lease_key = 'refresh-today'").toArray()[0];
        if (!lease || lease.token !== op.owner_token || lease.expires_at <= nowMs) return { acquired: false, operation: op };
        limit = 5 - (op.general_initial_reads_used ?? 5);
        if (limit <= 0) return { acquired: false, operation: op };
      } else {
        if (!actorKey || actorKey.length > 128) return { acquired: false, operation: op };
        const keys = ["refresh-status-op:" + actorKey + ":" + op.operation_id,
          ...(op.candidate_run_id ? ["refresh-status:" + actorKey + ":" + op.candidate_run_id] : [])];
        // Check both aliases before mutating either lease in this transaction.
        if (keys.some(key => {
          const row = this.storage.sql.exec<{ expires_at: number }>(
            "SELECT expires_at FROM refresh_leases WHERE lease_key = ?", key).toArray()[0];
          return row && row.expires_at > nowMs;
        })) return { acquired: false, operation: op };
        for (const key of keys) if (!reserveStatus(key, nowMs, 25_000)) throw new Error("Dispatch status admission changed.");
        limit = 5;
      }
    } else {
      const job = this.storage.sql.exec<{ polls: number; expires_at: number; state: string;
        scope: string; kind: string; run_id: number | null }>(
        "SELECT polls, expires_at, state, scope, kind, run_id FROM fresh_jobs WHERE request_id = ?", op.operation_id).toArray()[0];
      if (!job || job.state !== "active") return { acquired: false, operation: op };
      if (intent === "terminal_inspection") {
        if (job.polls !== 3 || op.terminal_inspection_used || !actorKey || actorKey.length > 128
          || !op.descriptor || op.descriptor.scope !== job.scope || op.descriptor.mode !== job.kind
          || op.owner_key !== "targeted:" + job.scope || job.run_id !== op.candidate_run_id
          || (job.kind === "night" ? op.origin !== "targeted_night"
            : job.kind !== "activity" || !["targeted_activity", "targeted_repair"].includes(op.origin))
          || !Number.isSafeInteger(job.expires_at) || job.expires_at < 0) return { acquired: false, operation: op };
        // Persist with the claim/CAS in the coordinator's transaction, before I/O.
        // Unknown, failed and interrupted observations never refund this opportunity.
        op.terminal_inspection_used = true;
      } else {
        if (job.polls >= 3 || nowMs >= job.expires_at && intent !== "status") return { acquired: false, operation: op };
        this.storage.sql.exec("UPDATE fresh_jobs SET polls = polls + 1 WHERE request_id = ? AND polls < 3", op.operation_id);
      }
      limit = nowMs >= job.expires_at ? 1 : 2;
    }
    const previous = op.revision++;
    op.read_claim = { claim_id: crypto.randomUUID(), claim_revision: op.revision, intent,
      run_get_limit: limit, run_gets_used: 0,
      receipt_used: false, report_used: false, diagnostics_used: false,
      ...(op.origin === "general" && intent === "status" ? {
        status_actor: actorKey, status_run_reserved: op.candidate_run_id !== null,
      } : {}) };
    return { acquired: true, operation: this.save(op, previous), claim: op.read_claim };
  }

  take(identity: DispatchIdentity, claim: DispatchClaim, expectedRevision: number,
    kind: "run" | "receipt" | "report" | "diagnostics"): DispatchOperation | null {
    const op = this.current(identity);
    if (!op || !this.owns(op) || op.evidence_conflict || op.revision !== expectedRevision || !op.read_claim
      || op.read_claim.claim_id !== claim.claim_id || op.read_claim.claim_revision !== claim.claim_revision) return null;
    const owned = op.read_claim;
    if (kind === "run") {
      if (owned.run_gets_used >= owned.run_get_limit || !op.startup_receipt
        || op.origin === "general" && owned.intent === "status" && !owned.status_run_reserved) return null;
      if (op.origin === "general" && owned.intent === "initial") {
        if (op.general_initial_reads_used === null || op.general_initial_reads_used >= 5) return null;
        op.general_initial_reads_used++;
      }
      owned.run_gets_used++;
    } else {
      if (!["receipt", "report", "diagnostics"].includes(kind)
        || kind !== "receipt" && owned.run_gets_used === 0) return null;
      const key = `${kind}_used` as "receipt_used" | "report_used" | "diagnostics_used";
      if (owned[key]) return null;
      owned[key] = true;
    }
    const previous = op.revision++;
    return this.save(op, previous);
  }

  receipt(identity: DispatchIdentity, claim: DispatchClaim, expectedRevision: number, receipt: unknown, nowMs: number,
    reserveStatus: (key: string, now: number, ttl: number) => boolean):
    { operation: DispatchOperation | null; receipt_verified: boolean } {
    const op = this.current(identity);
    const no = (operation = op) => ({ operation, receipt_verified: false });
    if (!Number.isFinite(nowMs) || !op || !this.owns(op) || op.revision !== expectedRevision || !op.read_claim
      || op.read_claim.claim_id !== claim.claim_id || op.read_claim.claim_revision !== claim.claim_revision
      || !op.read_claim.receipt_used || op.evidence_conflict || !("startup_receipt" in op)) return no();
    const parsed = dispatchReceiptSchema.safeParse(receipt);
    if (!parsed.success) return no();
    const evidence = parsed.data;
    if (!receiptMatches(op, op.descriptor, evidence)
      || op.candidate_run_id !== null && op.candidate_run_id !== evidence.run_id
      || op.startup_receipt && JSON.stringify(op.startup_receipt) !== JSON.stringify(evidence)
      || this.otherRunOwner(evidence.run_id, op.operation_id)) {
      const previous = op.revision++;
      op.evidence_conflict = true;
      return no(this.save(op, previous));
    }
    const previous = op.revision++;
    op.candidate_run_id = evidence.run_id;
    op.startup_receipt ??= evidence;
    if (!op.verified_binding) op.dispatch_state = "candidate";
    this.linkFreshCandidate(op);
    if (op.origin === "general" && op.read_claim.intent === "status" && !op.read_claim.status_run_reserved) {
      const actor = op.read_claim.status_actor;
      op.read_claim.status_run_reserved = Boolean(actor
        && reserveStatus("refresh-status:" + actor + ":" + evidence.run_id, nowMs, 25_000));
    }
    return { operation: this.save(op, previous), receipt_verified: true };
  }

  finish(identity: DispatchIdentity, claim: DispatchClaim, expectedRevision: number, evidence: unknown,
    nowMs: number): { operation: DispatchOperation | null; observation_committed: boolean } {
    if (!Number.isFinite(nowMs)) throw new Error("Invalid dispatch observation time.");
    const op = this.current(identity);
    if (!op || !this.owns(op) || !op.read_claim || op.read_claim.claim_id !== claim.claim_id
      || op.read_claim.claim_revision !== claim.claim_revision) return { operation: op, observation_committed: false };
    const previous = op.revision;
    let committed = false;
    // An old GET may close only its own still-current claim, never commit data.
    if (expectedRevision === op.revision && evidence !== null && !op.evidence_conflict) {
      const parsed = dispatchReadEvidenceSchema.safeParse(evidence);
      if (parsed.success && parsed.data.binding && op.descriptor && op.startup_receipt) {
        const observed = parsed.data, binding = observed.binding!;
        const matches = binding.source_sha === op.startup_receipt.source.source_sha
          && observed.run_id === op.candidate_run_id && binding.run_id === observed.run_id
          && binding.repository === op.descriptor.repository && binding.workflow === op.descriptor.workflow
          && binding.ref === op.descriptor.ref && JSON.stringify(binding.coverage) === JSON.stringify(op.descriptor)
          && (!op.verified_binding || op.verified_binding.run_id === binding.run_id
            && op.verified_binding.repository === binding.repository && op.verified_binding.workflow === binding.workflow
            && op.verified_binding.ref === binding.ref && op.verified_binding.source_sha === binding.source_sha);
        if (!matches) {
          op.evidence_conflict = true;
        } else {
          committed = true;
          op.verified_binding ??= binding;
          op.last_observation = { status: observed.status, conclusion: observed.conclusion, observed_at: nowMs };
          op.dispatch_state = observed.status === "completed" ? "verified_terminal" : "verified_active";
          if (op.dispatch_state === "verified_terminal") {
            op.owner_released_at = nowMs;
            this.storage.sql.exec("UPDATE fresh_jobs SET state = ? WHERE request_id = ? AND state = 'active'",
              observed.conclusion === "success" ? "completed" : "failed", op.operation_id);
          }
        }
      }
    }
    op.revision++;
    op.read_claim = null; // Close only this caller's claim; never refund its ticket.
    return { operation: this.save(op, previous), observation_committed: committed };
  }
}
