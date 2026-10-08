/** Additive status contract. Workflow success and package readiness are separate. */
import { z } from "zod";
import type { RefreshRun } from "./github";

const duration = z.number().finite().nonnegative().nullable();
const timestamp = z.iso.datetime({ offset: true });
export const latestNightReportSchema = z.object({
  schema_version: z.literal(1), kind: z.literal("latest-night"),
  wake_date: z.iso.date(), scope: z.string(), checked_at: timestamp,
  source_checked: z.boolean(), status: z.enum(["stored", "pending"]),
  sleep_status: z.enum(["not_checked", "stored", "garmin_not_ready", "wrong_date", "invalid_response", "import_error"]),
  hrv_status: z.enum(["not_checked", "stored", "garmin_not_ready", "wrong_date", "invalid_response", "import_error"]),
}).refine(value => value.scope === `night/${value.wake_date}`)
  .refine(value => value.status !== "stored" || value.sleep_status === "stored" && value.hrv_status === "stored")
  .refine(value => !value.source_checked || [value.sleep_status, value.hrv_status]
    .every(status => status === "stored" || status === "garmin_not_ready"));
export const pipelineDiagnosticsSchema = z.object({
  schema_version: z.literal(1), kind: z.literal("refresh-diagnostics"),
  mode: z.enum(["general", "activity", "night"]), status: z.enum(["complete", "partial", "failed"]),
  started_at: timestamp, finished_at: timestamp, elapsed_ms: duration, login_ms: duration,
  source_checks: z.array(z.object({ scope: z.string().max(40), checked: z.boolean().nullable(),
    checked_at: timestamp.nullable(), outcome: z.enum(["checked", "not_ready", "failed", "not_checked"]) })
    .refine(check => check.checked !== true || check.checked_at !== null)).max(10),
  stages: z.array(z.object({ stage: z.string().max(40), status: z.string().max(40), elapsed_ms: duration,
    garmin_fetch_ms: duration, r2_read_ms: duration, r2_write_ms: duration,
    r2_get_ms: duration.optional(), r2_head_ms: duration.optional(),
    r2_list_ms: duration.optional(), r2_inventory_ms: duration.optional(),
    activity_file_import_ms: duration, coach_input_ms: duration,
    garmin_connectapi_calls: duration, garmin_connectapi_errors: duration,
    r2_sdk_operations: z.record(z.string(), z.number().finite().nonnegative()),
  })).max(50),
});
export type PipelineDiagnostics = z.infer<typeof pipelineDiagnosticsSchema>;
export const activityRepairReportSchema = z.object({ schema_version: z.literal(1), kind: z.literal("activity-repair"),
  scope: z.string().regex(/^activity\/(latest|\d{4}-\d{2}-\d{2})\/(latest|\d{1,20})\/(new|known)$/),
  checked_at: timestamp, source_checked: z.literal(false), activity_id: z.string().regex(/^garmin-\d{1,20}$/),
  activity_date: z.iso.date(), status: z.enum(["ready", "coach_pending"]), coach_status: z.string().max(80) });
export const syncStatusSchema = z.object({
  kind: z.enum(["general", "activity", "night", "unknown"]),
  job_state: z.enum(["not_started", "accepted", "queued", "running", "completed", "failed", "unknown"]),
  job_conclusion: z.string().nullable(),
  job_start_confirmed: z.boolean().nullable().optional(),
  source_checked: z.boolean().nullable(), source_checked_at: timestamp.nullable(),
  current_job_source_checked: z.boolean().nullable().optional(),
  current_job_source_checked_at: timestamp.nullable().optional(),
  stored_source_checked_at: timestamp.nullable().optional(),
  data_state: z.enum(["ready", "partial", "source_pending", "stale", "unknown", "blocked", "error"]),
  complete: z.boolean().nullable(), fresh: z.boolean().nullable(), missing_components: z.array(z.string()),
  user_action_required: z.boolean(),
  polling_state: z.enum(["continue", "stopped", "complete"]),
  next_action: z.enum(["read_data", "check_later", "retry_later", "fix_configuration", "select_activity", "none"]),
});
export const latencySchema = z.object({
  github_queue_ms: duration, github_startup_ms: duration, pipeline_ms: duration,
  request_elapsed_ms: duration, request_to_ready_observed_ms: duration,
  garmin_fetch_ms: duration, r2_read_ms: duration, r2_write_ms: duration,
  activity_file_import_ms: duration, coach_input_ms: duration,
});
export type SyncStatus = z.infer<typeof syncStatusSchema>;
type Snapshot = { status: string; complete: boolean; source_fresh: boolean;
  source_checked_at: string | null; source_outcome?: string; missing_components: string[]; package: Record<string, unknown> };

export function syncStatus(options: { kind: SyncStatus["kind"]; run?: RefreshRun | null;
  accepted?: boolean; snapshot?: Snapshot; polling?: boolean; reason?: string;
  pipeline?: PipelineDiagnostics | null; sourceChecked?: boolean | null; sourceCheckedAt?: string | null }): SyncStatus {
  const { run, snapshot, reason, pipeline } = options;
  const checks = pipeline?.source_checks ?? [];
  const checked = options.sourceChecked !== undefined ? options.sourceChecked : (checks.length ? checks.every(c => c.checked === true) :
    snapshot?.source_checked_at ? true : null);
  // Keep the legacy stored-receipt fallback above. Current-job evidence must
  // come only from this run's report/diagnostics, including its own check time.
  const reportTime = timestamp.safeParse(options.sourceCheckedAt);
  const currentChecked = run ? options.sourceChecked !== undefined ? options.sourceChecked
    : checks.length && checks.every(c => c.checked === true) ? true
      : checks.some(c => c.checked === false) ? false : null : null;
  const currentCheckedAt = currentChecked === true ? reportTime.success ? reportTime.data
    : checks.filter(c => c.checked && c.checked_at).map(c => c.checked_at!)
      .sort((a, b) => Date.parse(a) - Date.parse(b)).at(-1) ?? null : null;
  const checkedAt = currentCheckedAt ?? checks.filter(c => c.checked && c.checked_at).map(c => c.checked_at!).sort().at(-1)
    ?? snapshot?.source_checked_at ?? null;
  const ready = snapshot?.complete && snapshot.source_fresh && snapshot.status === "ready";
  const selection = ["ambiguous_activity", "activity_date_unknown", "unsupported_sport"].includes(snapshot?.status ?? "");
  const coachBlocked = ["no_effective_profile", "endurance_data_unavailable"].includes(String(snapshot?.package.coach_status));
  const accessBlocked = ["dispatch_rejected", "configuration_error", "status_error"].includes(reason ?? "");
  const blocked = selection || coachBlocked || accessBlocked;
  const failed = run?.status === "completed" && run.conclusion !== "success";
  const sourcePending = checks.some(c => c.outcome === "not_ready") || snapshot?.source_outcome === "not_ready"
    || ["no_recent_activity", "no_new_activity", "expected_activity_missing"].includes(snapshot?.status ?? "");
  return {
    kind: options.kind,
    job_state: run ? run.status === "completed" ? failed ? "failed" : "completed"
      : run.status === "in_progress" ? "running" : "queued"
      : reason === "status_error" ? "unknown" : options.accepted ? "accepted" : "not_started",
    job_conclusion: run?.conclusion ?? null,
    job_start_confirmed: run ? true : reason === "status_error" ? null
      : options.accepted || reason === "dispatch_rejected" ? false : null,
    source_checked: checked, source_checked_at: checked === true ? checkedAt : null,
    current_job_source_checked: currentChecked, current_job_source_checked_at: currentCheckedAt,
    stored_source_checked_at: snapshot?.source_checked_at ?? null,
    data_state: blocked ? "blocked" : ready ? "ready" : failed || checks.some(c => c.outcome === "failed") ? "error" : sourcePending ? "source_pending"
      : snapshot ? snapshot.complete ? "stale" : Object.keys(snapshot.package).length ? "partial" : "unknown"
        : pipeline?.status === "failed" ? "error" : pipeline?.status === "partial" ? "partial" : "unknown",
    complete: snapshot?.complete ?? null, fresh: snapshot?.source_fresh ?? null,
    missing_components: snapshot?.missing_components ?? [], user_action_required: blocked,
    polling_state: options.polling ? "continue" : run?.status === "completed" || ready ? "complete" : "stopped",
    next_action: selection ? "select_activity" : coachBlocked || accessBlocked ? "fix_configuration"
      : ready ? "read_data" : options.polling || run && run.status !== "completed" || !run && options.accepted ? "check_later"
        : "retry_later",
  };
}

function between(end: string | number | null | undefined, start: string | number | null | undefined): number | null {
  const number = (value: string | number | null | undefined) => typeof value === "number" ? value
    : typeof value === "string" ? Date.parse(value) : NaN;
  const diff = number(end) - number(start);
  return Number.isFinite(diff) && diff >= 0 ? diff : null;
}
export function latency(run: RefreshRun | null, pipeline?: PipelineDiagnostics | null,
  requestedAt?: number, ready = false, now = Date.now()): z.infer<typeof latencySchema> {
  const sum = (key: "garmin_fetch_ms" | "r2_read_ms" | "r2_write_ms" | "activity_file_import_ms" | "coach_input_ms") => {
    const stages = pipeline?.stages ?? [];
    const values = stages.map(s => s[key]);
    // Component timings cover only invoked components; an uninvoked component is unknown.
    if (key === "activity_file_import_ms" || key === "coach_input_ms") {
      const measured = values.filter((v): v is number => v != null);
      return measured.length ? Math.round(measured.reduce((a, b) => a + b, 0)) : null;
    }
    return values.length && values.every(v => v != null) ? Math.round(values.reduce((a, b) => a! + b!, 0)!) : null;
  };
  const elapsed = between(now, requestedAt);
  return { github_queue_ms: between(run?.run_started_at, run?.created_at),
    github_startup_ms: between(pipeline?.started_at, run?.run_started_at), pipeline_ms: pipeline?.elapsed_ms ?? null,
    request_elapsed_ms: elapsed, request_to_ready_observed_ms: ready ? elapsed : null,
    garmin_fetch_ms: sum("garmin_fetch_ms"), r2_read_ms: sum("r2_read_ms"), r2_write_ms: sum("r2_write_ms"),
    activity_file_import_ms: sum("activity_file_import_ms"), coach_input_ms: sum("coach_input_ms") };
}
