import { getDispatchRun, type RefreshConfig, type RefreshRun } from "./github";
import { dispatchReadEvidenceSchema, type DispatchClaim, type DispatchIdentity, type DispatchOperation,
  type DispatchReadEvidence } from "./dispatch-operations";
import { latestActivityReportSchema } from "./latest-activity-report";
import { refreshReportSchema } from "./refresh-report";
import { latestNightReportSchema, activityRepairReportSchema, pipelineDiagnosticsSchema,
  type PipelineDiagnostics } from "./sync-status";

type Json = Record<string, unknown>;
type Read = (keys: string[]) => Promise<Json>;
export type DispatchObservation = { operation: DispatchOperation; run: RefreshRun | null;
  report: Json; pipeline: PipelineDiagnostics | null; fresh: boolean };

/** One admitted claim owns every control read; callbacks never issue I/O. */
export class DispatchReader {
  constructor(private readonly env: Env, private readonly config: RefreshConfig,
    private readonly read: Read, private readonly now: () => number = Date.now,
    private readonly fetcher: typeof fetch = fetch) {}

  async observe(operation: DispatchOperation, intent: DispatchClaim["intent"], actorKey: string): Promise<DispatchObservation> {
    const coordinator = this.env.REFRESH_COORDINATOR.getByName("global");
    const identity: DispatchIdentity = { operation_id: operation.operation_id, owner_key: operation.owner_key,
      owner_token: operation.owner_token };
    const claimed = await coordinator.claimDispatchRead(identity, intent, actorKey, this.now());
    const result: DispatchObservation = { operation: claimed.operation ?? operation, run: null,
      report: {}, pipeline: null, fresh: false };
    if (!claimed.acquired || !claimed.claim || !claimed.operation) return result;
    let current = claimed.operation;
    let evidence: DispatchReadEvidence | null = null;
    let committed = false;
    let candidateRun: RefreshRun | null = null;
    const take = async (kind: "run" | "receipt" | "report" | "diagnostics") => {
      const permitted = await coordinator.takeDispatchRead(identity, claimed.claim!, current.revision, kind, this.now());
      if (!permitted) return false;
      current = permitted;
      return true;
    };
    try {
      let receiptVerified = false;
      if (await take("receipt")) {
        const receipt = await this.read([`refresh/requests/${current.operation_id}.json`]);
        const recorded = await coordinator.recordDispatchReceipt(identity, claimed.claim, current.revision, receipt, this.now());
        // Never rebase a stale or rejected read onto the returned current revision.
        if (recorded.receipt_verified && recorded.operation) {
          current = recorded.operation;
          receiptVerified = true;
        }
      }
      if (receiptVerified && current.candidate_run_id && await take("run")) {
        const observed = await getDispatchRun(this.config, current.candidate_run_id, this.fetcher);
        candidateRun = observed.run;
        if (observed.source && current.descriptor && current.startup_receipt
          && observed.source.source_sha === current.startup_receipt.source.source_sha) {
          const descriptor = current.descriptor;
          let covered = observed.run.status !== "completed";
          if (!covered) {
            const raw = await take("report") ? await this.read([`refresh/reports/${observed.run.id}.json`]) : {};
            const diagnostics = await take("diagnostics") ? pipelineDiagnosticsSchema.safeParse(
              await this.read([`refresh/diagnostics/v1/${observed.run.id}.json`])) : null;
            const pipeline = diagnostics?.success ? diagnostics.data : null;
            const parsed = descriptor.mode === "general" ? refreshReportSchema.safeParse(raw)
              : descriptor.mode === "night" ? latestNightReportSchema.safeParse(raw)
              : descriptor.repair_only ? activityRepairReportSchema.safeParse(raw) : latestActivityReportSchema.safeParse(raw);
            const report = parsed.success ? parsed.data as Json : null;
            const generalStages: Record<string, string> = { summary: "refresh_summaries", health: "recent_health",
              body: "recent_body", activity_files: "manual_activity", coach_input: "manual_activity" };
            const stages = new Set(pipeline?.stages.map(stage => stage.stage));
            covered = Boolean(descriptor.mode === "general"
              ? report !== null && pipeline?.mode === "general"
                && (!descriptor.include_granular || stages.has("manual_activity"))
                && descriptor.components.every(component => generalStages[component] && stages.has(generalStages[component]))
              : report && report.scope === descriptor.scope
              && (descriptor.mode !== "night" || report.wake_date === descriptor.date)
              && (!descriptor.repair_only || report.activity_date === descriptor.date
                && report.activity_id === `garmin-${descriptor.activity_id}`)
              && (descriptor.mode !== "activity" || descriptor.repair_only || report.expected_date === descriptor.date)
              && (!pipeline || pipeline.mode === descriptor.mode));
            if (covered) { result.report = report!; result.pipeline = pipeline; }
          }
          if (covered) {
            const checked = dispatchReadEvidenceSchema.safeParse({ run_id: observed.run.id, status: observed.run.status,
              conclusion: observed.run.conclusion, binding: { run_id: observed.run.id, ...observed.source,
                correlation_source: "request_receipt", coverage: descriptor } });
            if (checked.success) evidence = checked.data;
          }
        }
      }
    } catch {
      // Missing/failed GET evidence cannot become a direct POST rejection.
    } finally {
      const finished = await coordinator.finishDispatchRead(identity, claimed.claim, current.revision, evidence, this.now());
      result.operation = finished.operation ?? current;
      committed = finished.observation_committed;
    }
    result.fresh = committed
      && result.operation.verified_binding?.run_id === candidateRun?.id && !result.operation.evidence_conflict;
    result.run = result.fresh ? candidateRun : null;
    if (!result.fresh) { result.report = {}; result.pipeline = null; }
    return result;
  }
}
