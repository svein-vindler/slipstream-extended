/** Synthetic startup metadata for control-path tests; actual producer parity is tested separately. */
import type { DispatchOperation } from "../src/dispatch-operations";

export function syntheticStartupReceipt(operation: DispatchOperation, runId: number) {
  if (!operation.descriptor) throw new Error("Synthetic startup fixture needs an explicit descriptor.");
  const { repository, workflow, ref, ...request } = operation.descriptor;
  return { schema_version: 1, kind: "refresh-dispatch-receipt", request_id: operation.operation_id, run_id: runId,
    source: { repository, workflow_path: workflow.includes("/") ? workflow : ".github/workflows/" + workflow,
      event: "workflow_dispatch", ref: ref.startsWith("refs/heads/") ? ref : "refs/heads/" + ref, source_sha: "a".repeat(40) },
    request };
}
