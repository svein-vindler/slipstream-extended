import { z } from "zod";
import type { DispatchOperation } from "./dispatch-operations";

export const dispatchStatusSchema = z.strictObject({ version: z.literal(1), operation_id: z.uuid(),
  state: z.enum(["unconfirmed", "candidate", "verified_active", "verified_terminal", "post_rejected"]),
  candidate_present: z.boolean(), evidence_conflict: z.boolean(), observed_at: z.iso.datetime().nullable(),
  polling_stopped: z.boolean(), duplicate_guard_held: z.boolean(), action_required: z.boolean(),
  next_action: z.enum(["inspect_dispatch", "none"]),
});

export function dispatchStatus(operation: DispatchOperation, pollingStopped = true): z.infer<typeof dispatchStatusSchema> {
  const held = operation.owner_released_at === null;
  const action = held && (pollingStopped || operation.evidence_conflict);
  return { version: 1, operation_id: operation.operation_id, state: operation.dispatch_state,
    candidate_present: operation.candidate_run_id !== null, evidence_conflict: operation.evidence_conflict,
    observed_at: operation.last_observation ? new Date(operation.last_observation.observed_at).toISOString() : null,
    polling_stopped: pollingStopped, duplicate_guard_held: held, action_required: action,
    next_action: action ? "inspect_dispatch" : "none" };
}
