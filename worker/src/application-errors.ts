/** Controlled application guidance is minted before the SDK flattens exceptions. */
import type { McpServer, RequestId, ServerContext, CallToolResult } from "@modelcontextprotocol/server";

const MESSAGES = {
  context_in_progress: "Another context append for this activity is pending. This attempt was not saved. Read coach_input and wait for the earlier operation to finish before submitting this change again. An earlier write with an unknown outcome must not be repeated; ask the maintainer if it remains unresolved.",
  gct_declaration: "The side declaration must match one verified running session and its exact stored FIT hash. Read the stored activity and confirm the source before retrying.",
  profile_in_progress: "This immutable profile write is already reserved. Read coach_profile to check whether it completed; do not create another version to bypass the reservation.",
  profile_capacity: "Coach profile capacity of 100 has been reached. Existing immutable profiles remain available.",
  calendar_date: "Invalid calendar date. Use YYYY-MM-DD with a valid calendar day.",
  reversed_dates: "start_date cannot be after end_date.",
  history_limit: "Health history is limited to 366 days per request.",
  full_granularity: "Full detail requires daily granularity.",
  daily_history_limit: "Daily summaries are limited to 31 days; use weekly or auto granularity.",
  weight_history_limit: "Weight history is limited to 31 calendar days per request.",
  morning_format: "Morning window times must use HH:MM (24-hour local time).",
  morning_order: "morning_start must be earlier than morning_end on the same day.",
  timezone: "timezone must be a valid IANA timezone, such as Europe/Oslo.",
  full_history_limit: "Full HRV readings or sleep stages are limited to 7 days per request.",
} as const;
type ApplicationFailure = { kind: keyof typeof MESSAGES }
  | { kind: "write_cooldown" | "write_daily_limit"; retrySeconds: number };

function guidanceText(failure: ApplicationFailure): string {
  if (failure.kind === "write_cooldown" || failure.kind === "write_daily_limit") {
    if (!Number.isSafeInteger(failure.retrySeconds) || failure.retrySeconds < 1 || failure.retrySeconds > 86_400) {
      throw new Error("Invalid write retry delay.");
    }
    const message = failure.kind === "write_daily_limit"
      ? "The daily MCP write safety limit has been reached."
      : "A similar write was accepted recently.";
    return `${message} Retry in ${failure.retrySeconds} seconds.`;
  }
  return MESSAGES[failure.kind];
}

// SDK validation copies results, so carry the controlled type alongside it.
// Each server is created for one HTTP request; reuse only the existing MCP ID.
const controlledErrors = new WeakMap<McpServer, Map<RequestId, ApplicationToolError>>();

export class ApplicationToolError extends Error {
  readonly #text: string;
  constructor(failure: ApplicationFailure) {
    const text = guidanceText(failure);
    super(text);
    this.#text = text;
  }
  toToolResult(): CallToolResult {
    const text = this.#text;
    const content: CallToolResult["content"] = [{ type: "text", text }];
    return { content, isError: true };
  }
}

/** Wrap the application callback, before SDK catch/serialization destroys error types. */
export async function applicationToolResult<T extends CallToolResult>(
  server: McpServer, context: ServerContext, operation: () => T | Promise<T>,
): Promise<T | CallToolResult> {
  const id = context.mcpReq.id;
  controlledErrors.get(server)?.delete(id);
  try { return await operation(); }
  catch (error) {
    if (error instanceof ApplicationToolError) {
      if (!context.mcpReq.signal.aborted) {
        let pending = controlledErrors.get(server);
        if (!pending) controlledErrors.set(server, pending = new Map());
        pending.set(id, error);
      }
      return error.toToolResult();
    }
    throw error;
  }
}

export function takeApplicationError(server: McpServer, id: RequestId): ApplicationToolError | undefined {
  const pending = controlledErrors.get(server);
  const error = pending?.get(id);
  pending?.delete(id);
  return error;
}
