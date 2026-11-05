/** Outgoing errors are data projections, never provider text or regex-redacted strings. */
import { McpServer, type JSONRPCMessage, type Transport } from "@modelcontextprotocol/server";
import { ApplicationToolError, takeApplicationError } from "./application-errors";
import { PayloadTooLargeError } from "./security";
import { GitHubRequestError, GitHubReadError, GitHubWorkflowRunsEnvelopeError, WORKFLOW_RUNS_UNVERIFIED, GITHUB_READ_UNVERIFIED } from "./github";
import { AccessConfigurationError } from "./access";
import { syncStatus, latency } from "./sync-status";

export function errorCategory(error: unknown): string {
  if (error instanceof PayloadTooLargeError) return "payload_too_large";
  if (error instanceof GitHubRequestError) return "github_request_failed";
  if (error instanceof GitHubReadError) return "github_read_failed";
  if (error instanceof GitHubWorkflowRunsEnvelopeError) return "github_response_invalid";
  if (error instanceof AccessConfigurationError) return "access_configuration";
  return "operation_failed";
}

const FAILURE_SITES = {
  mcp_auth: { component: "access", operation: "authenticate" },
  mcp_transport: { component: "mcp", operation: "transport" },
  mcp_request: { component: "mcp", operation: "request" },
  mcp_tool_callback: { component: "mcp", operation: "tool" },
  r2_activities: { component: "r2", operation: "read" },
  r2_health: { component: "r2", operation: "read" },
  refresh_report: { component: "refresh", operation: "report" },
  refresh_dispatch: { component: "refresh", operation: "dispatch" },
  refresh_status: { component: "refresh", operation: "status" },
  coach_profile_index: { component: "coach", operation: "profile_index" },
  coach_context_complete: { component: "coach", operation: "context_complete" },
} as const;

// Only fixed application names can become console operation values. Never use
// a request's tool name, arguments or an arbitrary future registration name.
const APPLICATION_TOOL_OPERATIONS = [
  "data_status", "list_activities", "activity_stats", "personal_bests", "list_sport_types", "search_activities",
  "strength_session", "endurance_session", "coach_profile", "add_coach_profile", "add_activity_context", "coach_input",
  "health_status", "daily_health", "health_trends", "body_composition", "weight_history",
  "sync_latest_activity", "sync_latest_night", "refresh_today", "refresh_status",
  "night_baseline", "night_signals_history", "night_signals", "night_context_history", "add_night_context",
  "night_training_history", "hrv_curve", "hrv_history", "sleep_detail", "sleep_history", "training_history",
] as const;

/** Console-only projection: callsites and typed facts, never exception text/keys/URLs. */
export function failureDiagnostic(error: unknown, site: keyof typeof FAILURE_SITES): Record<string, string | number> {
  const diagnostic: Record<string, string | number> = { error: errorCategory(error), ...FAILURE_SITES[site], site };
  if (error instanceof GitHubRequestError && Number.isInteger(error.status) && error.status >= 100 && error.status <= 599) {
    diagnostic.http_status = error.status;
    diagnostic.reason = error.status === 401 ? "authentication" : error.status === 403 ? "access_denied"
      : error.status === 404 ? "missing" : error.status === 429 ? "rate_limited"
      : error.status >= 500 ? "service_unavailable" : "request_failed";
  }
  if (error instanceof GitHubReadError) {
    switch (error.reason) {
      case "timeout": case "size": case "read": case "json": case "cleanup":
        diagnostic.reason = error.reason;
    }
  }
  return diagnostic;
}

export const TOOL_FAILURE = "The request could not be completed. Check arguments, access, configuration and service availability.";
const REFRESH_FAILURES = new Set([
  "Could not request the Garmin refresh. Check workflow configuration, access and service availability.",
  "Could not read Garmin refresh status. Check workflow configuration, access and service availability.",
]);

function safeToolResult(result: Record<string, unknown>, guidance?: ApplicationToolError): Record<string, unknown> {
  if (guidance) return guidance.toToolResult();
  // These structured errors are constructed by the two general refresh callbacks.
  // Preserve their existing status contract; SDK/provider errors have no structured result.
  const structured = result.structuredContent;
  if (structured && typeof structured === "object" && !Array.isArray(structured)) {
    const source = structured as Record<string, unknown>;
    const value: Record<string, unknown> = {};
    for (const key of ["accepted", "available", "terminal", "data_ready", "should_continue_polling"]) {
      if (typeof source[key] === "boolean") value[key] = source[key];
    }
    // Existing application status/latency are deliberately carried only by those callbacks.
    // Generic SDK errors do not supply these fields.
    const unverifiedDiscovery = source.message === WORKFLOW_RUNS_UNVERIFIED || source.message === GITHUB_READ_UNVERIFIED;
    if (unverifiedDiscovery || REFRESH_FAILURES.has(source.message as string)) {
      value.message = source.message;
      const status = syncStatus({
        kind: "accepted" in value ? "general" : "unknown",
        reason: unverifiedDiscovery ? undefined : "accepted" in value ? "configuration_error" : "status_error",
      });
      if (unverifiedDiscovery && !("accepted" in value)) status.job_state = "unknown";
      value.sync_status = status;
      value.latency = latency(null);
    } else {
      value.message = TOOL_FAILURE;
    }
    return { content: [{ type: "text", text: JSON.stringify(value, null, 2) }],
      structuredContent: value, isError: true };
  }
  return { content: [{ type: "text", text: TOOL_FAILURE }], isError: true };
}

export function safeMcpMessage(message: JSONRPCMessage, guidance?: ApplicationToolError): JSONRPCMessage {
  if ("error" in message) {
    return { jsonrpc: "2.0", id: message.id,
      error: { code: message.error.code, message: "The MCP request could not be completed." } };
  }
  if ("result" in message && message.result.isError === true) {
    const result = safeToolResult(message.result, guidance);
    // SDK encoding precedes this transport projection; modern clients require
    // this discriminator. Preserve only the known literal, never provider fields.
    if (message.result.resultType === "complete") result.resultType = "complete";
    return { ...message, result };
  }
  return message;
}

/** Supported public Transport seam covers execution AND SDK validation in JSON/SSE. */
export class PrivacyMcpServer extends McpServer {
  // Keep both public SDK registration overloads and forward callback arguments
  // exactly. The SDK catches callback exceptions before transport.onerror.
  override registerTool: McpServer["registerTool"] = (name: string, config: unknown, callback: (...args: never[]) => unknown) =>
    Reflect.apply(super.registerTool, this, [name, config, async (...args: unknown[]) => {
      try { return await Reflect.apply(callback, undefined, args); }
      catch (error) {
        console.error(JSON.stringify({ message: "MCP tool callback failed",
          ...failureDiagnostic(error, "mcp_tool_callback"),
          operation: APPLICATION_TOOL_OPERATIONS.find(operation => operation === name) ?? "tool",
        }));
        throw error;
      }
    }]);

  override async connect(transport: Transport): Promise<void> {
    const send = transport.send.bind(transport);
    transport.send = (message, options) => {
      const guidance = "id" in message && (typeof message.id === "string" || typeof message.id === "number")
        ? takeApplicationError(this, message.id) : undefined;
      return send(safeMcpMessage(message, guidance), options);
    };
    await super.connect(transport);
  }
}

/** SDK HTTP refusals happen before connect (e.g. Host/Origin validation). */
export async function safeMcpHttpError(response: Response): Promise<Response> {
  if (response.ok) return response;
  let body = "The MCP request could not be completed.";
  const headers = new Headers(response.headers);
  if (headers.get("content-type")?.includes("application/json")) {
    try {
      const message = await response.json() as JSONRPCMessage;
      if ("error" in message) body = JSON.stringify(safeMcpMessage(message));
      else body = JSON.stringify({ error: "The MCP request could not be completed." });
    } catch {
      body = JSON.stringify({ error: "The MCP request could not be completed." });
    }
  } else {
    await response.body?.cancel();
  }
  headers.delete("content-length");
  return new Response(body, { status: response.status, statusText: response.statusText, headers });
}
