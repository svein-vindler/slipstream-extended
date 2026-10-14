/** Outgoing errors are data projections, never provider text or regex-redacted strings. */
import { McpServer, type JSONRPCMessage, type Transport } from "@modelcontextprotocol/server";
import { ApplicationToolError, takeApplicationError } from "./application-errors";
import { PayloadTooLargeError } from "./security";
import { GitHubRequestError, WORKFLOW_RUNS_UNVERIFIED } from "./github";
import { syncStatus, latency } from "./sync-status";

export function errorCategory(error: unknown): string {
  if (error instanceof PayloadTooLargeError) return "payload_too_large";
  if (error instanceof GitHubRequestError) return "github_request_failed";
  return "operation_failed";
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
    const unverifiedDiscovery = source.message === WORKFLOW_RUNS_UNVERIFIED;
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
    return { ...message, result: safeToolResult(message.result, guidance) };
  }
  return message;
}

/** Supported public Transport seam covers execution AND SDK validation in JSON/SSE. */
export class PrivacyMcpServer extends McpServer {
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
