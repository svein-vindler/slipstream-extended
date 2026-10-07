/** Offline documentation helpers. Tool names/contracts come only from tools/list. */
export interface ToolContract {
  name: string;
  title?: string;
  description?: string;
  inputSchema: Record<string, unknown>;
  outputSchema?: Record<string, unknown>;
  annotations?: { readOnlyHint?: boolean; openWorldHint?: boolean };
}
export interface CatalogMode { writes: boolean; refresh: boolean; tools: ToolContract[] }
export interface Example { tool: string; arguments: Record<string, unknown> }

export function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") return Object.fromEntries(
    Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
      .map(([key, item]) => [key, canonical(item)]),
  );
  return value;
}
export function requireTool(tools: ToolContract[], name: string): ToolContract {
  const tool = tools.find(item => item.name === name);
  if (!tool) throw new Error(`Unregistered MCP tool: ${name}`);
  return tool;
}
export function documentationExamples(markdown: string): Example[] {
  return [...markdown.matchAll(/```json\s*\n([\s\S]*?)\n```/g)].flatMap(match => {
    const parsed = JSON.parse(match[1]);
    const examples = Array.isArray(parsed) ? parsed : [parsed];
    for (const item of examples) {
      if (!item || typeof item.tool !== "string" || !item.arguments
        || typeof item.arguments !== "object" || Array.isArray(item.arguments)) {
        throw new Error("Expected a documentation tool/arguments example");
      }
    }
    return examples;
  });
}

/** Markdown parsing is only a consumer check; tools/list remains authoritative. */
export function checkDocumentationNames(markdown: string, tools: ToolContract[]): void {
  const known = new Set(tools.map(tool => tool.name));
  const visit = (value: unknown) => {
    if (Array.isArray(value)) { value.forEach(visit); return; }
    if (!value || typeof value !== "object") return;
    const schema = value as Record<string, unknown>;
    if (schema.properties && typeof schema.properties === "object") {
      Object.keys(schema.properties).forEach(name => known.add(name));
    }
    if (Array.isArray(schema.enum)) schema.enum.forEach(item => { if (typeof item === "string") known.add(item); });
    Object.values(schema).forEach(visit);
  };
  for (const tool of tools) { visit(tool.inputSchema); visit(tool.outputSchema); }
  const prose = markdown.replace(/```[\s\S]*?```/g, "");
  for (const match of prose.matchAll(/`([a-z][a-z0-9]*_[a-z0-9_]+)`/g)) {
    if (!known.has(match[1])) throw new Error(`Unknown documented MCP name/field: ${match[1]}`);
  }
}

export function renderCatalog(modes: CatalogMode[]): string {
  const complete = modes.find(mode => mode.writes && mode.refresh);
  if (!complete) throw new Error("Full registration mode is required");
  const byName = new Map<string, ToolContract>();
  for (const tool of complete.tools) {
    if (byName.has(tool.name)) throw new Error(`Duplicate MCP tool: ${tool.name}`);
    byName.set(tool.name, tool);
  }
  for (const mode of modes) for (const tool of mode.tools) {
    const full = requireTool(complete.tools, tool.name);
    if (JSON.stringify(canonical(full)) !== JSON.stringify(canonical(tool))) {
      throw new Error(`Contract differs between registration modes: ${tool.name}`);
    }
  }
  const entries = [...byName.keys()].sort().map(name => {
    const tool = byName.get(name)!;
    const availability = modes.filter(mode => mode.tools.some(item => item.name === name));
    const needsWrites = availability.every(mode => mode.writes);
    const needsRefresh = availability.every(mode => mode.refresh);
    const condition = [needsWrites ? "MCP_WRITES_ENABLED must be literal true" : null,
      needsRefresh ? "GITHUB_ACTIONS_TOKEN and GITHUB_REPOSITORY must both be configured" : null]
      .filter(Boolean).join("; ") || "All authenticated registration modes";
    const effect = tool.annotations?.readOnlyHint === true
      ? tool.annotations.openWorldHint
        ? "Status read; may contact GitHub and update coordination/poll bookkeeping; starts no Garmin job."
        : "Stored-data read; starts no Garmin job and writes no fitness data."
      : tool.annotations?.openWorldHint
        ? "Explicit refresh authorization required; may dispatch a bounded job, contact Garmin and write private R2."
        : "Explicit user confirmation required; append-only profile/context write to private R2; no Garmin account write.";
    return `### \`${name}\`\n\n${tool.description ?? tool.title ?? name}\n\n`
      + `**Effect:** ${effect}\n\n**Availability:** ${condition}.\n\n`
      + `**Registered input schema** (bounds/defaults; runtime rules above also apply):\n\n`
      + `\`\`\`json\n${JSON.stringify(canonical(tool.inputSchema), null, 2)}\n\`\`\`\n`;
  });
  return `# Authoritative MCP tool catalog\n\n`
    + `Generated from the authenticated Worker \`tools/list\` response, including domain modules. Do not edit tool entries by hand.\n\n`
    + `| Profile writes | Refresh configured | Registered tools |\n| --- | --- | --- |\n`
    + [...modes].sort((a, b) => Number(a.writes) - Number(b.writes) || Number(a.refresh) - Number(b.refresh))
      .map(mode => `| ${mode.writes ? "on" : "off"} | ${mode.refresh ? "yes" : "no"} | ${mode.tools.length} |`).join("\n")
    + `\n\nProfile writes and refresh availability are independent. Disabling profile writes does not disable sync. Every mode still requires authentication and existing server limits. Annotations describe intent; server validation, transport controls and budgets enforce safety. No tool allows Garmin fitness-account writes.\n\n`
    + `## Data limits and runtime rules\n\n`
    + `- Summary coverage follows stored indexes and retained data; there is no fixed 30-day activity-history limit. Status tools describe summary coverage, not detailed-package readiness. Null/missing values are unknown, never zero. Summary dates alone do not establish an activity's Garmin-local day.\n`
    + `- Detail tools return normalized, GPS-free datasets. There is no tool for arbitrary URLs/R2 keys or raw FIT/TCX download. Summary availability does not guarantee detailed streams or Coach Input. Planned workout steps differ from executed laps.\n`
    + `- Sleep/HRV history: inclusive range at most 366 days; auto is daily through 31 days and weekly thereafter. Explicit daily summary is at most 31 days; full readings/stages at most 7 days and require daily/auto, never weekly. Dates must be real calendar dates in ascending order. Weekly history verifies recent seven days directly; older index-only rows are not proof of complete raw detail. Report coverage, per-day status and index_consistency.\n`
    + `- Night dates are Garmin-local wake-dates; night_of is the local sleep-start date. Use returned local timestamps/provenance, matching sleep context for HRV and IANA/DST fallback. Missing context stays unknown; do not manually shift dates or apply a fixed offset. Garmin HRV summaries and Slipstream-derived statistics have different provenance.\n`
    + `- Weight history: inclusive range at most 31 days; same-day local window with morning_start < morning_end; explicit timezone must be IANA-valid. Default window is [04:00,12:00). Select the earliest actual local weighing in the window. Daily averages, daily_health.weight_kg and later weighings never replace a missing morning value. Report selected_days, per-day status and time provenance.\n`
    + `- Targeted fresh activity/night requests accept only the server's recent-date window (last seven days or today's Garmin-local day, with a UTC boundary allowance). Supply expected_date/activity_id or wake_date to pin the request, especially during travel. R2 is checked first; existing source can support derived-data repair, and compatible jobs share IDs. Use sync_status.data_state=ready, matching ID/date, source_checked_at and missing_components. A successful job or legacy data_ready alone is insufficient.\n`
    + `- Follow refresh_status with the same targeted request_id only while should_continue_polling is true, respecting poll_after_seconds and retry_after_seconds. Targeted jobs allow three short polling windows. Stop on exhaustion, cooldown, source-pending, blocked state or 429; honor Retry-After and never start a replacement job to extend polling.\n`
    + `- Profile zones must be ordered and non-overlapping (30–250 BPM); context needs at least one non-null user field. Values/RPE/thresholds are user supplied. Versions are immutable; changed zones do not rewrite historical analyses. Existing payload, concurrency and write budgets also apply.\n`
    + `- Diagnostics may be absent for a read-only result, and legacy timings may be null/omitted. This is not a failed sync and does not authorize another job. Inventory timing includes/overlaps LIST timing: do not sum them. No later automatic follow-up exists unless a mechanism is separately agreed.\n\n`
    + `## Updating and detecting drift\n\n`
    + `From worker, run \`npm run test:runtime -- test-runtime/tool-catalog.test.ts\`. Existing CI's \`npm test\` includes this check; no new workflow, schedule, permissions or live service access is needed. After an intentional contract change, regenerate with \`npm run test:runtime -- test-runtime/tool-catalog.test.ts -u\`, review this file and the existing contract digest snapshots, then rerun without -u. Never update snapshots merely to hide unexplained drift.\n\n`
    + `The test obtains real registrations in all four availability modes, checks incomplete refresh configuration, compares this deterministic file, and validates workflow JSON examples against the registered input schemas. Input-schema checks do not replace handler refinements; existing history/weight/runtime tests cover those bounds. Unknown/obsolete names fail. Workflow scenario tests are synthetic contracts, not evidence of AI-client acceptance.\n\n`
    + `See [AI workflows](AI_WORKFLOWS.md), [prompt ideas](PROMPTS.md), [fresh data](FRESH_DATA.md) and [history semantics](HEALTH_HISTORY.md).\n\n`
    + entries.join("\n");
}
