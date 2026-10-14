/** Narrow GitHub Actions client used by the MCP refresh tools. */

const GITHUB_API_VERSION = "2026-03-10";

export interface RefreshConfig {
  repository: string;
  workflow: string;
  ref: string;
  token: string;
  cooldownMinutes: number;
}

export interface RefreshRun {
  id: number;
  status: string;
  conclusion: string | null;
  event: string | null;
  created_at: string;
  updated_at: string | null;
  html_url: string;
  run_started_at?: string | null;
}

export interface RefreshProgress {
  terminal: boolean;
  dataReady: boolean;
  shouldContinuePolling: boolean;
}

export interface PollRefreshOptions {
  runId?: number;
  excludeRunId?: number;
  maxPolls?: number;
  intervalMs?: number;
}

export type RefreshDecision =
  | { dispatch: true }
  | { dispatch: false; reason: "already_running" | "recent_success"; run: RefreshRun };

type Fetcher = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
type Sleeper = (delayMs: number) => Promise<void>;

function repositoryParts(repository: string): [string, string] {
  const parts = repository.split("/");
  if (parts.length !== 2 || parts.some((part) => !/^[A-Za-z0-9_.-]+$/.test(part))) {
    throw new Error("GITHUB_REPOSITORY must use the owner/repository format.");
  }
  return [parts[0], parts[1]];
}

function workflowPath(config: RefreshConfig): string {
  const [owner, repository] = repositoryParts(config.repository);
  return `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repository)}`
    + `/actions/workflows/${encodeURIComponent(config.workflow)}`;
}

function repositoryPath(config: RefreshConfig): string {
  const [owner, repository] = repositoryParts(config.repository);
  return `/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repository)}`;
}

function expectedWorkflowFile(config: RefreshConfig): string {
  const workflow = config.workflow.replaceAll("\\", "/");
  return workflow.includes("/") ? workflow : `.github/workflows/${workflow}`;
}

export function validateRefreshRunSource(config: RefreshConfig, value: unknown): void {
  const run = asObject(value);
  const rawPath = typeof run?.path === "string" ? run.path.split("@")[0] : "";
  if (rawPath !== expectedWorkflowFile(config)) {
    throw new Error("The requested run does not belong to the configured refresh workflow.");
  }
}

function headers(config: RefreshConfig): HeadersInit {
  return {
    Accept: "application/vnd.github+json",
    Authorization: `Bearer ${config.token}`,
    "Content-Type": "application/json",
    "User-Agent": "slipstream-mcp",
    "X-GitHub-Api-Version": GITHUB_API_VERSION,
  };
}

export type DispatchPostOutcome =
  | { kind: "ack_candidate"; run_id: number }
  | { kind: "post_rejected"; method: "POST"; phase: "dispatch"; status: number }
  | { kind: "unconfirmed"; reason: "transport" | "deadline" | "empty_ack" | "invalid_ack" | "other_http" };

export class GitHubRequestError extends Error {
  constructor(readonly status: number, readonly method: string, message: string) {
    super(message);
    this.name = "GitHubRequestError";
  }
}

export const WORKFLOW_RUNS_UNVERIFIED = "GitHub workflow-run discovery could not be verified.";

export class GitHubWorkflowRunsEnvelopeError extends Error {
  constructor() {
    super(WORKFLOW_RUNS_UNVERIFIED);
    this.name = "GitHubWorkflowRunsEnvelopeError";
  }
}

async function githubJson(
  config: RefreshConfig,
  path: string,
  init: RequestInit,
  fetcher: Fetcher,
): Promise<unknown> {
  const response = await fetcher(`https://api.github.com${path}`, {
    ...init,
    headers: { ...headers(config), ...init.headers },
  });
  if (!response.ok) {
    throw new GitHubRequestError(response.status, init.method ?? "GET",
      `GitHub Actions request failed with HTTP ${response.status}.`);
  }
  if (response.status === 204) return null;
  return response.json() as Promise<unknown>;
}

function asObject(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

export function normalizeRefreshRun(value: unknown): RefreshRun {
  const run = asObject(value);
  if (!run || !Number.isSafeInteger(run.id) || typeof run.status !== "string"
      || typeof run.created_at !== "string" || typeof run.html_url !== "string") {
    throw new Error("GitHub returned an invalid workflow-run response.");
  }
  return {
    id: run.id as number,
    status: run.status,
    conclusion: typeof run.conclusion === "string" ? run.conclusion : null,
    event: typeof run.event === "string" ? run.event : null,
    created_at: run.created_at,
    updated_at: typeof run.updated_at === "string" ? run.updated_at : null,
    html_url: run.html_url,
    ...(typeof run.run_started_at === "string" ? { run_started_at: run.run_started_at } : {}),
  };
}

export function refreshDecision(
  run: RefreshRun | null,
  nowMs: number,
  cooldownMinutes: number,
): RefreshDecision {
  if (!run) return { dispatch: true };
  // Scheduled refreshes do not import the specific workout and coach input
  // requested by an MCP user. GitHub's shared concurrency group will queue
  // this on-demand run behind an active scheduled run.
  if (run.event === "schedule") return { dispatch: true };
  if (run.status !== "completed") {
    return { dispatch: false, reason: "already_running", run };
  }
  const createdAt = Date.parse(run.created_at);
  const withinCooldown = Number.isFinite(createdAt)
    && nowMs - createdAt < Math.max(1, cooldownMinutes) * 60_000;
  if (run.conclusion === "success" && withinCooldown) {
    return { dispatch: false, reason: "recent_success", run };
  }
  return { dispatch: true };
}

export function refreshProgress(run: RefreshRun | null): RefreshProgress {
  const terminal = run?.status === "completed";
  return {
    terminal,
    dataReady: terminal && run?.conclusion === "success",
    shouldContinuePolling: !terminal,
  };
}

export async function latestRefreshRun(
  config: RefreshConfig,
  fetcher: Fetcher = fetch,
): Promise<RefreshRun | null> {
  const query = new URLSearchParams({ branch: config.ref, per_page: "1" });
  const payload = asObject(await githubJson(
    config,
    `${workflowPath(config)}/runs?${query.toString()}`,
    { method: "GET" },
    fetcher,
  ));
  const runs = payload?.workflow_runs;
  if (!Array.isArray(runs)) throw new GitHubWorkflowRunsEnvelopeError();
  return runs.length ? normalizeRefreshRun(runs[0]) : null;
}

export async function getRefreshRun(
  config: RefreshConfig,
  runId: number,
  fetcher: Fetcher = fetch,
): Promise<RefreshRun> {
  if (!Number.isSafeInteger(runId) || runId <= 0) {
    throw new Error("Refresh run ID must be a positive integer.");
  }
  const payload = await githubJson(
    config,
    `${repositoryPath(config)}/actions/runs/${runId}`,
    { method: "GET" },
    fetcher,
  );
  validateRefreshRunSource(config, payload);
  return normalizeRefreshRun(payload);
}

export type DispatchRunSource = { repository: string; workflow: string; ref: string; source_sha: string };

export async function getDispatchRun(config: RefreshConfig, runId: number, fetcher: Fetcher = fetch):
  Promise<{ run: RefreshRun; source: DispatchRunSource | null }> {
  if (!Number.isSafeInteger(runId) || runId <= 0) throw new Error("Invalid dispatch run ID.");
  const payload = asObject(await githubJson(config, repositoryPath(config) + "/actions/runs/" + runId,
    { method: "GET" }, fetcher));
  if (payload?.id !== runId) throw new Error("Dispatch run identity could not be verified.");
  validateRefreshRunSource(config, payload);
  const run = normalizeRefreshRun(payload);
  const branch = config.ref.startsWith("refs/heads/") ? config.ref.slice(11) : config.ref;
  const sha = payload?.head_sha;
  const matches = asObject(payload?.repository)?.full_name === config.repository
    && asObject(payload?.head_repository)?.full_name === config.repository
    && payload?.event === "workflow_dispatch" && payload.head_branch === branch
    && typeof sha === "string" && /^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(sha);
  return { run, source: matches ? { repository: config.repository, workflow: config.workflow,
    ref: config.ref, source_sha: sha as string } : null };
}

export async function pollRefreshRun(
  config: RefreshConfig,
  options: PollRefreshOptions = {},
  fetcher: Fetcher = fetch,
  sleeper: Sleeper = (delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs)),
): Promise<RefreshRun | null> {
  const maxPolls = Math.max(1, Math.min(10, options.maxPolls ?? 5));
  const intervalMs = Math.max(0, Math.min(10_000, options.intervalMs ?? 4_000));
  let run: RefreshRun | null = null;

  for (let poll = 0; poll < maxPolls; poll += 1) {
    const candidate = options.runId
      ? await getRefreshRun(config, options.runId, fetcher)
      : await latestRefreshRun(config, fetcher);
    run = candidate?.id === options.excludeRunId ? null : candidate;
    if (run?.status === "completed" || poll === maxPolls - 1) return run;
    await sleeper(intervalMs);
  }
  return run;
}

export async function dispatchRefresh(
  config: RefreshConfig,
  fetcher: Fetcher = fetch,
  correlation?: { requestId: string; newActivityExpected: boolean },
): Promise<DispatchPostOutcome> {
  if (correlation) {
    validateRequestId(correlation.requestId);
    if (typeof correlation.newActivityExpected !== "boolean") throw new Error("Invalid activity intent.");
  }
  return dispatchWithInputs(config, { include_granular: true,
    ...(correlation ? { sync_request_id: correlation.requestId, latest_new_activity_expected: correlation.newActivityExpected } : {}),
  }, fetcher);
}

function validateRequestId(value: string): void {
  if (typeof value !== "string" || !/^[a-f0-9]{8}-[a-f0-9]{4}-[1-8][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(value))
    throw new Error("Invalid request correlation ID.");
}

export async function dispatchLatestActivity(
  config: RefreshConfig,
  options: { activityId?: string; newActivityExpected: boolean; expectedDate?: string;
    requestId?: string; repairOnly?: boolean },
  fetcher: Fetcher = fetch,
): Promise<DispatchPostOutcome> {
  if (options.requestId !== undefined) validateRequestId(options.requestId);
  if (options.activityId && !/^\d{1,20}$/.test(options.activityId)) {
    throw new Error("Activity ID must be a numeric Garmin ID.");
  }
  if (options.expectedDate && !/^\d{4}-\d{2}-\d{2}$/.test(options.expectedDate)) {
    throw new Error("Expected date must use YYYY-MM-DD.");
  }
  return dispatchWithInputs(config, {
    latest_activity_only: true,
    latest_new_activity_expected: options.newActivityExpected,
    ...(options.requestId ? { sync_request_id: options.requestId } : {}),
    ...(options.repairOnly ? { latest_r2_only: true } : {}),
    ...(options.expectedDate ? { latest_expected_date: options.expectedDate } : {}),
    ...(options.activityId ? { latest_activity_id: options.activityId } : {}),
  }, fetcher);
}

export async function dispatchLatestNight(
  config: RefreshConfig, wakeDate: string, requestId: string,
  fetcher: Fetcher = fetch,
): Promise<DispatchPostOutcome> {
  validateRequestId(requestId);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(wakeDate)) throw new Error("Invalid wake-date");
  return dispatchWithInputs(config, { latest_night_only: true,
    latest_wake_date: wakeDate, sync_request_id: requestId, latest_new_activity_expected: false }, fetcher);
}

async function dispatchWithInputs(
  config: RefreshConfig,
  inputs: Record<string, boolean | string>,
  fetcher: Fetcher,
): Promise<DispatchPostOutcome> {
  const path = workflowPath(config); // Validate configuration before dispatch.
  let response: Response;
  try {
    response = await fetcher("https://api.github.com" + path + "/dispatches", {
      method: "POST", headers: headers(config),
      body: JSON.stringify({ ref: config.ref, inputs, return_run_details: true }),
    });
  } catch {
    // Only an observed response can prove a direct rejection, never an exception.
    return { kind: "unconfirmed", reason: "transport" };
  }
  if (!response.ok) {
    return [400, 401, 403, 404, 422, 429].includes(response.status)
      ? { kind: "post_rejected", method: "POST", phase: "dispatch", status: response.status }
      : { kind: "unconfirmed", reason: "other_http" };
  }
  if (response.status === 204) return { kind: "unconfirmed", reason: "empty_ack" };
  let payload: unknown;
  try { payload = await response.json(); }
  catch { return { kind: "unconfirmed", reason: "invalid_ack" }; }
  const receipt = asObject(payload);
  const runId = receipt?.workflow_run_id;
  return Number.isSafeInteger(runId) && (runId as number) > 0
    ? { kind: "ack_candidate", run_id: runId as number }
    : { kind: "unconfirmed", reason: "invalid_ack" };
}
