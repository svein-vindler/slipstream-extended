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
    const requestId = response.headers.get("x-github-request-id");
    const suffix = requestId ? ` (request ${requestId})` : "";
    throw new Error(`GitHub Actions request failed with HTTP ${response.status}${suffix}.`);
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
  };
}

export function refreshDecision(
  run: RefreshRun | null,
  nowMs: number,
  cooldownMinutes: number,
): RefreshDecision {
  if (!run) return { dispatch: true };
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
  return Array.isArray(runs) && runs.length ? normalizeRefreshRun(runs[0]) : null;
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
): Promise<RefreshRun | null> {
  const payload = await githubJson(
    config,
    `${workflowPath(config)}/dispatches`,
    {
      method: "POST",
      body: JSON.stringify({
        ref: config.ref,
        inputs: { include_granular: true },
        return_run_details: true,
      }),
    },
    fetcher,
  );
  // With return_run_details GitHub returns a compact receipt rather than the
  // complete workflow-run object. Resolve it through the run endpoint so the
  // configured-workflow validation still applies before the ID is exposed.
  if (payload === null) return null;
  const receipt = asObject(payload);
  const runId = receipt?.workflow_run_id;
  if (!Number.isSafeInteger(runId) || (runId as number) <= 0) {
    throw new Error("GitHub returned an invalid workflow-dispatch response.");
  }
  return getRefreshRun(config, runId as number, fetcher);
}
