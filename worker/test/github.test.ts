import { describe, expect, it } from "vitest";
import {
  RefreshConfig, dispatchRefresh, getRefreshRun, latestRefreshRun, normalizeRefreshRun,
  pollRefreshRun, refreshDecision, refreshProgress,
} from "../src/github";

const CONFIG: RefreshConfig = {
  repository: "owner/slipstream",
  workflow: "refresh.yml",
  ref: "main",
  token: "test-token",
  cooldownMinutes: 30,
};

const RUN = {
  id: 123,
  status: "completed",
  conclusion: "success",
  event: "workflow_dispatch",
  created_at: "2026-09-21T12:00:00Z",
  updated_at: "2026-09-21T12:01:00Z",
  html_url: "https://github.com/owner/slipstream/actions/runs/123",
};

describe("GitHub refresh helpers", () => {
  it("normalizes only the safe workflow-run fields", () => {
    expect(normalizeRefreshRun({ ...RUN, token: "must-not-leak" })).toEqual(RUN);
  });

  it("suppresses an active or recently successful refresh", () => {
    expect(refreshDecision({ ...RUN, status: "in_progress", conclusion: null }, Date.now(), 30))
      .toMatchObject({ dispatch: false, reason: "already_running" });
    expect(refreshDecision(RUN, Date.parse("2026-09-21T12:20:00Z"), 30))
      .toMatchObject({ dispatch: false, reason: "recent_success" });
    expect(refreshDecision(RUN, Date.parse("2026-09-21T12:31:00Z"), 30))
      .toEqual({ dispatch: true });
  });

  it("reads the latest run for the fixed workflow and branch", async () => {
    const fetcher = async (input: RequestInfo | URL, init?: RequestInit) => {
      expect(String(input)).toContain("/actions/workflows/refresh.yml/runs?branch=main&per_page=1");
      expect(init?.headers).toMatchObject({ Authorization: "Bearer test-token" });
      return Response.json({ workflow_runs: [RUN] });
    };
    await expect(latestRefreshRun(CONFIG, fetcher)).resolves.toEqual(RUN);
  });

  it("reads one specific workflow run by ID", async () => {
    const fetcher = async (input: RequestInfo | URL, init?: RequestInit) => {
      expect(String(input)).toBe("https://api.github.com/repos/owner/slipstream/actions/runs/123");
      expect(init?.method).toBe("GET");
      return Response.json({ ...RUN, path: ".github/workflows/refresh.yml" });
    };
    await expect(getRefreshRun(CONFIG, 123, fetcher)).resolves.toEqual(RUN);
  });

  it("rejects a run ID belonging to another workflow", async () => {
    const fetcher = async () => Response.json({
      ...RUN,
      path: ".github/workflows/destructive-admin-job.yml",
    });
    await expect(getRefreshRun(CONFIG, 123, fetcher)).rejects.toThrow(
      "does not belong to the configured refresh workflow",
    );
  });

  it("marks only a successful completed run as data-ready", () => {
    expect(refreshProgress(null)).toEqual({
      terminal: false,
      dataReady: false,
      shouldContinuePolling: true,
    });
    expect(refreshProgress({ ...RUN, status: "in_progress", conclusion: null })).toEqual({
      terminal: false,
      dataReady: false,
      shouldContinuePolling: true,
    });
    expect(refreshProgress(RUN)).toEqual({
      terminal: true,
      dataReady: true,
      shouldContinuePolling: false,
    });
    expect(refreshProgress({ ...RUN, conclusion: "failure" })).toEqual({
      terminal: true,
      dataReady: false,
      shouldContinuePolling: false,
    });
  });

  it("polls past the previous run until the dispatched run is complete", async () => {
    const responses = [
      { workflow_runs: [RUN] },
      { workflow_runs: [{ ...RUN, id: 124, status: "in_progress", conclusion: null }] },
      { workflow_runs: [{ ...RUN, id: 124 }] },
    ];
    let fetchIndex = 0;
    let sleeps = 0;
    const fetcher = async () => Response.json(responses[fetchIndex++]);
    const sleeper = async () => { sleeps += 1; };

    await expect(pollRefreshRun(CONFIG, {
      excludeRunId: 123,
      maxPolls: 3,
      intervalMs: 1,
    }, fetcher, sleeper)).resolves.toMatchObject({ id: 124, status: "completed" });
    expect(sleeps).toBe(2);
  });

  it("polls a known run until it reaches a terminal status", async () => {
    let fetchIndex = 0;
    const runs = [
      { ...RUN, path: ".github/workflows/refresh.yml", status: "queued", conclusion: null },
      { ...RUN, path: ".github/workflows/refresh.yml", status: "in_progress", conclusion: null },
      { ...RUN, path: ".github/workflows/refresh.yml" },
    ];
    const fetcher = async () => Response.json(runs[fetchIndex++]);

    await expect(pollRefreshRun(CONFIG, {
      runId: 123,
      maxPolls: 3,
      intervalMs: 1,
    }, fetcher, async () => {})).resolves.toEqual(RUN);
  });

  it("dispatches only the configured refresh with granular recent data", async () => {
    let request = 0;
    const fetcher = async (input: RequestInfo | URL, init?: RequestInit) => {
      request += 1;
      if (request === 1) {
        expect(String(input)).toBe("https://api.github.com/repos/owner/slipstream/actions/workflows/refresh.yml/dispatches");
        expect(init?.method).toBe("POST");
        expect(JSON.parse(String(init?.body))).toEqual({
          ref: "main",
          inputs: { include_granular: true },
          return_run_details: true,
        });
        return Response.json({
          workflow_run_id: RUN.id,
          run_url: `https://api.github.com/repos/owner/slipstream/actions/runs/${RUN.id}`,
          html_url: RUN.html_url,
        });
      }
      expect(String(input)).toBe(`https://api.github.com/repos/owner/slipstream/actions/runs/${RUN.id}`);
      expect(init?.method).toBe("GET");
      return Response.json({ ...RUN, path: ".github/workflows/refresh.yml" });
    };
    await expect(dispatchRefresh(CONFIG, fetcher)).resolves.toEqual(RUN);
  });

  it("rejects a malformed workflow-dispatch receipt", async () => {
    const fetcher = async () => Response.json({ id: RUN.id });
    await expect(dispatchRefresh(CONFIG, fetcher))
      .rejects.toThrow("invalid workflow-dispatch response");
  });

  it("accepts GitHub's empty successful dispatch response", async () => {
    const fetcher = async () => new Response(null, { status: 204 });
    await expect(dispatchRefresh(CONFIG, fetcher)).resolves.toBeNull();
  });

  it("rejects malformed repository configuration", async () => {
    await expect(latestRefreshRun({ ...CONFIG, repository: "too/many/parts" }, fetch))
      .rejects.toThrow("owner/repository");
  });
});
