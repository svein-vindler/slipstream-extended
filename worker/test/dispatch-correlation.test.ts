import { expect, it } from "vitest";
import { dispatchRefresh, dispatchLatestNight } from "../src/github";
const config = { repository: "synthetic/slipstream", workflow: "refresh.yml", ref: "main",
  token: "synthetic-offline-token", cooldownMinutes: 30 };
const id = "12345678-1234-4234-8234-123456789012";
it("passes the original general operation UUID and new-activity intent through existing workflow inputs", async () => {
  let captured: unknown;
  const fetcher: typeof fetch = async (_url, init) => { captured = JSON.parse(String(init?.body)); return new Response(null, { status: 204 }); };
  await dispatchRefresh(config, fetcher, { requestId: id, newActivityExpected: false });
  expect(captured).toEqual({ ref: "main", return_run_details: true,
    inputs: { include_granular: true, sync_request_id: id, latest_new_activity_expected: false } });
});
it("rejects invalid general UUID before a provider call", async () => {
  let calls = 0;
  const fetcher: typeof fetch = async () => { calls++; return new Response(null, { status: 204 }); };
  await expect(dispatchRefresh(config, fetcher, { requestId: "------------------------------------", newActivityExpected: false })).rejects.toThrow("request");
  expect(calls).toBe(0);
});
it("sends an explicit non-activity intent for the fixed night mode", async () => {
  let captured: unknown;
  const fetcher: typeof fetch = async (_url, init) => { captured = JSON.parse(String(init?.body)); return new Response(null, { status: 204 }); };
  await dispatchLatestNight(config, "2026-10-14", id, fetcher);
  expect(captured).toEqual({ ref: "main", return_run_details: true,
    inputs: { latest_night_only: true, latest_wake_date: "2026-10-14", sync_request_id: id, latest_new_activity_expected: false } });
});
