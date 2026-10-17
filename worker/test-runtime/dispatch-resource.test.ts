import { env, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { FreshDataService, freshScope, record, type FreshRequest } from "../src/fresh-data";

const NOW = Date.parse("2026-10-05T12:00:00Z");
const REQUEST: FreshRequest = { kind: "night", date: "2026-10-05" };
const CONFIG = { repository: "owner/synthetic", workflow: "refresh.yml", ref: "main",
  token: "synthetic-only", cooldownMinutes: 30 };
const RUN = { id: 55, status: "in_progress", conclusion: null, event: "workflow_dispatch",
  path: ".github/workflows/refresh.yml", created_at: "2026-10-05T12:00:00Z", updated_at: null,
  html_url: "https://github.com/owner/synthetic/actions/runs/55" };
let coordinator: ReturnType<Env["REFRESH_COORDINATOR"]["getByName"]>;
let testEnv: Env;
let keys: string[];
let counts: { github_get: number; github_post: number; r2_get: number; r2_head: number; r2_list: number;
  r2_put: number; fixture_put: number; returned_r2_body_bytes: number };

beforeEach(() => {
  keys = []; counts = { github_get: 0, github_post: 0, r2_get: 0, r2_head: 0, r2_list: 0, r2_put: 0, fixture_put: 0, returned_r2_body_bytes: 0 };
  coordinator = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
    const method = Reflect.get(target, property);
    if (typeof method !== "function") return method;
    return async (...params: unknown[]) => {
      if (property === "get") counts.r2_get++;
      if (property === "head") counts.r2_head++;
      if (property === "list") counts.r2_list++;
      if (property === "put") counts.r2_put++;
      return Reflect.apply(method, target, params);
    };
  } });
  testEnv = { ...env, SLIPSTREAM_DATA: bucket, HEALTH_TIMEZONE: "Europe/Oslo", REFRESH_COORDINATOR: { getByName: () => coordinator } } as Env;
  vi.spyOn(globalThis, "fetch").mockImplementation(async () => { throw new Error("External network forbidden in C8 probes"); });
});
afterEach(async () => { vi.restoreAllMocks(); if (keys.length) await env.SLIPSTREAM_DATA.delete(keys); });

async function read(keys: string[]) {
  for (const key of keys) {
    const object = await testEnv.SLIPSTREAM_DATA.get(key);
    if (object) {
      const text = await object.text(); counts.returned_r2_body_bytes += new TextEncoder().encode(text).length;
      return { key, data: JSON.parse(text) };
    }
  }
  return null;
}
function service(fetcher: typeof fetch, now = NOW) {
  const observed: typeof fetch = async (input, init) => {
    if (init?.method === "POST") counts.github_post++; else counts.github_get++;
    return fetcher(input, init);
  };
  return new FreshDataService(testEnv, { getActivities: async () => [], getR2Json: read,
    getCoachProfiles: async () => ({ keys: [], profiles: [] }) }, CONFIG, () => now, observed, async () => {});
}
async function usage() {
  return runInDurableObject(coordinator, (_instance, state) => state.storage.sql.exec<{ used: number }>(
    "SELECT used FROM daily_budgets WHERE budget_key = ?", "sync-latest-night").toArray()[0]?.used ?? 0);
}
async function receipt(id: string) {
  const key = `refresh/requests/${id}.json`; keys.push(key); counts.fixture_put++;
  await env.SLIPSTREAM_DATA.put(key, JSON.stringify({ scope: freshScope(REQUEST), run_id: RUN.id }));
}
async function report(scenario: string, began: number, extra: Record<string, unknown>) {
  const sql = await runInDurableObject(coordinator, (_instance, state) => ({
    total_changes: state.storage.sql.exec<{ n: number }>("SELECT total_changes() AS n").toArray()[0].n,
    database_bytes: state.storage.sql.databaseSize,
    operation_storage: state.storage.sql.exec("SELECT count(*) AS rows, coalesce(sum(length(payload)),0) AS payload_bytes FROM dispatch_operations_v1").toArray()[0],
  }));
  const ceilings: Record<string, number> = { unknown_after_expiry: 38, late_receipt_active_then_completed: 23,
    known_post_rejected: 5, post_receipt_get_denied: 21 };
  expect(counts.r2_get).toBeLessThanOrEqual(ceilings[scenario]);
  expect([counts.r2_head, counts.r2_list, counts.r2_put]).toEqual([0, 0, 0]);
  expect(sql.operation_storage.rows).toBe(1);
  expect(Number(sql.operation_storage.payload_bytes)).toBeLessThan(4096);
  console.log("C8_RESOURCE " + JSON.stringify({ scenario, synthetic_only: true, counts, elapsed_ms: performance.now() - began,
    worker_billed_cpu_ms: null, actual_network_requests: 0, garmin_requests: 0, sql, ...extra }));
}

it("C8 frozen-sequence resource: unknown POST stays shared after expiry and never becomes safely-not-accepted", async () => {
  const began = performance.now();
  const unknown: typeof fetch = async () => { throw new Error("synthetic lost POST acknowledgement"); };
  const first = await service(unknown).request(REQUEST); const id = String(first.request_id);
  expect(first.reason).toBe("dispatch_unconfirmed");
  for (let index = 0; index < 3; index++) await service(unknown, NOW + index * 1000).status((await coordinator.freshJob(id))!);
  const late = NOW + 3 * 60 * 60_000;
  const blocked = await service(unknown, late).request(REQUEST);
  const direct = await coordinator.beginFresh(freshScope(REQUEST), "night", JSON.stringify(REQUEST), late);
  expect(blocked.request_id).toBe(id); expect(direct).toMatchObject({ acquired: false, reason: "shared_job" });
  expect(await coordinator.activeFreshJob(freshScope(REQUEST), late)).toMatchObject({ state: "active", run_id: null, polls: 3 });
  expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(0); expect(await usage()).toBe(1);
  await report("unknown_after_expiry", began, { native_job_state: "active", expires_before_now: true,
    shared_same_request: true, polling_stopped: blocked.should_continue_polling === false, daily_used: await usage() });
});

it("C8 frozen-sequence resource: late correlated receipt retains candidate without invented source proof or new POST", async () => {
  const began = performance.now(); let complete = false;
  const provider: typeof fetch = async (_input, init) => {
    if (init?.method === "POST") throw new Error("synthetic response lost after possible acceptance");
    return Response.json({ ...RUN, status: complete ? "completed" : "in_progress", conclusion: complete ? "success" : null });
  };
  const first = await service(provider).request(REQUEST); const id = String(first.request_id);
  await receipt(id);
  const late = NOW + 3 * 60 * 60_000;
  const active = await service(provider, late).status((await coordinator.freshJob(id))!);
  expect(active).toMatchObject({ terminal: false, should_continue_polling: false,
    sync_status: { job_state: "unknown", job_start_confirmed: null },
    dispatch_status: { state: "unconfirmed", candidate_present: false, duplicate_guard_held: true } });
  complete = true;
  const finished = await service(provider, late + 1000).status((await coordinator.freshJob(id))!);
  expect(finished.terminal).toBe(false); expect(finished.data_ready).toBe(false);
  expect(await coordinator.activeFreshJob(freshScope(REQUEST), late + 1000)).toMatchObject({ request_id: id });
  expect(await coordinator.freshJob(id)).toMatchObject({ state: "active", run_id: null, polls: 3 });
  expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(0); expect(await usage()).toBe(1);
  await report("late_receipt_active_then_completed", began, { recovered_same_request: true,
    known_active_before_completion: false, known_completed: false, missing_frozen_source_proof_held: true, data_ready: finished.data_ready, daily_used: await usage() });
});

it("C8 frozen-sequence resource: explicit POST rejection terminates but preserves cooldown and budget", async () => {
  const began = performance.now();
  const reject: typeof fetch = async () => new Response(null, { status: 422 });
  const first = await service(reject).request(REQUEST);
  expect(first).toMatchObject({ reason: "dispatch_rejected", accepted: false, terminal: true });
  expect(await coordinator.activeFreshJob(freshScope(REQUEST), NOW)).toBeNull();
  const early = await coordinator.beginFresh(freshScope(REQUEST), "night", JSON.stringify(REQUEST), NOW + 1000);
  expect(early).toMatchObject({ acquired: false, reason: "cooldown" });
  expect(await usage()).toBe(1); expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(0);
  await report("known_post_rejected", began, { safely_not_accepted: true, cooldown_retained: true, daily_used: await usage() });
});

it("C8 frozen-sequence resource: accepted ACK awaits absent startup without reaching the denied GET", async () => {
  const began = performance.now();
  const provider: typeof fetch = async (_input, init) => init?.method === "POST"
    ? Response.json({ workflow_run_id: RUN.id }) : new Response(null, { status: 403 });
  const first = await service(provider).request(REQUEST);
  expect(first).toMatchObject({ reason: "awaiting_startup_receipt", accepted: true, terminal: false,
    should_continue_polling: true, data_ready: false });
  const late = await service(provider, NOW + 3 * 60 * 60_000).request(REQUEST);
  expect(late.request_id).toBe(first.request_id); expect(record(late.sync_status).job_start_confirmed).toBeNull();
  expect(await coordinator.activeFreshJob(freshScope(REQUEST), NOW + 3 * 60 * 60_000)).toMatchObject({ run_id: RUN.id });
  expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(0); expect(await usage()).toBe(1);
  await report("post_receipt_get_denied", began, { post_receipt_seen: true, candidate_run_binding_lost: false, provider_denial_reached: false, missing_startup_proof: true,
    reservation_retained: true, daily_used: await usage() });
});
