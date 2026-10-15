import { env } from "cloudflare:test";
import { afterEach, expect, it, vi } from "vitest";
import { getRefreshRun, GitHubReadError, GitHubRequestError } from "../src/github";
import { DispatchReader } from "../src/dispatch-reader";
import { R2Storage } from "../src/r2-storage";
import { syntheticStartupReceipt } from "../test-support/dispatch-receipt";

const CONFIG = { repository: "synthetic/slipstream", workflow: "refresh.yml", ref: "main", token: "synthetic", cooldownMinutes: 30 };
const T0 = Date.parse("2026-10-14T12:00:00Z"), CAP = 4_194_304;
const RUN = { id: 55, status: "in_progress", conclusion: null, event: "workflow_dispatch",
  path: ".github/workflows/refresh.yml", repository: { full_name: CONFIG.repository },
  head_repository: { full_name: CONFIG.repository }, head_branch: "main", head_sha: "a".repeat(40),
  created_at: new Date(T0).toISOString(), html_url: "https://github.com/synthetic/slipstream/actions/runs/55" };
const keys: string[] = [];
afterEach(async () => { vi.useRealTimers(); vi.restoreAllMocks(); if (keys.length) await env.SLIPSTREAM_DATA.delete(keys.splice(0)); });
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
}
const captured = <T>(promise: Promise<T>) => promise.then(value => ({ value, error: undefined }),
  (error: unknown) => ({ value: undefined, error }));
for (const size of [1_048_577, 2_097_183, CAP - 1, CAP]) {
  it(`accepts ${size} actual UTF-8 bytes despite false Content-Length`, async ({ task }) => {
    const prefix = new TextEncoder().encode(JSON.stringify({ ...RUN, extra: "ø" }));
    const bytes = new Uint8Array(size).fill(32); bytes.set(prefix);
    const response = new Response(bytes, { headers: { "content-length": "99999999" } });
    await expect(getRefreshRun(CONFIG, 55, async (_input, init) => {
      expect(init?.method).toBe("GET"); expect(init?.signal).toBeInstanceOf(AbortSignal); return response;
    })).resolves.toMatchObject({ id: 55 });
    expect(response.body!.locked).toBe(false);
    task.meta.d2 = { get: 1, post: 0, delivered_bytes: size, locked: false };
  });
}
it("cancels cap+1 before retaining the excess chunk or reading the tail", async ({ task }) => {
  let delivered = 0, canceled = 0;
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      const size = delivered < CAP ? Math.min(65536, CAP - delivered) : delivered === CAP ? 1 : 65536;
      delivered += size; controller.enqueue(new Uint8Array(size).fill(32));
    }, cancel() { canceled++; },
  }, { highWaterMark: 0 });
  const result = await captured(getRefreshRun(CONFIG, 55, async () => new Response(stream, { headers: { "content-length": "1" } })));
  expect(result.error).toMatchObject({ reason: "size" });
  expect({ delivered, canceled, locked: stream.locked }).toEqual({ delivered: CAP + 1, canceled: 1, locked: false });
  task.meta.d2 = { get: 1, post: 0, delivered_bytes: delivered, canceled, locked: stream.locked };
});
it("waits for aborted header I/O to settle instead of returning a raced result", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const aborted = deferred<void>(), release = deferred<void>(); let settled = false;
  const pending = captured(getRefreshRun(CONFIG, 55, async (_input, init) => {
    await new Promise<void>(resolve => init!.signal!.addEventListener("abort", () => { aborted.resolve(); resolve(); }, { once: true }));
    await release.promise; throw new Error("synthetic aborted transport");
  })).then(result => { settled = true; return result; });
  await vi.advanceTimersByTimeAsync(15_000); await aborted.promise;
  expect(settled).toBe(false); release.resolve();
  expect((await pending).error).toMatchObject({ reason: "timeout" });
  expect(vi.getTimerCount()).toBe(0);
});
it("uses one header+body timer and waits for pending read/cancel settlement", async ({ task }) => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const headers = deferred<Response>(), cleanup = deferred<void>(), cancelStarted = deferred<void>();
  let settled = false, canceled = 0;
  const stream = new ReadableStream<Uint8Array>({
    start(controller) { controller.enqueue(new TextEncoder().encode('{"id":55')); },
    cancel() { canceled++; cancelStarted.resolve(); return cleanup.promise; },
  });
  const pending = captured(getRefreshRun(CONFIG, 55, async () => headers.promise)).then(result => { settled = true; return result; });
  await vi.advanceTimersByTimeAsync(10_000); headers.resolve(new Response(stream));
  await vi.advanceTimersByTimeAsync(4_999); expect(settled).toBe(false); expect(canceled).toBe(0);
  await vi.advanceTimersByTimeAsync(1); await cancelStarted.promise;
  expect(settled).toBe(false); expect(stream.locked).toBe(true);
  cleanup.resolve(); expect((await pending).error).toMatchObject({ reason: "timeout" });
  expect(stream.locked).toBe(false); expect(vi.getTimerCount()).toBe(0);
  task.meta.d2 = { get: 1, post: 0, canceled, full_io_ms: 15_000, settled_before_release: false, locked: false };
});
it("preserves size failure when cancel rejects", async () => {
  const stream = new ReadableStream<Uint8Array>({ start(c) { c.enqueue(new Uint8Array(CAP + 1)); },
    cancel() { throw new Error("synthetic cancel failure"); } });
  expect((await captured(getRefreshRun(CONFIG, 55, async () => new Response(stream)))).error).toMatchObject({ reason: "size" });
  expect(stream.locked).toBe(false);
});
it("preserves read failure over rejected reader.closed", async () => {
  const stream = new ReadableStream<Uint8Array>({ start(c) { c.error(new Error("synthetic private read failure")); } });
  expect((await captured(getRefreshRun(CONFIG, 55, async () => new Response(stream)))).error).toMatchObject({ reason: "read" });
  expect(stream.locked).toBe(false);
});
it("keeps HTTP403 primary when body cancellation rejects", async () => {
  const stream = new ReadableStream<Uint8Array>({ cancel() { throw new Error("synthetic cleanup failure"); } });
  const result = await captured(getRefreshRun(CONFIG, 55, async () => new Response(stream, { status: 403 })));
  expect(result.error).toBeInstanceOf(GitHubRequestError); expect(result.error).toMatchObject({ status: 403, method: "GET" });
});
it("reports JSON failure after completed I/O with no timer or reader left", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const response = new Response("not JSON");
  const result = await captured(getRefreshRun(CONFIG, 55, async () => response));
  expect(result.error).toBeInstanceOf(GitHubReadError); expect(result.error).toMatchObject({ reason: "json" });
  expect(response.body!.locked).toBe(false); expect(vi.getTimerCount()).toBe(0);
});
it("does not claim success when lock cleanup fails", async () => {
  const response = Response.json(RUN), body = response.body!, nativeReader = body.getReader();
  const proxy = new Proxy(nativeReader, { get(target, field) {
    if (field === "releaseLock") return () => { target.releaseLock(); throw new Error("synthetic cleanup failure"); };
    const value = Reflect.get(target, field, target); return typeof value === "function" ? value.bind(target) : value;
  } });
  Object.defineProperty(body, "getReader", { value: () => proxy });
  expect((await captured(getRefreshRun(CONFIG, 55, async () => response))).error).toMatchObject({ reason: "cleanup" });
  expect(body.locked).toBe(false);
});
it("preserves a late positive owned observation without lease-age cutoff", async ({ task }) => {
  const coordinator = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const lease = await coordinator.reserve("refresh-today", T0, 120_000);
  const operation = (await coordinator.beginGeneralDispatch(lease.token!, {
    repository: CONFIG.repository, workflow: CONFIG.workflow, ref: CONFIG.ref, mode: "general", scope: null,
    date: null, activity_id: null, new_expected: false, repair_only: false, include_granular: true,
    components: ["summary", "health", "body", "activity_files", "coach_input"],
  }, T0))!;
  const key = "refresh/requests/" + operation.operation_id + ".json"; keys.push(key);
  await env.SLIPSTREAM_DATA.put(key, JSON.stringify(syntheticStartupReceipt(operation, RUN.id)));
  const entered = deferred<void>(), response = deferred<Response>();
  let now = T0 + 119_000, gets = 0;
  const storage = new R2Storage(env.SLIPSTREAM_DATA);
  const namespace = new Proxy(env.REFRESH_COORDINATOR, { get(target, field) {
    return field === "getByName" ? () => coordinator : Reflect.get(target, field);
  } });
  const reader = new DispatchReader({ ...env, REFRESH_COORDINATOR: namespace }, CONFIG, async paths => (await storage.getR2Json(paths))?.data ?? null,
    () => now, async () => { gets++; entered.resolve(); return response.promise; });
  const pending = reader.observe(operation, "initial", "synthetic-actor"); await entered.promise;
  await new Promise(resolve => setTimeout(resolve, 2000));
  now = T0 + 121_000; response.resolve(Response.json(RUN));
  const result = await pending;
  expect(result.fresh).toBe(true); expect(result.operation).toMatchObject({ dispatch_state: "verified_active", read_claim: null,
    owner_released_at: null, general_initial_reads_used: 1 });
  expect(gets).toBe(1);
  task.meta.d2 = { get: gets, post: 0, read_ms: 2000, owner_held: true, ticket_used: 1 };
});

it("holds the owned claim until timed-out fetch settles without refund or duplicate admission", async ({ task }) => {
  const coordinator = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const lease = await coordinator.reserve("refresh-today", T0, 120_000);
  const operation = (await coordinator.beginGeneralDispatch(lease.token!, {
    repository: CONFIG.repository, workflow: CONFIG.workflow, ref: CONFIG.ref, mode: "general", scope: null,
    date: null, activity_id: null, new_expected: false, repair_only: false, include_granular: true,
    components: ["summary", "health", "body", "activity_files", "coach_input"],
  }, T0))!;
  const key = "refresh/requests/" + operation.operation_id + ".json"; keys.push(key);
  await env.SLIPSTREAM_DATA.put(key, JSON.stringify(syntheticStartupReceipt(operation, RUN.id)));
  const namespace = new Proxy(env.REFRESH_COORDINATOR, { get(target, field) {
    return field === "getByName" ? () => coordinator : Reflect.get(target, field);
  } });
  let cutoff!: () => void, gets = 0, settled = false;
  const nativeSetTimeout = globalThis.setTimeout.bind(globalThis);
  vi.spyOn(globalThis, "setTimeout").mockImplementation((handler, delay, ...args) => {
    if (delay === 15_000) cutoff = () => handler(...args);
    return nativeSetTimeout(handler, delay, ...args);
  });
  const entered = deferred<void>(), cleanup = deferred<void>(), aborted = deferred<void>();
  const storage = new R2Storage(env.SLIPSTREAM_DATA);
  const reader = new DispatchReader({ ...env, REFRESH_COORDINATOR: namespace }, CONFIG,
    async paths => (await storage.getR2Json(paths))?.data ?? null, () => T0 + 119_000,
    async (_input, init) => {
      gets++; entered.resolve();
      await new Promise<void>(resolve => init!.signal!.addEventListener("abort", () => { aborted.resolve(); resolve(); }, { once: true }));
      await cleanup.promise; throw new Error("synthetic aborted fetch");
    });
  const pending = reader.observe(operation, "initial", "synthetic-actor").then(result => { settled = true; return result; });
  await entered.promise; expect(cutoff).toBeTypeOf("function"); cutoff(); await aborted.promise;
  expect(settled).toBe(false);
  const held = (await coordinator.dispatchOperation(operation.operation_id))!;
  expect(held.read_claim).not.toBeNull();
  const identity = { operation_id: operation.operation_id, owner_key: operation.owner_key, owner_token: operation.owner_token };
  expect((await coordinator.claimDispatchRead(identity, "initial", "second-actor", T0 + 121_000)).acquired).toBe(false);
  cleanup.resolve(); const result = await pending;
  expect(result.fresh).toBe(false); expect(result.run).toBeNull();
  expect(result.operation).toMatchObject({ dispatch_state: "candidate", read_claim: null, owner_released_at: null, general_initial_reads_used: 1 });
  expect(gets).toBe(1);
  task.meta.d2 = { get: gets, post: 0, timeout_callback_delay_ms: 15_000, cutoff_fired_synthetically: true,
    settled_before_cleanup: false, owner_held: true, ticket_used: 1, duplicate_admission: false };
});
