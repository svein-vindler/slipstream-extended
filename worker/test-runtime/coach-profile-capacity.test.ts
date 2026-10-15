import { env, createExecutionContext, waitOnExecutionContext, runInDurableObject } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";

const issuer = "https://synthetic-profile-capacity.cloudflareaccess.com";
const hostname = "profile-capacity.example";
const args = { name: "Synthetic new immutable profile", effective_from: "2026-01-01",
  zones: [{ label: "Synthetic zone", min_bpm: 90, max_bpm: 120 }], references: {} };
let privateKey: CryptoKey, jwk: Record<string, unknown>, testEnv: Env;
let coordinator: ReturnType<Env["REFRESH_COORDINATOR"]["getByName"]>;
let writes: string[];
let actor: string;
let beforeProfilePut: (() => Promise<void>) | undefined;
let failProfilePut: boolean;
let counts: { get: number; list: number; put: number; get_body_bytes: number; put_body_bytes: number };
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "capacity", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  const existing = await env.SLIPSTREAM_DATA.list({ prefix: "coach/", limit: 1000 });
  if (existing.objects.length) await env.SLIPSTREAM_DATA.delete(existing.objects.map(item => item.key));
  writes = []; actor = crypto.randomUUID(); beforeProfilePut = undefined;
  failProfilePut = false; counts = { get: 0, list: 0, put: 0, get_body_bytes: 0, put_body_bytes: 0 };
  coordinator = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const namespace = new Proxy(env.REFRESH_COORDINATOR, { get(target, property) {
    if (property === "getByName") return () => coordinator;
    return Reflect.get(target, property);
  } });
  const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
    const value = Reflect.get(target, property);
    if (typeof value !== "function") return value;
    return async (...params: unknown[]) => {
      if (property === "get") counts.get++;
      if (property === "list") counts.list++;
      if (property === "put") { counts.put++; counts.put_body_bytes += new TextEncoder().encode(String(params[1])).length; }
      if (property === "put") writes.push(String(params[0]));
      if (property === "put" && String(params[0]).startsWith("coach/profiles/v1/") && beforeProfilePut)
        await beforeProfilePut();
      if (property === "put" && String(params[0]).startsWith("coach/profiles/v1/") && failProfilePut)
        throw new Error("SYNTHETIC_UNCONFIRMED_PROFILE_PUT");
      const result = await Reflect.apply(value, target, params);
      if (property === "get" && result) counts.get_body_bytes += result.size;
      return result;
    };
  } });
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "capacity", MCP_HOSTNAME: hostname,
    MCP_WRITES_ENABLED: "true", SLIPSTREAM_DATA: bucket, REFRESH_COORDINATOR: namespace };
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    if (String(input) === issuer + "/cdn-cgi/access/certs") return Response.json({ keys: [jwk] });
    throw new Error("External network forbidden in synthetic profile capacity tests");
  });
});

it("reserves the final profile slot across actors while an earlier PUT is still in flight", async () => {
  await seed(99);
  let release!: () => void, entered!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  const started = new Promise<void>(resolve => { entered = resolve; });
  let first = true;
  beforeProfilePut = async () => { if (first) { first = false; entered(); await held; } };
  const left = call("add_coach_profile", args, crypto.randomUUID());
  await started;
  let right;
  try { right = await call("add_coach_profile", { ...args, name: "Synthetic concurrent second" }, crypto.randomUUID()); }
  finally { release(); }
  const original = await left;
  expect(original.structuredContent.saved).toBe(true);
  expect(right.isError).toBe(true);
  expect(writes.filter(key => key.startsWith("coach/profiles/v1/"))).toHaveLength(1);
  expect(await debits()).toBe(1);
  const read = await call("coach_profile", {});
  expect(read.isError).not.toBe(true); expect(read.structuredContent.profiles).toHaveLength(100);
});
afterEach(() => {
  console.log("C11_METRIC " + JSON.stringify({ case: expect.getState().currentTestName, ...counts,
    actual_provider_calls: 0, garmin_writes: 0, worker_billed_cpu_ms: null }));
  vi.restoreAllMocks();
});

async function seed(count: number) {
  const keys: string[] = [], profiles: Record<string, unknown>[] = [];
  for (let i = 0; i < count; i++) {
    const profile = { schema_version: 1, profile_id: `synthetic-${i}`, name: `Synthetic ${i}`,
      effective_from: "2020-01-01", sport: "running", source: "user", default_time_basis: "elapsed",
      zones: args.zones, references: {}, created_at: "2020-01-01T00:00:00Z" };
    const key = `coach/profiles/v1/2020-01-01/synthetic-${i}.json`;
    await env.SLIPSTREAM_DATA.put(key, JSON.stringify(profile));
    keys.push(key); profiles.push(profile);
  }
  await env.SLIPSTREAM_DATA.put("coach/indexes/profiles-v1.json", JSON.stringify({
    schema_version: 1, profile_keys: keys, profiles,
  }));
}
async function call(name: string, values: unknown, subject = actor, protocol = "2025-03-26") {
  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "capacity" }).setIssuer(issuer).setAudience("capacity")
    .setSubject(subject).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request("https://" + hostname + "/mcp", {
    method: "POST", headers: { host: hostname, "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token,
      "mcp-protocol-version": protocol,
      ...(protocol === "2026-07-28" ? { "mcp-method": "tools/call", "mcp-name": name } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: values,
      ...(protocol === "2026-07-28" ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol,
        [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }),
  }), testEnv, ctx);
  const body = await response.text(); await waitOnExecutionContext(ctx);
  const line = body.split("\n").find(item => item.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : body);
  return message.result ?? { rpcError: message.error };
}
async function debits() {
  return runInDurableObject(coordinator, (_instance, state) => state.storage.sql.exec<{ used: number }>(
    "SELECT used FROM daily_budgets").toArray().reduce((sum, item) => sum + item.used, 0));
}

it("rejects profile 101 before any PUT or accepted write debit and preserves reader access", async () => {
  await seed(100);
  const read = await call("coach_profile", {});
  expect(read.isError).not.toBe(true); expect(read.structuredContent.profiles).toHaveLength(100);
  const before = { ...counts };
  const result = await call("add_coach_profile", args);
  console.log("C11_WRITE_METRIC " + JSON.stringify({
    get: counts.get - before.get, list: counts.list - before.list, put: counts.put - before.put,
    get_body_bytes: counts.get_body_bytes - before.get_body_bytes,
    put_body_bytes: counts.put_body_bytes - before.put_body_bytes, accepted_write_debits: await debits(),
    saved: result.structuredContent?.saved === true, actual_provider_calls: 0,
  }));
  expect(result.isError).toBe(true);
  expect(writes).toEqual([]); expect(await debits()).toBe(0);
  const after = await call("coach_profile", {});
  expect(after.isError).not.toBe(true); expect(after.structuredContent.profiles).toHaveLength(100);
});

it.each(["2025-03-26", "2026-07-28"])("keeps 99/100 capacity, immutable reuse and rejection guidance with %s", async protocol => {
  await seed(99);
  const original = await call("coach_profile", {}, actor, protocol);
  expect(original.isError).not.toBe(true); expect(original.structuredContent.profiles).toHaveLength(99);
  const result = await call("add_coach_profile", args, actor, protocol);
  expect(result.structuredContent.saved).toBe(true);
  const body = await (await env.SLIPSTREAM_DATA.get(result.structuredContent.key))!.text();
  const written = [...writes];
  const repeat = await call("add_coach_profile", args, actor, protocol);
  expect(repeat.structuredContent.profile).toEqual(result.structuredContent.profile);
  expect(writes).toEqual(written); expect(await debits()).toBe(1);
  const rejected = await call("add_coach_profile", { ...args, name: "Synthetic changed profile version" }, actor, protocol);
  expect(rejected.isError).toBe(true); expect(JSON.stringify(rejected)).toContain("capacity of 100");
  expect(writes).toEqual(written); expect(await debits()).toBe(1);
  expect(await (await env.SLIPSTREAM_DATA.get(result.structuredContent.key))!.text()).toBe(body);
  const after = await call("coach_profile", {}, actor, protocol);
  expect(after.structuredContent.profiles).toHaveLength(100);
});

it("keeps legacy 101-profile storage fail-closed without deleting or charging a write", async () => {
  await seed(101);
  expect((await call("coach_profile", {})).isError).toBe(true);
  expect((await call("add_coach_profile", args)).isError).toBe(true);
  expect(writes).toEqual([]); expect(await debits()).toBe(0);
  expect((await env.SLIPSTREAM_DATA.list({ prefix: "coach/profiles/v1/", limit: 101 })).objects).toHaveLength(101);
});

it("does not expire a final-slot reservation while its delayed PUT can still arrive", async () => {
  await seed(99);
  let release!: () => void, entered!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  const started = new Promise<void>(resolve => { entered = resolve; });
  let first = true;
  beforeProfilePut = async () => { if (first) { first = false; entered(); await held; } };
  const left = call("add_coach_profile", args, crypto.randomUUID());
  await started;
  const now = Date.now(); vi.spyOn(Date, "now").mockReturnValue(now + 61_000);
  let right;
  try { right = await call("add_coach_profile", { ...args, name: "Synthetic after cooldown" }, crypto.randomUUID()); }
  finally { release(); }
  expect((await left).structuredContent.saved).toBe(true);
  expect(right.isError).toBe(true); expect(await debits()).toBe(1);
  expect(writes.filter(key => key.startsWith("coach/profiles/v1/"))).toHaveLength(1);
});

it("shares same-profile in-flight ownership and returns the stored bytes on later idempotent retry", async () => {
  await seed(99);
  let release!: () => void, entered!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  const started = new Promise<void>(resolve => { entered = resolve; });
  beforeProfilePut = async () => { entered(); await held; };
  const first = call("add_coach_profile", args, crypto.randomUUID());
  await started;
  let competing;
  try { competing = await call("add_coach_profile", args, crypto.randomUUID()); }
  finally { release(); }
  const saved = await first;
  expect(competing.isError).toBe(true); expect(JSON.stringify(competing)).toContain("already reserved");
  beforeProfilePut = undefined;
  const repeat = await call("add_coach_profile", args, crypto.randomUUID());
  expect(repeat.structuredContent.profile).toEqual(saved.structuredContent.profile);
  expect(writes.filter(key => key.startsWith("coach/profiles/v1/"))).toHaveLength(1);
  expect(await debits()).toBe(1);
});

it.each(["cooldown", "daily_limit"])("does not leak a capacity slot when the existing %s guard refuses", async reason => {
  await seed(98); testEnv.MCP_WRITE_DAILY_LIMIT = "1";
  expect((await call("add_coach_profile", args)).structuredContent.saved).toBe(true);
  if (reason === "daily_limit") { const now = Date.now(); vi.spyOn(Date, "now").mockReturnValue(now + 61_000); }
  const second = await call("add_coach_profile", { ...args, name: "Synthetic blocked profile" });
  expect(second.isError).toBe(true); expect(JSON.stringify(second)).toContain("Retry in");
  expect(await debits()).toBe(1);
  const third = await call("add_coach_profile", { ...args, name: "Synthetic other actor" }, crypto.randomUUID());
  expect(third.structuredContent.saved).toBe(true); expect(await debits()).toBe(2);
  expect((await call("coach_profile", {})).structuredContent.profiles).toHaveLength(100);
});

it("retains capacity after an unconfirmed PUT failure without a duplicate retry or another debit", async () => {
  await seed(99); failProfilePut = true;
  expect((await call("add_coach_profile", args)).isError).toBe(true);
  expect(await debits()).toBe(1);
  failProfilePut = false;
  const repeat = await call("add_coach_profile", args, crypto.randomUUID());
  expect(repeat.isError).toBe(true); expect(JSON.stringify(repeat)).toContain("already reserved");
  expect((await call("add_coach_profile", { ...args, name: "Synthetic cannot bypass hold" }, crypto.randomUUID())).isError).toBe(true);
  expect(await debits()).toBe(1);
  expect(writes.filter(key => key.startsWith("coach/profiles/v1/"))).toHaveLength(1);
  expect((await call("coach_profile", {})).structuredContent.profiles).toHaveLength(99);
});

it("keeps write-disabled registration and rejection before PUT or budget", async () => {
  await seed(99); testEnv.MCP_WRITES_ENABLED = "false";
  const result = await call("add_coach_profile", args);
  expect(result.rpcError).toBeDefined();
  expect(writes).toEqual([]); expect(await debits()).toBe(0);
});

it("admits two different actors into two remaining slots and keeps both profiles readable", async () => {
  await seed(98);
  let release!: () => void, entered!: () => void, count = 0;
  const held = new Promise<void>(resolve => { release = resolve; });
  const bothEntered = new Promise<void>(resolve => { entered = resolve; });
  beforeProfilePut = async () => { if (++count === 2) entered(); await held; };
  const left = call("add_coach_profile", args, crypto.randomUUID());
  const right = call("add_coach_profile", { ...args, name: "Synthetic second free slot" }, crypto.randomUUID());
  let admitted;
  try {
    admitted = await Promise.race([bothEntered.then(() => true), left.then(() => false), right.then(() => false)]);
  } finally { release(); }
  const results = await Promise.all([left, right]);
  expect(admitted).toBe(true); expect(results.every(result => result.structuredContent?.saved === true)).toBe(true);
  expect(await debits()).toBe(2);
  const read = await call("coach_profile", {});
  expect(read.isError).not.toBe(true); expect(read.structuredContent.profiles).toHaveLength(100);
});


it("rolls back slot and accepted budget together when native slot insertion fails", async () => {
  const profileKey = "coach/profiles/v1/2026-01-01/hr-" + "a".repeat(24) + ".json";
  const now = Date.UTC(2026, 9, 15, 12);
  const result = await runInDurableObject(coordinator, (instance, state) => {
    const sql = state.storage.sql;
    const nativeExec = sql.exec.bind(sql);
    // Inject failure only at the system storage boundary, after reserveBudgeted
    // has actually updated native SQLite. No production method is mocked.
    const injected = vi.spyOn(sql, "exec").mockImplementation((query, ...values) => {
      if (query.startsWith("INSERT INTO refresh_leases")
        && values[0] === "coach-profile-slot:" + profileKey) {
        throw new Error("SYNTHETIC_C11_SLOT_INSERT_FAILURE");
      }
      return nativeExec(query, ...values);
    });
    let thrown = "";
    try { instance.reserveCoachProfileWrite(profileKey, [], "rollback-actor", now, 1); }
    catch (error) { thrown = error instanceof Error ? error.message : "unexpected failure"; }
    finally { injected.mockRestore(); }
    return {
      thrown,
      used: state.storage.sql.exec<{ used: number }>("SELECT used FROM daily_budgets").toArray(),
      leases: state.storage.sql.exec<{ lease_key: string }>("SELECT lease_key FROM refresh_leases").toArray(),
    };
  });
  expect(result.thrown).toBe("SYNTHETIC_C11_SLOT_INSERT_FAILURE");
  expect(result.used).toEqual([]);
  expect(result.leases).toEqual([]);
  // Both the same actor's allowance and the same immutable key remain usable
  // after a confirmed local transaction abort; this is not refund after PUT.
  const accepted = await coordinator.reserveCoachProfileWrite(profileKey, [], "rollback-actor", now, 1);
  expect(accepted.acquired).toBe(true);
  expect(await debits()).toBe(1);
});

it("keeps unrelated lease namespaces separate from permanent profile slots", async () => {
  const now = Date.UTC(2026, 9, 15, 12);
  const legacy = Array.from({ length: 99 }, (_, i) => `coach/profiles/v1/2020-01-01/legacy-${i}.json`);
  const profileKey = "coach/profiles/v1/2026-01-01/hr-" + "b".repeat(24) + ".json";
  const unrelatedKey = "coach-profile-slotx:ordinary-lease";
  const unrelated = await coordinator.reserve(unrelatedKey, now, 60_000);
  expect(unrelated.acquired).toBe(true);
  const last = await coordinator.reserveCoachProfileWrite(profileKey, legacy, "namespace-actor", now, 2);
  expect(last.acquired).toBe(true);
  expect((await coordinator.release(unrelatedKey, unrelated.token!)).released).toBe(true);
  expect((await coordinator.reserve(unrelatedKey, now + 61_000, 60_000)).acquired).toBe(true);
  const held = await coordinator.reserveCoachProfileWrite(profileKey, legacy, "another-actor", now + 61_000, 2);
  expect(held).toMatchObject({ acquired: false, reason: "profile_in_progress" });
  const overflow = await coordinator.reserveCoachProfileWrite(
    "coach/profiles/v1/2026-01-01/hr-" + "c".repeat(24) + ".json", legacy, "third-actor", now + 61_000, 2,
  );
  expect(overflow).toMatchObject({ acquired: false, reason: "profile_capacity" });
  expect(await debits()).toBe(1);
});

it("retains an unconfirmed final slot across UTC rollover and ordinary budget cleanup", async () => {
  const now = Date.UTC(2026, 9, 15, 23, 59, 30);
  const legacy = Array.from({ length: 99 }, (_, i) => `coach/profiles/v1/2020-01-01/legacy-${i}.json`);
  const profileKey = "coach/profiles/v1/2026-01-01/hr-" + "d".repeat(24) + ".json";
  expect((await coordinator.reserveCoachProfileWrite(profileKey, legacy, "day-actor", now, 1)).acquired).toBe(true);
  const tomorrow = now + 90_000;
  expect(await coordinator.reserveCoachProfileWrite(profileKey, legacy, "day-actor", tomorrow, 1))
    .toMatchObject({ acquired: false, reason: "profile_in_progress" });
  expect(await coordinator.reserveCoachProfileWrite(
    "coach/profiles/v1/2026-01-01/hr-" + "e".repeat(24) + ".json", legacy, "fresh-actor", tomorrow, 1,
  )).toMatchObject({ acquired: false, reason: "profile_capacity" });
  // Existing ordinary daily-budget cleanup may remove old accounting rows;
  // it must not reclaim a slot whose unknown PUT can still arrive.
  expect((await coordinator.reserveBudgeted("context-day", "context-day", now + 3 * 86_400_000, 60_000, 1)).acquired).toBe(true);
  expect(await coordinator.reserveCoachProfileWrite(profileKey, legacy, "new-day-actor", now + 3 * 86_400_000, 1))
    .toMatchObject({ acquired: false, reason: "profile_in_progress" });
  const slots = await runInDurableObject(coordinator, (_instance, state) => state.storage.sql.exec<{ token: string }>(
    "SELECT token FROM refresh_leases WHERE lease_key GLOB ?", "coach-profile-slot:*",
  ).toArray());
  expect(slots).toEqual([{ token: profileKey }]);
});
