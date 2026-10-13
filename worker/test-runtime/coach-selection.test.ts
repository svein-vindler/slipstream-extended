import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";
import { clearSummaryCaches } from "../src/r2-storage";
import { FreshDataService } from "../src/fresh-data";
import { R2Storage } from "../src/r2-storage";
import { ActivityCoachTools } from "../src/activity-coach-tools";
import fixtures from "./generated/coach-versions.json";

const issuer = "https://coach-selection-synthetic.cloudflareaccess.com";
const hostname = "coach-selection.example";
let privateKey: CryptoKey;
let pointerFailure = false, legacyTruncated = false, legacyClockAmbiguous = false;
let legacyUploadTimes: Map<string, Date | undefined> | null = null;
const reads: string[] = [];
const operations = { get: 0, head: 0, list: 0, put: 0, delete: 0 };
const bucket = new Proxy(env.SLIPSTREAM_DATA, {
  get(target, property) {
    const value = Reflect.get(target, property);
    if (typeof value !== "function") return value;
    return (...args: unknown[]) => {
      if (property in operations) operations[property as keyof typeof operations]++;
      if (property === "get") {
        reads.push(String(args[0]));
        if (pointerFailure && String(args[0]).endsWith("latest-ready.json")) throw new Error("synthetic provider text");
      }
      const result = Reflect.apply(value, target, args);
      if (property === "list" && legacyTruncated) return Promise.resolve(result).then(listed => ({ ...listed, truncated: true }));
      if (property === "list" && legacyClockAmbiguous) return Promise.resolve(result).then(listed => ({ ...listed,
        objects: listed.objects.map((object: R2Object) => ({ ...object, uploaded: new Date(0) })) }));
      if (property === "get" && legacyClockAmbiguous) return Promise.resolve(result).then(object => object && new Proxy(object, {
        get(target, field) {
          if (field === "uploaded") return new Date(0);
          const item = Reflect.get(target, field, target);
          return typeof item === "function" ? item.bind(target) : item;
        },
      }));
      if (property === "list" && legacyUploadTimes) return Promise.resolve(result).then(listed => ({ ...listed,
        objects: listed.objects.map((object: R2Object) => legacyUploadTimes!.has(object.key)
          ? { ...object, uploaded: legacyUploadTimes!.get(object.key) } : object) }));
      if (property === "get" && legacyUploadTimes?.has(String(args[0]))) return Promise.resolve(result).then(object => object && new Proxy(object, {
        get(target, field) {
          if (field === "uploaded") return legacyUploadTimes!.get(String(args[0]));
          const item = Reflect.get(target, field, target);
          return typeof item === "function" ? item.bind(target) : item;
        },
      }));
      return result;
    };
  },
});
function resetCounts() { reads.length = 0; Object.keys(operations).forEach(key => { operations[key as keyof typeof operations] = 0; }); }
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  const jwk = { ...await exportJWK(pair.publicKey), kid: "coach-selection", alg: "RS256", use: "sig" };
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== issuer + "/cdn-cgi/access/certs") throw new Error("External request forbidden in Coach Input contract");
    return Response.json({ keys: [jwk] });
  });
});
afterAll(() => vi.restoreAllMocks());

async function call(protocol: string, expectError = false) {
  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "coach-selection" }).setIssuer(issuer).setAudience("coach-selection")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  const modern = protocol === "2026-07-28";
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token,
      "mcp-protocol-version": protocol, ...(modern ? { "mcp-method": "tools/call", "mcp-name": "coach_input" } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: {
      name: "coach_input", arguments: { activity_id: "garmin-1" },
      ...(modern ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}),
    } }),
  }), { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "coach-selection", MCP_HOSTNAME: hostname,
    MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "", SLIPSTREAM_DATA: bucket,
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } }, ctx);
  const body = await response.text();
  await waitOnExecutionContext(ctx);
  expect(response.status, body).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  const line = body.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : body);
  expect(message.error, body).toBeUndefined();
  if (expectError) {
    expect(message.result.isError, body).toBe(true);
    expect(body).not.toContain("synthetic provider text");
    return message.result;
  }
  expect(message.result.isError, body).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  return message.result.structuredContent;
}

for (const mode of ["bulk", "targeted", "mixed"] as const) {
  for (const protocol of ["2025-03-26", "2026-07-28"]) {
    it(`reads current Python-generated A → B → A from ${mode} through ${protocol}`, async ({ annotate }) => {
      const existing = await env.SLIPSTREAM_DATA.list({ prefix: "activities/2026/1/" });
      if (existing.objects.length) await env.SLIPSTREAM_DATA.delete(existing.objects.map(value => value.key));
      const previous = new Map<string, string>();
      for (const step of fixtures[mode]) {
        for (const [key, encoded] of Object.entries(step.objects)) {
          if (previous.get(key) === encoded) continue;
          if (key.includes("/canonical/")) expect(previous.has(key)).toBe(false);
          await env.SLIPSTREAM_DATA.put(key, Uint8Array.from(atob(encoded), c => c.charCodeAt(0)));
          previous.set(key, encoded);
        }
        clearSummaryCaches(); resetCounts();
        const result = await call(protocol);
        expect(result.available).toBe(true);
        expect(result.analysis.analysis_id).toBe(step.expected_id);
        expect(result.analysis.summary.moving_seconds).toBe(step.moving_seconds);
        expect(operations.put + operations.delete).toBe(0);
        expect(operations.list).toBe(0);
        await annotate(JSON.stringify({ mode, protocol, moving: step.moving_seconds, ...operations, garmin: 0 }), "coach-selection-operations");
        // The same selected analysis is used by the R2-only freshness reader.
        const storage = new R2Storage(bucket);
        const tools = new ActivityCoachTools({ ...env, SLIPSTREAM_DATA: bucket }, "synthetic", storage);
        const fresh = new FreshDataService({ ...env, SLIPSTREAM_DATA: bucket }, {
          getActivities: () => storage.getActivities(),
          getR2Json: (keys, limits) => storage.getR2Json(keys, limits),
          getCoachProfiles: () => tools.getCoachProfiles(),
        }, { repository: "", workflow: "refresh.yml", ref: "main", token: "", cooldownMinutes: 30 },
        () => Date.parse("2026-09-30T12:00:00Z"));
        const snapshot = await fresh.snapshot({ kind: "activity", activityId: "1", date: "2026-09-30" });
        expect(snapshot.complete).toBe(true);
        expect((snapshot.package.coach_input as { analysis_id: string }).analysis_id).toBe(step.expected_id);
        expect(operations.put + operations.delete).toBe(0);
        for (const [key, encoded] of previous) {
          if (key.includes("/canonical/")) {
            const bytes = new Uint8Array(await (await env.SLIPSTREAM_DATA.get(key))!.arrayBuffer());
            expect(btoa(String.fromCharCode(...bytes))).toBe(encoded);
          }
        }
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      expect(fixtures[mode][0].expected_id).toBe(fixtures[mode][2].expected_id);
      expect([...previous.keys()].filter(key => key.includes("/canonical/"))).toHaveLength(2);
    });
  }
}

async function seedFinal(includePointer = true) {
  pointerFailure = false; legacyTruncated = false; legacyClockAmbiguous = false;
  const existing = await env.SLIPSTREAM_DATA.list({ prefix: "activities/2026/1/" });
  if (existing.objects.length) await env.SLIPSTREAM_DATA.delete(existing.objects.map(value => value.key));
  for (const [key, encoded] of Object.entries(fixtures.bulk[2].objects)) {
    if (!includePointer && key.endsWith("latest-ready.json")) continue;
    await env.SLIPSTREAM_DATA.put(key, Uint8Array.from(atob(encoded), c => c.charCodeAt(0)));
  }
  clearSummaryCaches(); resetCounts();
}
const pointerKey = "activities/2026/1/coach-input/v1/latest-ready.json";
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`preserves bounded legacy fallback only for an absent pointer through ${protocol}`, async () => {
    await seedFinal(false);
    // Legacy fixtures have one current object; no new selector existed then.
    const key = `activities/2026/1/coach-input/v1/canonical/${fixtures.bulk[1].expected_id}.json`;
    await env.SLIPSTREAM_DATA.delete(key);
    const result = await call(protocol);
    expect(result.available).toBe(true);
    expect(result.analysis.analysis_id).toBe(fixtures.bulk[0].expected_id);
    expect(operations.list).toBe(1);
    expect(operations.put + operations.delete).toBe(0);
  });
  it(`accepts scoped legacy targeted pointers through ${protocol}`, async () => {
    await seedFinal();
    const pointer = JSON.parse(await (await env.SLIPSTREAM_DATA.get(pointerKey))!.text());
    delete pointer.selection_version;
    await new Promise(resolve => setTimeout(resolve, 10));
    await env.SLIPSTREAM_DATA.put(pointerKey, JSON.stringify(pointer));
    resetCounts();
    const result = await call(protocol);
    expect(result.analysis.analysis_id).toBe(fixtures.bulk[2].expected_id);
    expect(operations.list).toBe(1);
    expect(operations.put + operations.delete).toBe(0);
  });
  for (const failure of ["foreign", "dangling", "schema", "malformed", "oversize", "provider", "truncated", "ambiguous"] as const) {
    it(`never selects unrelated/stale history after ${failure} selection failure through ${protocol}`, async () => {
      await seedFinal(failure !== "truncated");
      if (failure === "foreign" || failure === "dangling" || failure === "schema") {
        const pointer = JSON.parse(await (await env.SLIPSTREAM_DATA.get(pointerKey))!.text());
        if (failure === "foreign") pointer.analysis_key = "activities/2026/2/coach-input/v1/canonical/" + "a".repeat(24) + ".json";
        if (failure === "dangling") pointer.analysis_key = "activities/2026/1/coach-input/v1/canonical/" + "a".repeat(24) + ".json";
        if (failure === "schema") pointer.schema_version = 2;
        await env.SLIPSTREAM_DATA.put(pointerKey, JSON.stringify(pointer));
      }
      if (failure === "ambiguous") {
        const pointer = JSON.parse(await (await env.SLIPSTREAM_DATA.get(pointerKey))!.text());
        delete pointer.selection_version;
        await env.SLIPSTREAM_DATA.put(pointerKey, JSON.stringify(pointer));
        legacyClockAmbiguous = true;
      }
      if (failure === "malformed") await env.SLIPSTREAM_DATA.put(pointerKey, "{synthetic provider text");
      if (failure === "oversize") await env.SLIPSTREAM_DATA.put(pointerKey, "x".repeat(64 * 1024 + 1));
      pointerFailure = failure === "provider"; legacyTruncated = failure === "truncated";
      resetCounts();
      try {
        const isError = ["malformed", "oversize", "provider", "truncated"].includes(failure);
        const result = await call(protocol, isError);
        if (!isError) expect(result.available).toBe(false);
        expect(reads.some(key => key.startsWith("activities/2026/2/"))).toBe(false);
        expect(operations.put + operations.delete).toBe(0);
        if (failure !== "truncated" && failure !== "ambiguous") expect(operations.list).toBe(0);
      } finally { pointerFailure = false; legacyTruncated = false; legacyClockAmbiguous = false; }
    });
  }
}


for (const [failure, steps] of Object.entries(fixtures.recoveries)) {
  for (const protocol of ["2025-03-26", "2026-07-28"]) {
    it(`serves only the committed current selection across ${failure} and retry through ${protocol}`, async () => {
      pointerFailure = false; legacyTruncated = false; legacyClockAmbiguous = false;
      const existing = await env.SLIPSTREAM_DATA.list({ prefix: "activities/2026/1/" });
      if (existing.objects.length) await env.SLIPSTREAM_DATA.delete(existing.objects.map(value => value.key));
      const previous = new Map<string, string>();
      for (const step of steps) {
        for (const [key, encoded] of Object.entries(step.objects)) {
          if (previous.get(key) === encoded) continue;
          if (key.includes("/canonical/")) expect(previous.has(key)).toBe(false);
          await env.SLIPSTREAM_DATA.put(key, Uint8Array.from(atob(encoded), c => c.charCodeAt(0)));
          previous.set(key, encoded);
        }
        clearSummaryCaches(); resetCounts();
        const result = await call(protocol);
        expect(result.analysis.analysis_id).toBe(step.expected_id);
        expect(result.analysis.summary.moving_seconds).toBe(step.moving_seconds);
        expect(operations.list + operations.put + operations.delete).toBe(0);
      }
      expect([...previous.keys()].filter(key => key.includes("/canonical/"))).toHaveLength(2);
    });
  }
}


for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`preserves newer legacy bulk commits without trusting an older legacy pointer through ${protocol}`, async () => {
    await seedFinal();
    const pointer = JSON.parse(await (await env.SLIPSTREAM_DATA.get(pointerKey))!.text());
    pointer.analysis_key = `activities/2026/1/coach-input/v1/canonical/${fixtures.bulk[1].expected_id}.json`;
    delete pointer.selection_version;
    await env.SLIPSTREAM_DATA.put(pointerKey, JSON.stringify(pointer));
    await new Promise(resolve => setTimeout(resolve, 20));
    const aKey = `activities/2026/1/coach-input/v1/canonical/${fixtures.bulk[0].expected_id}.json`;
    // Simulate an older producer's already-completed bulk upsert; reader writes remain forbidden.
    const aBytes = await (await env.SLIPSTREAM_DATA.get(aKey))!.arrayBuffer();
    await env.SLIPSTREAM_DATA.put(aKey, aBytes);
    resetCounts();
    const result = await call(protocol);
    expect(result.analysis.analysis_id).toBe(fixtures.bulk[0].expected_id);
    expect(result.analysis.summary.moving_seconds).toBe(1800);
    expect(operations.put + operations.delete).toBe(0);
  });
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  for (const state of ["absent", "unknown-pointer-schema", "unknown-analysis-schema", "dangling"] as const) {
    it(`distinguishes ${state} from verified absence with safe guidance through ${protocol}`, async () => {
      await seedFinal(state !== "absent");
      if (state === "absent") {
        const stored = await env.SLIPSTREAM_DATA.list({ prefix: "activities/2026/1/coach-input/v1/canonical/" });
        await env.SLIPSTREAM_DATA.delete(stored.objects.map(object => object.key));
      } else {
        const pointer = JSON.parse(await (await env.SLIPSTREAM_DATA.get(pointerKey))!.text());
        if (state === "unknown-pointer-schema") {
          pointer.schema_version = 2;
          await env.SLIPSTREAM_DATA.put(pointerKey, JSON.stringify(pointer));
        } else if (state === "dangling") {
          pointer.analysis_key = "activities/2026/1/coach-input/v1/canonical/" + "a".repeat(24) + ".json";
          await env.SLIPSTREAM_DATA.put(pointerKey, JSON.stringify(pointer));
        } else {
          const analysis = (await new R2Storage(env.SLIPSTREAM_DATA).getR2Json([pointer.analysis_key]))!.data as Record<string, unknown>;
          analysis.schema_version = 2;
          await env.SLIPSTREAM_DATA.put(pointer.analysis_key, JSON.stringify(analysis));
        }
      }
      resetCounts();
      const result = await call(protocol);
      expect(result.available).toBe(false);
      expect(result.analysis).toBeUndefined();
      expect(result.message).toBe(state === "absent"
        ? "No coach input is stored yet. Add a coach profile and let the R2-only coach backfill run."
        : "The current coach input could not be verified. Retry this read later; if it persists, ask the maintainer to check stored analysis compatibility.");
      expect(operations.put + operations.delete).toBe(0);
    });
  }
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  for (const withOlderPointer of [false, true]) {
    for (const chronology of ["tied", "invalid", "missing", "ordered"] as const) {
      it(`requires verifiable unique legacy chronology (${chronology}, older pointer=${withOlderPointer}) through ${protocol}`, async ({ annotate }) => {
        await seedFinal(withOlderPointer);
        const prefix = "activities/2026/1/coach-input/v1/canonical/";
        const aKey = prefix + fixtures.bulk[0].expected_id + ".json";
        const bKey = prefix + fixtures.bulk[1].expected_id + ".json";
        legacyUploadTimes = new Map([[aKey, new Date(1000)], [bKey, new Date(chronology === "ordered" ? 2000 : 1000)]]);
        if (chronology === "invalid") legacyUploadTimes.set(bKey, new Date(NaN));
        if (chronology === "missing") legacyUploadTimes.set(bKey, undefined);
        if (withOlderPointer) {
          const cKey = prefix + "c".repeat(24) + ".json";
          const old = { ...(await new R2Storage(env.SLIPSTREAM_DATA).getR2Json([aKey]))!.data as Record<string, unknown>,
            analysis_id: "c".repeat(24) };
          await env.SLIPSTREAM_DATA.put(cKey, JSON.stringify(old));
          await env.SLIPSTREAM_DATA.put(pointerKey, JSON.stringify({ schema_version: 1, analysis_key: cKey }));
          legacyUploadTimes.set(cKey, new Date(0));
          legacyUploadTimes.set(pointerKey, new Date(500));
        }
        resetCounts();
        try {
          const result = await call(protocol);
          expect(result.available).toBe(chronology === "ordered");
          if (chronology === "ordered") {
            expect(result.analysis.analysis_id).toBe(fixtures.bulk[1].expected_id);
            expect(result.analysis.summary.moving_seconds).toBe(1900);
            expect(result.message).toBe("Coach input is available.");
          } else {
            expect(result.analysis).toBeUndefined();
            expect(result.message).toBe("The current coach input could not be verified. Retry this read later; if it persists, ask the maintainer to check stored analysis compatibility.");
          }
          expect(operations.put + operations.delete).toBe(0);
          await annotate(JSON.stringify({ chronology, withOlderPointer, protocol, ...operations, garmin: 0 }), "legacy-operations");
        } finally { legacyUploadTimes = null; }
      });
    }
  }
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`rejects a missing original legacy pointer reference before choosing newer history through ${protocol}`, async ({ annotate }) => {
    await seedFinal();
    const prefix = "activities/2026/1/coach-input/v1/canonical/";
    const cKey = prefix + "c".repeat(24) + ".json";
    const aKey = prefix + fixtures.bulk[0].expected_id + ".json";
    const bKey = prefix + fixtures.bulk[1].expected_id + ".json";
    expect(await env.SLIPSTREAM_DATA.get(cKey)).toBeNull();
    await env.SLIPSTREAM_DATA.put(pointerKey, JSON.stringify({ schema_version: 1, analysis_key: cKey }));
    legacyUploadTimes = new Map([[aKey, new Date(1000)], [bKey, new Date(2000)], [pointerKey, new Date(500)]]);
    resetCounts();
    try {
      const result = await call(protocol);
      await annotate(JSON.stringify({ protocol, ...operations, garmin: 0 }), "missing-legacy-reference-operations");
      expect(result.available).toBe(false);
      expect(result.analysis).toBeUndefined();
      expect(result.message).toBe("The current coach input could not be verified. Retry this read later; if it persists, ask the maintainer to check stored analysis compatibility.");
      expect(reads).not.toContain(bKey);
      expect(operations.list).toBe(1);
      expect(operations.put + operations.delete).toBe(0);
      // The broken reference and the stored A/B bytes remain untouched.
      expect(await env.SLIPSTREAM_DATA.get(cKey)).toBeNull();
      for (const key of [aKey, bKey]) {
        const bytes = new Uint8Array(await (await env.SLIPSTREAM_DATA.get(key))!.arrayBuffer());
        expect(btoa(String.fromCharCode(...bytes))).toBe(fixtures.bulk[2].objects[key as keyof typeof fixtures.bulk[2]["objects"]]);
      }
    } finally { legacyUploadTimes = null; }
  });
}
