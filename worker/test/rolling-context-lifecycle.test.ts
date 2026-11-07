/** Pure producer/R2 JSON contract; not an authenticated MCP or workerd test. */
import { expect, it } from "vitest";
import { contextBodySha, prepareContextInvalidation, finishContextInvalidation } from "../src/rolling-context";

const QUEUE = "health/night-products/v1/refresh-queue.json";
const day = "2026-11-07", id = "nctx-" + "a".repeat(24);
const eventKey = `health/night-context/v1/2026/11/${day}/20261107T080000000-${id}.json`;
const body = JSON.stringify({ wake_date: day, context_id: id, source: "user", illness: true });
const busyRow = { rolling_window: 28, channel: "hrv", start_date: "2026-10-11", end_date: day,
  phase: "publishing", sources: {}, inflight: { key: "synthetic-product", revision: "unknown" }, completed: { product: "retained-proof" } };

class MemoryR2 {
  objects = new Map<string, { text: string; etag: string }>();
  calls: Array<{ operation: string; key: string }> = [];
  sequence = 0;
  failRead?: string;
  failQueueReadAt?: number;
  queueReads = 0;
  rejectPut?: string;
  seed(key: string, value: unknown) { this.objects.set(key, { text: JSON.stringify(value), etag: `r${++this.sequence}` }); }
  async get(key: string) {
    this.calls.push({ operation: "get", key });
    if (key === QUEUE && ++this.queueReads === this.failQueueReadAt) throw new Error("SYNTHETIC_QUEUE_READBACK_FAILURE");
    if (this.failRead === key) throw new Error("SYNTHETIC_READBACK_FAILURE");
    const value = this.objects.get(key);
    if (!value) return null;
    return { size: new TextEncoder().encode(value.text).byteLength, etag: value.etag,
      httpMetadata: { contentType: "application/json" }, body: new Response(value.text).body! };
  }
  async put(key: string, text: string, options: { onlyIf?: { etagMatches?: string; etagDoesNotMatch?: string } }) {
    this.calls.push({ operation: "put", key });
    if (this.rejectPut === key) return null;
    const previous = this.objects.get(key);
    if (options.onlyIf?.etagMatches !== undefined && previous?.etag !== options.onlyIf.etagMatches) return null;
    if (options.onlyIf?.etagDoesNotMatch === "*" && previous) return null;
    const etag = `r${++this.sequence}`;
    this.objects.set(key, { text, etag }); return { etag };
  }
  // Synthetic R2 transport boundary: only the two production-used methods exist.
  get binding() { return this as unknown as R2Bucket; }
  queue() { return JSON.parse(this.objects.get(QUEUE)!.text); }
}
function setup(row: Record<string, unknown> = busyRow) {
  const store = new MemoryR2();
  store.seed(QUEUE, { schema_version: 1, kind: "night-baseline-refresh-queue",
    pending: { [`rolling/28/${day}/hrv`]: row }, rolling: {
      eligible: [[day, "2026-12-31"]], dirty: Object.fromEntries([28,30,90].map(n => [String(n), { heart_rate: [], hrv: [] }])) } });
  return store;
}
it("commits a known saved marker and durable dirty generation while retaining an active publisher", async () => {
  const store = setup();
  const intent = { wake_date: day, key: eventKey, sha256: await contextBodySha(body), state: "intent" as const };
  let attempts = 0;
  await prepareContextInvalidation(store.binding, id, intent, () => { attempts++; });
  const ack = await store.put(eventKey, body, { onlyIf: { etagDoesNotMatch: "*" } });
  expect(ack?.etag).toBe("r3");
  expect(await finishContextInvalidation(store.binding, id, intent, body, ack!.etag)).toBe(true);
  const queue = store.queue();
  expect(queue.pending[`rolling/28/${day}/hrv`]).toEqual(busyRow);
  expect(queue.rolling_context_intents[id]).toEqual({ ...intent, state: "saved", saved_revision: '"r3"' });
  expect(queue.rolling.dirty["28"].hrv).toEqual([["2026-11-07", "2026-12-04"]]);
  expect(queue.rolling_discovery).toEqual([["2026-11-07", "2027-02-04"]]);
  expect(attempts).toBe(1);
  expect(store.calls.filter(call => call.operation === "get")).toHaveLength(5);
  expect(store.calls.filter(call => call.operation === "put")).toHaveLength(3);
});

it("accepts a later known context transaction while a saved generation awaits publisher quiescence", async () => {
  const { inflight: _inflight, ...normalBusy } = busyRow;
  const store = setup({ ...normalBusy, completed: {} });
  const intent = { wake_date: day, key: eventKey, sha256: await contextBodySha(body), state: "intent" as const };
  await prepareContextInvalidation(store.binding, id, intent, () => {});
  const ack = await store.put(eventKey, body, { onlyIf: { etagDoesNotMatch: "*" } });
  expect(await finishContextInvalidation(store.binding, id, intent, body, ack!.etag)).toBe(true);
  const secondId = "nctx-" + "b".repeat(24);
  const secondBody = JSON.stringify({ wake_date: day, context_id: secondId, source: "user", illness: false });
  const secondKey = eventKey.replace(id, secondId);
  const second = { wake_date: day, key: secondKey, sha256: await contextBodySha(secondBody), state: "intent" as const };
  await prepareContextInvalidation(store.binding, secondId, second, () => {});
  const secondAck = await store.put(secondKey, secondBody, { onlyIf: { etagDoesNotMatch: "*" } });
  expect(await finishContextInvalidation(store.binding, secondId, second, secondBody, secondAck!.etag)).toBe(true);
  const queue = store.queue();
  expect(queue.pending[`rolling/28/${day}/hrv`]).toEqual({ ...normalBusy, completed: {} });
  expect(Object.values(queue.rolling_context_intents).map((value: any) => value.state)).toEqual(["saved", "saved"]);
  expect(queue.rolling_context_intents[secondId].saved_revision).toBe('"r6"');
  expect(queue.rolling.dirty["28"].hrv).toEqual([["2026-11-07", "2026-12-04"]]);
});

it.each(["missing", "wrong-revision", "wrong-body"])("retains the original unresolved intent when event readback is %s", async failure => {
  const store = setup();
  const intent = { wake_date: day, key: eventKey, sha256: await contextBodySha(body), state: "intent" as const };
  await prepareContextInvalidation(store.binding, id, intent, () => {});
  const ack = await store.put(eventKey, body, { onlyIf: { etagDoesNotMatch: "*" } });
  if (failure === "missing") store.objects.delete(eventKey);
  if (failure === "wrong-body") store.objects.get(eventKey)!.text = '{"synthetic":"different"}';
  await expect(finishContextInvalidation(store.binding, id, intent, body,
    failure === "wrong-revision" ? "other-revision" : ack!.etag)).rejects.toThrow();
  expect(store.queue().rolling_context_intents[id]).toEqual(intent);
  expect(store.calls.filter(call => call.operation === "put")).toHaveLength(2);
});

it("does not report causal completion after a known final queue CAS rejection", async () => {
  const store = setup();
  const intent = { wake_date: day, key: eventKey, sha256: await contextBodySha(body), state: "intent" as const };
  await prepareContextInvalidation(store.binding, id, intent, () => {});
  const ack = await store.put(eventKey, body, { onlyIf: { etagDoesNotMatch: "*" } });
  store.rejectPut = QUEUE;
  await expect(finishContextInvalidation(store.binding, id, intent, body, ack!.etag)).rejects.toThrow();
  expect(store.queue().rolling_context_intents[id]).toEqual(intent);
  expect(store.calls.filter(call => call.operation === "put")).toHaveLength(3);
});

it("retains caller uncertainty if the saved-marker queue readback fails after its PUT", async () => {
  const store = setup();
  const intent = { wake_date: day, key: eventKey, sha256: await contextBodySha(body), state: "intent" as const };
  await prepareContextInvalidation(store.binding, id, intent, () => {});
  const ack = await store.put(eventKey, body, { onlyIf: { etagDoesNotMatch: "*" } });
  store.failQueueReadAt = 4;
  await expect(finishContextInvalidation(store.binding, id, intent, body, ack!.etag)).rejects.toThrow("SYNTHETIC_QUEUE_READBACK_FAILURE");
  // The remote write may have happened; a failed read is not an accepted completion.
  expect(store.queue().rolling_context_intents[id].state).toBe("saved");
  expect(store.calls.filter(call => call.operation === "put")).toHaveLength(3);
});
