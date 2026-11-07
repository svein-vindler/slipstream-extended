/** Durable causal invalidation for explicit context appends; no source scan or product computation. */
import { readStreamLimited } from "./security";

const QUEUE_KEY = "health/night-products/v1/refresh-queue.json";
const MAX_BYTES = 262144, MAX_PENDING = 128, MAX_RANGES = 512;
const WINDOWS = [28, 30, 90] as const, CHANNELS = ["heart_rate", "hrv"] as const;
type Range = [string, string];
type JsonObject = Record<string, unknown>;
interface Intent { wake_date: string; key: string; sha256: string; state: "intent" }
interface SavedIntent { wake_date: string; key: string; sha256: string; state: "saved"; saved_revision: string }
interface Queue extends JsonObject {
  schema_version: 1; kind: "night-baseline-refresh-queue"; pending: Record<string, JsonObject>;
  rolling_context_intents?: Record<string, Intent | SavedIntent>;
  rolling_discovery?: Range[];
  rolling?: { eligible: Range[]; dirty: Record<string, Record<string, Range[]>> };
}
interface Snapshot { text: string; etag: string }
export class ContextQueueRejected extends Error {
  constructor() { super("Context queue conditional write was rejected."); }
}
function requireValue(value: unknown): asserts value {
  if (!value) throw new Error("Rolling context evidence is unverified.");
}
function object(value: unknown): value is JsonObject { return value !== null && typeof value === "object" && !Array.isArray(value); }
function validDay(value: unknown): value is string {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value) && value >= "0001-01-01"
    && Number.isFinite(Date.parse(value + "T00:00:00Z")) && new Date(value + "T00:00:00Z").toISOString().slice(0,10) === value;
}
function addDays(day: string, count: number): string {
  const result = new Date(Date.parse(day + "T00:00:00Z") + count * 86400000).toISOString().slice(0,10);
  requireValue(validDay(result)); return result;
}
function merge(ranges: Range[]): Range[] {
  const result: Range[] = [];
  for (const [first,last] of [...ranges].sort(([a],[b]) => a < b ? -1 : a > b ? 1 : 0)) {
    const previous = result.at(-1);
    if (previous && Date.parse(first + "T00:00:00Z") - Date.parse(previous[1] + "T00:00:00Z") <= 86400000) previous[1] = previous[1] > last ? previous[1] : last;
    else result.push([first,last]);
  }
  requireValue(result.length <= MAX_RANGES); return result;
}
function ranges(value: unknown): asserts value is Range[] {
  requireValue(Array.isArray(value) && value.length <= MAX_RANGES);
  for (const item of value) requireValue(Array.isArray(item) && item.length === 2 && item.every(validDay) && item[0] <= item[1]);
  requireValue(JSON.stringify(merge(value as Range[])) === JSON.stringify(value));
}
function validate(value: unknown): asserts value is Queue {
  requireValue(object(value) && value.schema_version === 1 && value.kind === "night-baseline-refresh-queue"
    && object(value.pending) && Object.keys(value.pending).length <= MAX_PENDING);
  for (const row of Object.values(value.pending)) requireValue(object(row) && ["dirty","publishing"].includes(String(row.phase)) && object(row.sources));
  if (value.rolling_discovery !== undefined) ranges(value.rolling_discovery);
  if (value.rolling !== undefined) {
    requireValue(object(value.rolling) && Object.keys(value.rolling).sort().join() === "dirty,eligible" && object(value.rolling.dirty)); ranges(value.rolling.eligible);
    requireValue(Object.keys(value.rolling.dirty).sort().join() === "28,30,90");
    for (const n of WINDOWS) {
      const channels = value.rolling.dirty[String(n)];
      requireValue(object(channels) && Object.keys(channels).sort().join() === "heart_rate,hrv");
      for (const channel of CHANNELS) {
        const endpoints = channels[channel]; ranges(endpoints);
        for (const [first,last] of endpoints) requireValue(value.rolling.eligible.some(([a,b]) => a <= first && last <= b));
      }
    }
  }
  if (value.rolling_context_intents !== undefined) {
    requireValue(object(value.rolling_context_intents) && Object.keys(value.rolling_context_intents).length <= MAX_PENDING);
    for (const [id, intent] of Object.entries(value.rolling_context_intents)) {
      requireValue(/^nctx-[a-f0-9]{24}$/.test(id) && object(intent) && validDay(intent.wake_date)
        && typeof intent.sha256 === "string" && /^[a-f0-9]{64}$/.test(intent.sha256));
      if (intent.state === "intent") requireValue(Object.keys(intent).sort().join() === "key,sha256,state,wake_date");
      else requireValue(intent.state === "saved" && Object.keys(intent).sort().join() === "key,saved_revision,sha256,state,wake_date"
        && typeof intent.saved_revision === "string" && intent.saved_revision.length <= 1024
        && /^"[^"\\\x00-\x20\x7f]+"$/.test(intent.saved_revision));
      const prefix = `health/night-context/v1/${intent.wake_date.slice(0,4)}/${intent.wake_date.slice(5,7)}/${intent.wake_date}/`;
      requireValue(typeof intent.key === "string" && intent.key.startsWith(prefix)
        && new RegExp(`^\\d{8}T\\d{9}-${id}\\.json$`).test(intent.key.slice(prefix.length)));
    }
  }
}
async function snapshot(bucket: R2Bucket, key: string, cap: number): Promise<Snapshot | null> {
  const stored = await bucket.get(key);
  if (!stored) return null;
  if (stored.size > cap || stored.httpMetadata?.contentEncoding || stored.httpMetadata?.contentType !== "application/json") {
    try { await stored.body.cancel(); } catch { /* Preserve the primary bound. */ }
    throw new Error("Rolling context object exceeds its bound.");
  }
  const bytes = await readStreamLimited(stored.body, cap, "Rolling context object exceeds its bound.");
  return { text: new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes), etag: stored.etag };
}
async function readQueue(bucket: R2Bucket): Promise<{ queue: Queue; before: Snapshot | null }> {
  const before = await snapshot(bucket, QUEUE_KEY, MAX_BYTES);
  const queue: unknown = before ? JSON.parse(before.text) : { schema_version: 1, kind: "night-baseline-refresh-queue", pending: {} };
  validate(queue); return { queue, before };
}
async function saveQueue(bucket: R2Bucket, queue: Queue, before: Snapshot | null, beforePut: () => void): Promise<void> {
  validate(queue);
  const body = JSON.stringify(queue); requireValue(new TextEncoder().encode(body).byteLength <= MAX_BYTES);
  beforePut();
  const ack = await bucket.put(QUEUE_KEY, body, { httpMetadata: { contentType: "application/json" },
    onlyIf: before ? { etagMatches: before.etag } : { etagDoesNotMatch: "*" } });
  if (!ack) throw new ContextQueueRejected();
  const after = await snapshot(bucket, QUEUE_KEY, MAX_BYTES);
  requireValue(after && after.etag === ack.etag && after.text === body);
}
export async function contextBodySha(body: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(body));
  return [...new Uint8Array(digest)].map(value => value.toString(16).padStart(2,"0")).join("");
}
export async function prepareContextInvalidation(bucket: R2Bucket, id: string, intent: Intent, beforePut: () => void): Promise<void> {
  const { queue, before } = await readQueue(bucket);
  const intents = queue.rolling_context_intents ??= {};
  requireValue(Object.keys(intents).length < MAX_PENDING && !Object.hasOwn(intents, id));
  intents[id] = intent;
  await saveQueue(bucket, queue, before, beforePut);
}
function removeDay(values: Range[], day: string): Range[] {
  return values.flatMap(([first,last]): Range[] => {
    if (day < first || day > last) return [[first,last]];
    const result: Range[] = [];
    if (first < day) result.push([first,addDays(day,-1)]);
    if (day < last) result.push([addDays(day,1),last]);
    return result;
  });
}
function invalidate(queue: Queue, day: string): boolean {
  // Discovery is bounded work metadata, not a claim that these endpoints exist.
  queue.rolling_discovery = merge([...(queue.rolling_discovery ?? []), [day,addDays(day,89)]]);
  if (!queue.rolling) return true;
  const value = queue.rolling;
  for (const n of WINDOWS) {
    const end = addDays(day,n-1);
    const targets = value.eligible.filter(([a,b]) => a <= end && b >= day)
      .map(([a,b]): Range => [a > day ? a : day, b < end ? b : end]);
    for (const channel of CHANNELS) value.dirty[String(n)][channel] = merge([...value.dirty[String(n)][channel], ...targets]);
  }
  let safe = true;
  for (const [name,row] of Object.entries(queue.pending)) {
    if (row.rolling_window === undefined) continue;
    const n = row.rolling_window, channel = row.channel, end = row.end_date;
    requireValue(typeof n === "number" && WINDOWS.includes(n as 28 | 30 | 90) && typeof channel === "string" && CHANNELS.includes(channel as "heart_rate" | "hrv")
      && validDay(end) && row.start_date === addDays(end,1-n) && name === `rolling/${n}/${end}/${channel}`);
    if (end < day || end > addDays(day,n-1)) continue;
    const bindings = [row.sources, row.following_sources ?? {}];
    const held = bindings.some(source => !object(source) || Object.values(source).some(binding => !object(binding)
      || binding.state !== "ready" || typeof binding.sha256 !== "string" || !/^[a-f0-9]{64}$/.test(binding.sha256)
      || typeof binding.revision !== "string" || binding.revision.length > 1024 || !/^".+"$/.test(binding.revision)));
    if (row.phase !== "dirty" || row.inflight || held) { safe = false; continue; }
    delete row.blocked; delete row.validated_progress; row.completed = {};
    value.dirty[String(n)][channel] = removeDay(value.dirty[String(n)][channel], end);
  }
  return safe;
}
export async function finishContextInvalidation(bucket: R2Bucket, id: string, intent: Intent, body: string, revision: string): Promise<boolean> {
  const event = await snapshot(bucket, intent.key, 16 * 1024);
  requireValue(event && event.etag === revision && event.text === body && await contextBodySha(event.text) === intent.sha256);
  const { queue, before } = await readQueue(bucket);
  const stored = queue.rolling_context_intents?.[id];
  requireValue(stored && stored.key === intent.key && stored.wake_date === intent.wake_date && stored.sha256 === intent.sha256 && stored.state === "intent");
  const safe = invalidate(queue, intent.wake_date);
  if (safe) delete queue.rolling_context_intents![id];
  else queue.rolling_context_intents![id] = { ...intent, state: "saved", saved_revision: `"${event.etag}"` };
  // A busy publisher keeps its own row. This exact CAS/readback makes the known
  // append and following generation durable; it does not claim products are fresh.
  await saveQueue(bucket, queue, before, () => {});
  return true;
}
