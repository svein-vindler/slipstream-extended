/** Synthetic public MCP regression controls; no timing threshold or external storage. */
import { describe, expect, it } from "vitest";
import { Client, InMemoryTransport } from "@modelcontextprotocol/client";
import { McpServer } from "@modelcontextprotocol/server";
import { nightContext, validatedHealthTimezone, type NightFormatting } from "../src/night-context";
import { NightSignalsTools, nightSignalsSchema, nightSignalsHistorySchema } from "../src/night-signals";

type Point = { timestamp: string; value: number };
type Night = { date: string; start: string; end: string; points: Point[] };
const key = (kind: string, day: string) => `health/${kind}/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
const encoder = new TextEncoder();

async function open(nights: Night[], zone = "Europe/Oslo") {
  const objects = new Map<string, Uint8Array>();
  const operations: string[] = [];
  const put = (kind: string, day: string, value: unknown) => objects.set(key(kind, day), encoder.encode(JSON.stringify(value)));
  for (const night of nights) {
    put("sleep/v1", night.date, { schema_version: 1, source: "garmin", date: night.date,
      sleep_start_gmt: night.start, sleep_end_gmt: night.end });
    put("signals/v1", night.date, { schema_version: 1, source: "garmin", date: night.date, channels: {
      heart_rate: { status: "available", unit: "bpm", quality_flags: [], readings: night.points.map(point => ({
        ...point, time_basis: "utc", timestamp_unit: "iso8601", timestamp_field: "heartRateValues.timestamp",
      })) },
    } });
    put("hrv", night.date, { schema_version: 1, source: "garmin", date: night.date,
      readings: night.points.map(point => ({ timestamp: point.timestamp, hrv_ms: point.value + 10,
        time_basis: "utc", timestamp_unit: "iso8601", timestamp_field: "readingTimeGMT" })) });
  }
  // Model only the public storage boundary: each GET owns a fresh stream.
  const bucket = {
    async get(objectKey: string) {
      operations.push("get");
      const bytes = objects.get(objectKey);
      return bytes ? { size: bytes.byteLength, etag: "synthetic-revision", body: new ReadableStream<Uint8Array>({
        start(controller) { controller.enqueue(bytes); controller.close(); },
      }) } : null;
    },
    async list() { operations.push("list"); return { objects: [], truncated: false, delimitedPrefixes: [] }; },
    async put() { operations.push("put"); throw new Error("Unexpected storage write"); },
    async delete() { operations.push("delete"); throw new Error("Unexpected storage deletion"); },
    async head() { operations.push("head"); throw new Error("Unexpected storage HEAD"); },
  };
  const server = new McpServer({ name: "night-regression", version: "1" });
  new NightSignalsTools({ SLIPSTREAM_DATA: bucket, HEALTH_TIMEZONE: zone } as unknown as Env).registerTools(server);
  const client = new Client({ name: "night-regression-client", version: "1" });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await Promise.all([client.connect(clientTransport), server.connect(serverTransport)]);
  return {
    async call(name: string, args: Record<string, unknown>) {
      const result = await client.callTool({ name, arguments: args });
      expect(result.isError ?? false).toBe(false);
      const text = result.content?.find(item => item.type === "text");
      expect(text?.type).toBe("text");
      expect(JSON.parse(text && text.type === "text" ? text.text : "null")).toEqual(result.structuredContent);
      return result.structuredContent;
    },
    async close() {
      try { await client.close(); } finally { await server.close(); }
      expect(operations.every(operation => operation === "get" || operation === "list")).toBe(true);
    },
  };
}

const stamp = (time: string) => `2026-10-25T${time}Z`;
const fixture: Night = {
  date: "2026-10-25", start: stamp("00:07:00"), end: stamp("01:04:00"),
  // Deliberately unordered, with exact and millisecond-adjacent boundaries.
  points: [
    ["01:04:00", 250], ["00:30:00", 88], ["00:14:59.999", 44], ["01:02:00", 100],
    ["00:06:59.999", 5], ["00:22:00", 80], ["00:07:00", 40], ["00:21:59.999", 64],
    ["01:03:59.999", 104], ["00:15:00", 60], ["00:29:59.999", 84],
  ].map(([time, value]) => ({ timestamp: stamp(String(time)), value: Number(value) })),
};
type BinCase = {
  resolution: "15m" | "30m"; alignment: "sleep_start" | "utc_clock"; origin: string;
  bins: Array<[string, string, number | null, number]>;
};
// Worked allocation/median examples are independent of the cursor algorithm.
const binCases: BinCase[] = [
  { resolution: "15m", alignment: "sleep_start", origin: "00:07:00", bins: [
    ["00:07:00", "00:22:00", 52, 4], ["00:22:00", "00:37:00", 84, 3],
    ["00:37:00", "00:52:00", null, 0], ["00:52:00", "01:04:00", 102, 2],
  ] },
  { resolution: "15m", alignment: "utc_clock", origin: "00:00:00", bins: [
    ["00:07:00", "00:15:00", 42, 2], ["00:15:00", "00:30:00", 72, 4],
    ["00:30:00", "00:45:00", 88, 1], ["00:45:00", "01:00:00", null, 0],
    ["01:00:00", "01:04:00", 102, 2],
  ] },
  { resolution: "30m", alignment: "sleep_start", origin: "00:07:00", bins: [
    ["00:07:00", "00:37:00", 64, 7], ["00:37:00", "01:04:00", 102, 2],
  ] },
  { resolution: "30m", alignment: "utc_clock", origin: "00:00:00", bins: [
    ["00:07:00", "00:30:00", 62, 6], ["00:30:00", "01:00:00", 88, 1],
    ["01:00:00", "01:04:00", 102, 2],
  ] },
];

describe("night signal compaction through the registered MCP tools", () => {
  it.each(binCases)("keeps $resolution/$alignment boundaries, empty bins and terminal clipping", async example => {
    const connection = await open([fixture]);
    try {
      const value = nightSignalsSchema.parse(await connection.call("night_signals", {
        date: fixture.date, resolution: example.resolution, alignment: example.alignment,
      }));
      expect(value.bin_origin_utc).toBe(stamp(example.origin));
      for (const [name, offset] of [["heart_rate", 0], ["hrv", 10]] as const) {
        const channel = value.channels[name]!;
        expect(channel.bins).toEqual(example.bins.map(([start, end, median, count]) => ({
          start_utc: stamp(start), end_utc: stamp(end), median: median === null ? null : median + offset, count,
        })));
        expect(channel.valid_points).toBe(9);
        expect(channel.points).toEqual([]);
        expect(channel.quality.outside_interval).toBe(2);
      }
    } finally { await connection.close(); }
  });

  it("retains sorted native points and source clocks without binning or replaying the exclusive end", async () => {
    const connection = await open([fixture]);
    try {
      const value = nightSignalsSchema.parse(await connection.call("night_signals", { date: fixture.date, resolution: "native" }));
      expect(value.channels.heart_rate?.bins).toEqual([]);
      expect(value.channels.heart_rate?.points.map(point => point.value)).toEqual([40, 44, 60, 64, 80, 84, 88, 100, 104]);
      expect(value.channels.heart_rate?.points[1]).toMatchObject({ timestamp_utc: stamp("00:14:59.999"),
        source_timestamp: stamp("00:14:59.999"), time_basis: "utc", timestamp_unit: "iso8601" });
      expect(value.sleep_start_utc).toBe(fixture.start);
      expect(value.sleep_end_utc).toBe(fixture.end);
    } finally { await connection.close(); }
  });
});

const formatting = (): NightFormatting => ({ zones: new Map(), instants: new Map() });
describe("reused date formatters retain per-instant timezone semantics", () => {
  it("resolves both repeated autumn times and skips the spring gap while reusing formatting state", () => {
    const state = formatting();
    const autumn = nightContext("2026-10-25", "2026-10-25T00:30:00Z", "2026-10-25T01:30:00Z",
      "Europe/Oslo", null, null, null, state);
    expect(autumn).toMatchObject({ sleep_start_local: "2026-10-25T02:30:00+02:00",
      sleep_end_local: "2026-10-25T02:30:00+01:00", sleep_midpoint_local: "2026-10-25T02:00:00+01:00" });
    const spring = nightContext("2026-03-29", "2026-03-29T00:30:00Z", "2026-03-29T01:30:00Z",
      "Europe/Oslo", null, null, null, state);
    expect(spring).toMatchObject({ sleep_start_local: "2026-03-29T01:30:00+01:00",
      sleep_end_local: "2026-03-29T03:30:00+02:00", sleep_midpoint_local: "2026-03-29T03:00:00+02:00" });
  });

  it("keeps actual, configured and source clocks separate across reused zones and date rollover", () => {
    const state = formatting();
    const start = "2026-06-11T15:30:00Z", end = "2026-06-11T22:30:00Z";
    const localStart = "2026-06-12T00:30:00", localEnd = "2026-06-12T07:30:00";
    const actual = nightContext("2026-06-12", start, end, "UTC", localStart, localEnd, "Pacific/Auckland", state);
    expect(actual).toMatchObject({ actual_night_timezone: "Pacific/Auckland", night_of: "2026-06-12",
      sleep_start_local: "2026-06-12T03:30:00+12:00", sleep_end_local: "2026-06-12T10:30:00+12:00",
      configured_projection: { timezone: "UTC", sleep_start_local: "2026-06-11T15:30:00+00:00" },
      source_time: { sleep_start_gmt: start, sleep_end_gmt: end,
        sleep_start_garmin_local: localStart, sleep_end_garmin_local: localEnd } });
    expect(actual.time_quality_flags).toContain("source_local_conflicts_actual_timezone");
    const travel = nightContext("2026-06-12", start, end, "Asia/Kathmandu", localStart, localEnd, null, state);
    expect(travel).toMatchObject({ timezone: null, actual_night_timezone: null, local_time_source: "garmin_local",
      sleep_start_local: "2026-06-12T00:30:00+09:00", sleep_end_local: "2026-06-12T07:30:00+09:00",
      configured_projection: { timezone: "Asia/Kathmandu", sleep_start_local: "2026-06-11T21:15:00+05:45" } });
    expect(validatedHealthTimezone(" Europe/Oslo ", state)).toBe("Europe/Oslo");
    expect(validatedHealthTimezone("Invalid/Zone", state)).toBeNull();
    expect(validatedHealthTimezone("x".repeat(129), state)).toBeNull();
    expect(validatedHealthTimezone(undefined, state)).toBeNull();
  });

  it("reuses formatters within bulk calls but owns fresh formatting state for every MCP request", async () => {
    const nights = ["2026-10-24", "2026-10-25", "2026-10-26"].map(date => ({
      date, start: `${date}T00:30:00Z`, end: `${date}T01:30:00Z`, points: [],
    }));
    const connection = await open(nights);
    const second = await open(nights, "Asia/Kathmandu");
    const descriptor = Object.getOwnPropertyDescriptor(Intl, "DateTimeFormat")!;
    const original = Intl.DateTimeFormat;
    let constructors = 0;
    Object.defineProperty(Intl, "DateTimeFormat", { ...descriptor, value: new Proxy(original, {
      construct(target, args, newTarget) { constructors++; return Reflect.construct(target, args, newTarget); },
    }) });
    const args = { start_date: "2026-10-24", end_date: "2026-10-26", resolution: "30m" };
    try {
      for (let request = 0; request < 2; request++) {
        constructors = 0;
        const value = nightSignalsHistorySchema.parse(await connection.call("night_signals_history", args));
        expect(value.nights).toHaveLength(3);
        expect(constructors).toBe(2); // One zone validator and one instant formatter, independent of night count.
        expect(value.nights[1].night_context).toMatchObject({ sleep_start_local: "2026-10-25T02:30:00+02:00",
          sleep_end_local: "2026-10-25T02:30:00+01:00" });
      }
      constructors = 0;
      const other = nightSignalsHistorySchema.parse(await second.call("night_signals_history", args));
      expect(constructors).toBe(2);
      expect(other.nights[1].night_context).toMatchObject({ sleep_start_local: "2026-10-25T06:15:00+05:45",
        sleep_end_local: "2026-10-25T07:15:00+05:45" });
    } finally {
      Object.defineProperty(Intl, "DateTimeFormat", descriptor);
      try { await connection.close(); } finally { await second.close(); }
    }
  });
});

