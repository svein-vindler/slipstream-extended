/** Synthetic stored source -> authenticated JSON/SSE night signal contract. */
import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { beforeAll, beforeEach, afterEach, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";

type Row = Record<string, unknown>;
const DAY = "2026-10-25", ISSUER = "https://night-signals.cloudflareaccess.com", HOST = "night-signals.example";
const START = "2026-10-24T23:56:00Z", END = "2026-10-25T00:06:00Z";
const sleepKey = (day = DAY) => `health/sleep/v1/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
const signalKey = (day: string) => `health/signals/v1/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
const hrvKey = (day = DAY) => `health/hrv/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
const stored = new Set<string>(), calls: Array<{operation: string; key: string}> = [];
const responseSamples: Row[]=[];
let token: string, privateKey: CryptoKey, jwk: Row, testEnv: Env, rpcId = 0, failedGet: string | undefined, expiredGet: string | undefined, clockOffset=0;
const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
  const value = Reflect.get(target, property);
  if (typeof value !== "function") return value;
  return async (...args: unknown[]) => {
    calls.push({operation: String(property), key: String(args[0])});
    if(property==="get"&&String(args[0])===failedGet)throw new Error("SYNTHETIC_READ_FAILURE_PRIVATE");
    const result=await Reflect.apply(value, target, args);
    if(property==="get"&&String(args[0])===expiredGet)clockOffset=10001;
    return result;
  };
}});
const reading = (timestamp: string, value: number) => ({timestamp, value, time_basis: "utc", timestamp_field: "heartRateValues.timestamp", timestamp_unit: "iso8601"});
async function put(key: string, value: unknown) {
  stored.add(key); await env.SLIPSTREAM_DATA.put(key, JSON.stringify(value));
}
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", {extractable: true}); privateKey = pair.privateKey;
  jwk = {...await exportJWK(pair.publicKey), kid: "night-signals", alg: "RS256", use: "sig"};
});
beforeEach(async () => {
  calls.length = 0; responseSamples.length=0; rpcId = 0; failedGet=undefined; expiredGet=undefined; clockOffset=0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url === `${ISSUER}/cdn-cgi/access/certs`) return Response.json({keys: [jwk]});
    throw new Error("Unexpected external request in synthetic night signals test");
  });
  token = await new SignJWT({type: "app", email: "synthetic@example.invalid"})
    .setProtectedHeader({alg: "RS256", kid: "night-signals"}).setIssuer(ISSUER).setAudience("night-signals")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  testEnv = {...env, ACCESS_TEAM_DOMAIN: ISSUER, ACCESS_AUD: "night-signals", MCP_HOSTNAME: HOST,
    HEALTH_TIMEZONE: "Europe/Oslo", MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    SLIPSTREAM_DATA: bucket, MCP_RATE_LIMITER: {limit: async () => ({success: true})}};
  await put(sleepKey(), {schema_version: 1, source: "garmin", date: DAY, sleep_start_gmt: START, sleep_end_gmt: END,
    sleep_start_garmin_local: "2026-10-25T01:56:00", sleep_end_garmin_local: "2026-10-25T02:06:00"});
  await put(signalKey("2026-10-24"), {schema_version: 1, source: "garmin", date: "2026-10-24", channels: {
    heart_rate: {status: "available", unit: "bpm", readings: [reading(START,80), reading("2026-10-24T23:58:00Z",48)], quality_flags: []}}});
  await put(signalKey(DAY), {schema_version: 1, source: "garmin", date: DAY, channels: {
    heart_rate: {status: "available", unit: "bpm", readings: [reading("2026-10-25T00:00:00Z",46), reading("2026-10-25T00:02:00Z",44),
      reading("2026-10-25T00:04:00Z",42), reading(END,20)], quality_flags: []}}});
  await put(hrvKey(), {schema_version: 1, source: "garmin", date: DAY,
    readings: [{timestamp: "2026-10-25T00:00:00Z", hrv_ms: 55, time_basis: "utc", timestamp_field: "readingTimeGMT", timestamp_unit: "iso8601"}]});
});
afterEach(async context => {
  context.task.meta.nightSignalResources={responses:[...responseSamples],operations:Object.fromEntries(["get","head","list","put","delete"].map(operation=>[operation,calls.filter(call=>call.operation===operation).length]))};
  try {expect(calls.filter(call => ["put","delete"].includes(call.operation))).toEqual([]);}
  finally {await env.SLIPSTREAM_DATA.delete([...stored]); stored.clear(); vi.restoreAllMocks();}
});
async function rpc(protocol: string, method: string, params: Row) {
  const modern = protocol === "2026-07-28", id = ++rpcId, context = createExecutionContext(), started=performance.now();
  const response = await worker.fetch(new Request(`https://${HOST}/mcp`, {method: "POST", headers: {
    host: HOST, "content-type": "application/json", accept: "application/json, text/event-stream",
    "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
    ...(modern ? {"mcp-method": method, ...(method === "tools/call" ? {"mcp-name": String(params.name)} : {})} : {})},
    body: JSON.stringify({jsonrpc: "2.0", id, method, params: {...params, ...(modern ? {_meta: {
      [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {}}} : {})}})}), testEnv, context);
  const raw = await response.text(); await waitOnExecutionContext(context);
  expect(response.status).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  expect(response.headers.get("cache-control")).toContain("no-store");
  const line = raw.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : raw); expect(message.id).toBe(id);
  if(method==="tools/call"){
    const rawBytes=new TextEncoder().encode(raw).byteLength;
    expect(rawBytes).toBeLessThanOrEqual(512*1024);
    responseSamples.push({tool:String(params.name),protocol,raw_response_bytes:rawBytes,elapsed_ms:performance.now()-started,resources:message.result?.structuredContent?.resources,status:message.result?.structuredContent?.status});
  }
  return message;
}
async function call(protocol: string, name: string, args: Row) {
  if (protocol !== "2026-07-28") await rpc(protocol, "initialize", {protocolVersion: protocol, capabilities: {}, clientInfo: {name:"night-signals-test",version:"1"}});
  return rpc(protocol, "tools/call", {name, arguments: args});
}
function success(message: Row) {
  expect(message.error).toBeUndefined();
  const result = message.result as {isError?: boolean; content: Array<{text: string}>; structuredContent: Row};
  expect(result.isError).not.toBe(true);
  expect(JSON.parse(result.content[0].text)).toEqual(result.structuredContent);
  return result.structuredContent;
}
it.each(["2026-07-28", "2025-03-26"])("%s reads both source days, clips the exclusive end and reports a covered ten-minute HR median", async protocol => {
  const value = success(await call(protocol, "night_signals", {date: DAY}));
  expect(value).toMatchObject({wake_date: DAY, sleep_start_utc: START, sleep_end_utc: END,
    channels: {heart_rate: {status:"available",unit:"bpm",valid_points:5,observed_cadence_seconds:120,measurement_window_seconds:null,measurement_window_basis:"not_documented_in_stored_source",
      coverage:1, minimum: {value:46,method:"lowest-10-minute-median-v1",window_start_utc:START,window_end_utc:END,coverage:1}},
      hrv:{status:"available",unit:"ms",valid_points:1}}});
  expect((value.channels as Row).heart_rate).toMatchObject({quality:{outside_interval:1}});
});it.each(["2026-07-28", "2025-03-26"])("%s returns half-open 15/30-minute medians with explicit sleep-start origins", async protocol => {
  await put(sleepKey(),{schema_version:1,source:"garmin",date:DAY,sleep_start_gmt:"2026-10-25T00:00:00Z",sleep_end_gmt:"2026-10-25T00:45:00Z"});
  await put(signalKey(DAY),{schema_version:1,source:"garmin",date:DAY,channels:{heart_rate:{status:"available",unit:"bpm",quality_flags:[],readings:[
    reading("2026-10-25T00:00:00Z",40),reading("2026-10-25T00:02:00Z",44),reading("2026-10-25T00:14:00Z",48),
    reading("2026-10-25T00:15:00Z",60),reading("2026-10-25T00:29:00Z",64),reading("2026-10-25T00:30:00Z",80)]}}});
  const fifteen=success(await call(protocol,"night_signals",{date:DAY,resolution:"15m"}));
  expect(fifteen).toMatchObject({resolution:"15m",alignment:"sleep_start",bin_origin_utc:"2026-10-25T00:00:00Z"});
  const heart=(fifteen.channels as Row).heart_rate as {bins:Array<{median:number|null;count:number}>;points:unknown[]};
  expect(heart.bins.map(bin=>[bin.median,bin.count])).toEqual([[44,3],[62,2],[80,1]]);
  expect(heart.points).toEqual([]);
  const thirty=success(await call(protocol,"night_signals",{date:DAY,resolution:"30m"}));
  expect((((thirty.channels as Row).heart_rate as {bins:unknown[]}).bins)).toMatchObject([{median:48,count:5},{median:80,count:1}]);
});it.each(["2026-07-28", "2025-03-26"])("%s aligns to UTC clock bins and retains explicit empty intervals without interpolation", async protocol => {
  await put(sleepKey(),{schema_version:1,source:"garmin",date:DAY,sleep_start_gmt:"2026-10-25T00:07:00Z",sleep_end_gmt:"2026-10-25T00:38:00Z"});
  await put(signalKey(DAY),{schema_version:1,source:"garmin",date:DAY,channels:{heart_rate:{status:"available",unit:"bpm",quality_flags:[],readings:[
    reading("2026-10-25T00:07:00Z",40),reading("2026-10-25T00:31:00Z",80)]}}});
  const value=success(await call(protocol,"night_signals",{date:DAY,resolution:"15m",alignment:"utc_clock"}));
  expect(value).toMatchObject({alignment:"utc_clock",bin_origin_utc:"2026-10-25T00:00:00Z"});
  expect(((value.channels as Row).heart_rate as {bins:unknown[]}).bins).toEqual([
    {start_utc:"2026-10-25T00:07:00Z",end_utc:"2026-10-25T00:15:00Z",median:40,count:1},
    {start_utc:"2026-10-25T00:15:00Z",end_utc:"2026-10-25T00:30:00Z",median:null,count:0},
    {start_utc:"2026-10-25T00:30:00Z",end_utc:"2026-10-25T00:38:00Z",median:80,count:1}]);
});it.each(["2026-07-28", "2025-03-26"])("%s bulk reads31 compact nights and refuses native8/compact32 before storage", async protocol => {
  const value=success(await call(protocol,"night_signals_history",{start_date:"2026-10-01",end_date:"2026-10-31",resolution:"30m"}));
  expect(value).toMatchObject({requested_nights:31,resolution:"30m",status:"partial"});
  expect((value.nights as Row[])).toHaveLength(31);
  expect((value.nights as Row[]).find(night=>night.wake_date===DAY)).toMatchObject({channels:{heart_rate:{valid_points:5},hrv:{valid_points:1}}});
  expect((value.resources as Row).r2_get_attempts).toBe(64);
  const before=calls.length;
  const native=success(await call(protocol,"night_signals_history",{start_date:"2026-10-01",end_date:"2026-10-08"}));
  expect(native).toMatchObject({status:"invalid_request",nights:[],quality_flags:["native_night_limit_7"]});
  const compact=success(await call(protocol,"night_signals_history",{start_date:"2026-10-01",end_date:"2026-11-01",resolution:"15m"}));
  expect(compact).toMatchObject({status:"invalid_request",nights:[],quality_flags:["compact_night_limit_31"]});
  expect(calls.length).toBe(before);
});it.each(["2026-07-28", "2025-03-26"])("%s rejects impossible dates, local-only HRV and conflicting instants while preserving source units", async protocol => {
  await put(signalKey(DAY),{schema_version:1,source:"garmin",date:DAY,channels:{heart_rate:{status:"available",unit:"bpm",quality_flags:[],readings:[
    reading("2026-10-25T00:00:00Z",46),reading("2026-10-25T00:00:00Z",46),
    reading("2026-10-25T00:02:00Z",44),reading("2026-10-25T00:02:00Z",45),reading("2026-10-25T00:04:00Z",42),
    reading("2026-02-30T00:00:00Z",20),reading("2026-10-25T00:03:00Z",0)]}}});
  await put(hrvKey(),{schema_version:1,source:"garmin",date:DAY,readings:[
    {timestamp:1792886400,hrv_ms:55,time_basis:"utc",timestamp_field:"readingTimeGMT",timestamp_unit:"epoch_seconds"},
    {timestamp:1792886400001,hrv_ms:56,time_basis:"utc",timestamp_field:"readingTimeGMT",timestamp_unit:"epoch_milliseconds"},
    {timestamp:"2026-10-25T00:01:00",hrv_ms:25,time_basis:"local_only",timestamp_field:"readingTimeLocal",timestamp_unit:"iso8601"}]});
  const value=success(await call(protocol,"night_signals",{date:DAY}));
  const channels=value.channels as Row;
  expect(channels.heart_rate).toMatchObject({quality:{invalid:2,outside_interval:0,duplicates:1,conflicting_timestamps:1},valid_points:4,minimum:{value:47,coverage:.8,valid_points:4}});
  expect(channels.hrv).toMatchObject({quality:{ambiguous_time:1},valid_points:2});
  expect((channels.hrv as {points:unknown[]}).points).toMatchObject([
    {timestamp_utc:"2026-10-25T00:00:00Z",source_timestamp:1792886400,timestamp_unit:"epoch_seconds"},
    {timestamp_utc:"2026-10-25T00:00:00.001Z",source_timestamp:1792886400001,timestamp_unit:"epoch_milliseconds"}]);
});it.each(["2026-07-28", "2025-03-26"])("%s keeps source read errors, wrong-date objects, explicit empty and absence distinct", async protocol => {
  failedGet=signalKey(DAY);
  const failed=success(await call(protocol,"night_signals",{date:DAY}));
  expect(failed).toMatchObject({status:"partial",channels:{heart_rate:{valid_points:2,quality_flags:expect.arrayContaining(["source_read_error"])},hrv:{status:"available"}}});
  expect(JSON.stringify(failed)).not.toContain("SYNTHETIC_READ_FAILURE_PRIVATE");
  failedGet=undefined;
  await put(signalKey(DAY),{schema_version:1,source:"garmin",date:"2026-10-26",channels:{heart_rate:{status:"no_data",unit:"bpm",readings:[]}}});
  const invalid=success(await call(protocol,"night_signals",{date:DAY}));
  expect(invalid).toMatchObject({status:"partial",channels:{heart_rate:{quality_flags:expect.arrayContaining(["source_invalid_schema"])}}});
  for(const day of ["2026-10-24",DAY])await put(signalKey(day),{schema_version:1,source:"garmin",date:day,channels:{heart_rate:{status:"no_data",unit:"bpm",readings:[]}}});
  await put(hrvKey(),{schema_version:1,source:"garmin",date:DAY,available:false,readings:[]});
  const empty=success(await call(protocol,"night_signals",{date:DAY}));
  expect(empty).toMatchObject({channels:{heart_rate:{status:"no_data",valid_points:0},hrv:{status:"no_data",valid_points:0}}});
  await env.SLIPSTREAM_DATA.delete(sleepKey());
  const missing=success(await call(protocol,"night_signals",{date:DAY}));
  expect(missing).toMatchObject({status:"not_stored",channels:{heart_rate:{status:"not_stored"},hrv:{status:"not_stored"}}});
});it.each(["2026-07-28", "2025-03-26"])("%s preserves source clocks and actual-zone DST projection without claiming the home zone is travel evidence", async protocol => {
  await put(sleepKey(),{schema_version:1,source:"garmin",date:DAY,actual_night_timezone:"Europe/Oslo",
    sleep_start_gmt:"2026-10-24T21:58:00Z",sleep_end_gmt:"2026-10-25T06:48:00Z",
    sleep_start_garmin_local:"2026-10-24T22:58:00",sleep_end_garmin_local:"2026-10-25T07:48:00"});
  const value=success(await call(protocol,"night_signals",{date:DAY}));
  expect(value).toMatchObject({actual_night_timezone:"Europe/Oslo",timezone:"Europe/Oslo",night_context:{
    sleep_start_local:"2026-10-24T23:58:00+02:00",sleep_end_local:"2026-10-25T07:48:00+01:00",
    source_time:{sleep_start_gmt:"2026-10-24T21:58:00Z",sleep_start_garmin_local:"2026-10-24T22:58:00"}}});
  expect(value.quality_flags).not.toContain("actual_night_timezone_unknown");
});async function putGzip(key:string,value:unknown){
  const stream=new Blob([JSON.stringify(value)]).stream().pipeThrough(new CompressionStream("gzip"));
  const compressed=new Uint8Array(await new Response(stream).arrayBuffer());
  stored.add(key);await env.SLIPSTREAM_DATA.put(key,compressed);
}
it("bounds cumulative decode work even when each compressed legacy source exceeds its own decoded limit",async()=>{
  for(let day=1;day<=10;day++){
    const date=`2026-10-${String(day).padStart(2,"0")}`;
    await put(sleepKey(date),{schema_version:1,source:"garmin",date,sleep_start_gmt:`${date}T00:00:00Z`,sleep_end_gmt:`${date}T00:10:00Z`});
    await put(signalKey(date),{schema_version:1,source:"garmin",date,channels:{heart_rate:{status:"no_data",unit:"bpm",readings:[]}}});
    await putGzip(`${hrvKey(date)}.gz`,{schema_version:1,source:"garmin",date,readings:[],padding:"x".repeat(2*1024*1024)});
  }
  const value=success(await call("2026-07-28","night_signals_history",{start_date:"2026-10-01",end_date:"2026-10-10",resolution:"30m"}));
  expect(value).toMatchObject({status:"budget_exhausted",nights:[],quality_flags:["decoded_byte_limit"]});
  expect((value.resources as Row).decoded_budget_bytes).toBeGreaterThan(16*1024*1024);
});it.each(["2026-07-28","2025-03-26"])("%s refuses excess native points without returning an unbounded curve or a false returned-point count",async protocol=>{
  await put(sleepKey(),{schema_version:1,source:"garmin",date:DAY,sleep_start_gmt:"2026-10-25T00:00:00Z",sleep_end_gmt:"2026-10-25T01:23:20Z"});
  const base=1792886400;
  await putGzip(`${signalKey(DAY)}.gz`,{schema_version:1,source:"garmin",date:DAY,channels:{heart_rate:{status:"available",unit:"bpm",readings:
    Array.from({length:5000},(_,i)=>({timestamp:base+i,value:60,time_basis:"utc",timestamp_unit:"epoch_seconds"}))}}});
  await env.SLIPSTREAM_DATA.delete(signalKey(DAY));
  const value=success(await call(protocol,"night_signals",{date:DAY}));
  expect(value).toMatchObject({status:"budget_exhausted",quality_flags:["native_point_limit"],resources:{returned_points:0},channels:{heart_rate:{points:[]},hrv:{points:[]}}});
});it("caps input points before processing ambiguous/invalid observations",async()=>{
  await putGzip(`${signalKey(DAY)}.gz`,{schema_version:1,source:"garmin",date:DAY,channels:{heart_rate:{status:"available",unit:"bpm",readings:Array(100001).fill(null)}}});
  await env.SLIPSTREAM_DATA.delete(signalKey(DAY));
  const value=success(await call("2026-07-28","night_signals",{date:DAY}));
  expect(value).toMatchObject({status:"budget_exhausted",quality_flags:["input_point_limit"],resources:{returned_points:0}});
  expect((value.resources as Row).input_points).toBeGreaterThan(100000);
});
it("cannot return success after an owned R2 read crosses the result deadline",async()=>{
  const realNow=performance.now.bind(performance);
  vi.spyOn(performance,"now").mockImplementation(()=>realNow()+clockOffset);
  expiredGet=sleepKey();
  const value=success(await call("2026-07-28","night_signals",{date:DAY}));
  expect(value).toMatchObject({status:"budget_exhausted",quality_flags:["deadline_exceeded"],resources:{r2_get_attempts:1,returned_points:0}});
});
it.each(["2026-07-28","2025-03-26"])("%s bounds both text and structured MCP channels without silently truncating a dense native night",async protocol=>{
  await put(sleepKey(),{schema_version:1,source:"garmin",date:DAY,sleep_start_gmt:"2026-10-25T00:00:00Z",sleep_end_gmt:"2026-10-25T16:40:00Z"});
  await putGzip(`${signalKey(DAY)}.gz`,{schema_version:1,source:"garmin",date:DAY,channels:{heart_rate:{status:"available",unit:"bpm",readings:
    Array.from({length:2000},(_,i)=>({timestamp:1792886400+i*30,value:60,time_basis:"utc",timestamp_unit:"epoch_seconds"}))}}});
  await env.SLIPSTREAM_DATA.delete(signalKey(DAY));
  const value=success(await call(protocol,"night_signals",{date:DAY}));
  expect(value).toMatchObject({status:"output_limit",resources:{returned_points:0},channels:{heart_rate:{points:[]},hrv:{points:[]}}});
  expect((value.resources as Row).structured_bytes).toBeLessThanOrEqual(256*1024);
  expect((value.resources as Row).envelope_bytes).toBeLessThanOrEqual(512*1024);
});it("includes the documented source-calendar day even when authoritative actual-zone projection changes that date",async()=>{
  await put(sleepKey(),{schema_version:1,source:"garmin",date:DAY,actual_night_timezone:"Europe/Oslo",
    sleep_start_gmt:"2026-10-25T00:30:00Z",sleep_end_gmt:"2026-10-25T00:40:00Z",
    sleep_start_garmin_local:"2026-10-24T23:30:00",sleep_end_garmin_local:"2026-10-24T23:40:00"});
  await put(signalKey("2026-10-24"),{schema_version:1,source:"garmin",date:"2026-10-24",channels:{heart_rate:{status:"available",unit:"bpm",readings:[
    reading("2026-10-25T00:30:00Z",40),reading("2026-10-25T00:32:00Z",42)]}}});
  const value=success(await call("2026-07-28","night_signals",{date:DAY}));
  expect(value).toMatchObject({night_of:DAY,channels:{heart_rate:{valid_points:2,source_dates:["2026-10-24",DAY]}}});
});it.each(["2026-07-28","2025-03-26"])("%s fits31 ordinary populated eight-hour nights in compact30-minute output",async protocol=>{
  for(let index=0;index<31;index++){
    const day=`2026-10-${String(index+1).padStart(2,"0")}`;
    const base=Date.parse(`${day}T00:00:00Z`)/1000;
    await put(sleepKey(day),{schema_version:1,source:"garmin",date:day,sleep_start_gmt:`${day}T00:00:00Z`,sleep_end_gmt:`${day}T08:00:00Z`});
    await put(signalKey(day),{schema_version:1,source:"garmin",date:day,channels:{heart_rate:{status:"available",unit:"bpm",readings:
      Array.from({length:240},(_,i)=>({timestamp:base+i*120,value:60,time_basis:"utc",timestamp_unit:"epoch_seconds"}))}}});
    await put(hrvKey(day),{schema_version:1,source:"garmin",date:day,readings:
      Array.from({length:96},(_,i)=>({timestamp:base+i*300,hrv_ms:50,time_basis:"utc",timestamp_unit:"epoch_seconds"}))});
  }
  const value=success(await call(protocol,"night_signals_history",{start_date:"2026-10-01",end_date:"2026-10-31",resolution:"30m"}));
  expect(value.status).toBe("available");
  expect(value.nights as Row[]).toHaveLength(31);
  expect(value.resources).toMatchObject({r2_get_attempts:93,input_points:10416,returned_points:992});
  expect((value.resources as Row).structured_bytes).toBeLessThanOrEqual(256*1024);
  expect((value.resources as Row).envelope_budget_bytes).toBeLessThanOrEqual(512*1024);
});it("cannot publish a late success when owned result serialization crosses the deadline",async()=>{
  const realNow=performance.now.bind(performance),stringify=JSON.stringify.bind(JSON);
  vi.spyOn(performance,"now").mockImplementation(()=>realNow()+clockOffset);
  vi.spyOn(JSON,"stringify").mockImplementation((value,replacer,space)=>{
    const text=stringify(value,replacer as never,space);
    if(value&&typeof value==="object"&&value.method_version==="night-signals-v1"&&value.status==="available")clockOffset=10001;
    return text;
  });
  const value=success(await call("2026-07-28","night_signals",{date:DAY}));
  expect(value).toMatchObject({status:"budget_exhausted",quality_flags:expect.arrayContaining(["deadline_exceeded"]),resources:{returned_points:0},channels:{heart_rate:{points:[]},hrv:{points:[]}}});
});
